import time

import elastic_transport
import elasticsearch
import pandas as pd
import uvicorn

from elasticsearch import Elasticsearch

from config import HOST, INDEX_NAME, API_KEY, API_TOKEN
from tqdm import tqdm
from fastapi import FastAPI

app = FastAPI()

class ElasticsearchManager:
    def __init__(self, api_key, host, index_name):
        self.es = Elasticsearch(host, api_key=api_key)
        self.index_name = index_name

    def search(self, query: str, size: int = 5) -> dict:
        try:
            start_time = time.time()
            result = []
            query = query.replace('/', '\\/')
            if not query:
                return {"data": result, "req_time": time.time() - start_time}
            video_results = self.es.search(index=self.index_name, q=query, size=size)
            for vid in video_results["hits"]["hits"]:
                result.append({"id": vid["_source"]["id"], "title": vid["_source"]["title"], "speed": vid['_score']})
            return {"data": result, "req_time": time.time() - start_time}
        except elasticsearch.BadRequestError:
            return {"data": [], "req_time": 0}

    def insert(self, insert_data: list, pipeline="ent-search-generic-ingestion"):
        self.es.bulk(operations=insert_data, pipeline=pipeline)

    def prepare_json_data(self, in_data: dict):
        return [
            {"index": {"_index": self.index_name, "_id": in_data["id"]}},
            {
                "id": in_data["id"],
                "title": in_data["title"],
                "_extract_binary_content": True,
                "_reduce_whitespace": True,
                "_run_ml_inference": False
            }
        ]

    def delete_all(self, index_name):
        es_with_options = self.es.options(ignore_status=[400, 404])
        es_with_options.delete_by_query(index=index_name, body={"query": {"match_all": {}}})

    @staticmethod
    def requests_by_batches(path='videos.parquet', bs=100_000):
        try:
            videos_df = pd.read_parquet(path, engine='fastparquet')
        except:
            videos_df = pd.read_parquet(path)
        data = list(map(lambda x: {'id': x[0], 'title': x[1]}, list(zip(list(videos_df['video_id'].values), list(videos_df['video_title'].values)))))
        for i in tqdm(range(0, len(data), bs)):
            if i <= 28409885:
                continue
            yield data[i:i + bs]

    def create_sample_sub(self, size=5):
        queries = pd.read_csv('data/test_dataset_submission_queries.csv')[:100]
        submit = pd.read_csv('data/sample_submission.csv')
        for i in tqdm(range(queries.shape[0])):
            found = list(map(lambda x: x['id'], self.search(queries['query'][i], size=size)["data"]))[:size]
            submit['video_id'][i * size: i * size + len(found)] = found
            submit['query'][i * size: i * size + size] = queries['query'][i]

        submit.to_csv('submit.csv', index=False)

    def fast_insert(self, t: bool = False):
        if t:
            for req in self.requests_by_batches(bs=10000):
                try:
                    doc = []
                    for video in req:
                        new_vid = self.prepare_json_data(video)
                        doc.extend(new_vid)
                    self.insert(doc)
                except elastic_transport.ConnectionTimeout:
                    print("Timeout")
                    continue
            print("Done")


@app.get("/search")
def search(query: str, size: int = 5, token: str = ""):
    if token != API_TOKEN:
        return {"error": "Invalid token"}
    es_manager = ElasticsearchManager(API_KEY, HOST, INDEX_NAME)
    return es_manager.search(query, size)["data"]

@app.get("/status")
def status():
    return {"status": "ok"}

# Запуск сервера
if __name__ == "__main__":
    uvicorn.run(app, port=8000)
