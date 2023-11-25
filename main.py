import time

import elastic_transport
import pandas as pd

from elasticsearch import Elasticsearch

from config import HOST, INDEX_NAME, API_KEY
from tqdm import tqdm


class ElasticsearchManager:
    def __init__(self, api_key, host="http://localhost:9200", index_name="your_index_name"):
        self.es = Elasticsearch(host, api_key=api_key)
        self.index_name = index_name

    def search(self, query: str, size: int = 5) -> dict:
        start_time = time.time()
        result = []
        if not query:
            return {"data": result, "req_time": time.time() - start_time}
        video_results = self.es.search(index=self.index_name, q=query, size=size)
        for vid in video_results["hits"]["hits"]:
            result.append({"id": vid["_source"]["id"], "title": vid["_source"]["title"], "speed": vid['_score']})
        return {"data": result, "req_time": time.time() - start_time}

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
                # "_run_ml_inference": True
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

    def create_sample_sub(self):
        pass

    @staticmethod
    def fast_insert(t: bool = False):
        if t:
            for req in es_manager.requests_by_batches(bs=10000):
                try:
                    doc = []
                    for video in req:
                        new_vid = es_manager.prepare_json_data(video)
                        doc.extend(new_vid)
                    es_manager.insert(doc)
                except elastic_transport.ConnectionTimeout:
                    print("Timeout")
                    continue
            print("Done")



# Пример использования
es_manager = ElasticsearchManager(API_KEY, HOST, INDEX_NAME)
n_l = False

if n_l:
    es_manager.fast_insert(t=False)
else:
    print(es_manager.search("кровати ")["req_time"])
    # print(es_manager.update_index_mapping())

