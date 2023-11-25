from envparse import env

CONFIG_FILE = ".env"

env.read_envfile(CONFIG_FILE)

HOST = env.str("HOST")
API_KEY = env.str("API_KEY")
INDEX_NAME = env.str("INDEX_NAME")
API_TOKEN = env.str("TOKEN")
