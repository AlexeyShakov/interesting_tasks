from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )
    items_getter_workers: int
    service_interviewers: int
    cpu_workers: int


@lru_cache(maxsize=1)
def get_config() -> Config:
    return Config()
