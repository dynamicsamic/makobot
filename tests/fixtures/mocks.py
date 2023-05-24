from functools import partial

import pytest
import pytest_asyncio

from app.db import shared
from app.db.shared import get_session
from app.toolbox.yandex_disk import YandexDisk

from .db import engine


@pytest_asyncio.fixture
async def yadisk_returns_true(monkeypatch):
    async def return_true(*args, **kwargs):
        return True

    # Patch YaDisk code to prevent real Yandex Disk calls
    monkeypatch.setattr(YandexDisk, "check_token", return_true)
    monkeypatch.setattr(YandexDisk, "download_file", return_true)


@pytest.fixture
def get_inmemory_session(monkeypatch, engine):
    monkeypatch.setattr(
        shared, "get_session", partial(get_session, engine=engine)
    )
