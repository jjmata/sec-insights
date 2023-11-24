from pathlib import Path
from fire import Fire
from tqdm import tqdm
import asyncio
from file_utils import get_available_docs, Filing
from fastapi.encoders import jsonable_encoder
from app.models.db import Document
from app.schema import (
    JpaDocumentMetadata,
    DocumentMetadataMap,
    DocumentMetadataKeysEnum,
    JpaDocumentTypeEnum,
    Document,
)
from app.db.session import SessionLocal
from app.api import crud

DEFAULT_DOC_DIR = "data/"


async def upsert_document(doc_dir: str, filing: Filing):
    doc_type = (
        JpaDocumentTypeEnum.ESCRITURA
    )
    jpa_doc_metadata = JpaDocumentMetadata(
        notary_name="Test",
        doc_type=doc_type,
        year=2023,
    )
    metadata_map: DocumentMetadataMap = {
        DocumentMetadataKeysEnum.JPA_DOCUMENT: jsonable_encoder(
            jpa_doc_metadata.dict(exclude_none=True)
        )
    }
    with open(filing.file_path, 'rb') as f:
        doc = Document(file=f.read(), metadata_map=metadata_map, url="file:///" + filing.file_path)
    async with SessionLocal() as db:
        await crud.upsert_document_by_url(db, doc)


async def async_upsert_documents_from_filings(doc_dir: str):
    """
    Upserts JPA documents into the database based on what has been downloaded to the filesystem.
    """
    filings = get_available_docs(doc_dir)
    for filing in tqdm(filings, desc="Upserting docs from filings"):
        await upsert_document(doc_dir, filing)


def main_upsert_documents_from_filings(doc_dir: str = DEFAULT_DOC_DIR):
    """
    Upserts JPA documents into the database based on what has been downloaded to the filesystem.
    """

    asyncio.run(async_upsert_documents_from_filings(doc_dir))


if __name__ == "__main__":
    Fire(main_upsert_documents_from_filings)
