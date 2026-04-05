from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_CSV_PATH = BASE_DIR / "healthcare_dataset.csv"
PARQUET_DIR = BASE_DIR / "processed_parquet"
PARQUET_FILE = PARQUET_DIR / "healthcare_encrypted.parquet"
EXPORT_DIR = BASE_DIR / "exports"
EXPORT_CSV_PATH = EXPORT_DIR / "name_age_export.csv"
VERIFY_REPORT_PATH = EXPORT_DIR / "verification_report.json"
PUBLIC_KEY_PATH = BASE_DIR / "public_key.pem"
PRIVATE_KEY_PATH = BASE_DIR / "private_key.pem"
ENCRYPT_COLUMN = "Billing Amount"


def ensure_dirs() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def validate_source() -> str:
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f"Raw CSV not found: {RAW_CSV_PATH}")
    return str(RAW_CSV_PATH)


def _ensure_keys() -> tuple[object, object]:
    if PUBLIC_KEY_PATH.exists() and PRIVATE_KEY_PATH.exists():
        public_key = serialization.load_pem_public_key(PUBLIC_KEY_PATH.read_bytes())
        private_key = serialization.load_pem_private_key(PRIVATE_KEY_PATH.read_bytes(), password=None)
        return public_key, private_key

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    PRIVATE_KEY_PATH.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    PUBLIC_KEY_PATH.write_bytes(
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return public_key, private_key


def _encrypt_value(value: str, public_key: object) -> str:
    ciphertext = public_key.encrypt(
        value.encode("utf-8"),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return ciphertext.hex()


def _decrypt_value(value: str, private_key: object) -> str:
    plaintext = private_key.decrypt(
        bytes.fromhex(value),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return plaintext.decode("utf-8")


def encrypt_csv_to_parquet() -> str:
    validate_source()
    ensure_dirs()
    public_key, _ = _ensure_keys()

    encrypted_rows: list[dict[str, object]] = []
    with RAW_CSV_PATH.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_id, row in enumerate(reader, start=1):
            encrypted_row = dict(row)
            billing_amount = encrypted_row.pop(ENCRYPT_COLUMN)
            encrypted_row["row_id"] = row_id
            encrypted_row[f"{ENCRYPT_COLUMN}_encrypted"] = _encrypt_value(billing_amount, public_key)
            encrypted_rows.append(encrypted_row)

    table = pa.Table.from_pylist(encrypted_rows)
    pq.write_table(table, PARQUET_FILE)
    return str(PARQUET_FILE)


def decrypt_verify_and_export_csv() -> str:
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")

    ensure_dirs()
    _, private_key = _ensure_keys()

    parquet_rows = pq.read_table(PARQUET_FILE).to_pylist()

    source_by_row_id: dict[int, dict[str, str]] = {}
    with RAW_CSV_PATH.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_id, row in enumerate(reader, start=1):
            source_by_row_id[row_id] = row

    checked_rows = 0
    with EXPORT_CSV_PATH.open("w", newline="", encoding="utf-8") as export_handle:
        writer = csv.writer(export_handle)
        writer.writerow(["Name", "Age"])

        for row in parquet_rows:
            row_id = int(row["row_id"])
            source_row = source_by_row_id[row_id]
            decrypted_amount = _decrypt_value(row[f"{ENCRYPT_COLUMN}_encrypted"], private_key)
            original_amount = source_row[ENCRYPT_COLUMN]

            if decrypted_amount != original_amount:
                raise ValueError(
                    f"Verification failed for row_id={row_id}: expected {original_amount}, got {decrypted_amount}"
                )

            writer.writerow([row["Name"], row["Age"]])
            checked_rows += 1

    VERIFY_REPORT_PATH.write_text(
        json.dumps(
            {
                "status": "verified",
                "rows_checked": checked_rows,
                "encrypted_column": ENCRYPT_COLUMN,
                "parquet_file": str(PARQUET_FILE),
                "export_csv": str(EXPORT_CSV_PATH),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return str(EXPORT_CSV_PATH)
