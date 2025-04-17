"""
Private config
"""

# Oracle Vector Store
VECTOR_DB_USER = "XXXX"
VECTOR_DB_PWD = "YYYY"
VECTOR_WALLET_PWD = "ZZZZ"
VECTOR_DSN = "adbv01"
VECTOR_WALLET_DIR = "/Users/lsaetta/Progetti/xxxx/wallet"

CONNECT_ARGS = {
    "user": VECTOR_DB_USER,
    "password": VECTOR_DB_PWD,
    "dsn": VECTOR_DSN,
    "config_dir": VECTOR_WALLET_DIR,
    "wallet_location": VECTOR_WALLET_DIR,
    "wallet_password": VECTOR_WALLET_PWD,
}
