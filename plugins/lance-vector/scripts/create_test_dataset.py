#!/usr/bin/env python3
"""
Create a test Lance dataset for OSS integration testing.

This script creates a .lance dataset with IVF-PQ index that can be uploaded
to Alibaba Cloud OSS for testing the Elasticsearch lance-vector plugin.

Usage:
    # Create local dataset
    python create_test_dataset.py /tmp/test-vectors.lance

    # Create and upload to OSS (requires OSS credentials)
    python create_test_dataset.py /tmp/test-vectors.lance --upload oss://my-bucket/test-data/
"""

import argparse
import numpy as np
import lance
import pyarrow as pa
import os
import sys
from oss2 import Auth, Bucket

def create_dataset(n_vectors=1000, dims=128):
    """Create a test dataset with random normalized vectors."""
    print(f"Creating {n_vectors} vectors with {dims} dimensions...")

    # Generate random vectors and normalize (L2 norm)
    vectors = np.random.randn(n_vectors, dims).astype(np.float32)
    vectors = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)

    # Create PyArrow table with fixed-size list array for vectors
    schema = pa.schema([
        pa.field("_id", pa.string()),
        pa.field("vector", pa.list_(pa.float32(), list_size=dims)),
        pa.field("category", pa.string())
    ])

    # Create fixed-size list array for vectors
    vector_array = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.flatten(), type=pa.float32()),
        dims
    )

    table = pa.Table.from_pydict({
        "_id": [f"doc{i}" for i in range(n_vectors)],
        "vector": vector_array,
        "category": np.random.choice(["tech", "science", "business"], n_vectors)
    }, schema=schema)

    return table

def write_lance_dataset(table, path, index_params=None):
    """Write table to Lance format and create IVF-PQ index."""
    print(f"Writing Lance dataset to: {path}")

    # Write dataset
    dataset = lance.write_dataset(table, path)

    # Create IVF-PQ index for fast vector search
    if index_params is None:
        index_params = {
            "column": "vector",
            "index_type": "IVF_PQ",
            "metric": "cosine",
            "num_partitions": 32,
            "num_sub_vectors": 16
        }

    print(f"Creating IVF-PQ index with params: {index_params}")
    dataset.create_index(**index_params)

    print(f"Dataset created successfully!")
    print(f"  - Rows: {dataset.count_rows()}")
    print(f"  - Schema: {dataset.schema}")

    return dataset

def upload_to_oss(local_path, oss_uri, oss_key_id, oss_key_secret, oss_endpoint):
    """Upload dataset to Alibaba Cloud OSS."""
    import oss2

    # Parse oss://bucket/path
    if not oss_uri.startswith("oss://"):
        raise ValueError(f"Invalid OSS URI: {oss_uri}")

    uri_parts = oss_uri[6:].split("/", 1)
    bucket_name = uri_parts[0]
    prefix = uri_parts[1] if len(uri_parts) > 1 else ""

    # Create OSS bucket
    auth = Auth(oss_key_id, oss_key_secret)
    bucket = Bucket(auth, oss_endpoint, bucket_name)

    print(f"Uploading to OSS bucket: {bucket_name}, prefix: {prefix}")

    # Upload all files from local_path to prefix
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            oss_key = os.path.join(prefix, relative_path)

            print(f"  Uploading: {relative_path} -> {oss_key}")
            bucket.put_object_from_file(oss_key, local_file)

    print(f"Upload complete!")
    print(f"  OSS URI: {oss_uri}")

def main():
    parser = argparse.ArgumentParser(description="Create test Lance dataset for OSS integration")
    parser.add_argument("output_path", help="Output path for .lance dataset")
    parser.add_argument("--n-vectors", type=int, default=1000, help="Number of vectors")
    parser.add_argument("--dims", type=int, default=128, help="Vector dimensions")
    parser.add_argument("--upload", metavar="OSS_URI", help="Upload to OSS after creation")
    parser.add_argument("--oss-key-id", help="OSS Access Key ID (from env: OSS_ACCESS_KEY_ID)")
    parser.add_argument("--oss-key-secret", help="OSS Access Key Secret (from env: OSS_ACCESS_KEY_SECRET)")
    parser.add_argument("--oss-endpoint", help="OSS Endpoint (from env: OSS_ENDPOINT)")

    args = parser.parse_args()

    # Create dataset
    table = create_dataset(args.n_vectors, args.dims)
    write_lance_dataset(table, args.output_path)

    # Upload to OSS if requested
    if args.upload:
        oss_key_id = args.oss_key_id or os.environ.get("OSS_ACCESS_KEY_ID")
        oss_key_secret = args.oss_key_secret or os.environ.get("OSS_ACCESS_KEY_SECRET")
        oss_endpoint = args.oss_endpoint or os.environ.get("OSS_ENDPOINT")

        if not all([oss_key_id, oss_key_secret, oss_endpoint]):
            print("Error: OSS credentials required for upload")
            print("Set via --oss-* flags or environment variables (OSS_ACCESS_KEY_ID, etc.)")
            sys.exit(1)

        upload_to_oss(args.output_path, args.upload, oss_key_id, oss_key_secret, oss_endpoint)

if __name__ == "__main__":
    main()
