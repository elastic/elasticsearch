/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Higher-level Java API for the parquet-rs native library.
 * <p>
 * The native Rust library is built from {@code libs/parquet-rs/native/} and
 * provides Parquet file metadata and schema operations via the C ABI.
 * Low-level Panama FFI bindings live in {@code libs/native}; this module
 * provides the consumer-facing Java abstractions on top.
 */
module org.elasticsearch.parquetrs {
    requires transitive org.elasticsearch.nativeaccess;
    requires org.elasticsearch.logging;

    exports org.elasticsearch.parquetrs;
}
