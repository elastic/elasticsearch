/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * The on-disk encoding of a catalog {@link SourceVariant}'s resource. Matches
 * elastic/esql-planning#1650's first dimension: {@code PARQUET x {UNCOMPRESSED, SNAPPY, ZSTD, GZIP}} and
 * {@code {NDJSON, CSV, TSV} x {ZSTD, GZIP}}.
 */
public enum PublicDataFormat {
    PARQUET,
    NDJSON,
    CSV,
    TSV;

    public static PublicDataFormat parse(String value) {
        return PublicDataFormat.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
