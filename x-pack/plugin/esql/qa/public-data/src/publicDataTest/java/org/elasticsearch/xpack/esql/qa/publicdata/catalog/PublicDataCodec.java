/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * The compression codec of a catalog {@link SourceVariant}'s resource, matching
 * elastic/esql-planning#1650's compression dimension. Not every {@link PublicDataFormat} supports every
 * codec: Parquet natively supports {@code UNCOMPRESSED}/{@code SNAPPY}/{@code ZSTD}/{@code GZIP} page
 * compression, while the text formats ({@code NDJSON}/{@code CSV}/{@code TSV}) are normally only ever
 * exercised whole-object-compressed with {@code GZIP} or {@code ZSTD} -- {@link PublicDataCatalogValidator}
 * enforces this pairing so an invalid combination (e.g. an {@code UNCOMPRESSED} CSV variant) is rejected
 * before it reaches the runner. {@code BZIP2} is the one deliberate exception: it migrates the disabled
 * {@code ExternalFileBzip2NdJsonCountIT#testExternalBzip2NdJsonStatsCountNycTaxisDocuments} rally-tracks
 * NDJSON fixture into this suite (elastic/esql-planning#1650's "migrate the NDJSON taxis suite" item);
 * bzip2 is outside the GA text-format codec set and is snapshot-build-only (elastic/esql-planning#938).
 */
public enum PublicDataCodec {
    UNCOMPRESSED,
    SNAPPY,
    ZSTD,
    GZIP,
    BZIP2;

    public static PublicDataCodec parse(String value) {
        return PublicDataCodec.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
