/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Real Parquet files, byte-for-byte as written by <b>pyarrow</b> (the writer pandas, Spark and Iceberg use) — not by
 * this repo's test fixture code. Each file holds the same instant, 2024-01-01T00:00:00Z, in a different physical
 * shape, so a declaration's job is only to recover it.
 * <p>
 * The bytes live as binary resources under {@code /real-parquet/}, checked in as {@code .parquet} files rather than
 * generated at test time so the suite stays hermetic (no python, no network) while the bytes stay genuinely
 * third-party: a fixture we wrote ourselves could encode the same misunderstanding as the reader, which is exactly
 * what this is meant to catch. Regenerate with pyarrow if a shape needs adding (write the file, drop it in the
 * resource dir); see {@code FromDatasetIT#testRealParquetTimestampShapesAllReachTheSameInstant}.
 */
final class RealParquetFixtures {

    private RealParquetFixtures() {}

    static byte[] bytes(String name) {
        String resource = "/real-parquet/" + name + ".parquet";
        try (InputStream in = RealParquetFixtures.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("no real-parquet fixture named [" + name + "] at [" + resource + "]");
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("failed reading real-parquet fixture [" + name + "]", e);
        }
    }
}
