/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.github.luben.zstd.ZstdOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Payload generators for {@link HeapAttackExternalIT}. Produce raw bytes for NDJSON and CSV
 * inputs sized to exceed the cluster's request-breaker budget when materialized, and a helper
 * that wraps any payload in zstd framing so the test exercises the compression-layer
 * allocations on the read side too.
 * <p>
 * All output is produced in the test JVM and pushed into the in-memory S3 fixture via
 * {@code S3FixtureUtils.addBlobToFixture}; the cluster reads it over the fixture's HTTP
 * endpoint exactly as it would a real S3 bucket.
 * <p>
 * Parquet support was scoped out together with the Parquet test scenarios — when those are
 * re-enabled, restore {@code parquetManyRows} (and the matching {@code Format.PARQUET} enum
 * value) here along with the parquet-mr/Hadoop {@code javaRestTestImplementation} deps in
 * {@code build.gradle}.
 */
final class HeapAttackExternalFixtures {

    /** Compression variants the test exercises. */
    enum Compression {
        NONE(""),
        ZSTD(".zst");

        final String extension;

        Compression(String extension) {
            this.extension = extension;
        }
    }

    /** Source formats the test exercises. */
    enum Format {
        NDJSON("ndjson"),
        CSV("csv");

        final String extension;

        Format(String extension) {
            this.extension = extension;
        }
    }

    private HeapAttackExternalFixtures() {}

    /**
     * Wrap {@code raw} in the configured compression. {@link Compression#NONE} is a no-op.
     */
    static byte[] maybeCompress(byte[] raw, Compression compression) throws IOException {
        if (compression == Compression.NONE) {
            return raw;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStream zstd = new ZstdOutputStream(out)) {
            zstd.write(raw);
        }
        return out.toByteArray();
    }

    /**
     * NDJSON file with {@code rowCount} records of the form {@code {"id":<n>}}. Streams directly
     * to bytes (no intermediate {@code String}) so peak test-JVM memory stays close to the final
     * payload size.
     */
    static byte[] ndjsonManyRows(int rowCount, int distinctKeys) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(rowCount * 12);
        PrintStream out = new PrintStream(baos, false, StandardCharsets.UTF_8);
        for (int i = 0; i < rowCount; i++) {
            out.print("{\"id\":");
            out.print(i % distinctKeys);
            out.print("}\n");
        }
        out.flush();
        return baos.toByteArray();
    }

    /**
     * CSV file with header {@code id\n} and {@code rowCount} rows of small integer values. Streams
     * directly to bytes so peak test-JVM memory stays close to the final payload size.
     */
    static byte[] csvManyRows(int rowCount, int distinctKeys) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(rowCount * 8);
        PrintStream out = new PrintStream(baos, false, StandardCharsets.UTF_8);
        out.print("id\n");
        for (int i = 0; i < rowCount; i++) {
            out.print(i % distinctKeys);
            out.print('\n');
        }
        out.flush();
        return baos.toByteArray();
    }

    /**
     * Format-dispatching wrapper for the many-rows scenarios.
     */
    static byte[] manyRows(Format format, int rowCount, int distinctKeys) throws IOException {
        return switch (format) {
            case NDJSON -> ndjsonManyRows(rowCount, distinctKeys);
            case CSV -> csvManyRows(rowCount, distinctKeys);
        };
    }
}
