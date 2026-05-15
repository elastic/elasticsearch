/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.github.luben.zstd.ZstdOutputStream;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Payload generators for {@link HeapAttackExternalIT}. Produce raw bytes for Parquet, NDJSON,
 * and CSV inputs sized to exceed the cluster's request-breaker budget when materialized, and a
 * helper that wraps any payload in zstd framing so the test exercises the compression-layer
 * allocations on the read side too.
 * <p>
 * All output is produced in the test JVM and pushed into the in-memory S3 fixture via
 * {@code S3FixtureUtils.addBlobToFixture}; the cluster reads it over the fixture's HTTP
 * endpoint exactly as it would a real S3 bucket.
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
        PARQUET("parquet"),
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
     * Parquet file with one INT64 {@code id} column and {@code rowCount} rows. Designed for the
     * stats/sort/many-files scenarios where row count rather than per-value size is what matters.
     */
    static byte[] parquetManyRows(int rowCount, int distinctKeys) throws IOException {
        MessageType schema = new MessageType("schema", Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("id"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = inMemoryOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                // Use parquet-mr's own Configuration to avoid pulling in Hadoop's full bootstrap.
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) (i % distinctKeys));
                writer.write(g);
            }
        }
        return baos.toByteArray();
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
            case PARQUET -> parquetManyRows(rowCount, distinctKeys);
            case NDJSON -> ndjsonManyRows(rowCount, distinctKeys);
            case CSV -> csvManyRows(rowCount, distinctKeys);
        };
    }

    private static OutputFile inMemoryOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionStream(baos);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionStream(baos);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static PositionOutputStream positionStream(ByteArrayOutputStream baos) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                baos.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                baos.write(b, off, len);
                position += len;
            }
        };
    }
}
