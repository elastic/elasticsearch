/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/** Writes small Parquet files in memory for the tests. */
public final class ParquetTestFiles {

    private ParquetTestFiles() {}

    /**
     * A file with three columns: {@code id} is a required INT64 with value {@code row * 7}; {@code n} is an optional INT32 that is
     * missing when {@code row % 3 == 0} and {@code row} otherwise; {@code name} is an optional string that is missing when
     * {@code row % 5 == 0} and {@code "name-" + row} otherwise. A small row group budget gives the file many row groups.
     */
    public static byte[] threeColumnFile(int rows) throws IOException {
        return threeColumnFile(rows, true);
    }

    /** The same file, with or without the statistics in the footer. */
    public static byte[] threeColumnFile(int rows, boolean statistics) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("n")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("rows");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(16 * 1024L)
                .withStatisticsEnabled(statistics)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group group = factory.newGroup().append("id", i * 7L);
                if (i % 3 != 0) {
                    group.append("n", i);
                }
                if (i % 5 != 0) {
                    group.append("name", "name-" + i);
                }
                writer.write(group);
            }
        }
        return out.toByteArray();
    }

    public static StorageObject storageObject(byte[] data) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://test.parquet";
            }
        };
    }
}
