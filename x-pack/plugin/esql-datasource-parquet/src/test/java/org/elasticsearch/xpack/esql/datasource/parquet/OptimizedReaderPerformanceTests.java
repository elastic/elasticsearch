/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Smoke test verifying the optimized reader completes without catastrophic overhead
 * on in-memory data. On in-memory storage with zero I/O latency, the prefetch pipeline
 * adds constant overhead that doesn't pay off, so we use a generous tolerance (5x).
 * The real benefit is visible only with network-latent storage (see {@link PrefetchLatencySimulationTests}).
 */
public class OptimizedReaderPerformanceTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testOptimizedNotSlowerThanBaseline() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, CompressionCodecName.SNAPPY, 4096, 1024, 20_000);
        StorageObject storageObject = createStorageObject(parquetData);

        int warmups = 3;
        int measured = 5;

        for (int i = 0; i < warmups; i++) {
            readAll(new ParquetFormatReader(blockFactory, false), storageObject);
            readAll(new ParquetFormatReader(blockFactory, true), storageObject);
        }

        long[] baselineTimes = new long[measured];
        long[] optimizedTimes = new long[measured];
        for (int i = 0; i < measured; i++) {
            long start = System.nanoTime();
            readAll(new ParquetFormatReader(blockFactory, false), storageObject);
            baselineTimes[i] = System.nanoTime() - start;

            start = System.nanoTime();
            readAll(new ParquetFormatReader(blockFactory, true), storageObject);
            optimizedTimes[i] = System.nanoTime() - start;
        }

        long baselineMedian = median(baselineTimes);
        long optimizedMedian = median(optimizedTimes);

        logger.info(
            "Performance smoke test: baseline={}ms, optimized={}ms (ratio={}.{}x)",
            baselineMedian / 1_000_000,
            optimizedMedian / 1_000_000,
            optimizedMedian / Math.max(baselineMedian, 1),
            (optimizedMedian * 10 / Math.max(baselineMedian, 1)) % 10
        );

        assertTrue(
            "Optimized reader is more than 5x slower (in-memory smoke test): baseline="
                + (baselineMedian / 1_000_000)
                + "ms, optimized="
                + (optimizedMedian / 1_000_000)
                + "ms",
            optimizedMedian <= baselineMedian * 5
        );
    }

    private void readAll(ParquetFormatReader reader, StorageObject storageObject) throws IOException {
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                iter.next();
            }
        }
    }

    private static long median(long[] values) {
        java.util.Arrays.sort(values);
        return values[values.length / 2];
    }

    private byte[] createParquetFile(MessageType schema, CompressionCodecName codec, int rgSize, int pageSize, int rowCount)
        throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withRowGroupSize(rgSize)
                .withPageSize(pageSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("name", "item_" + (i % 100));
                g.add("value", i * 10);
                g.add("score", i * 0.1);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
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
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://perf-test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return outputStream.size();
                    }

                    @Override
                    public void write(int b) {
                        outputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        outputStream.write(b, off, len);
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
                return "memory://perf-test.parquet";
            }
        };
    }
}
