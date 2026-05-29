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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class OptimizedParquetDynamicThresholdTests extends ESTestCase {

    private static final MessageType REQUIRED_LONG_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .named("dynamic_threshold_test");
    private static final MessageType OPTIONAL_LONG_SCHEMA = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .named("dynamic_threshold_test");

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testRowGroupSkipWithMonotonicAscendingData() throws Exception {
        byte[] data = writeLongParquet(REQUIRED_LONG_SCHEMA, 1L, 2 * 1024 * 1024, 1_000, i -> (long) i);

        List<Long> rows = readIdsWithThreshold(data, threshold(9L, true, false));

        assertThat(rows.size(), lessThan(1_000));
        assertTrue(rows.contains(0L));
        assertTrue(rows.contains(9L));
        assertFalse(rows.contains(900L));
    }

    public void testPageLevelSkipWithinSingleRowGroup() throws Exception {
        byte[] data = writeLongParquet(REQUIRED_LONG_SCHEMA, 64L * 1024 * 1024, 64, 2_000, i -> (long) i);

        List<Long> rows = readIdsWithThreshold(data, threshold(9L, true, false));

        assertThat(rows.size(), lessThan(2_000));
        assertTrue(rows.contains(0L));
        assertTrue(rows.contains(9L));
        assertFalse(rows.contains(1_900L));
    }

    public void testSinglePageRowGroupKeepsNegativeControlRows() throws Exception {
        byte[] data = writeLongParquet(REQUIRED_LONG_SCHEMA, 64L * 1024 * 1024, 2 * 1024 * 1024, 2_000, i -> (long) i);

        List<Long> rows = readIdsWithThreshold(data, threshold(9L, true, false));

        assertThat(rows.size(), equalTo(2_000));
    }

    public void testNoFurtherCandidatesExhaustsImmediately() throws Exception {
        byte[] data = writeLongParquet(REQUIRED_LONG_SCHEMA, 1L, 64, 1_000, i -> (long) i);
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(true, true);
        SharedNumericThreshold channel = supplier.get();
        channel.markNoFurtherCandidates();

        List<Long> rows = readIdsWithThreshold(
            data,
            new DynamicThreshold("id", org.elasticsearch.compute.data.ElementType.LONG, true, true, channel)
        );

        assertThat(rows.size(), equalTo(0));
    }

    public void testNullsLastCanSkipNullAndDominatedRowGroups() throws Exception {
        byte[] data = writeLongParquet(OPTIONAL_LONG_SCHEMA, 1L, 2 * 1024 * 1024, 300, i -> i < 100 ? null : (long) i);

        List<Long> rows = readIdsWithThreshold(data, threshold(109L, true, false));

        assertThat(rows.size(), lessThan(300));
        assertTrue(rows.contains(100L));
        assertTrue(rows.contains(109L));
        assertFalse(rows.contains(250L));
    }

    private DynamicThreshold threshold(long value, boolean ascending, boolean nullsFirst) {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(ascending, nullsFirst);
        SharedNumericThreshold channel = supplier.get();
        channel.offer(value);
        return new DynamicThreshold("id", org.elasticsearch.compute.data.ElementType.LONG, ascending, nullsFirst, channel);
    }

    private List<Long> readIdsWithThreshold(byte[] data, DynamicThreshold threshold) throws IOException {
        try (
            threshold;
            CloseableIterator<Page> iterator = reader(threshold).read(storageObject(data), FormatReadContext.of(List.of("id"), 128))
        ) {
            List<Long> values = new ArrayList<>();
            while (iterator.hasNext()) {
                Page page = iterator.next();
                try {
                    LongBlock block = page.getBlock(0);
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        values.add(block.isNull(p) ? null : block.getLong(block.getFirstValueIndex(p)));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            return values;
        }
    }

    private ParquetFormatReader reader(DynamicThreshold threshold) {
        return (ParquetFormatReader) new ParquetFormatReader(blockFactory, true).withDynamicThreshold(threshold);
    }

    private byte[] writeLongParquet(MessageType schema, long rowGroupSize, int pageSize, int rows, ValueForPosition valueForPosition)
        throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(output(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group group = factory.newGroup();
                Long value = valueForPosition.value(i);
                if (value != null) {
                    group.add("id", value);
                }
                writer.write(group);
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile output(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionOutputStream(outputStream);
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

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream outputStream) {
        return new PositionOutputStream() {
            private long position;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
                position += len;
            }
        };
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
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
                return StoragePath.of("memory://dynamic-threshold.parquet");
            }
        };
    }

    @FunctionalInterface
    private interface ValueForPosition {
        Long value(int position);
    }
}
