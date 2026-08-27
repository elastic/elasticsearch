/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
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
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
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
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Parity test for repeated/list columns. The optimized iterator builds its own {@code PageReadStore}
 * via {@code PrefetchedRowGroupBuilder} and feeds it to parquet-mr's {@code ColumnReadStoreImpl}
 * (the row-at-a-time path used for list columns). This means decoding still goes through
 * parquet-mr's {@code ColumnReader}, but page parsing, decompression, and offsets all flow
 * through our custom code. Verify that decoded values match the baseline path bit-for-bit across
 * codec / writer-version / nullability axes.
 */
public class ListColumnParityTests extends ESTestCase {

    private final CompressionCodecName codec;
    private final WriterVersion writerVersion;
    private final boolean nullable;
    private final String description;

    public ListColumnParityTests(String description, CompressionCodecName codec, WriterVersion writerVersion, boolean nullable) {
        this.description = description;
        this.codec = codec;
        this.writerVersion = writerVersion;
        this.nullable = nullable;
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        CompressionCodecName[] codecs = {
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4_RAW };
        for (CompressionCodecName codec : codecs) {
            for (WriterVersion v : new WriterVersion[] { WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0 }) {
                String tag = codec.name() + "/" + (v == WriterVersion.PARQUET_1_0 ? "V1" : "V2");
                params.add(new Object[] { tag + "/non-null", codec, v, false });
                params.add(new Object[] { tag + "/nullable", codec, v, true });
            }
        }
        return params;
    }

    public void testBaselineAndOptimizedProduceSameListsForAllCodecs() throws Exception {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        // Mixed schema with both an int list and a string list, plus a flat scalar to exercise
        // the maxRepLevel == 0 + maxRepLevel > 0 mixed-projection path in the iterator.
        Type intList = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("ints");
        Type strList = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("strs");
        Type longList = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT64).named("longs");
        MessageType schema = new MessageType("list_parity", intList, strList, longList);

        byte[] parquetData = writeFile(schema);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader baseline = new ParquetFormatReader(blockFactory, false);
        ParquetFormatReader optimized = new ParquetFormatReader(blockFactory, true);

        List<Page> baselinePages;
        try (CloseableIterator<Page> iter = baseline.read(storageObject, FormatReadContext.of(null, 64))) {
            baselinePages = collect(iter);
        }
        List<Page> optimizedPages;
        try (CloseableIterator<Page> iter = optimized.read(storageObject, FormatReadContext.of(null, 64))) {
            optimizedPages = collect(iter);
        }

        assertPagesEqual(baselinePages, optimizedPages, description);
    }

    private byte[] writeFile(MessageType schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withWriterVersion(writerVersion)
                .withRowGroupSize(8 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            // 256 rows is enough to span multiple pages with pageSize=512 and produce both small
            // and empty lists, plus null lists when nullable=true.
            for (int row = 0; row < 256; row++) {
                Group g = groupFactory.newGroup();
                int listLen = row % 5; // 0..4 elements
                if (nullable && row % 11 == 0) {
                    // entire ints group left absent -> null list
                } else {
                    Group ints = g.addGroup("ints");
                    for (int k = 0; k < listLen; k++) {
                        if (nullable && (row + k) % 7 == 0) {
                            ints.addGroup("list"); // element absent -> null in list
                        } else {
                            ints.addGroup("list").append("element", row * 10 + k);
                        }
                    }
                }
                if (nullable && row % 13 == 0) {
                    // null string list
                } else {
                    Group strs = g.addGroup("strs");
                    for (int k = 0; k < listLen; k++) {
                        if (nullable && (row * 31 + k) % 17 == 0) {
                            strs.addGroup("list");
                        } else {
                            strs.addGroup("list").append("element", "v_" + row + "_" + k);
                        }
                    }
                }
                if (nullable && row % 17 == 0) {
                    // null long list
                } else {
                    Group longs = g.addGroup("longs");
                    for (int k = 0; k < listLen; k++) {
                        longs.addGroup("list").append("element", (long) row * 1_000L + k);
                    }
                }
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private List<Page> collect(CloseableIterator<Page> iter) {
        List<Page> pages = new ArrayList<>();
        while (iter.hasNext()) {
            pages.add(iter.next());
        }
        return pages;
    }

    private void assertPagesEqual(List<Page> expected, List<Page> actual, String desc) {
        assertThat(desc + ": page count", actual.size(), equalTo(expected.size()));
        for (int p = 0; p < expected.size(); p++) {
            Page ep = expected.get(p);
            Page ap = actual.get(p);
            assertThat(desc + " page " + p + ": position count", ap.getPositionCount(), equalTo(ep.getPositionCount()));
            assertThat(desc + " page " + p + ": block count", ap.getBlockCount(), equalTo(ep.getBlockCount()));
            for (int b = 0; b < ep.getBlockCount(); b++) {
                assertListBlocksEqual(ep.getBlock(b), ap.getBlock(b), desc + " page " + p + " block " + b);
            }
        }
    }

    private void assertListBlocksEqual(
        org.elasticsearch.compute.data.Block expected,
        org.elasticsearch.compute.data.Block actual,
        String ctx
    ) {
        for (int pos = 0; pos < expected.getPositionCount(); pos++) {
            boolean en = expected.isNull(pos);
            assertThat(ctx + " null@" + pos, actual.isNull(pos), equalTo(en));
            if (en) {
                continue;
            }
            int eCount = expected.getValueCount(pos);
            int aCount = actual.getValueCount(pos);
            assertThat(ctx + " valueCount@" + pos, aCount, equalTo(eCount));
            int eStart = expected.getFirstValueIndex(pos);
            int aStart = actual.getFirstValueIndex(pos);
            for (int k = 0; k < eCount; k++) {
                if (expected instanceof IntBlock eb && actual instanceof IntBlock ab) {
                    assertThat(ctx + " int@" + pos + "[" + k + "]", ab.getInt(aStart + k), equalTo(eb.getInt(eStart + k)));
                } else if (expected instanceof LongBlock eb && actual instanceof LongBlock ab) {
                    assertThat(ctx + " long@" + pos + "[" + k + "]", ab.getLong(aStart + k), equalTo(eb.getLong(eStart + k)));
                } else if (expected instanceof BytesRefBlock eb && actual instanceof BytesRefBlock ab) {
                    assertThat(
                        ctx + " bytes@" + pos + "[" + k + "]",
                        ab.getBytesRef(aStart + k, new BytesRef()),
                        equalTo(eb.getBytesRef(eStart + k, new BytesRef()))
                    );
                } else {
                    throw new AssertionError("unexpected block type pair: " + expected.getClass() + " vs " + actual.getClass());
                }
            }
        }
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
                return StoragePath.of("memory://list-parity.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionStream(outputStream);
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
                return "memory://list-parity.parquet";
            }
        };
    }

    private static PositionOutputStream positionStream(ByteArrayOutputStream outputStream) {
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
}
