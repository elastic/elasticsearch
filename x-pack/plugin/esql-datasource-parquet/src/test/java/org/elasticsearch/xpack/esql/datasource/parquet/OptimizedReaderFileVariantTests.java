/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
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
 * Parameterized test that generates Parquet files under varied configurations and asserts
 * identical output between the baseline and optimized readers.
 */
public class OptimizedReaderFileVariantTests extends ESTestCase {

    private final CompressionCodecName codec;
    private final WriterVersion writerVersion;
    private final int rowGroupSize;
    private final int pageSize;
    private final int rowCount;
    private final boolean nullable;
    private final String description;

    public OptimizedReaderFileVariantTests(
        String description,
        CompressionCodecName codec,
        WriterVersion writerVersion,
        int rowGroupSize,
        int pageSize,
        int rowCount,
        boolean nullable
    ) {
        this.description = description;
        this.codec = codec;
        this.writerVersion = writerVersion;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.rowCount = rowCount;
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

        // V1 retains the original RG/page-size matrix so all prior coverage stays intact.
        for (CompressionCodecName codec : codecs) {
            String prefix = codec.name() + "/V1";
            params.add(new Object[] { prefix + "/1RG/non-null", codec, WriterVersion.PARQUET_1_0, 1024 * 1024, 1024 * 1024, 100, false });
            params.add(new Object[] { prefix + "/1RG/nullable", codec, WriterVersion.PARQUET_1_0, 1024 * 1024, 1024 * 1024, 100, true });
            params.add(new Object[] { prefix + "/multiRG/non-null", codec, WriterVersion.PARQUET_1_0, 1024, 256, 500, false });
            params.add(new Object[] { prefix + "/multiRG/nullable", codec, WriterVersion.PARQUET_1_0, 1024, 256, 500, true });
            params.add(new Object[] { prefix + "/smallPage/non-null", codec, WriterVersion.PARQUET_1_0, 8 * 1024, 128, 200, false });
        }

        // V2 page format uses uncompressed rep/def levels and only compresses the data portion -
        // a distinct decode path in PrefetchedPageReader. Cover the same codec / nullable axes for
        // V2 across both 1RG and multiRG layouts to exercise both filtered and sequential builder
        // paths under V2.
        for (CompressionCodecName codec : codecs) {
            String prefix = codec.name() + "/V2";
            params.add(new Object[] { prefix + "/1RG/non-null", codec, WriterVersion.PARQUET_2_0, 1024 * 1024, 1024 * 1024, 100, false });
            params.add(new Object[] { prefix + "/1RG/nullable", codec, WriterVersion.PARQUET_2_0, 1024 * 1024, 1024 * 1024, 100, true });
            params.add(new Object[] { prefix + "/multiRG/non-null", codec, WriterVersion.PARQUET_2_0, 1024, 256, 500, false });
            params.add(new Object[] { prefix + "/multiRG/nullable", codec, WriterVersion.PARQUET_2_0, 1024, 256, 500, true });
        }

        return params;
    }

    public void testBaselineAndOptimizedProduceSameOutput() throws Exception {
        // Test variants reuse the same StorageObject path with different file contents; the shared
        // FooterByteCache is keyed by (path, length) and would otherwise serve a stale footer from
        // a prior variant that happens to share the same byte length. Clear it to ensure each
        // variant reads its own footer bytes from the StorageObject.
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();

        MessageType schema;
        if (nullable) {
            schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT64)
                .named("id")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .optional(PrimitiveType.PrimitiveTypeName.INT32)
                .named("value")
                .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                .named("score")
                .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                .named("flag")
                .named("test_schema");
        } else {
            schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT64)
                .named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named("value")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                .named("score")
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                .named("flag")
                .named("test_schema");
        }

        byte[] parquetData = createParquetFile(schema);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader baseline = new ParquetFormatReader(blockFactory, false);
        ParquetFormatReader optimized = new ParquetFormatReader(blockFactory, true);

        List<Page> baselinePages;
        try (CloseableIterator<Page> iter = baseline.read(storageObject, FormatReadContext.of(null, 1024))) {
            baselinePages = collectPages(iter);
        }
        List<Page> optimizedPages;
        try (CloseableIterator<Page> iter = optimized.read(storageObject, FormatReadContext.of(null, 1024))) {
            optimizedPages = collectPages(iter);
        }

        assertPagesEqual(baselinePages, optimizedPages, description);
    }

    private byte[] createParquetFile(MessageType schema) throws IOException {
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
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                if (nullable && i % 3 == 0) {
                    // leave name null
                } else {
                    g.add("name", "item_" + i);
                }
                if (nullable && i % 5 == 0) {
                    // leave value null
                } else {
                    g.add("value", i * 10);
                }
                if (nullable && i % 7 == 0) {
                    // leave score null
                } else {
                    g.add("score", i * 0.5);
                }
                if (nullable && i % 4 == 0) {
                    // leave flag null
                } else {
                    g.add("flag", i % 2 == 0);
                }
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private List<Page> collectPages(CloseableIterator<Page> iter) {
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
            assertThat(desc + ": block count page " + p, ap.getBlockCount(), equalTo(ep.getBlockCount()));
            assertThat(desc + ": position count page " + p, ap.getPositionCount(), equalTo(ep.getPositionCount()));
            for (int b = 0; b < ep.getBlockCount(); b++) {
                assertBlocksEqual(ep.getBlock(b), ap.getBlock(b), desc + " page " + p + " block " + b);
            }
        }
    }

    private void assertBlocksEqual(org.elasticsearch.compute.data.Block expected, org.elasticsearch.compute.data.Block actual, String ctx) {
        assertThat(ctx + " position count", actual.getPositionCount(), equalTo(expected.getPositionCount()));
        for (int pos = 0; pos < expected.getPositionCount(); pos++) {
            boolean expNull = expected.isNull(pos);
            assertThat(ctx + " null at " + pos, actual.isNull(pos), equalTo(expNull));
            if (expNull == false) {
                if (expected instanceof LongBlock lb && actual instanceof LongBlock lab) {
                    assertThat(
                        ctx + " long@" + pos,
                        lab.getLong(lab.getFirstValueIndex(pos)),
                        equalTo(lb.getLong(lb.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof IntBlock ib && actual instanceof IntBlock iab) {
                    assertThat(
                        ctx + " int@" + pos,
                        iab.getInt(iab.getFirstValueIndex(pos)),
                        equalTo(ib.getInt(ib.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof DoubleBlock db && actual instanceof DoubleBlock dab) {
                    assertThat(
                        ctx + " double@" + pos,
                        dab.getDouble(dab.getFirstValueIndex(pos)),
                        equalTo(db.getDouble(db.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof BooleanBlock bb && actual instanceof BooleanBlock bab) {
                    assertThat(
                        ctx + " bool@" + pos,
                        bab.getBoolean(bab.getFirstValueIndex(pos)),
                        equalTo(bb.getBoolean(bb.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof BytesRefBlock brb && actual instanceof BytesRefBlock brab) {
                    assertThat(
                        ctx + " bytes@" + pos,
                        brab.getBytesRef(brab.getFirstValueIndex(pos), new org.apache.lucene.util.BytesRef()),
                        equalTo(brb.getBytesRef(brb.getFirstValueIndex(pos), new org.apache.lucene.util.BytesRef()))
                    );
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
                return StoragePath.of("memory://variant-test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return createPositionOutputStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return createPositionOutputStream(outputStream);
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
                return "memory://variant-test.parquet";
            }
        };
    }

    private static PositionOutputStream createPositionOutputStream(ByteArrayOutputStream outputStream) {
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
