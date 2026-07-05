/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies split-range statistics from the native footer path include per-column keys used by
 * {@code PushAggregatesToExternalSource} for MIN/MAX (parity with Java {@code ParquetFormatReader}).
 */
public class ParquetRsDiscoverSplitRangesStatsTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testDiscoverSplitRangesIncludesColumnMinMaxKeys() throws Exception {
        Path parquetPath = createTempDir().resolve("parquet_rs_split_column_stats.parquet");
        writeMultiRowGroupParquetWithStats(parquetPath);

        ParquetRsFormatReader reader = new ParquetRsFormatReader(blockFactory);
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(localFileStorageObject(parquetPath));
        assertFalse("expected footer split ranges for a multi-row-group file", ranges.isEmpty());

        boolean sawChunkSizeKey = false;
        long bestMin = Long.MAX_VALUE;
        long bestMax = Long.MIN_VALUE;
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> st = range.statistics();
            assertTrue(st.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertThat(st.get(SourceStatisticsSerializer.STATS_ROW_COUNT), instanceOf(Long.class));

            Object sizeBytes = st.get(SourceStatisticsSerializer.STATS_SIZE_BYTES);
            assertNotNull(sizeBytes);
            assertThat(sizeBytes, instanceOf(Long.class));
            assertThat(
                "_stats.size_bytes carries row-group uncompressed totals (may match on-disk span when uncompressed)",
                (Long) sizeBytes,
                greaterThanOrEqualTo(range.length())
            );

            if (st.containsKey(SourceStatisticsSerializer.columnSizeBytesKey("eventdate"))) {
                sawChunkSizeKey = true;
            }
            Object colMin = st.get(SourceStatisticsSerializer.columnMinKey("eventdate"));
            Object colMax = st.get(SourceStatisticsSerializer.columnMaxKey("eventdate"));
            if (colMin instanceof Long lmin) {
                bestMin = Math.min(bestMin, lmin);
            }
            if (colMax instanceof Long lmax) {
                bestMax = Math.max(bestMax, lmax);
            }
        }
        assertEquals(1000L, bestMin);
        assertEquals(1599L, bestMax);
        assertTrue(sawChunkSizeKey);
    }

    /**
     * Build a narrow schema with wide rows so multiple row groups are produced; IDs and eventdates
     * yield deterministic Parquet chunk statistics readable from the footer alone.
     */
    private static void writeMultiRowGroupParquetWithStats(Path path) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("eventdate")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("multi_rg_with_stats_schema");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputFile outputFile = outputFileOverStream(bos);
        String pad = "x".repeat(200);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(12 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < 600; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("eventdate", 1000L + i);
                g.add("payload", pad);
                writer.write(g);
            }
        }
        Files.write(path, bos.toByteArray());
    }

    private static OutputFile outputFileOverStream(ByteArrayOutputStream stream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                OutputStream delegate = stream;
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        delegate.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        delegate.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
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
                return "memory://fixture.parquet";
            }
        };
    }

    private static StorageObject localFileStorageObject(Path path) {
        String uriString = path.toUri().toString();
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return Files.newInputStream(path);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                InputStream base = Files.newInputStream(path);
                long skipped = 0;
                while (skipped < position) {
                    long n = base.skip(position - skipped);
                    if (n <= 0) {
                        break;
                    }
                    skipped += n;
                }
                return new InputStream() {
                    private long remaining = length;

                    @Override
                    public int read() throws IOException {
                        if (remaining <= 0) {
                            return -1;
                        }
                        int v = base.read();
                        if (v >= 0) {
                            remaining--;
                        }
                        return v;
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        if (remaining <= 0) {
                            return -1;
                        }
                        int toRead = (int) Math.min(len, remaining);
                        int n = base.read(b, off, toRead);
                        if (n > 0) {
                            remaining -= n;
                        }
                        return n;
                    }

                    @Override
                    public void close() throws IOException {
                        base.close();
                    }
                };
            }

            @Override
            public long length() throws IOException {
                return Files.size(path);
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return Files.exists(path);
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uriString);
            }
        };
    }
}
