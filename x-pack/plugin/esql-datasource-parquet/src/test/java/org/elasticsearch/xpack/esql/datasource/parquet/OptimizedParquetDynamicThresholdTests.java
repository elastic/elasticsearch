/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.Nullable;
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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
    private static final MessageType REQUIRED_STRING_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .named("dynamic_threshold_test");
    private static final MessageType OPTIONAL_STRING_SCHEMA = Types.buildMessage()
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .named("dynamic_threshold_test");
    // Physically BINARY, read as keyword, but lacking a StringLogicalTypeAnnotation: the BYTES_REF gate
    // must refuse to prune it (mirrors ORC's isStringFamily gate) since its statistics are not
    // guaranteed to share BytesRef order.
    private static final MessageType UNANNOTATED_BINARY_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("name")
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

    public void testBufferedRowsSurviveNoFurtherCandidatesFlipBetweenHasNextAndNext() throws Exception {
        // Deterministic reproduction of the intermittent NoSuchElementException from next(): a single row
        // group of materialized rows, and a bound that prunes nothing so the first hasNext() decodes the
        // group and leaves rowsRemainingInGroup > 0. We then flip the early-termination flag (as a
        // concurrent TopN worker would) between hasNext() and next(). next() must still drain the
        // already-decoded batch; before the fix hasNext() consulted the flag first and turned the
        // available batch into a NoSuchElementException.
        byte[] data = writeLongParquet(REQUIRED_LONG_SCHEMA, 64L * 1024 * 1024, 1024, 500, i -> (long) i);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(true, false).get();
        channel.offer(10_000L); // bound above every value, so no row group is skipped
        DynamicThreshold threshold = new DynamicThreshold("id", ElementType.LONG, true, false, channel);
        try (
            threshold;
            CloseableIterator<Page> iterator = reader(threshold).read(storageObject(data), FormatReadContext.of(List.of("id"), 128))
        ) {
            assertTrue("first hasNext() must materialize the row group", iterator.hasNext());
            channel.markNoFurtherCandidates();
            Page page = iterator.next();
            try {
                assertThat(page.getPositionCount(), greaterThan(0));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testNullsLastCanSkipNullAndDominatedRowGroups() throws Exception {
        byte[] data = writeLongParquet(OPTIONAL_LONG_SCHEMA, 1L, 2 * 1024 * 1024, 300, i -> i < 100 ? null : (long) i);

        List<Long> rows = readIdsWithThreshold(data, threshold(109L, true, false));

        assertThat(rows.size(), lessThan(300));
        assertTrue(rows.contains(100L));
        assertTrue(rows.contains(109L));
        assertFalse(rows.contains(250L));
    }

    public void testRowGroupSkipWithMonotonicAscendingStrings() throws Exception {
        byte[] data = writeStringParquet(REQUIRED_STRING_SCHEMA, 1L, 2 * 1024 * 1024, 1_000, OptimizedParquetDynamicThresholdTests::key);

        List<String> rows = readNamesWithThreshold(data, bytesRefThreshold(key(9), true, false));

        assertThat(rows.size(), lessThan(1_000));
        assertTrue(rows.contains(key(0)));
        assertTrue(rows.contains(key(9)));
        assertFalse(rows.contains(key(900)));
    }

    public void testRowGroupSkipDescendingStrings() throws Exception {
        byte[] data = writeStringParquet(REQUIRED_STRING_SCHEMA, 1L, 2 * 1024 * 1024, 1_000, OptimizedParquetDynamicThresholdTests::key);

        List<String> rows = readNamesWithThreshold(data, bytesRefThreshold(key(990), false, false));

        assertThat(rows.size(), lessThan(1_000));
        assertTrue(rows.contains(key(999)));
        assertTrue(rows.contains(key(990)));
        assertFalse(rows.contains(key(0)));
    }

    public void testNullsLastCanSkipNullAndDominatedStringRowGroups() throws Exception {
        byte[] data = writeStringParquet(OPTIONAL_STRING_SCHEMA, 1L, 2 * 1024 * 1024, 300, i -> i < 100 ? null : key(i));

        List<String> rows = readNamesWithThreshold(data, bytesRefThreshold(key(109), true, false));

        assertThat(rows.size(), lessThan(300));
        assertTrue(rows.contains(key(100)));
        assertTrue(rows.contains(key(109)));
        assertFalse(rows.contains(key(250)));
    }

    public void testNullsFirstKeepsNullStringRowGroups() throws Exception {
        byte[] data = writeStringParquet(OPTIONAL_STRING_SCHEMA, 1L, 2 * 1024 * 1024, 300, i -> i < 100 ? null : key(i));

        List<String> rows = readNamesWithThreshold(data, bytesRefThreshold(key(109), true, true));

        // Under NULLS FIRST nulls sort first, so null-only row groups can never be dominated: they
        // (and the smallest non-null group) must survive even though high non-null groups are skipped.
        assertTrue(rows.contains(null));
        assertTrue(rows.contains(key(100)));
    }

    public void testUnannotatedBinaryColumnIsNeverSkipped() throws Exception {
        // Same data and dominating bound as testRowGroupSkipWithMonotonicAscendingStrings, which prunes
        // ~990 row groups; here the column carries no StringLogicalTypeAnnotation, so the gate keeps
        // every row group. Guards the Parquet/ORC type-gate parity.
        byte[] data = writeStringParquet(UNANNOTATED_BINARY_SCHEMA, 1L, 2 * 1024 * 1024, 1_000, OptimizedParquetDynamicThresholdTests::key);

        List<String> rows = readNamesWithThreshold(data, bytesRefThreshold(key(9), true, false));

        assertThat(rows.size(), equalTo(1_000));
    }

    public void testNoBoundKeepsAllStringRows() throws Exception {
        byte[] data = writeStringParquet(REQUIRED_STRING_SCHEMA, 1L, 2 * 1024 * 1024, 500, OptimizedParquetDynamicThresholdTests::key);
        // A channel with nothing offered exposes no bound, so no row group may be skipped.
        SharedMinCompetitive.KeyConfig keyConfig = new SharedMinCompetitive.KeyConfig(ElementType.BYTES_REF, TopNEncoder.UTF8, true, false);
        SharedMinCompetitive channel = new SharedMinCompetitive.Supplier(blockFactory.breaker(), List.of(keyConfig)).get();

        List<String> rows = readNamesWithThreshold(data, new DynamicThreshold("name", true, false, channel));

        assertThat(rows.size(), equalTo(500));
    }

    /**
     * The TopN threshold rail's unit matrix — the regression floor for touching {@code rawValueFromStats}.
     *
     * <p>The threshold bound is published from DECODED blocks; the row-group statistics it is compared against hold
     * the file's RAW values. Those agree only when decode is the identity. One arm of {@code rawValueFromStats}
     * serves EVERY {@code INT64} sort column, so a change there touches the identity cells too — this pins each one
     * so a conversion slip cannot silently turn a working sort into a wrong one.
     *
     * <p>Ascending is the safe direction by accident: an under-scaled raw stat reads as SMALLER than the bound, so
     * nothing is dominated and nothing is skipped. Descending is where a skew drops the row groups holding the true
     * maximum, which is why every skew cell below is asserted DESC.
     */
    public void testThresholdUnitMatrixOverInt64SortColumns() throws Exception {
        // rows 0..999 as raw values; the largest live in the LAST row group, so a skewed DESC threshold skips them.
        // scale = what decode multiplies a raw value by, so the bound below lands in each cell's DECODED domain.
        record Cell(String name, MessageType schema, @Nullable String declaredFormat, long scale) {}
        List<Cell> cells = List.of(
            // Identity cells only. This unit harness reads a bare projection, which cannot faithfully set up a
            // RESCALED sort column: it has no way to declare the ESQL type (only a format), and the descriptor it
            // hands the iterator does not carry the file's timestamp unit the way the production read path does. So
            // the rescaling cells are proven end to end instead, over a real declaration: the declared-FORMAT rescale
            // (epoch_second over a bare int64) by FromDatasetIT#testScalingDifferentialAcrossFilterSortAndAggregate,
            // and the ANNOTATION rescale (TIMESTAMP(MICROS) -> date_nanos / declared date / declared long) by
            // FromDatasetIT#testScalingDifferentialOverAnnotatedSortColumns. What this test guards is the other
            // direction: the identity reads must keep pruning exactly as they do today, since one arm of
            // rawValueFromStats serves every INT64 sort column.
            new Cell("bare INT64, no declaration", REQUIRED_LONG_SCHEMA, null, 1L),
            new Cell("TIMESTAMP(MILLIS) -> datetime", annotatedLong(LogicalTypeAnnotation.TimeUnit.MILLIS), null, 1L),
            new Cell("TIMESTAMP(NANOS) -> date_nanos", annotatedLong(LogicalTypeAnnotation.TimeUnit.NANOS), null, 1L)
        );

        List<String> broken = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] data = writeLongParquet(cell.schema(), 1L, 2 * 1024 * 1024, 1_000, i -> (long) i);
            // The bound is what TopN publishes: a DECODED value, here the decode of row 500. Descending dominance is
            // `rawMax < bound`. A correct rail lifts row 999's raw stat into the decoded domain (999*scale >= bound)
            // and keeps it. A unit-blind rail compares the RAW 999 against a bound that is `scale` times too large,
            // decides the group is dominated, and skips the rows holding the true maximum. With scale == 1 the two
            // are the same comparison, which is exactly why those cells are correct today and must stay so.
            long decodedBound = 500L * cell.scale();
            List<Long> rows = readIdsWithThreshold(data, threshold(decodedBound, false, false), cell.declaredFormat());
            if (rows.contains(999L) == false) {
                broken.add(
                    "[" + cell.name() + "] dropped the true maximum: raw stats were compared against a decoded bound of " + decodedBound
                );
            }
        }
        assertTrue("the threshold rail skipped row groups it had no right to skip:\n  " + String.join("\n  ", broken), broken.isEmpty());
    }

    /** An INT64 carrying a timestamp annotation, so the decode's unit differs from the file's raw values. */
    private static MessageType annotatedLong(LogicalTypeAnnotation.TimeUnit unit) {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, unit))
            .named("id")
            .named("threshold_test");
    }

    private DynamicThreshold threshold(long value, boolean ascending, boolean nullsFirst) {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(ascending, nullsFirst);
        SharedNumericThreshold channel = supplier.get();
        channel.offer(value);
        return new DynamicThreshold("id", org.elasticsearch.compute.data.ElementType.LONG, ascending, nullsFirst, channel);
    }

    /**
     * Build a {@code BYTES_REF} threshold whose competitive bound is {@code boundValue}, encoded
     * exactly as the {@code TopNOperator}'s {@code KeyExtractor} would: a single non-null marker byte
     * followed by the directional UTF-8 key.
     */
    private DynamicThreshold bytesRefThreshold(String boundValue, boolean ascending, boolean nullsFirst) {
        SharedMinCompetitive.KeyConfig keyConfig = new SharedMinCompetitive.KeyConfig(
            ElementType.BYTES_REF,
            TopNEncoder.UTF8,
            ascending,
            nullsFirst
        );
        SharedMinCompetitive channel = new SharedMinCompetitive.Supplier(blockFactory.breaker(), List.of(keyConfig)).get();
        TopNEncoder encoder = TopNEncoder.UTF8;
        try (BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(blockFactory.breaker(), "bound")) {
            // Prepend the non-null marker, i.e. SortOrder.nonNul() = nullsFirst ? BIG_NULL (0x02) :
            // SMALL_NULL (0x01) -- the opposite sentinel of the null marker for this nulls position.
            // Locked against the real KeyExtractor by
            // SharedMinCompetitiveTests#testManualBoundEncodingMatchesKeyExtractor.
            builder.append(nullsFirst ? (byte) 0x02 : (byte) 0x01);
            encoder.toSortable(ascending).encodeBytesRef(new BytesRef(boundValue), builder);
            channel.offer(builder.bytesRefView());
        }
        return new DynamicThreshold("name", ascending, nullsFirst, channel);
    }

    private List<String> readNamesWithThreshold(byte[] data, DynamicThreshold threshold) throws IOException {
        try (
            threshold;
            CloseableIterator<Page> iterator = reader(threshold).read(storageObject(data), FormatReadContext.of(List.of("name"), 128))
        ) {
            List<String> values = new ArrayList<>();
            BytesRef scratch = new BytesRef();
            while (iterator.hasNext()) {
                Page page = iterator.next();
                try {
                    BytesRefBlock block = page.getBlock(0);
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        values.add(block.isNull(p) ? null : block.getBytesRef(block.getFirstValueIndex(p), scratch).utf8ToString());
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            return values;
        }
    }

    private static String key(int position) {
        return String.format(Locale.ROOT, "key%06d", position);
    }

    private List<Long> readIdsWithThreshold(byte[] data, DynamicThreshold threshold) throws IOException {
        return readIdsWithThreshold(data, threshold, null);
    }

    private List<Long> readIdsWithThreshold(byte[] data, DynamicThreshold threshold, @Nullable String declaredFormat) throws IOException {
        try (
            threshold;
            CloseableIterator<Page> iterator = reader(threshold, declaredFormat).read(
                storageObject(data),
                FormatReadContext.of(List.of("id"), 128)
            )
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
        return reader(threshold, null);
    }

    /** @param declaredFormat a declared date format on the sort column, or {@code null} for an inferred read. */
    private ParquetFormatReader reader(DynamicThreshold threshold, @Nullable String declaredFormat) {
        ParquetFormatReader base = (ParquetFormatReader) new ParquetFormatReader(blockFactory, true).withDynamicThreshold(threshold);
        return declaredFormat == null ? base : (ParquetFormatReader) base.withDeclaredDateFormats(Map.of("id", declaredFormat));
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

    private byte[] writeStringParquet(MessageType schema, long rowGroupSize, int pageSize, int rows, StringForPosition valueForPosition)
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
                String value = valueForPosition.value(i);
                if (value != null) {
                    group.add("name", value);
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

    @FunctionalInterface
    private interface StringForPosition {
        String value(int position);
    }
}
