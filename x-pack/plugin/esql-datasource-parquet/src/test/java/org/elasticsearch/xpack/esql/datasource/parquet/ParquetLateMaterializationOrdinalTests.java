/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties;
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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Late materialization evaluates pushed predicates against a per-row-group dictionary-match memo
 * indexed by row-group dictionary ordinal, while this fixture is safely below the compaction
 * threshold and its emitted ordinal blocks must therefore carry a fully referenced dictionary.
 * These tests drive {@link OptimizedParquetColumnIterator} end to end and check both at once: the
 * survivors are exactly right (a memo applied to renumbered, compact ordinals would let wrong rows
 * through) and every emitted ordinal block in the fixture is fully referenced.
 *
 * <p>The fixture is one row group with a dictionary-encoded {@code kw} column whose row-group
 * dictionary starts {@code a, b, c, x, y, z}, followed by {@code d} through {@code i}, and is laid
 * out so that the first batch holds only {@code a, b, c} and the second only {@code x, y, z}.
 * Compacted, both batches would get a three-entry dictionary, and compact ordinal 0 would be
 * {@code a} in the first and {@code x} in the second: a dictionary-size check can't tell them
 * apart, only the survivors can. Three of twelve entries is safely below the share threshold.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.esql.datasource.parquet.OptimizedParquetColumnIterator:DEBUG",
    reason = "the two-phase mode is asserted from its debug log line"
)
public class ParquetLateMaterializationOrdinalTests extends ESTestCase {

    /** Rows per batch; at least 10, the minimum for the dictionary short-circuit and so for the memo. */
    private static final int BATCH = 30;
    private static final int ROWS = 4 * BATCH;
    private static final String[] FIRST = { "a", "b", "c" };
    private static final String[] SECOND = { "x", "y", "z" };
    private static final String[] THIRD = { "d", "e", "f" };
    private static final String[] FOURTH = { "g", "h", "i" };
    private static final String TWO_PHASE_ENABLED = "Two-phase I/O enabled for*";

    private static final ReferenceAttribute KW = new ReferenceAttribute(Source.EMPTY, "kw", DataType.KEYWORD);

    private BlockFactory blockFactory;
    private byte[] data;

    @Before
    public void writeFixture() throws IOException {
        assertTrue(BATCH >= 10);
        assertTrue(12 * PageColumnReader.SKIP_DICTIONARY_RATIO > BATCH);
        assertTrue(FIRST.length * 2 <= 12);
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        data = writeFile();
    }

    public void testEquals() throws IOException {
        assertBothModes(new Equals(Source.EMPTY, KW, keyword("a"), null), v -> v.equals("a"), false);
    }

    public void testIn() throws IOException {
        assertBothModes(in("a", "y"), v -> v.equals("a") || v.equals("y"), false);
    }

    /**
     * The first batch survives whole, so its predicate block is emitted as decoded and must be
     * compacted at emission (3 of 12 entries); the second loses its {@code z} rows. Had the second batch
     * been evaluated with a memo built for the first batch's compact dictionary, its {@code z} rows
     * (compact ordinal 2, like {@code c}) would have survived.
     */
    public void testInWithIntactBatch() throws IOException {
        Set<String> selected = Set.of("a", "b", "c", "x", "y");
        assertBothModes(in("a", "b", "c", "x", "y"), selected::contains, true);
    }

    public void testStartsWith() throws IOException {
        assertBothModes(new StartsWith(Source.EMPTY, KW, keyword("a")), v -> v.startsWith("a"), false);
    }

    public void testLike() throws IOException {
        assertBothModes(new WildcardLike(Source.EMPTY, KW, new WildcardPattern("y*")), v -> v.startsWith("y"), false);
    }

    /**
     * Row-group statistics prove {@code kw >= "a"} for every row, so the row group skips late
     * materialization and is read by the standard path, whose readers compact on read.
     */
    public void testTriviallyPassingRowGroup() throws IOException {
        Expression filter = new GreaterThanOrEqual(Source.EMPTY, KW, keyword("a"), null);
        List<Page> pages = read(filter, false, FormatReader.NO_LIMIT);
        try {
            assertSurvivors(pages, v -> true, true);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Two-phase with a row budget smaller than the first batch's survivors: the predicate block is
     * truncated by {@code sliceBlockHead} (a filter, so a plain block) and the projection blocks with it.
     */
    public void testTwoPhaseRowBudgetTruncation() throws IOException {
        int limit = 20;
        try (var mockLog = MockLog.capture(OptimizedParquetColumnIterator.class)) {
            mockLog.addExpectation(twoPhase(true));
            List<Page> pages = read(in("a", "b", "c", "x", "y"), true, limit);
            try {
                mockLog.assertAllExpectationsMatched();
                Set<Long> ids = new TreeSet<>();
                for (Page page : pages) {
                    assertPageOrdinalsFullyReferenced(page);
                    LongBlock idBlock = page.getBlock(1);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        ids.add(idBlock.getLong(idBlock.getFirstValueIndex(p)));
                    }
                }
                Set<Long> expected = new TreeSet<>();
                for (long id = 0; id < limit; id++) {
                    expected.add(id);
                }
                assertEquals(expected, ids);
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
    }

    /**
     * Runs {@code filter} through per-batch late materialization (synchronous storage) and through
     * two-phase decoding (storage with native async reads), asserting which mode ran.
     */
    private void assertBothModes(Expression filter, Predicate<String> expected, boolean expectIntactBatch) throws IOException {
        for (boolean async : new boolean[] { false, true }) {
            try (var mockLog = MockLog.capture(OptimizedParquetColumnIterator.class)) {
                mockLog.addExpectation(twoPhase(async));
                List<Page> pages = read(filter, async, FormatReader.NO_LIMIT);
                try {
                    mockLog.assertAllExpectationsMatched();
                    int predicateOrdinalBlocks = assertSurvivors(pages, expected, expectIntactBatch);
                    if (expectIntactBatch) {
                        assertThat(
                            "async=" + async + ": the intact batch's predicate block is emitted",
                            predicateOrdinalBlocks,
                            greaterThan(0)
                        );
                    }
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    private static MockLog.LoggingExpectation twoPhase(boolean enabled) {
        String logger = OptimizedParquetColumnIterator.class.getCanonicalName();
        return enabled
            ? new MockLog.SeenEventExpectation("two-phase", logger, Level.DEBUG, TWO_PHASE_ENABLED)
            : new MockLog.UnseenEventExpectation("two-phase", logger, Level.DEBUG, TWO_PHASE_ENABLED);
    }

    /**
     * Asserts the emitted rows are exactly those whose {@code kw} matches, with the right values in
     * every column, and that every emitted ordinal block in this below-threshold fixture is fully
     * referenced.
     *
     * @return the number of {@code kw} blocks emitted as ordinal blocks
     */
    private static int assertSurvivors(List<Page> pages, Predicate<String> matches, boolean expectOrdinals) {
        List<Long> ids = new ArrayList<>();
        int kwOrdinalBlocks = 0;
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            assertPageOrdinalsFullyReferenced(page);
            BytesRefBlock kw = page.getBlock(0);
            LongBlock id = page.getBlock(1);
            BytesRefBlock pad = page.getBlock(2);
            if (kw.asOrdinals() != null) {
                kwOrdinalBlocks++;
            }
            for (int p = 0; p < page.getPositionCount(); p++) {
                int row = Math.toIntExact(id.getLong(id.getFirstValueIndex(p)));
                ids.add((long) row);
                assertEquals("kw of row " + row, kwFor(row), kw.getBytesRef(kw.getFirstValueIndex(p), scratch).utf8ToString());
                assertEquals("pad of row " + row, padFor(row), pad.getBytesRef(pad.getFirstValueIndex(p), scratch).utf8ToString());
            }
        }
        List<Long> expected = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            if (matches.test(kwFor(row))) {
                expected.add((long) row);
            }
        }
        assertEquals("exact survivors, in file order", expected, ids);
        if (expectOrdinals) {
            assertThat("kw must reach the output as ordinal blocks for the check to mean anything", kwOrdinalBlocks, greaterThan(0));
        }
        return kwOrdinalBlocks;
    }

    private static void assertPageOrdinalsFullyReferenced(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            if (block instanceof BytesRefBlock bytes && bytes.asOrdinals() != null) {
                OrdinalBytesRefBlock ordinals = bytes.asOrdinals();
                PageColumnReaderOrdinalCompactionTests.assertFullyReferenced(ordinals);
            }
        }
    }

    private List<Page> read(Expression filter, boolean async, int rowLimit) throws IOException {
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        FormatReadContext.Builder context = FormatReadContext.builder().batchSize(BATCH).projectedColumns(List.of("kw", "id", "pad"));
        if (rowLimit != FormatReader.NO_LIMIT) {
            context.rowLimit(rowLimit);
        }
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> it = reader.read(new InMemoryStorageObject(data, async), context.build())) {
            while (it.hasNext()) {
                pages.add(it.next());
            }
        } catch (RuntimeException | IOException e) {
            pages.forEach(Page::releaseBlocks);
            throw e;
        }
        return pages;
    }

    private static Expression in(String... values) {
        return new In(Source.EMPTY, KW, Arrays.stream(values).map(v -> (Expression) keyword(v)).toList());
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    private static String kwFor(int row) {
        return switch (row / BATCH) {
            case 0 -> FIRST[row % FIRST.length];
            case 1 -> SECOND[row % SECOND.length];
            case 2 -> THIRD[row % THIRD.length];
            case 3 -> FOURTH[row % FOURTH.length];
            default -> throw new AssertionError("row out of range [" + row + "]");
        };
    }

    /** Wide, so the projection-only columns dominate the bytes and the two-phase ratio gate opens. */
    private static String padFor(int row) {
        return "p".repeat(512) + "_" + row;
    }

    /**
     * One row group and one page per column (no page-index pruning), dictionary-encoded, written in
     * row order so the {@code kw} dictionary is {@code a, b, c, x, y, z, d, e, f, g, h, i}.
     */
    private static byte[] writeFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("kw")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("pad")
            .named("late_materialization_ordinals");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(1024 * 1024)
                .withRowGroupSize(16 * 1024 * 1024L)
                .withPageSize(1024 * 1024)
                .build()
        ) {
            for (int row = 0; row < ROWS; row++) {
                Group g = factory.newGroup();
                g.append("kw", kwFor(row));
                g.append("id", (long) row);
                g.append("pad", padFor(row));
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static final class InMemoryStorageObject extends AbstractTestStorageObject {
        private final byte[] data;
        private final boolean nativeAsync;

        InMemoryStorageObject(byte[] data, boolean nativeAsync) {
            this.data = data;
            this.nativeAsync = nativeAsync;
        }

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
            return StoragePath.of("memory://late_materialization_ordinals.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return nativeAsync;
        }
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
        };
    }
}
