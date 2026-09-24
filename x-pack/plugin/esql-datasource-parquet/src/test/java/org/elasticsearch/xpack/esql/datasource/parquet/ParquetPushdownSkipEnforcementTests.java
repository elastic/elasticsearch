/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.hamcrest.Matchers.lessThan;

/**
 * Pins the whole skip chain: an ES|QL filter becomes a Parquet {@link FilterPredicate}, and that predicate makes
 * parquet-mr drop row groups the filter cannot match. Nothing else in the suite asserts that a row group is actually
 * skipped — the correctness tests answer the same rows either way, so deleting a translation arm leaves them green
 * while the reader silently decodes the whole file again.
 *
 * <p>Both families are pinned together, and the multivalue forms are asserted to skip <em>exactly</em> what their
 * scalar siblings skip. The one place they legitimately differ — a bound sitting on a row-group boundary — has its
 * own test below, so the difference has to be changed deliberately rather than drifted into.
 */
public class ParquetPushdownSkipEnforcementTests extends ESTestCase {

    /** Ids run 0..TOTAL_ROWS-1 in order, so every row group holds a contiguous, disjoint id range. */
    private static final int TOTAL_ROWS = 4096;
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("id")
        .named("skip_enforcement_test");

    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);
    private CircuitBreaker breaker;
    private PlainCompressionCodecFactory codecFactory;
    private byte[] file;

    @Before
    public void writeFixture() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        breaker = blockFactory.breaker();
        codecFactory = new PlainCompressionCodecFactory();
        file = writeBucketedFile();
    }

    @After
    public void releaseCodecFactory() throws Exception {
        codecFactory.release();
    }

    public void testEqualitySkipsEveryRowGroupButTheMatchingOne() throws IOException {
        int target = TOTAL_ROWS / 2 + 7;
        assertSkipParity(
            new Equals(Source.EMPTY, id(), lit(target), null),
            new MvContains(Source.EMPTY, id(), lit(target)),
            (min, max) -> min <= target && target <= max
        );
    }

    public void testRangeSkipsEveryRowGroupOutsideTheWindow() throws IOException {
        int lower = TOTAL_ROWS / 4;
        int upper = TOTAL_ROWS / 4 + 300;
        assertSkipParity(
            new Range(Source.EMPTY, id(), lit(lower), true, lit(upper), true, null),
            new MvInRange(Source.EMPTY, id(), lit(lower), lit(upper)),
            (min, max) -> min <= upper && max >= lower
        );
    }

    public void testOneSidedUpperBoundSkipsTheGroupsAbove() throws IOException {
        int bound = TOTAL_ROWS / 3;
        assertSkipParity(
            new LessThan(Source.EMPTY, id(), lit(bound), null),
            new MvLess(Source.EMPTY, id(), lit(bound)),
            (min, max) -> min <= bound
        );
    }

    public void testOneSidedLowerBoundSkipsTheGroupsBelow() throws IOException {
        int bound = 2 * TOTAL_ROWS / 3;
        assertSkipParity(
            new GreaterThan(Source.EMPTY, id(), lit(bound), null),
            new MvGreater(Source.EMPTY, id(), lit(bound)),
            (min, max) -> max >= bound
        );
    }

    public void testValueSetSkipsTheGroupsHoldingNoneOfTheValues() throws IOException {
        int first = 7;
        int second = TOTAL_ROWS / 2 + 7;
        assertSkipParity(
            new In(Source.EMPTY, id(), List.of(lit(first), lit(second))),
            new MvIntersects(Source.EMPTY, id(), new Literal(Source.EMPTY, List.of(first, second), DataType.INTEGER)),
            // parquet-mr's statistics filter prunes a value set on the set's min/max envelope rather than testing
            // membership per group, so groups lying between two distant values survive. Both families inherit that
            // identically; this pins it so a change in parquet-mr's behaviour surfaces here rather than in a benchmark.
            (min, max) -> min <= second && max >= first
        );
    }

    /**
     * The one asymmetry, pinned so it cannot drift: the multivalue one-sided forms push their bound inclusively
     * whatever their strictness option says, because a closed interval prunes a superset of what the open one does and
     * the retained filter computes the exact answer anyway. On a bound sitting exactly on a row group's maximum that
     * costs one extra row group — the group whose values are all strictly below the bound except the bound itself.
     * Tightening the translation to the exact operator is a deliberate change that flips this assertion.
     */
    public void testMultivalueOneSidedFormsPushTheInclusiveBoundOnARowGroupBoundary() throws IOException {
        int boundary = rowGroupMax(1); // the largest id in the second row group
        Set<Integer> strict = keptRowGroups(pushedPredicate(new GreaterThan(Source.EMPTY, id(), lit(boundary), null)));
        Set<Integer> inclusive = keptRowGroups(pushedPredicate(new MvGreater(Source.EMPTY, id(), lit(boundary))));
        assertFalse("the scalar strict bound skips the group whose maximum is the bound", strict.contains(1));
        assertTrue("mv_greater pushes the inclusive bound, so that group survives", inclusive.contains(1));
        assertEquals("the difference must be exactly that one group", strict.size() + 1, inclusive.size());
    }

    /**
     * Asserts that both forms push a predicate, that the predicate skips something, and that the surviving groups are
     * exactly those whose min/max could hold a match — computed here from the file's own statistics, independently of
     * how many row groups the writer chose to emit.
     */
    private void assertSkipParity(Expression scalar, Expression mv, BiPredicate<Integer, Integer> couldMatch) throws IOException {
        Set<Integer> expected = groupsThatCouldMatch(couldMatch);
        assertThat("the fixture must let some group be skipped", expected.size(), lessThan(rowGroupCount()));
        assertSkips(scalar, expected);
        assertSkips(mv, expected);
    }

    private void assertSkips(Expression expr, Set<Integer> expected) throws IOException {
        String tag = "[" + expr.getClass().getSimpleName() + "] ";
        FilterPredicate predicate = pushedPredicate(expr);
        assertNotNull(tag + "pushed no predicate, so no row group can be skipped", predicate);
        assertEquals(tag + "surviving row groups", expected, keptRowGroups(predicate));
    }

    private FilterPredicate pushedPredicate(Expression expr) {
        return new ParquetPushedExpressions(List.of(expr)).toFilterPredicate(SCHEMA);
    }

    /** The independent oracle: which groups hold a value the filter could match, read from row-group statistics. */
    private Set<Integer> groupsThatCouldMatch(BiPredicate<Integer, Integer> couldMatch) throws IOException {
        Set<Integer> expected = new LinkedHashSet<>();
        try (ParquetFileReader reader = openReader()) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            for (int i = 0; i < blocks.size(); i++) {
                Statistics<?> stats = blocks.get(i).getColumns().get(0).getStatistics();
                if (couldMatch.test(((Number) stats.genericGetMin()).intValue(), ((Number) stats.genericGetMax()).intValue())) {
                    expected.add(i);
                }
            }
        }
        return expected;
    }

    private int rowGroupMax(int ordinal) throws IOException {
        try (ParquetFileReader reader = openReader()) {
            return ((Number) reader.getRowGroups().get(ordinal).getColumns().get(0).getStatistics().genericGetMax()).intValue();
        }
    }

    private int rowGroupCount() throws IOException {
        try (ParquetFileReader reader = openReader()) {
            return reader.getRowGroups().size();
        }
    }

    private ParquetFileReader openReader() throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(new InMemoryStorageObject(file), footerByteCache, breaker),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    /** Mirrors ParquetFormatReader.computeSurvivingRowGroups: same filter levels, same parquet-mr call. */
    private Set<Integer> keptRowGroups(FilterPredicate predicate) throws IOException {
        Set<Integer> kept = new LinkedHashSet<>();
        try (ParquetFileReader reader = openReader()) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            List<BlockMetaData> survivors = RowGroupFilter.filterRowGroups(
                List.of(
                    RowGroupFilter.FilterLevel.STATISTICS,
                    RowGroupFilter.FilterLevel.DICTIONARY,
                    RowGroupFilter.FilterLevel.BLOOMFILTER
                ),
                FilterCompat.get(predicate),
                blocks,
                reader
            );
            int idx = 0;
            for (int i = 0; i < blocks.size() && idx < survivors.size(); i++) {
                if (blocks.get(i) == survivors.get(idx)) {
                    kept.add(i);
                    idx++;
                }
            }
        }
        return kept;
    }

    private static ReferenceAttribute id() {
        return new ReferenceAttribute(Source.EMPTY, "id", DataType.INTEGER);
    }

    private static Literal lit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private byte[] writeBucketedFile() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(wrapOutput(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(SCHEMA)
                .withRowGroupSize(1L) // one row group per flush, i.e. per ROWS_PER_GROUP rows below
                .withPageSize(64)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                writer.write(factory.newGroup().append("id", i));
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile wrapOutput(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return createOrOverwrite(blockSizeHint);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
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
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://skip-enforcement.parquet");
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
        }
    }
}
