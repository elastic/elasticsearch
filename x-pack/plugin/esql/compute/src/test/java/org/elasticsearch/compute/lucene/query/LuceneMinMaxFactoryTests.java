/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.LuceneMinMaxFactory.Function;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Tests for {@link LuceneMinMaxFactory}: several {@code MIN}/{@code MAX} functions over one field answered from the doc values
 * skipper where a segment has no deletions, and by scanning doc values otherwise.
 */
public class LuceneMinMaxFactoryTests extends LuceneMinMaxOperatorTestCase {
    private static final String FIELD = "field";

    private final Directory directory = newDirectory();
    private final List<IndexReader> readers = new ArrayList<>();

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(directory);
    }

    /** The values of a single page: one value and one {@code seen} flag per function. */
    private record Result(long[] values, boolean[] seen) {}

    private ElementType randomElementType() {
        return randomFrom(ElementType.INT, ElementType.LONG);
    }

    /** A random value that is neither of the initial (empty) results, so a missing value can never look like a real one. */
    private long randomValue(ElementType type) {
        return type == ElementType.INT
            ? randomIntBetween(Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1)
            : randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1);
    }

    private static void addDoc(RandomIndexWriter writer, String id, Long value) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.NO));
        if (value != null) {
            doc.add(SortedNumericDocValuesField.indexedField(FIELD, value));
        }
        writer.addDocument(doc);
    }

    private IndexReader track(IndexReader reader) {
        readers.add(reader);
        return reader;
    }

    private LuceneMinMaxFactory factory(IndexReader reader, ElementType type, Query query, List<Function> functions) {
        return new LuceneMinMaxFactory(
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            c -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.SHARD,
            1,
            FIELD,
            type,
            functions,
            LuceneOperator.NO_LIMIT,
            () -> 0L,
            QueryWarnings.EMIT
        );
    }

    private Result run(Supplier<DriverContext> contexts, LuceneMinMaxFactory factory, ElementType type, int functions) {
        List<Page> pages = new ArrayList<>();
        DriverContext ctx = contexts.get();
        Driver driver = TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(pages::add));
        new TestDriverRunner().run(List.of(driver));
        // an index without any segment may not emit a page at all
        assertThat(pages.size(), lessThanOrEqualTo(1));
        long[] values = new long[functions];
        boolean[] seen = new boolean[functions];
        for (Page page : pages) {
            try {
                assertThat(page.getPositionCount(), equalTo(1));
                assertThat(page.getBlockCount(), equalTo(2 * functions));
                for (int i = 0; i < functions; i++) {
                    values[i] = type == ElementType.INT
                        ? ((IntBlock) page.getBlock(2 * i)).getInt(0)
                        : ((LongBlock) page.getBlock(2 * i)).getLong(0);
                    seen[i] = ((BooleanBlock) page.getBlock(2 * i + 1)).getBoolean(0);
                }
            } finally {
                page.releaseBlocks();
            }
        }
        return new Result(values, seen);
    }

    private Result run(LuceneMinMaxFactory factory, ElementType type, int functions) {
        return run(this::driverContext, factory, type, functions);
    }

    /** Wraps the reader so that doc values iteration fails on leaves without deletions, i.e. where only metadata may be read. */
    private static IndexReader failDocValuesOnLeavesWithoutDeletions(IndexReader reader) throws IOException {
        return new FilterDirectoryReader((DirectoryReader) reader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
                        if (in.hasDeletions() == false) {
                            throw new AssertionError("doc values read on a leaf without deletions, the metadata should be used");
                        }
                        return in.getSortedNumericDocValues(field);
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }

    /** Indexes random docs over several segments and returns {min, max} of everything indexed. */
    private long[] indexRandomSegments(ElementType type, int valuesPerDoc) throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            int segments = between(2, 5);
            int id = 0;
            for (int s = 0; s < segments; s++) {
                for (int d = between(1, 200); d > 0; d--) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(id++), Field.Store.NO));
                    for (int v = 0; v < valuesPerDoc; v++) {
                        long value = randomValue(type);
                        min = Math.min(min, value);
                        max = Math.max(max, value);
                        doc.add(SortedNumericDocValuesField.indexedField(FIELD, value));
                    }
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            readers.add(writer.getReader());
        }
        return new long[] { min, max };
    }

    private IndexReader lastReader() {
        return readers.get(readers.size() - 1);
    }

    public void testSkipperShortcutDoesNotReadDocValues() throws IOException {
        ElementType type = randomElementType();
        long[] minMax = indexRandomSegments(type, 1);
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));

        Result minThenMax = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(minThenMax.values, equalTo(new long[] { minMax[0], minMax[1] }));
        assertThat(minThenMax.seen, equalTo(new boolean[] { true, true }));

        Result maxThenMin = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MAX, Function.MIN)), type, 2);
        assertThat(maxThenMin.values, equalTo(new long[] { minMax[1], minMax[0] }));
    }

    /**
     * The planner turns "no query" into a {@code match_all} with a zero boost, which reaches the operator wrapped in a
     * {@link BoostQuery}. The shortcut has to look through the wrapper.
     */
    public void testBoostedMatchAllUsesShortcut() throws IOException {
        ElementType type = randomElementType();
        long[] minMax = indexRandomSegments(type, 1);
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));
        Query boosted = new BoostQuery(Queries.ALL_DOCS_INSTANCE, 0.0f);

        Result result = run(factory(reader, type, boosted, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.values, equalTo(new long[] { minMax[0], minMax[1] }));
    }

    public void testDuplicateFunctions() throws IOException {
        ElementType type = randomElementType();
        long[] minMax = indexRandomSegments(type, 1);
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));

        Result result = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MIN, Function.MAX)), type, 3);
        assertThat(result.values, equalTo(new long[] { minMax[0], minMax[0], minMax[1] }));
        assertThat(result.seen, equalTo(new boolean[] { true, true, true }));
    }

    /** Multi-valued fields: the minimum is the smallest of all values and the maximum the largest, on the shortcut and the scan. */
    public void testMultiValued() throws IOException {
        ElementType type = randomElementType();
        long[] minMax = indexRandomSegments(type, between(2, 4));
        IndexReader reader = lastReader();

        Result shortcut = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(shortcut.values, equalTo(new long[] { minMax[0], minMax[1] }));

        Query scan = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, Long.MIN_VALUE, Long.MAX_VALUE);
        Result scanned = run(factory(reader, type, scan, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(scanned.values, equalTo(new long[] { minMax[0], minMax[1] }));
    }

    /**
     * Segment bounds include deleted documents, so a segment with deletions must be scanned. The segment without deletions
     * must still be answered from metadata.
     */
    public void testSegmentWithDeletionsFallsBackToScan() throws IOException {
        ElementType type = randomElementType();
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            // segment without deletions
            for (int i = 0; i < 20; i++) {
                addDoc(writer, "a" + i, 100L + i);
            }
            writer.commit();
            // segment whose extreme values are deleted afterwards
            addDoc(writer, "lowest", -1000L);
            addDoc(writer, "highest", 1000L);
            for (int i = 0; i < 20; i++) {
                addDoc(writer, "b" + i, 300L + i);
            }
            writer.commit();
            writer.deleteDocuments(new Term("id", "lowest"));
            writer.deleteDocuments(new Term("id", "highest"));
            writer.commit();
            readers.add(writer.getReader());
        }
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));

        Result result = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.values, equalTo(new long[] { 100L, 319L }));
        assertThat(result.seen, equalTo(new boolean[] { true, true }));
    }

    /** Documents without a value are ignored, and a segment without any value does not break the others. */
    public void testMissingValues() throws IOException {
        ElementType type = randomElementType();
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            addDoc(writer, "x1", null);
            addDoc(writer, "x2", null);
            writer.commit();
            addDoc(writer, "y1", 7L);
            addDoc(writer, "y2", null);
            addDoc(writer, "y3", -3L);
            writer.commit();
            readers.add(writer.getReader());
        }
        Result result = run(factory(lastReader(), type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.values, equalTo(new long[] { -3L, 7L }));
        assertThat(result.seen, equalTo(new boolean[] { true, true }));
    }

    public void testNoDocumentHasAValue() throws IOException {
        ElementType type = randomElementType();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            addDoc(writer, "1", null);
            addDoc(writer, "2", null);
            readers.add(writer.getReader());
        }
        Result result = run(factory(lastReader(), type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.seen, equalTo(new boolean[] { false, false }));
        long emptyMin = type == ElementType.INT ? Integer.MAX_VALUE : Long.MAX_VALUE;
        long emptyMax = type == ElementType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE;
        assertThat(result.values, equalTo(new long[] { emptyMin, emptyMax }));
    }

    public void testEmptyIndex() throws IOException {
        ElementType type = randomElementType();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            readers.add(writer.getReader());
        }
        Result result = run(factory(lastReader(), type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.seen, equalTo(new boolean[] { false, false }));
    }

    /**
     * Fields indexed with points and doc values but no skipper, like standard date and number fields, are answered from the
     * points metadata where a segment has no deletions: doc values are only read for segments with deletions.
     */
    public void testPointsField() throws IOException {
        ElementType type = randomElementType();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int s = 0; s < between(1, 4); s++) {
                for (int d = between(1, 100); d > 0; d--) {
                    long value = randomValue(type);
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                    Document doc = new Document();
                    doc.add(
                        type == ElementType.INT
                            ? new IntField(FIELD, (int) value, Field.Store.NO)
                            : new LongField(FIELD, value, Field.Store.NO)
                    );
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            readers.add(writer.getReader());
        }
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));
        Result result = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.values, equalTo(new long[] { min, max }));
        assertThat(result.seen, equalTo(new boolean[] { true, true }));
    }

    /** Segment bounds from points include deleted documents too, so segments with deletions are scanned. */
    public void testPointsFieldWithDeletionsFallsBackToScan() throws IOException {
        ElementType type = randomElementType();
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 20; i++) {
                addPointsDoc(writer, type, "a" + i, 100L + i);
            }
            writer.commit();
            addPointsDoc(writer, type, "lowest", -1000L);
            addPointsDoc(writer, type, "highest", 1000L);
            for (int i = 0; i < 20; i++) {
                addPointsDoc(writer, type, "b" + i, 300L + i);
            }
            writer.commit();
            writer.deleteDocuments(new Term("id", "lowest"));
            writer.deleteDocuments(new Term("id", "highest"));
            writer.commit();
            readers.add(writer.getReader());
        }
        IndexReader reader = track(failDocValuesOnLeavesWithoutDeletions(lastReader()));
        Result result = run(factory(reader, type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)), type, 2);
        assertThat(result.values, equalTo(new long[] { 100L, 319L }));
    }

    private static void addPointsDoc(RandomIndexWriter writer, ElementType type, String id, long value) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.NO));
        doc.add(type == ElementType.INT ? new IntField(FIELD, (int) value, Field.Store.NO) : new LongField(FIELD, value, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void testWithCranky() throws IOException {
        ElementType type = randomElementType();
        indexRandomSegments(type, 1);
        try {
            run(
                this::crankyDriverContext,
                factory(lastReader(), type, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX)),
                type,
                2
            );
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testRejectsBadArguments() throws IOException {
        indexRandomSegments(ElementType.LONG, 1);
        IndexReader reader = lastReader();
        expectThrows(IllegalArgumentException.class, () -> factory(reader, ElementType.LONG, Queries.ALL_DOCS_INSTANCE, List.of()));
        expectThrows(
            IllegalArgumentException.class,
            () -> factory(reader, ElementType.DOUBLE, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> factory(reader, ElementType.BOOLEAN, Queries.ALL_DOCS_INSTANCE, List.of(Function.MAX))
        );
    }

    @Override
    protected LuceneMinMaxFactory simple(SimpleOptions options) {
        try {
            indexRandomSegments(ElementType.LONG, 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return factory(lastReader(), ElementType.LONG, Queries.ALL_DOCS_INSTANCE, List.of(Function.MIN, Function.MAX));
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneMinMaxOperator\\[shards = \\[test], maxPageSize = \\d+, remainingDocs=2147483647]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneMinMaxOperator\\[type = LONG, functions = \\[MIN, MAX], dataPartitioning = (AUTO|DOC|SHARD|SEGMENT), fieldName = "
                + FIELD
                + ", limit = 2147483647]"
        );
    }
}
