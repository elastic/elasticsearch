/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SourceOperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests.assertAllRefCountedSameInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

public class LuceneTopNSourceOperatorTests extends SourceOperatorTestCase {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneTopNSourceOperator.Factory simple(SimpleOptions options) {
        return simple(DataPartitioning.SHARD, 10_000, 100);
    }

    private static IndexReader simpleReader(Directory dir, int size, int commitEvery) {
        try (
            RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            for (int d = 0; d < size; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            return writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private LuceneTopNSourceOperator.Factory simple(DataPartitioning dataPartitioning, int size, int limit) {
        int commitEvery = Math.max(1, size / 10);
        reader = simpleReader(directory, size, commitEvery);
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { null }));
            }
        };
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        long estimatedPerRowSortSize = 16;
        return new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            dataPartitioning,
            LuceneSourceOperator.Factory::autoStrategy,
            taskConcurrency,
            maxPageSize,
            limit,
            sorts,
            estimatedPerRowSortSize,
            scoring,
            () -> 0L,
            QueryWarnings.EMIT
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex(
            "LuceneTopNSourceOperator\\[shards = \\[test], maxPageSize = \\d+, limit = 100, needsScore = "
                + scoring
                + ", sorts = \\[\\{.+}]]"
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneTopNSourceOperator"
                + "\\[dataPartitioning = (DOC|SHARD|SEGMENT), maxPageSize = \\d+, limit = 100, needsScore = "
                + scoring
                + ", sorts = \\[\\{.+}]]"
        );
    }

    // TODO tests for the other data partitioning configurations

    public void testShardDataPartitioning() {
        testShardDataPartitioning(driverContext());
    }

    public void testShardDataPartitioningWithCranky() {
        try {
            testShardDataPartitioning(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    void testShardDataPartitioning(DriverContext context) {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(context, size, limit);
    }

    public void testWithCranky() {
        try {
            int size = between(1_000, 20_000);
            int limit = between(10, size);
            testSimple(crankyDriverContext(), size, limit);
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public void testEmpty() {
        testEmpty(driverContext());
    }

    public void testEmptyWithCranky() {
        try {
            testEmpty(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    void testEmpty(DriverContext context) {
        testSimple(context, 0, between(10, 10_000));
    }

    protected void testSimple(DriverContext ctx, int size, int limit) {
        LuceneTopNSourceOperator.Factory factory = simple(DataPartitioning.SHARD, size, limit);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD, ElementType.LONG);

        List<Page> results = new ArrayList<>();
        new TestDriverRunner().run(
            TestDriverFactory.create(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add))
        );
        OperatorTestCase.assertDriverContext(ctx);

        long expectedS = 0;
        for (Page page : results) {
            if (limit - expectedS < factory.maxPageSize()) {
                assertThat(page.getPositionCount(), equalTo((int) (limit - expectedS)));
            } else {
                assertThat(page.getPositionCount(), equalTo(factory.maxPageSize()));
            }
            LongBlock sBlock = page.getBlock(initialBlockIndex(page));
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), equalTo(expectedS++));
            }
        }
        assertAllRefCountedSameInstance(results);
        int pages = (int) Math.ceil((float) Math.min(size, limit) / factory.maxPageSize());
        assertThat(results, hasSize(pages));
    }

    /**
     * A driver that collects for longer than the status interval returns to its driver from the collecting loop, so that the driver can
     * report its status. When that happens in the iteration that scores the last slice, the operator must still emit what it collected:
     * it used to report that it was finished, and the collected documents were dropped without any error.
     */
    public void testTheResultsAreEmittedWhenTheLastSliceEndsAfterTheStatusInterval() throws IOException {
        int size = 1_000;
        int limit = 10;
        // The slow query sleeps once per segment, so the index must have exactly one segment to keep the run time bounded. A
        // RandomIndexWriter flushes at random points and NoMergePolicy never merges them, so use a plain writer and add all documents
        // as one block: a block is never split across segments.
        List<List<IndexableField>> docs = new ArrayList<>(size);
        for (int d = 0; d < size; d++) {
            docs.add(List.of(new SortedNumericDocValuesField("s", d)));
        }
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.addDocuments(docs);
        }
        reader = DirectoryReader.open(directory);
        assertThat(reader.leaves(), hasSize(1));
        ShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { null }));
            }
        };
        // the one slice of the shard is scored in one chunk, which takes longer than the status interval
        Query slow = new SlowQuery(Queries.ALL_DOCS_INSTANCE, Driver.DEFAULT_STATUS_INTERVAL.millis() + 200);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(slow, List.of())
        );
        LuceneTopNSourceOperator.Factory factory = new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(shardContext),
            queryFunction,
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            0,
            between(limit, 10_000),
            limit,
            List.of(new FieldSortBuilder("s")),
            16,
            scoring,
            () -> 0L,
            QueryWarnings.EMIT
        );
        DriverContext ctx = driverContext();
        List<Page> results = new ArrayList<>();
        new TestDriverRunner().run(
            TestDriverFactory.create(ctx, factory.get(ctx), List.of(), new TestResultPageSinkOperator(results::add))
        );
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(limit));
        OperatorTestCase.assertDriverContext(ctx);
    }

    /** A query that matches every document and whose bulk scorer takes a while to score a range. */
    private static final class SlowQuery extends Query {
        private final Query delegate;
        private final long sleepMillis;

        SlowQuery(Query delegate, long sleepMillis) {
            this.delegate = delegate;
            this.sleepMillis = sleepMillis;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new FilterWeight(this, delegate.createWeight(searcher, scoreMode, boost)) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    ScorerSupplier inner = super.scorerSupplier(context);
                    if (inner == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return inner.get(leadCost);
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            BulkScorer scorer = inner.bulkScorer();
                            return new BulkScorer() {
                                @Override
                                public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                                    int next = scorer.score(collector, acceptDocs, min, max);
                                    try {
                                        Thread.sleep(sleepMillis);
                                    } catch (InterruptedException e) {
                                        throw new AssertionError(e);
                                    }
                                    return next;
                                }

                                @Override
                                public long cost() {
                                    return scorer.cost();
                                }
                            };
                        }

                        @Override
                        public long cost() {
                            return inner.cost();
                        }
                    };
                }
            };
        }

        @Override
        public String toString(String field) {
            return "slow(" + delegate.toString(field) + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other) && delegate.equals(((SlowQuery) other).delegate);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + delegate.hashCode();
        }
    }

    public void testAccumulateSearchLoad() throws IOException {
        Directory dir0 = newDirectory();
        Directory dirLarge = newDirectory();
        IndexReader r0 = null;
        IndexReader rLarge = null;
        try {
            r0 = simpleReader(dir0, 0, 1);
            rLarge = simpleReader(dirLarge, 2000, 100);

            List<ShardContext> shardContexts = List.of(new LuceneSourceOperatorTests.MockShardContext(r0, 0) {
                @Override
                public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                    SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                    return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { null }));
                }
            }, new LuceneSourceOperatorTests.MockShardContext(rLarge, 1) {
                @Override
                public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                    SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                    return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { null }));
                }
            });

            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
                new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
            );
            int taskConcurrency = 0;
            int maxPageSize = between(10, 100);
            List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
            long estimatedPerRowSortSize = 16;
            LuceneTopNSourceOperator.Factory factory = new LuceneTopNSourceOperator.Factory(
                new IndexedByShardIdFromList<>(shardContexts),
                queryFunction,
                DataPartitioning.SHARD,
                DataPartitioning.AutoStrategy.DEFAULT,
                taskConcurrency,
                maxPageSize,
                10,
                sorts,
                estimatedPerRowSortSize,
                scoring,
                () -> 0L,
                QueryWarnings.EMIT
            );
            DriverContext ctx = driverContext();
            LuceneTopNSourceOperator sourceOperator = (LuceneTopNSourceOperator) factory.get(ctx);
            List<Page> results = new ArrayList<>();
            new TestDriverRunner().run(
                TestDriverFactory.create(ctx, sourceOperator, List.of(), new TestResultPageSinkOperator(results::add))
            );

            var res = sourceOperator.shardLoadDelta(System.nanoTime());
            assertThat(res.size(), equalTo(0));

            long totalPositions = results.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(totalPositions, equalTo(10L));

            double load0 = shardContexts.get(0).stats().stats().getTotal().getSearchLoadRate();
            double loadLarge = shardContexts.get(1).stats().stats().getTotal().getSearchLoadRate();

            // Shard with no docs may still see minimal load from query rewrite, but should be <= others
            assertThat(loadLarge, greaterThan(0d));
            assertThat(loadLarge, greaterThanOrEqualTo(load0));

            OperatorTestCase.assertDriverContext(ctx);
        } finally {
            IOUtils.close(r0, rLarge, dir0, dirLarge);
        }
    }

    // Scores are not interesting to this test, but enabled conditionally and effectively ignored just for coverage.
    private final boolean scoring = randomBoolean();

    // Returns the initial block index, ignoring the score block if scoring is enabled
    private int initialBlockIndex(Page page) {
        assert page.getBlock(0) instanceof DocBlock : "expected doc block at index 0";
        if (scoring) {
            assert page.getBlock(1) instanceof DoubleBlock : "expected double block at index 1";
            return 2;
        } else {
            return 1;
        }
    }
}
