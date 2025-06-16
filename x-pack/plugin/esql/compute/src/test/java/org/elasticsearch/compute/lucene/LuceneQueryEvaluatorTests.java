/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.OperatorTests;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ShuffleDocsOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.BlockDocValuesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.compute.test.OperatorTestCase.randomPageSize;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for testing Lucene query evaluators.
 */
public abstract class LuceneQueryEvaluatorTests<T extends Vector, U extends Vector.Builder> extends ComputeTestCase {

    private static final String FIELD = "g";

    @SuppressWarnings("unchecked")
    public void testDenseCollectorSmall() throws IOException {
        try (LuceneQueryEvaluator.DenseCollector<U> collector = createDenseCollector(0, 2)) {
            collector.setScorer(getScorer());
            collector.collect(0);
            collector.collect(1);
            collector.collect(2);
            collector.finish();
            try (T result = (T) collector.build()) {
                for (int i = 0; i <= 2; i++) {
                    assertCollectedResultMatch(result, i, true);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testDenseCollectorSimple() throws IOException {
        try (LuceneQueryEvaluator.DenseCollector<U> collector = createDenseCollector(0, 10)) {
            collector.setScorer(getScorer());
            collector.collect(2);
            collector.collect(5);
            collector.finish();
            try (T result = (T) collector.build()) {
                for (int i = 0; i < 11; i++) {
                    assertCollectedResultMatch(result, i, i == 2 || i == 5);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testDenseCollector() throws IOException {
        int length = between(1, 10_000);
        int min = between(0, Integer.MAX_VALUE - length - 1);
        int max = min + length;
        boolean[] expected = new boolean[length];
        try (LuceneQueryEvaluator.DenseCollector<U> collector = createDenseCollector(min, max)) {
            collector.setScorer(getScorer());
            for (int i = 0; i < length; i++) {
                expected[i] = randomBoolean();
                if (expected[i]) {
                    collector.collect(min + i);
                }
            }
            collector.finish();
            try (T result = (T) collector.build()) {
                for (int i = 0; i < length; i++) {
                    assertCollectedResultMatch(result, i, expected[i]);
                }
            }
        }
    }

    /**
     * Create a dense collector for the given range.
     */
    protected abstract LuceneQueryEvaluator.DenseCollector<U> createDenseCollector(int min, int max);

    /**
     * Chceks that the collected results at the given position corresponds to a match or no match
     */
    protected abstract void assertCollectedResultMatch(T resultVector, int position, boolean isMatch);

    public void testTermQuery() throws IOException {
        Set<String> values = values();
        String term = values.iterator().next();
        List<Page> results = runQuery(values, new TermQuery(new Term(FIELD, term)), false);
        assertTermsQuery(results, Set.of(term), 1);
    }

    public void testTermQueryShuffled() throws IOException {
        Set<String> values = values();
        String term = values.iterator().next();
        List<Page> results = runQuery(values, new ConstantScoreQuery(new TermQuery(new Term(FIELD, term))), true);
        assertTermsQuery(results, Set.of(term), 1);
    }

    public void testTermsQuery() throws IOException {
        testTermsQuery(false);
    }

    public void testTermsQueryShuffled() throws IOException {
        testTermsQuery(true);
    }

    private void testTermsQuery(boolean shuffleDocs) throws IOException {
        Set<String> values = values();
        Iterator<String> itr = values.iterator();
        TreeSet<String> matching = new TreeSet<>();
        TreeSet<BytesRef> matchingBytes = new TreeSet<>();
        int expectedMatchCount = between(2, values.size());
        for (int i = 0; i < expectedMatchCount; i++) {
            String v = itr.next();
            matching.add(v);
            matchingBytes.add(new BytesRef(v));
        }
        List<Page> results = runQuery(values, new TermInSetQuery(MultiTermQuery.CONSTANT_SCORE_REWRITE, FIELD, matchingBytes), shuffleDocs);
        assertTermsQuery(results, matching, expectedMatchCount);
    }

    protected void assertTermsQuery(List<Page> results, Set<String> matching, int expectedMatchCount) {
        int matchCount = 0;
        for (Page page : results) {
            int initialBlockIndex = termsBlockIndex(page);
            BytesRefVector terms = page.<BytesRefBlock>getBlock(initialBlockIndex).asVector();
            @SuppressWarnings("unchecked")
            T resultVector = (T) page.getBlock(resultsBlockIndex(page)).asVector();
            for (int i = 0; i < page.getPositionCount(); i++) {
                BytesRef termAtPosition = terms.getBytesRef(i, new BytesRef());
                boolean isMatch = matching.contains(termAtPosition.utf8ToString());
                assertTermResultMatch(resultVector, i, isMatch);
                if (isMatch) {
                    matchCount++;
                }
            }
        }
        assertThat(matchCount, equalTo(expectedMatchCount));
    }

    /**
     * Checks that the result at the given position corresponds to a term match or no match
     */
    protected abstract void assertTermResultMatch(T resultVector, int position, boolean isMatch);

    private List<Page> runQuery(Set<String> values, Query query, boolean shuffleDocs) throws IOException {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        return withReader(values, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            LuceneQueryEvaluator.ShardConfig shard = new LuceneQueryEvaluator.ShardConfig(searcher.rewrite(query), searcher);
            List<Operator> operators = new ArrayList<>();
            if (shuffleDocs) {
                operators.add(new ShuffleDocsOperator(blockFactory));
            }
            operators.add(
                new ValuesSourceReaderOperator(
                    blockFactory,
                    List.of(
                        new ValuesSourceReaderOperator.FieldInfo(
                            FIELD,
                            ElementType.BYTES_REF,
                            unused -> new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader(FIELD)
                        )
                    ),
                    List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> {
                        throw new UnsupportedOperationException();
                    }, 0.2)),
                    0
                )
            );
            LuceneQueryEvaluator.ShardConfig[] shards = new LuceneQueryEvaluator.ShardConfig[] {
                new LuceneQueryEvaluator.ShardConfig(searcher.rewrite(query), searcher) };
            operators.add(createOperator(blockFactory, shards));
            List<Page> results = new ArrayList<>();
            Driver driver = new Driver(
                "test",
                driverContext,
                luceneOperatorFactory(reader, new MatchAllDocsQuery(), usesScoring()).get(driverContext),
                operators,
                new TestResultPageSinkOperator(results::add),
                () -> {}
            );
            OperatorTestCase.runDriver(driver);
            OperatorTests.assertDriverContext(driverContext);
            return results;
        });
    }

    private <T> T withReader(Set<String> values, CheckedFunction<DirectoryReader, T, IOException> run) throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            for (String value : values) {
                writer.addDocument(List.of(new KeywordField(FIELD, value, Field.Store.NO)));
            }
            writer.commit();
            try (DirectoryReader reader = writer.getReader()) {
                return run.apply(reader);
            }
        }
    }

    private Set<String> values() {
        int maxNumDocs = between(10, 1_000);
        int keyLength = randomIntBetween(1, 10);
        Set<String> values = new HashSet<>();
        for (int i = 0; i < maxNumDocs; i++) {
            values.add(randomAlphaOfLength(keyLength));
        }
        return values;
    }

    /**
     * A {@link DriverContext} with a non-breaking-BigArrays.
     */
    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    // Returns the initial block index, ignoring the score block if scoring is enabled
    protected int termsBlockIndex(Page page) {
        assert page.getBlock(0) instanceof DocBlock : "expected doc block at index 0";
        if (usesScoring()) {
            assert page.getBlock(1) instanceof DoubleBlock : "expected double block at index 1";
            return 2;
        } else {
            return 1;
        }
    }

    private static LuceneOperator.Factory luceneOperatorFactory(IndexReader reader, Query query, boolean scoring) {
        final ShardContext searchContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        return new LuceneSourceOperator.Factory(
            List.of(searchContext),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT,
            scoring
        );
    }

    // Returns the block index for the results to check
    protected abstract int resultsBlockIndex(Page page);

    /**
     * Returns a test scorer to use for scoring docs. Can be null
     */
    protected abstract Scorable getScorer();

    /**
     * Create the operator to test
     */
    protected abstract Operator createOperator(BlockFactory blockFactory, LuceneQueryEvaluator.ShardConfig[] shards);

    /**
     * Should the test use scoring?
     */
    protected abstract boolean usesScoring();
}
