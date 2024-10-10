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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.OperatorTests;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.DenseCollector;
import org.elasticsearch.compute.operator.ComputeTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.ShuffleDocsOperator;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.BlockDocValuesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.compute.operator.OperatorTestCase.randomPageSize;
import static org.hamcrest.Matchers.equalTo;

public class LuceneQueryExpressionEvaluatorTests extends ComputeTestCase {
    private static final String FIELD = "g";

    public void testDenseCollectorSmall() {
        try (DenseCollector collector = new DenseCollector(blockFactory(), 0, 2)) {
            collector.collect(0);
            collector.collect(1);
            collector.collect(2);
            collector.finish();
            try (BooleanVector result = collector.build()) {
                for (int i = 0; i <= 2; i++) {
                    assertThat(result.getBoolean(i), equalTo(true));
                }
            }
        }
    }

    public void testDenseCollectorSimple() {
        try (DenseCollector collector = new DenseCollector(blockFactory(), 0, 10)) {
            collector.collect(2);
            collector.collect(5);
            collector.finish();
            try (BooleanVector result = collector.build()) {
                for (int i = 0; i < 11; i++) {
                    assertThat(result.getBoolean(i), equalTo(i == 2 || i == 5));
                }
            }
        }
    }

    public void testDenseCollector() {
        int length = between(1, 10_000);
        int min = between(0, Integer.MAX_VALUE - length - 1);
        int max = min + length + 1;
        boolean[] expected = new boolean[length];
        try (DenseCollector collector = new DenseCollector(blockFactory(), min, max)) {
            for (int i = 0; i < length; i++) {
                expected[i] = randomBoolean();
                if (expected[i]) {
                    collector.collect(min + i);
                }
            }
            collector.finish();
            try (BooleanVector result = collector.build()) {
                for (int i = 0; i < length; i++) {
                    assertThat(result.getBoolean(i), equalTo(expected[i]));
                }
            }
        }
    }

    public void testTermQuery() throws IOException {
        Set<String> values = values();
        String term = values.iterator().next();
        List<Page> results = runQuery(values, new TermQuery(new Term(FIELD, term)), false);
        assertTermQuery(term, results);
    }

    public void testTermQueryShuffled() throws IOException {
        Set<String> values = values();
        String term = values.iterator().next();
        List<Page> results = runQuery(values, new TermQuery(new Term(FIELD, term)), true);
        assertTermQuery(term, results);
    }

    private void assertTermQuery(String term, List<Page> results) {
        int matchCount = 0;
        for (Page page : results) {
            BytesRefVector terms = page.<BytesRefBlock>getBlock(1).asVector();
            BooleanVector matches = page.<BooleanBlock>getBlock(2).asVector();
            for (int i = 0; i < page.getPositionCount(); i++) {
                BytesRef termAtPosition = terms.getBytesRef(i, new BytesRef());
                assertThat(matches.getBoolean(i), equalTo(termAtPosition.utf8ToString().equals(term)));
                if (matches.getBoolean(i)) {
                    matchCount++;
                }
            }
        }
        assertThat(matchCount, equalTo(1));
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
        int matchCount = 0;
        for (Page page : results) {
            BytesRefVector terms = page.<BytesRefBlock>getBlock(1).asVector();
            BooleanVector matches = page.<BooleanBlock>getBlock(2).asVector();
            for (int i = 0; i < page.getPositionCount(); i++) {
                BytesRef termAtPosition = terms.getBytesRef(i, new BytesRef());
                assertThat(matches.getBoolean(i), equalTo(matching.contains(termAtPosition.utf8ToString())));
                if (matches.getBoolean(i)) {
                    matchCount++;
                }
            }
        }
        assertThat(matchCount, equalTo(expectedMatchCount));
    }

    private List<Page> runQuery(Set<String> values, Query query, boolean shuffleDocs) throws IOException {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        return withReader(values, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            LuceneQueryExpressionEvaluator.ShardConfig shard = new LuceneQueryExpressionEvaluator.ShardConfig(
                searcher.rewrite(query),
                searcher
            );
            LuceneQueryExpressionEvaluator luceneQueryEvaluator = new LuceneQueryExpressionEvaluator(
                blockFactory,
                new LuceneQueryExpressionEvaluator.ShardConfig[] { shard },
                0
            );

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
                    })),
                    0
                )
            );
            operators.add(new EvalOperator(blockFactory, luceneQueryEvaluator));
            List<Page> results = new ArrayList<>();
            Driver driver = new Driver(
                driverContext,
                luceneOperatorFactory(reader, new MatchAllDocsQuery(), LuceneOperator.NO_LIMIT).get(driverContext),
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

    static LuceneOperator.Factory luceneOperatorFactory(IndexReader reader, Query query, int limit) {
        final ShardContext searchContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        return new LuceneSourceOperator.Factory(
            List.of(searchContext),
            ctx -> query,
            randomFrom(DataPartitioning.values()),
            randomIntBetween(1, 10),
            randomPageSize(),
            limit
        );
    }
}
