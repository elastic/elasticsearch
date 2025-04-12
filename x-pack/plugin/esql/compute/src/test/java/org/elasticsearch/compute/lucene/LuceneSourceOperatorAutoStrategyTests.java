/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link LuceneSourceOperator.Factory#autoStrategy} method. It picks
 * the strategy based on complex rules around the query.
 */
public class LuceneSourceOperatorAutoStrategyTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "%s -> %s")
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { new MatchAllDocsQuery(), LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { new ConstantScoreQuery(new MatchAllDocsQuery()), LuceneSliceQueue.PartitioningStrategy.DOC },

            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00Z\" | ....
             * when all @timestamps are in range
             */
            new Object[] {
                new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0.0F),
                LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { new MatchNoDocsQuery(), LuceneSliceQueue.PartitioningStrategy.SHARD },

            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00Z\" | STATS SUM(b)
             * when all @timestamps are in range and all docs have b.
             */
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0.0F), BooleanClause.Occur.MUST)
                    .add(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0.0F), BooleanClause.Occur.MUST)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.DOC },

            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00.120Z\" | STATS SUM(b)
             * when *some* @timestamps are in range and all docs have b
             */
            new Object[] {
                new BooleanQuery.Builder().add(
                    new BoostQuery(
                        new ConstantScoreQuery(
                            new IndexOrDocValuesQuery(
                                LongPoint.newRangeQuery("@timestamp", 1735689600121L, 9223372036854775807L),
                                SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 1735689600121L, 9223372036854775807L)
                            )
                        ),
                        0.0F
                    ),
                    BooleanClause.Occur.MUST
                ).add(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0.0F), BooleanClause.Occur.MUST).build(),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT },

            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
                    .add(new TermQuery(new Term("a", "a")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new TermQuery(new Term("a", "a")), BooleanClause.Occur.SHOULD)
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.SHARD },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("a", "a")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new MatchNoDocsQuery(), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("a", "a")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT }
        );
    }

    private final Query query;
    private final LuceneSliceQueue.PartitioningStrategy expectedUnlimited;

    public LuceneSourceOperatorAutoStrategyTests(Query query, LuceneSliceQueue.PartitioningStrategy expectedUnlimited) {
        this.query = query;
        this.expectedUnlimited = expectedUnlimited;
    }

    public void testAutoStrategyLimited() {
        Function<Query, LuceneSliceQueue.PartitioningStrategy> auto = LuceneSourceOperator.Factory.autoStrategy(
            between(1, LuceneOperator.NO_LIMIT - 1)
        );
        assertThat(auto.apply(query), equalTo(LuceneSliceQueue.PartitioningStrategy.SHARD));
    }

    public void testAutoStrategyUnlimited() {
        Function<Query, LuceneSliceQueue.PartitioningStrategy> auto = LuceneSourceOperator.Factory.autoStrategy(LuceneOperator.NO_LIMIT);
        assertThat(auto.apply(query), equalTo(expectedUnlimited));
    }
}
