/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

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
            new Object[] { Queries.ALL_DOCS_INSTANCE, LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), LuceneSliceQueue.PartitioningStrategy.DOC },

            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00Z\" | ....
             * when all @timestamps are in range
             */
            new Object[] {
                new BoostQuery(new ConstantScoreQuery(LongPoint.newRangeQuery("@timestamp", 1735689600121L, Long.MAX_VALUE)), 0.0F),
                LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { Queries.NO_DOCS_INSTANCE, LuceneSliceQueue.PartitioningStrategy.SHARD },

            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00Z\" | STATS SUM(b)
             * when all @timestamps are in range and all docs have b.
             */
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(
                        new BoostQuery(
                            new IndexOrDocValuesQuery(
                                LongPoint.newRangeQuery("@timestamp", 1735689600121L, Long.MAX_VALUE),
                                SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 1735689600121L, Long.MAX_VALUE)
                            ),
                            0.0F
                        ),
                        BooleanClause.Occur.MUST
                    )
                    .add(new BoostQuery(new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), 0.0F), BooleanClause.Occur.MUST)
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
                                LongPoint.newRangeQuery("@timestamp", 1735689600121L, 2735689600122L),
                                SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 1735689600121L, 2735689600122L)
                            )
                        ),
                        0.0F
                    ),
                    BooleanClause.Occur.MUST
                ).add(new BoostQuery(new ConstantScoreQuery(Queries.ALL_DOCS_INSTANCE), 0.0F), BooleanClause.Occur.MUST).build(),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            /*
             * FROM test | WHERE @timestamp > \"2025-01-01T00:00:00.120Z\" | STATS SUM(b)
             */
            new Object[] {
                new ConstantScoreQuery(
                    new IndexOrDocValuesQuery(
                        LongPoint.newRangeQuery("@timestamp", 1735689600121L, 2735689600122L),
                        SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 1735689600121L, 2735689600122L)
                    )
                ),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.MUST)
                    .add(new TermQuery(new Term("color", "yellow")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(new TermQuery(new Term("color", "red")), BooleanClause.Occur.SHOULD)
                    .add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.MUST_NOT)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.SHARD },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("color", "blue")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] {
                new BooleanQuery.Builder() // formatter
                    .add(Queries.NO_DOCS_INSTANCE, BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("color", "red")), BooleanClause.Occur.SHOULD)
                    .build(),
                LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { new TermQuery(new Term("color", "blue")), LuceneSliceQueue.PartitioningStrategy.DOC },
            new Object[] { new WildcardQuery(new Term("color", "b*")), LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] { new WildcardQuery(new Term("color", "bl*")), LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] { new WildcardQuery(new Term("color", "blu*")), LuceneSliceQueue.PartitioningStrategy.SEGMENT },
            new Object[] {
                new TermInSetQuery("color", List.of(new BytesRef("red"), new BytesRef("blue"))),
                LuceneSliceQueue.PartitioningStrategy.SEGMENT }
        );
    }

    private final Query query;
    private final LuceneSliceQueue.PartitioningStrategy expectedUnlimited;
    private IndexReader indexReader;
    private Directory directory;

    public LuceneSourceOperatorAutoStrategyTests(Query query, LuceneSliceQueue.PartitioningStrategy expectedUnlimited) {
        this.query = query;
        this.expectedUnlimited = expectedUnlimited;
    }

    @Before
    public void setUpLuceneIndex() throws Exception {
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);
        List<Long> points = List.of(1735689600121L, 1735689600122L, 1735689600121L, 2735689600122L, 3735689600121L, 3735689600122L);
        List<String> colors = List.of("red", "blue", "red", "green", "yellow", "black");
        for (int i = 0; i < points.size(); i++) {
            Document doc = new Document();
            long pt = points.get(i);
            doc.add(new LongPoint("@timestamp", pt));
            doc.add(new NumericDocValuesField("@timestamp", pt));
            String color = colors.get(i);
            doc.add(new KeywordField("color", color, Field.Store.NO));
            doc.add(new SortedSetDocValuesField("color", new BytesRef(color)));
            writer.addDocument(doc);
        }
        indexReader = DirectoryReader.open(writer);
        IOUtils.close(writer);
    }

    @After
    public void deleteLuceneIndex() throws Exception {
        IOUtils.close(indexReader, directory);
    }

    public void testAutoStrategyLimited() throws Exception {
        Function<Query, LuceneSliceQueue.PartitioningStrategy> auto = LuceneSourceOperator.Factory.autoStrategy(
            between(1, LuceneOperator.NO_LIMIT - 1)
        );
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Query rewritten = searcher.rewrite(query);
        assertThat(query.toString(), auto.apply(rewritten), equalTo(LuceneSliceQueue.PartitioningStrategy.SHARD));
    }

    public void testAutoStrategyUnlimited() throws Exception {
        Function<Query, LuceneSliceQueue.PartitioningStrategy> auto = LuceneSourceOperator.Factory.autoStrategy(LuceneOperator.NO_LIMIT);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Query rewritten = searcher.rewrite(query);
        assertThat(query.toString(), auto.apply(rewritten), equalTo(expectedUnlimited));
    }
}
