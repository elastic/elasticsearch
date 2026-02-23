/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.MinCompetitiveQuery;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.DummyBlockLoaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MinCompetitiveCombinedTests extends ComputeTestCase {
    private static final int NUM_DOCS = 10_000;
    private static final int TOP_COUNT = 10;
    private final Directory directory = newDirectory();
    private IndexReader reader;
    private SearchExecutionContext executionContext;
    private SharedMinCompetitive.Supplier minCompetitiveSupplier;
    private EsMinCompetitiveQueries minCompetitiveQueries;
    private MinCompetitiveQuery.Factory minCompetitiveQuery;
    private DriverContext ctx;

    public void testOneSource() {
        try (
            TopNOperator topN = topNOperator();
            LuceneSourceOperator sourceOperator = sourceOp(SortedNumericDocValuesField.newSlowRangeQuery("f", Long.MIN_VALUE, 99))
        ) {
            drainToTopN(sourceOperator, topN);
            List<Long> expected = LongStream.range(90, 100).boxed().collect(Collectors.toList());
            assertThat(topNResults(topN), equalTo(expected.reversed()));
            assertThat(sourceOperator.status().documentsFound(), equalTo(100L));
        }
    }

    public void testTwoSources() {
        try (
            TopNOperator topN = topNOperator();
            LuceneSourceOperator source1 = sourceOp(SortedNumericDocValuesField.newSlowRangeQuery("f", 60, 99));
            LuceneSourceOperator source2 = sourceOp(SortedNumericDocValuesField.newSlowRangeQuery("f", Long.MIN_VALUE, 30))
        ) {
            drainToTopN(source1, topN);
            drainToTopN(source2, topN);
            List<Long> expected = LongStream.range(90, 100).boxed().collect(Collectors.toList());
            assertThat(topNResults(topN), equalTo(expected.reversed()));
            assertThat(source1.status().documentsFound(), equalTo(40L));
            assertThat(source2.status().documentsFound(), equalTo(0L));
        }
    }

    public void testTwoSourcesInterweaved() {
        try (
            TopNOperator topN = topNOperator();
            LuceneSourceOperator source1 = sourceOp(SortedNumericDocValuesField.newSlowRangeQuery("f", 60, 99));
            LuceneSourceOperator source2 = sourceOp(SortedNumericDocValuesField.newSlowRangeQuery("f", Long.MIN_VALUE, 30))
        ) {
            drainOnePageToTopN(source1, topN);
            drainOnePageToTopN(source2, topN);
            drainToTopN(source1, topN);
            drainToTopN(source2, topN);
            List<Long> expected = LongStream.range(90, 100).boxed().collect(Collectors.toList());
            assertThat(topNResults(topN), equalTo(expected.reversed()));
            assertThat(source1.status().documentsFound(), equalTo(40L));
            assertThat(source2.status().documentsFound(), greaterThanOrEqualTo(0L));
        }
    }

    private LuceneSourceOperator sourceOp(Query query) {
        int maxPageSize = between(1, NUM_DOCS);
        LuceneSliceQueue queue = LuceneSliceQueue.create(
            defaultShardContext(),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.SHARD,
            null,
            1,
            i -> ScoreMode.COMPLETE_NO_SCORES
        );
        return new LuceneSourceOperator(
            defaultShardContext(),
            ctx.blockFactory(),
            maxPageSize,
            queue,
            Integer.MAX_VALUE,
            Limiter.NO_LIMIT,
            false,
            minCompetitiveQuery
        );
    }

    private void drainToTopN(LuceneSourceOperator source, TopNOperator topN) {
        while (source.isFinished() == false) {
            drainOnePageToTopN(source, topN);
        }
    }

    private void drainOnePageToTopN(LuceneSourceOperator source, TopNOperator topN) {
        Page out = source.getOutput();
        if (out == null) {
            return;
        }
        if (out.getPositionCount() == 0) {
            out.close();
            return;
        }
        loadI(List.of(out)).forEach(topN::addInput);
    }

    private List<Page> loadI(List<Page> input) {
        List<Page> result = new ArrayList<>();
        MappedFieldType.BlockLoaderContext blContext = new DummyBlockLoaderContext("test");
        List<ValuesSourceReaderOperator.FieldInfo> fields = List.of(
            new ValuesSourceReaderOperator.FieldInfo(
                "f",
                ElementType.LONG,
                false,
                i -> ValuesSourceReaderOperator.load(executionContext.getFieldType("f").blockLoader(blContext))
            )
        );
        ValuesSourceReaderOperator.ShardContext shardContext = new ValuesSourceReaderOperator.ShardContext(reader, null, 0);
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts = new IndexedByShardIdFromList<>(List.of(shardContext));
        try (ValuesSourceReaderOperator reader = new ValuesSourceReaderOperator(ctx, Long.MAX_VALUE, fields, shardContexts, false, 0)) {
            for (Page in : input) {
                reader.addInput(in);
                Page out;
                while ((out = reader.getOutput()) != null) {
                    try {
                        Block loaded = out.getBlock(1);
                        loaded.incRef();
                        result.add(new Page(loaded));
                    } finally {
                        out.close();
                    }
                }
            }
        }
        return result;
    }

    @Before
    public void setup() throws IOException {
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < NUM_DOCS; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("f", d));
                doc.add(new LongPoint("f", d));
                writer.addDocument(doc);
            }
            reader = writer.getReader();
        }
        minCompetitiveSupplier = new SharedMinCompetitive.Supplier(
            blockFactory().breaker(),
            List.of(new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, false, false))
        );
        EsQueryExec.MinCompetitiveSetup minCompetitiveSetup = new EsQueryExec.MinCompetitiveSetup(minCompetitiveSupplier, "f");
        executionContext = EsMinCompetitiveQueriesTests.ctx(new IndexSearcher(reader));
        minCompetitiveQueries = new EsMinCompetitiveQueries(minCompetitiveSetup, executionContext);
        minCompetitiveQuery = new MinCompetitiveQuery.Factory(
            minCompetitiveSupplier,
            (ctx, page) -> minCompetitiveQueries.buildMinCompetitiveQuery(page)
        );
        BlockFactory blockFactory = blockFactory();
        ctx = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private IndexedByShardId<ShardContext> defaultShardContext() {
        ShardContext shardContext = new EsPhysicalOperationProviders.DefaultShardContext(0, () -> {}, executionContext, AliasFilter.EMPTY);
        return new IndexedByShardIdFromList<>(List.of(shardContext));
    }

    private TopNOperator topNOperator() {
        return new TopNOperator(
            ctx.blockFactory(),
            ctx.blockFactory().breaker(),
            TOP_COUNT,
            List.of(ElementType.LONG),
            List.of(TopNEncoder.DEFAULT_SORTABLE),
            List.of(new TopNOperator.SortOrder(0, false, false)),
            Integer.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            minCompetitiveSupplier
        );
    }

    private List<Long> topNResults(TopNOperator topN) {
        List<Long> result = new ArrayList<>();
        topN.finish();
        while (topN.isFinished() == false) {
            try (Page out = topN.getOutput()) {
                LongBlock iBlock = out.getBlock(0);
                LongVector iVector = iBlock.asVector();
                for (int p = 0; p < out.getPositionCount(); p++) {
                    result.add(iVector.getLong(p));
                }
            }
        }
        return result;
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }
}
