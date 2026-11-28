/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests.MockShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@link LuceneSourceOperator} reading the docs a {@link DocFilter} tree passes through {@link DocFilterQuery}. Every
 * partitioning must emit exactly the docs a brute-force filter accepts, in doc order within a segment, and a limit must
 * cut the stream at that many docs.
 */
public class DocFilterQueryTests extends ComputeTestCase {
    private BlockFactory factory;
    private DocFilterFixture index;
    private final Map<String, Weight> weights = new HashMap<>();
    private MockShardContext shard;

    @Before
    public void buildIndex() throws IOException {
        factory = blockFactory();
        index = new DocFilterFixture();
        shard = new MockShardContext(index.reader, 0);
    }

    @After
    public void closeIndex() throws IOException {
        index.close();
    }

    public void testTheOperatorEmitsTheDocsTheTreePasses() throws IOException {
        IntPredicate keep = doc -> index.category[doc].equals("a") && index.expensive(doc);
        DataPartitioning partitioning = randomFrom(DataPartitioning.values());
        List<Integer> docs = run(partitioning, LuceneOperator.NO_LIMIT, between(16, 1024), expensiveInCategoryA());
        assertThat(new TreeSet<>(docs), equalTo(index.expected(keep)));
        assertThat(docs, hasSize(index.expected(keep).size()));
    }

    public void testAnOrTreeEmitsTheUnionOnceEach() throws IOException {
        IntPredicate keep = doc -> index.category[doc].equals("a") || index.expensive(doc);
        List<Integer> docs = run(DataPartitioning.SEGMENT, LuceneOperator.NO_LIMIT, between(16, 1024), () -> {
            return new OrDocFilter(category("a"), index.expensive(new DocFilterFixture.Columns(factory), factory));
        });
        assertThat(new TreeSet<>(docs), equalTo(index.expected(keep)));
        assertThat(docs, hasSize(index.expected(keep).size()));
    }

    public void testALimitCutsTheStreamAtThatManyDocs() throws IOException {
        IntPredicate keep = doc -> index.category[doc].equals("a") && index.expensive(doc);
        TreeSet<Integer> all = index.expected(keep);
        int limit = between(1, all.size() - 1);
        List<Integer> docs = run(DataPartitioning.SHARD, limit, between(16, 128), expensiveInCategoryA());
        assertThat(docs, hasSize(limit));
        assertThat(all.containsAll(docs), equalTo(true));
        assertThat(docs, equalTo(new ArrayList<>(all).subList(0, limit)));
    }

    public void testATreeThatKeepsNothingEmitsNothing() throws IOException {
        List<Integer> docs = run(randomFrom(DataPartitioning.values()), LuceneOperator.NO_LIMIT, between(16, 256), () -> category("none"));
        assertThat(docs, hasSize(0));
    }

    private Supplier<DocFilter> expensiveInCategoryA() {
        return () -> new AndDocFilter(category("a"), index.expensive(new DocFilterFixture.Columns(factory), factory));
    }

    private DocFilter category(String value) {
        return NativeDocFilter.of(weights.computeIfAbsent(value, v -> {
            try {
                return NativeDocFilter.weight(shard.searcher(), new TermQuery(new Term("category", v)));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }));
    }

    /** The global doc ids the operator emits, in emission order. */
    private List<Integer> run(DataPartitioning partitioning, int limit, int maxPageSize, Supplier<DocFilter> tree) throws IOException {
        DocFilterQuery query = new DocFilterQuery(tree, randomFrom(37, 64, 128, 4096), "test");
        LuceneSourceOperator.Factory operator = new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(shard),
            ignored -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            partitioning,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            1,
            maxPageSize,
            limit,
            false,
            () -> 0L,
            randomFrom(1, LuceneSliceQueue.MIN_DOCS_PER_SLICE),
            QueryWarnings.EMIT
        );
        DriverContext driverContext = new DriverContext(nonBreakingBigArrays(), factory, null);
        List<Page> pages = new ArrayList<>();
        new TestDriverRunner().run(
            TestDriverFactory.create(driverContext, operator.get(driverContext), List.of(), new TestResultPageSinkOperator(pages::add))
        );
        OperatorTestCase.assertDriverContext(driverContext);
        List<LeafReaderContext> leaves = index.reader.leaves();
        List<Integer> docs = new ArrayList<>();
        for (Page page : pages) {
            DocVector vector = ((DocBlock) page.getBlock(0)).asVector();
            for (int position = 0; position < page.getPositionCount(); position++) {
                docs.add(leaves.get(vector.segments().getInt(position)).docBase + vector.docs().getInt(position));
            }
            page.releaseBlocks();
        }
        return docs;
    }
}
