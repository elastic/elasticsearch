/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link QueryWarnings} bridge wired through real {@link LuceneOperator}s. A
 * {@link SingleValueMatchQuery} node is shared (by identity) across every {@link LuceneOperator} that
 * scans its shard, because {@link LuceneSliceQueue#create} builds one Lucene {@code Weight} per
 * shard/query and every {@code DOC}-partitioned slice of that shard reuses it (see
 * {@code LuceneSliceQueue.create}). Two drivers doing exactly that must end up resolving that one
 * shared node to two distinct, independently-capped {@link Warnings} instances -- one per driver -- and
 * the bridge's thread-local binding must never leak across drivers even if a driver throws.
 * <p>
 *     The {@link IndexFieldData} backing the {@link SingleValueMatchQuery} is a stub here: real field
 *     values are irrelevant to what's under test. {@code LuceneOperator} populates its warnings map from
 *     {@link Query#visit} the moment it loads a shard's {@code Weight} -- before any doc is actually
 *     scored (see {@code LuceneOperator#populateSingleValueQueryWarnings}) -- so we only need the query
 *     to survive {@code IndexSearcher#rewrite} unchanged (see {@link #stubFieldData}), not to match
 *     anything.
 * </p>
 */
public class LuceneOperatorSingleValueQueryWarningsTests extends ComputeTestCase {

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory(), null);
    }

    /**
     * A stub {@link IndexFieldData} that reports itself as multi-valued, so
     * {@link SingleValueMatchQuery#rewrite} keeps the query as-is (a single-valued-everywhere field
     * would rewrite away to a plain "match all", which would erase the very node we want
     * {@code LuceneOperator} to find) without ever needing a real mapped field or real documents.
     */
    private static IndexFieldData<?> stubFieldData(String fieldName) {
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        when(fieldData.load(any())).thenReturn(leafFieldData);
        SortedBinaryDocValues sortedBinaryDocValues = mock(SortedBinaryDocValues.class);
        when(leafFieldData.getBytesValues()).thenReturn(sortedBinaryDocValues);
        // Report as multi-valued only for rewrite() purposes (see javadoc above) -- that check only
        // looks at getValueMode()/getSparsity(), not at any real per-doc values.
        when(sortedBinaryDocValues.getValueMode()).thenReturn(SortedBinaryDocValues.ValueMode.MULTI_VALUED);
        // When actually scoring, alternate between single-valued (1) and multi-valued (2) docs.
        // Single-valued docs match the query and fill pages normally -- so operators yield at page
        // boundaries and both get a fair share of the slice queue. Multi-valued docs trigger
        // registerException(), which is how Warnings instances are lazily created in each operator's
        // map (Warnings creation is no longer pre-populated; it happens on first registerException).
        try {
            when(sortedBinaryDocValues.advanceExact(anyInt())).thenReturn(true);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        int[] count = { 0 };
        when(sortedBinaryDocValues.docValueCount()).thenAnswer(inv -> ++count[0] % 2 == 0 ? 2 : 1);
        return fieldData;
    }

    private Query pushdownQuery(QueryWarnings bridge, String fieldName) {
        SingleValueMatchQuery singleValueMatchQuery = new SingleValueMatchQuery(
            stubFieldData(fieldName),
            bridge,
            new TestWarningsSource("test"),
            "single-value function encountered multi-value"
        );
        // AND-composed with the "real" pushdown query, mirroring SingleValueQuery.AbstractBuilder#simple.
        return new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.FILTER)
            .add(singleValueMatchQuery, BooleanClause.Occur.FILTER)
            .build();
    }

    private static SingleValueMatchQuery findSingleValueMatchQuery(Query query) {
        AtomicReference<SingleValueMatchQuery> found = new AtomicReference<>();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query leaf) {
                if (leaf instanceof SingleValueMatchQuery svmq) {
                    found.set(svmq);
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return found.get();
    }

    private LuceneSourceOperator.Factory docPartitionedFactory(
        LuceneSourceOperatorTests.MockShardContext shardContext,
        Query query,
        QueryWarnings warnings
    ) {
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromList<>(List.of(shardContext)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.DOC,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            2,
            10,
            LuceneOperator.NO_LIMIT,
            false,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            warnings
        );
    }

    private LuceneSourceOperator.Factory shardPartitionedFactory(
        List<LuceneSourceOperatorTests.MockShardContext> shardContexts,
        Query query,
        QueryWarnings warnings
    ) {
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromList<>(shardContexts),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            2,
            10,
            LuceneOperator.NO_LIMIT,
            false,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            warnings
        );
    }

    /**
     * Two drivers, both {@code DOC}-partitioned slices of the very same shard/query, so they share the
     * exact same {@link SingleValueMatchQuery} instance (and Lucene {@code Weight}) -- see
     * {@code LuceneSliceQueue.create}. Each must end up with its own {@link Warnings} for that node.
     */
    public void testTwoDriversOnOneShardGetIndependentWarnings() throws IOException {
        QueryWarnings warnings = QueryWarnings.EMIT;
        Query query = pushdownQuery(warnings, "mv");
        SingleValueMatchQuery matchQueryNode = findSingleValueMatchQuery(query);

        try (Directory dir = newDirectory()) {
            // Needs at least two DOC slices on a single segment; MIN_DOCS_PER_SLICE * 5 comfortably
            // splits into (at least) two groups at taskConcurrency=2 (mirrors DocPartitioningQueryCacheTests).
            final int numDocs = LuceneSliceQueue.MIN_DOCS_PER_SLICE * 5;
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int d = 0; d < numDocs; d++) {
                    writer.addDocument(new Document());
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                LuceneSourceOperatorTests.MockShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
                LuceneSourceOperator.Factory factory = docPartitionedFactory(shardContext, query, warnings);
                assertThat("test setup needs at least 2 slices to share the shard's weight", factory.taskConcurrency(), not(1));

                LuceneSourceOperator op1 = (LuceneSourceOperator) factory.get(driverContext());
                LuceneSourceOperator op2 = (LuceneSourceOperator) factory.get(driverContext());
                try {
                    // Interleave single getOutput() calls, exactly like two drivers pulling work from a
                    // shared queue would -- draining op1 to completion before touching op2 would let it
                    // race through every slice on its own, leaving nothing for op2.
                    while (op1.isFinished() == false || op2.isFinished() == false) {
                        step(op1);
                        step(op2);
                    }

                    Map<Query, Warnings> map1 = op1.singleValueQueryWarnings();
                    Map<Query, Warnings> map2 = op2.singleValueQueryWarnings();
                    assertThat(map1.get(matchQueryNode), notNullValue());
                    assertThat(map2.get(matchQueryNode), notNullValue());
                    // Same query node (by identity, and by shared Weight), but two different drivers
                    // must never share a Warnings instance -- that's the whole point of the bridge.
                    assertThat(map1.get(matchQueryNode), not(sameInstance(map2.get(matchQueryNode))));
                    // Consume the warnings emitted by the multi-valued docs (every other doc in the stub
                    // reports two values, triggering a registerException() call that creates Warnings
                    // lazily -- which is exactly what this test exercises).
                    assertWarnings(
                        "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
                    );
                } finally {
                    op1.close();
                    op2.close();
                }
            }
        }
    }

    private static void step(LuceneSourceOperator op) {
        if (op.isFinished()) {
            return;
        }
        Page page = op.getOutput();
        if (page != null) {
            page.releaseBlocks();
        }
    }

    /**
     * If a driver's {@link LuceneOperator#getOutput} throws, the bridge's thread-local binding must
     * still be cleared -- otherwise the next driver to use this shared bridge would hit the reentrancy
     * guard in {@link QueryWarnings#bind} and fail for a completely unrelated reason.
     */
    public void testThreadLocalClearedEvenWhenOperatorThrows() throws IOException {
        QueryWarnings warnings = QueryWarnings.EMIT;
        Query query = pushdownQuery(warnings, "mv");

        Directory dir0 = newDirectory();
        Directory dir1 = newDirectory();
        try (
            IndexWriter w0 = new IndexWriter(dir0, new IndexWriterConfig());
            IndexWriter w1 = new IndexWriter(dir1, new IndexWriterConfig())
        ) {
            w0.addDocument(new Document());
            w1.addDocument(new Document());
        }
        IndexReader reader0 = DirectoryReader.open(dir0);
        IndexReader reader1 = DirectoryReader.open(dir1);
        try {
            List<LuceneSourceOperatorTests.MockShardContext> shardContexts = List.of(
                new LuceneSourceOperatorTests.MockShardContext(reader0, 0),
                new LuceneSourceOperatorTests.MockShardContext(reader1, 1)
            );
            LuceneSourceOperator.Factory factory = shardPartitionedFactory(shardContexts, query, warnings);

            LuceneSourceOperator op1 = (LuceneSourceOperator) factory.get(driverContext());
            LuceneSourceOperator op2 = (LuceneSourceOperator) factory.get(driverContext());
            try {
                // Force op1's first getOutput() call to blow up by closing its shard's reader out from
                // under it before any scan happens.
                reader0.close();
                expectThrows(RuntimeException.class, op1::getOutput);

                // If the bridge's thread-local hadn't been cleared in LuceneOperator#getOutput's
                // finally block, this call -- which binds op2's own map on the very same bridge -- would
                // fail with an unrelated IllegalStateException ("already bound") rather than running (or
                // failing on its own terms, e.g. finding no more slices).
                Page p2 = op2.getOutput();
                if (p2 != null) {
                    p2.releaseBlocks();
                }
            } finally {
                op1.close();
                op2.close();
            }
        } finally {
            closeQuietly(reader0);
            reader1.close();
            dir0.close();
            dir1.close();
        }
    }

    private static void closeQuietly(IndexReader reader) {
        try {
            reader.close();
        } catch (Exception e) {
            // already closed by the test itself; nothing more to do here
        }
    }
}
