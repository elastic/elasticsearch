/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link QueryWarnings} bridge wired through real {@link LuceneOperator}s. A
 * {@link SingleValueMatchQuery} node is shared (by identity) across every {@link LuceneOperator} that
 * scans its shard, because {@link LuceneSliceQueue#create} builds one Lucene {@code Weight} per
 * shard/query and every {@code DOC}-partitioned slice of that shard reuses it. Two drivers doing
 * exactly that must emit warnings independently into their own per-driver sink — the bridge's
 * thread-local binding must never leak across drivers even if a driver throws.
 */
public class LuceneOperatorSingleValueQueryWarningsTests extends ComputeTestCase {

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory(), null);
    }

    /**
     * A stub {@link IndexFieldData} that reports itself as multi-valued, so
     * {@link SingleValueMatchQuery#rewrite} keeps the query as-is (a single-valued-everywhere field
     * would rewrite away to a plain "match all", erasing the node we want {@code LuceneOperator} to
     * find) without ever needing a real mapped field or real documents.
     */
    private static IndexFieldData<?> stubFieldData(String fieldName) {
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        when(fieldData.load(any())).thenReturn(leafFieldData);
        SortedBinaryDocValues sortedBinaryDocValues = mock(SortedBinaryDocValues.class);
        when(leafFieldData.getBytesValues()).thenReturn(sortedBinaryDocValues);
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
            new WarningSourceLocation(1, 1, "test"),
            "single-value function encountered multi-value"
        );
        // AND-composed with the "real" pushdown query, mirroring SingleValueQuery.AbstractBuilder#simple.
        return new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
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
            List.of(shardContext),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.DOC,
            2,
            10,
            LuceneOperator.NO_LIMIT,
            false,
            warnings
        );
    }

    private LuceneSourceOperator.Factory shardPartitionedFactory(
        List<LuceneSourceOperatorTests.MockShardContext> shardContexts,
        Query query,
        QueryWarnings warnings
    ) {
        return new LuceneSourceOperator.Factory(
            shardContexts,
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            DataPartitioning.SHARD,
            2,
            10,
            LuceneOperator.NO_LIMIT,
            false,
            warnings
        );
    }

    /**
     * Two drivers, both {@code DOC}-partitioned slices of the very same shard/query, so they share the
     * exact same {@link SingleValueMatchQuery} instance (and Lucene {@code Weight}). Each must emit
     * warnings independently into its own per-driver sink.
     */
    public void testTwoDriversOnOneShardGetIndependentWarnings() throws IOException {
        QueryWarnings warnings = QueryWarnings.EMIT;
        Query query = pushdownQuery(warnings, "mv");
        assertThat("findSingleValueMatchQuery must locate the node", findSingleValueMatchQuery(query), not((SingleValueMatchQuery) null));

        try (Directory dir = newDirectory()) {
            // Needs at least two DOC slices on a single segment; 50_000 docs comfortably
            // splits into at least two groups at taskConcurrency=2.
            final int numDocs = 50_000;
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int d = 0; d < numDocs; d++) {
                    writer.addDocument(new Document());
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                LuceneSourceOperatorTests.MockShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
                LuceneSourceOperator.Factory factory = docPartitionedFactory(shardContext, query, warnings);
                assertThat("test setup needs at least 2 slices to share the shard's weight", factory.taskConcurrency(), not(1));

                DriverContext dc1 = driverContext();
                DriverContext dc2 = driverContext();
                LuceneSourceOperator op1 = (LuceneSourceOperator) factory.get(dc1);
                LuceneSourceOperator op2 = (LuceneSourceOperator) factory.get(dc2);
                try {
                    // Interleave single getOutput() calls, exactly like two drivers pulling work from a
                    // shared queue would — draining op1 to completion before touching op2 would let it
                    // race through every slice on its own, leaving nothing for op2.
                    while (op1.isFinished() == false || op2.isFinished() == false) {
                        step(op1);
                        step(op2);
                    }

                    // Consume warnings emitted by the multi-valued docs (every other doc in the stub
                    // reports two values, triggering registerException() → Warnings creation).
                    dc1.finish();
                    dc2.finish();
                    List<String> collected = new ArrayList<>(dc1.warnings());
                    collected.addAll(dc2.warnings());
                    assertThat(
                        collected,
                        hasItems(
                            "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
                            "Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
                        )
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
     * still be cleared — otherwise the next driver to use this shared bridge would hit the reentrancy
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
                // finally block, this call — which binds op2's own map on the very same bridge — would
                // fail with an unrelated IllegalStateException ("already bound") rather than running.
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
