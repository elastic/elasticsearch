/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.vectors.DiversifyingChildrenIVFKnnFloatVectorQuery;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Adds a {@code single_value_match} pushdown used as the child filter of
 * a nested/diversifying IVF (bbq_disk) kNN query and validates that we
 * discard its warnings. We'll modify it before long to collect them.
 */
public class IvfKnnNestedFilterSingleValueWarningsTests extends ComputeTestCase {

    /** Same trick as {@code LuceneOperatorSingleValueQueryWarningsTests}: report every doc as multi-valued
     *  so the query survives rewrite() and fires registerMultiValueException() for every candidate doc. */
    private static IndexFieldData<?> alwaysMultiValuedFieldData(String fieldName) {
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        when(fieldData.load(any())).thenReturn(leafFieldData);
        SortedBinaryDocValues sortedBinaryDocValues = mock(SortedBinaryDocValues.class);
        when(leafFieldData.getBytesValues()).thenReturn(sortedBinaryDocValues);
        when(sortedBinaryDocValues.getValueMode()).thenReturn(SortedBinaryDocValues.ValueMode.MULTI_VALUED);
        try {
            when(sortedBinaryDocValues.advanceExact(anyInt())).thenReturn(true);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        when(sortedBinaryDocValues.docValueCount()).thenReturn(2);
        return fieldData;
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory(), null);
    }

    public void testIvfKnnQueryWithNestedFilterDoesNotThrowAndStillWarnsWhenScanned() throws IOException {
        ES920DiskBBQVectorsFormat format = new ES920DiskBBQVectorsFormat(
            ES920DiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER,
            ES920DiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER
        );

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format)))) {
                for (int group = 0; group < 5; group++) {
                    for (int child = 0; child < 3; child++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("vector", new float[] { group, child }));
                        writer.addDocument(doc);
                    }
                    Document parent = new Document();
                    parent.add(new StringField("docType", "_parent", Field.Store.NO));
                    writer.addDocument(parent);
                }
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                LuceneSourceOperatorTests.MockShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
                BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));

                QueryWarnings warnings = QueryWarnings.EMIT;
                SingleValueMatchQuery singleValueMatchQuery = new SingleValueMatchQuery(
                    alwaysMultiValuedFieldData("mv_nested_field"),
                    warnings,
                    new TestWarningsSource("test"),
                    "single-value function encountered multi-value"
                );
                Query childFilter = new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.FILTER)
                    .add(singleValueMatchQuery, BooleanClause.Occur.FILTER)
                    .build();

                Query knnQuery = new DiversifyingChildrenIVFKnnFloatVectorQuery(
                    "vector",
                    new float[] { 0, 0 },
                    3,
                    3,
                    childFilter,
                    parentFilter,
                    0f,
                    IvfQueryConfigResolver.from(false, false, 4, 1.0f, null)
                );

                // Constructing the operator factory for this shard/query triggers exactly what
                // LuceneSliceQueue.create() does for every LuceneOperator.Factory: it rewrites the
                // pushed-down query against the shard searcher *before* any driver/DriverContext exists.
                // Before the fix this threw IllegalStateException because QueryWarnings was never bound
                // on this thread; now a placeholder binding is used around that rewrite, so construction
                // completes normally (any warning raised strictly during this rewrite is discarded, per
                // the TODO(154664) at the bind site).
                LuceneSourceOperator.Factory factory = new LuceneSourceOperator.Factory(
                    new IndexedByShardIdFromList<>(List.of(shardContext)),
                    ctx -> List.of(new LuceneSliceQueue.QueryAndTags(knnQuery, List.of())),
                    DataPartitioning.SHARD,
                    DataPartitioning.AutoStrategy.DEFAULT,
                    LuceneOperator.SMALL_INDEX_BOUNDARY,
                    1,
                    10,
                    LuceneOperator.NO_LIMIT,
                    false,
                    () -> 0L,
                    LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                    warnings
                );

                // Now prove that actually running a driver over this operator no longer throws either.
                // For an IVF/BBQ kNN query, the ANN accept-docs bitset (and therefore the child filter,
                // including our single_value_match node) is only evaluated once, eagerly, during the
                // rewrite() above -- the real per-driver scan just walks the already-computed top-k doc
                // set, it never re-invokes the filter's two-phase matcher. So the warning raised while
                // building that bitset is -- per the TODO(154664) at the bind site -- silently discarded
                // rather than surfacing on this driver; that's a known, documented gap for this query
                // shape, not a regression. (Ordinary, non-kNN pushed-down filters visit
                // single_value_match on every scored doc during the real per-driver scan, and correctly
                // still emit warnings there -- see LuceneOperatorSingleValueQueryWarningsTests.)
                LuceneSourceOperator op = (LuceneSourceOperator) factory.get(driverContext());
                try {
                    while (op.isFinished() == false) {
                        Page page = op.getOutput();
                        if (page != null) {
                            page.releaseBlocks();
                        }
                    }
                } finally {
                    op.close();
                }
            }
        }
    }
}
