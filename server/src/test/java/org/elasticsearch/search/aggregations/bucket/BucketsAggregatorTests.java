/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BucketsAggregatorTests extends AggregatorTestCase {

    private List<AggregationContext> toRelease = new ArrayList<>();

    @Override
    protected AggregationContext createAggregationContext(IndexSearcher indexSearcher, Query query, MappedFieldType... fieldTypes)
        throws IOException {
        AggregationContext context = super.createAggregationContext(indexSearcher, query, fieldTypes);
        // Generally, we should avoid doing this, but this test doesn't do anything with reduction, so it should be safe here
        toRelease.add(context);
        return context;
    }

    @After
    public void releaseContext() {
        Releasables.close(toRelease);
        toRelease.clear();
    }

    public BucketsAggregator buildMergeAggregator() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("numeric", 0));
                indexWriter.addDocument(document);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                AggregationContext context = createAggregationContext(
                    indexSearcher,
                    null,
                    new NumberFieldMapper.NumberFieldType("test", NumberFieldMapper.NumberType.INTEGER)
                );

                return new BucketsAggregator("test", AggregatorFactories.EMPTY, context, null, null, null) {
                    @Override
                    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
                        return null;
                    }

                    @Override
                    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
                        return new InternalAggregation[0];
                    }

                    @Override
                    public InternalAggregation buildEmptyAggregation() {
                        return null;
                    }
                };
            }
        }
    }

    public void testBucketMergeNoDelete() throws IOException {
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        for (int i = 0; i < 10; i++) {
            mergeAggregator.incrementBucketDocCount(i, i);
        }

        mergeAggregator.rewriteBuckets(10, bucket -> bucket % 5);

        for (int i = 0; i < 5; i++) {
            // The i'th bucket should now have all docs whose index % 5 = i
            // This is buckets i and i + 5
            // i + (i+5) = 2*i + 5
            assertEquals(mergeAggregator.getDocCounts().get(i), (2 * i) + 5);
        }
        for (int i = 5; i < 10; i++) {
            assertEquals(mergeAggregator.getDocCounts().get(i), 0);
        }
    }

    public void testBucketMergeAndDelete() throws IOException {
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        int sum = 0;
        for (int i = 0; i < 20; i++) {
            mergeAggregator.incrementBucketDocCount(i, i);
            if (5 <= i && i < 15) {
                sum += i;
            }
        }

        // Put the buckets in indices 5 ... 14 into bucket 5, and delete the rest of the buckets
        mergeAggregator.rewriteBuckets(10, bucket -> (5 <= bucket && bucket < 15) ? 5 : -1);

        assertEquals(mergeAggregator.getDocCounts().size(), 10); // Confirm that the 10 other buckets were deleted
        for (int i = 0; i < 10; i++) {
            assertEquals(mergeAggregator.getDocCounts().get(i), i == 5 ? sum : 0);
        }
    }
}
