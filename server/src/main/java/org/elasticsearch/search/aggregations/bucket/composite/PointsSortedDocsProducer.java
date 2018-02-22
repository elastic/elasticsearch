/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.function.ToLongFunction;

/**
 * A {@link SortedDocsProducer} that can sort documents based on numerics indexed in the
 * provided field.
 */
class PointsSortedDocsProducer extends SortedDocsProducer {
    private final ToLongFunction<byte[]> bucketFunction;
    private final byte[] lowerPointQuery;
    private final byte[] upperPointQuery;

    PointsSortedDocsProducer(String field, ToLongFunction<byte[]> bucketFunction, byte[] lowerPointQuery, byte[] upperPointQuery) {
        super(field);
        this.bucketFunction = bucketFunction;
        this.lowerPointQuery = lowerPointQuery;
        this.upperPointQuery = upperPointQuery;
    }

    @Override
    void processLeaf(Query query, CompositeValuesCollectorQueue queue,
                     LeafReaderContext context, LeafBucketCollector sub) throws IOException {
        final PointValues values = context.reader().getPointValues(field);
        if (values == null) {
            // no value for the field
            return;
        }
        long lowerBucket = Long.MIN_VALUE;
        Comparable<?> lowerValue = queue.getLowerValueLeadSource();
        if (lowerValue != null) {
            if (lowerValue.getClass() != Long.class) {
                throw new IllegalStateException("expected Long, got " + lowerValue.getClass());
            }
            lowerBucket = (Long) lowerValue;
        }

        long upperBucket = Long.MAX_VALUE;
        Comparable<?> upperValue = queue.getUpperValueLeadSource();
        if (upperValue != null) {
            if (upperValue.getClass() != Long.class) {
                throw new IllegalStateException("expected Long, got " + upperValue.getClass());
            }
            upperBucket = (Long) upperValue;
        }

        Visitor visitor =
            new Visitor(context, queue, sub, values.getBytesPerDimension(), lowerBucket, upperBucket);
        try {
            values.intersect(visitor);
            visitor.flush();
        } catch (CollectionTerminatedException exc) {}
    }

    private class Visitor implements PointValues.IntersectVisitor {
        final LeafReaderContext context;
        final CompositeValuesCollectorQueue queue;
        final LeafBucketCollector sub;
        final int maxDoc;
        final int bytesPerDim;
        final long lowerBucket;
        final long upperBucket;

        DocIdSetBuilder builder;
        long lastBucket;
        boolean first = true;

        Visitor(LeafReaderContext context, CompositeValuesCollectorQueue queue,
                LeafBucketCollector sub, int bytesPerDim, long lowerBucket, long upperBucket) {
            this.context = context;
            this.maxDoc = context.reader().maxDoc();
            this.queue = queue;
            this.sub = sub;
            this.lowerBucket = lowerBucket;
            this.upperBucket = upperBucket;
            this.builder = new DocIdSetBuilder(maxDoc);
            this.bytesPerDim = bytesPerDim;
        }

        @Override
        public void visit(int docID) throws IOException {
            throw new IllegalStateException("should never be called");
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            if (compare(packedValue, packedValue) != PointValues.Relation.CELL_CROSSES_QUERY) {
                return;
            }

            long bucket = bucketFunction.applyAsLong(packedValue);
            if (first == false && bucket != lastBucket) {
                final DocIdSet docIdSet = builder.build();
                if (processBucket(queue, context, sub, docIdSet.iterator(), lastBucket) &&
                    // lower bucket is inclusive
                    lowerBucket != lastBucket) {
                    // this bucket does not have any competitive composite buckets,
                    // we can early terminate the collection because the remaining buckets are guaranteed
                    // to be greater than this bucket.
                    throw new CollectionTerminatedException();
                }
                builder = new DocIdSetBuilder(maxDoc);
            }
            lastBucket = bucket;
            first = false;
            builder.grow(1).add(docID);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            if ((upperPointQuery != null && StringHelper.compare(bytesPerDim, minPackedValue, 0, upperPointQuery, 0) > 0) ||
                    (lowerPointQuery != null && StringHelper.compare(bytesPerDim, maxPackedValue, 0, lowerPointQuery, 0) < 0)) {
                // does not match the query
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            // check the current bounds
            if (lowerBucket != Long.MIN_VALUE) {
                long maxBucket = bucketFunction.applyAsLong(maxPackedValue);
                if (maxBucket < lowerBucket) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }

            if (upperBucket != Long.MAX_VALUE) {
                long minBucket = bucketFunction.applyAsLong(minPackedValue);
                if (minBucket > upperBucket) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        public void flush() throws IOException {
            if (first == false && builder != null) {
                final DocIdSet docIdSet = builder.build();
                processBucket(queue, context, sub, docIdSet.iterator(), lastBucket);
                builder = null;
            }
        }
    }
}
