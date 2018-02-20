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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

/**
 * A producer that visits composite buckets in the order of the value indexed in the leading source of the composite
 * definition. It can be used to control which documents should be collected to produce the top composite buckets
 * without visiting all documents in an index.
 */
abstract class SortedDocsProducer {
    protected final String field;

    private SortedDocsProducer(String field) {
        this.field = field;
    }

    /**
     * Visits all non-deleted documents in <code>iterator</code> and pass documents that contain competitive composite buckets
     * to the provided <code>sub</code> collector.
     * Returns true if the queue is full and the current <code>leadSourceBucket</code> did not produce any competitive
     * composite buckets.
     */
    protected boolean processBucket(CompositeValuesCollectorQueue queue, LeafReaderContext context,
                                    LeafBucketCollector sub, DocIdSetIterator iterator, Comparable<?> leadSourceBucket) throws IOException {
        final int[] topCompositeCollected = new int[1];
        final boolean[] hasCollected = new boolean[1];
        final LeafBucketCollector queueCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                hasCollected[0] = true;
                int slot = queue.addIfCompetitive();
                if (slot != -1) {
                    topCompositeCollected[0]++;
                    sub.collect(doc, slot);
                }
            }
        };
        final Bits liveDocs = context.reader().getLiveDocs();
        final LeafBucketCollector collector = queue.getLeafCollector(leadSourceBucket, context, queueCollector);
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs == null || liveDocs.get(iterator.docID())) {
                collector.collect(iterator.docID());
            }
        }
        if (queue.isFull() &&
                hasCollected[0] &&
                topCompositeCollected[0] == 0) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if this producer can be used to process segments (and bypass search).
     */
    abstract boolean isApplicable(Query query);

    /**
     * Populates the queue with the composite buckets present in the <code>context</code>.
     */
    abstract void processLeaf(Query query, CompositeValuesCollectorQueue queue,
                              LeafReaderContext context, LeafBucketCollector sub) throws IOException;

    /**
     * Creates a {@link SortedDocsProducer} from the provided <code>config</code> or returns null if there is
     * no implementation of producer that can handle the config.
     */
    static SortedDocsProducer createProducerOrNull(CompositeValuesSourceConfig config) {
        if (config.fieldContext() == null ||
                config.fieldContext().fieldType() == null ||
                // the field is not indexed
                config.fieldContext().fieldType().indexOptions() == IndexOptions.NONE ||
                // the field sort does not match the terms/numerics sort
                config.reverseMul() == -1) {
            return null;
        }

        MappedFieldType ft = config.fieldContext().fieldType();
        if (ft instanceof NumberFieldMapper.NumberFieldType) {
            if (config.valuesSource() instanceof ValuesSource.Numeric &&
                    ((ValuesSource.Numeric) config.valuesSource()).isFloatingPoint()) {
                return null;
            }
            switch (ft.typeName()) {
                case "integer":
                    return createInteger(ft.name());

                case "long":
                    return createLong(ft.name());

                default:
                    return null;
            }
        } else if (ft instanceof DateFieldMapper.DateFieldType) {
            if (config.valuesSource().getClass() == RoundingValuesSource.class) {
                final RoundingValuesSource source = (RoundingValuesSource) config.valuesSource();
                return createLongWithRounding(ft.name(), source::round);
            } else {
                return createLong(ft.name());
            }
        } else if (ft instanceof KeywordFieldMapper.KeywordFieldType) {
            return createTerms(ft.name());
        } else {
            return null;
        }
    }

    /**
     * Creates a {@link SortedDocsProducer} based on indexed terms.
     */
    static SortedDocsProducer createTerms(String field) {
        return new TermsSortedDocsProducer(field);
    }

    /**
     * Creates a {@link SortedDocsProducer} based on indexed integers.
     */
    static SortedDocsProducer createInteger(String field) {
        return new LongSortedDocsProducer(field, b -> IntPoint.decodeDimension(b, 0));
    }

    /**
     * Creates a {@link SortedDocsProducer} based on indexed longs.
     */
    static SortedDocsProducer createLong(String field) {
        return new LongSortedDocsProducer(field, b -> LongPoint.decodeDimension(b, 0));
    }

    /**
     * Creates a {@link SortedDocsProducer} based on indexed longs rounded with the provided <code>rounding</code>.
     */
    static SortedDocsProducer createLongWithRounding(String field, LongUnaryOperator rounding) {
        return new LongSortedDocsProducer(field,
            (b) -> {
                long value = LongPoint.decodeDimension(b, 0);
                return rounding.applyAsLong(value);
            });
    }

    /**
     * A {@link SortedDocsProducer} that can sort documents based on terms indexed in the
     * provided field.
     */
    private static class TermsSortedDocsProducer extends SortedDocsProducer {
        private TermsSortedDocsProducer(String field) {
            super(field);
        }

        @Override
        boolean isApplicable(Query query) {
            return query == null ||
                    query.getClass() == MatchAllDocsQuery.class;
        }

        @Override
        void processLeaf(Query query,  CompositeValuesCollectorQueue queue, LeafReaderContext context, LeafBucketCollector sub) throws IOException {
            assert isApplicable(query);
            final Terms terms = context.reader().terms(field);
            if (terms == null) {
                // no value for the field
                return;
            }
            BytesRef lowerValue = (BytesRef) queue.getLowerValueLeadSource();
            BytesRef upperValue = (BytesRef) queue.getUpperValueLeadSource();
            final TermsEnum te = terms.iterator();
            if (lowerValue != null) {
                if (te.seekCeil(lowerValue) == TermsEnum.SeekStatus.END) {
                    return;
                }
            } else {
                if (te.next() == null) {
                    return;
                }
            }
            PostingsEnum reuse = null;
            boolean first = true;
            do {
                if (upperValue != null && upperValue.compareTo(te.term()) < 0) {
                    break;
                }
                reuse = te.postings(reuse, PostingsEnum.NONE);
                if (processBucket(queue, context, sub, reuse, te.term()) && !first) {
                    // this bucket does not have any competitive composite buckets,
                    // we can early terminate the collection because the remaining buckets are guaranteed
                    // to be greater than this bucket.
                    break;
                }
                first = false;
            } while (te.next() != null);
        }
    }

    /**
     * A {@link SortedDocsProducer} that can sort documents based on numerics indexed in the
     * provided field.
     */
    private static class LongSortedDocsProducer extends SortedDocsProducer {
        private final ToLongFunction<byte[]> bucketFunction;

        private LongSortedDocsProducer(String field, ToLongFunction<byte[]> bucketFunction) {
            super(field);
            this.bucketFunction = bucketFunction;
        }

        @Override
        boolean isApplicable(Query query) {
            return query.getClass() == MatchAllDocsQuery.class ||
                // if the query is a range query over the same field
                (query instanceof PointRangeQuery && field.equals((((PointRangeQuery) query).getField())));
        }

        @Override
        void processLeaf(Query query, CompositeValuesCollectorQueue queue,
                         LeafReaderContext context, LeafBucketCollector sub) throws IOException {
            assert isApplicable(query);
            final PointValues values = context.reader().getPointValues(field);
            if (values == null) {
                // no value for the field
                return;
            }
            final byte[] lowerPoint;
            final byte[] upperPoint;
            if (query instanceof PointRangeQuery) {
                final PointRangeQuery rangeQuery = (PointRangeQuery) query;
                lowerPoint = rangeQuery.getLowerPoint();
                upperPoint = rangeQuery.getUpperPoint();
            } else {
                lowerPoint = null;
                upperPoint = null;
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
                new Visitor(context, queue, sub, values.getBytesPerDimension(), lowerPoint, upperPoint, lowerBucket, upperBucket);
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
            final byte[] lowerPoint;
            final byte[] upperPoint;
            final long lowerBucket;
            final long upperBucket;

            DocIdSetBuilder builder;
            long lastBucket;
            boolean first = true;

            Visitor(LeafReaderContext context, CompositeValuesCollectorQueue queue,
                    LeafBucketCollector sub, int bytesPerDim,
                    byte[] lowerPoint, byte[] upperPoint, long lowerBucket, long upperBucket) {
                this.context = context;
                this.maxDoc = context.reader().maxDoc();
                this.queue = queue;
                this.sub = sub;
                this.lowerPoint = lowerPoint;
                this.upperPoint = upperPoint;
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
                if ((upperPoint != null && StringHelper.compare(bytesPerDim, minPackedValue, 0, upperPoint, 0) > 0) ||
                        (lowerPoint != null && StringHelper.compare(bytesPerDim, maxPackedValue, 0, lowerPoint, 0) < 0)) {
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
}
