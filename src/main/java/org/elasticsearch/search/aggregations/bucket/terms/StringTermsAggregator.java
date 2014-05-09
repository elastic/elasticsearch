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
package org.elasticsearch.search.aggregations.bucket.terms;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Iterators2;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.*;

/**
 * An aggregator of string values.
 */
public class StringTermsAggregator extends AbstractStringTermsAggregator {

    private final ValuesSource valuesSource;
    protected final BytesRefHash bucketOrds;
    private final IncludeExclude includeExclude;
    private BytesValues values;

    public StringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                                 InternalOrder order, BucketCountThresholds bucketCountThresholds,
                                 IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent) {

        super(name, factories, estimatedBucketCount, aggregationContext, parent, order, bucketCountThresholds);
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        bucketOrds = new BytesRefHash(estimatedBucketCount, aggregationContext.bigArrays());
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final BytesRef bytes = values.nextValue();
            if (includeExclude != null && !includeExclude.accept(bytes)) {
                continue;
            }
            final int hash = values.currentValueHash();
            assert hash == bytes.hashCode();
            long bucketOrdinal = bucketOrds.add(bytes, hash);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }
        }
    }

    /** Returns an iterator over the field data terms. */
    private static Iterator<BytesRef> terms(final BytesValues.WithOrdinals bytesValues, boolean reverse) {
        final Ordinals.Docs ordinals = bytesValues.ordinals();
        if (reverse) {
            return new UnmodifiableIterator<BytesRef>() {

                long i = ordinals.getMaxOrd() - 1;

                @Override
                public boolean hasNext() {
                    return i >= Ordinals.MIN_ORDINAL;
                }

                @Override
                public BytesRef next() {
                    bytesValues.getValueByOrd(i--);
                    return bytesValues.copyShared();
                }

            };
        } else {
            return new UnmodifiableIterator<BytesRef>() {

                long i = Ordinals.MIN_ORDINAL;

                @Override
                public boolean hasNext() {
                    return i < ordinals.getMaxOrd();
                }

                @Override
                public BytesRef next() {
                    bytesValues.getValueByOrd(i++);
                    return bytesValues.copyShared();
                }

            };
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        if (bucketCountThresholds.getMinDocCount() == 0 && (order != InternalOrder.COUNT_DESC || bucketOrds.size() < bucketCountThresholds.getRequiredSize())) {
            // we need to fill-in the blanks
            List<BytesValues.WithOrdinals> valuesWithOrdinals = Lists.newArrayList();
            for (AtomicReaderContext ctx : context.searchContext().searcher().getTopReaderContext().leaves()) {
                context.setNextReader(ctx);
                final BytesValues values = valuesSource.bytesValues();
                if (values instanceof BytesValues.WithOrdinals) {
                    valuesWithOrdinals.add((BytesValues.WithOrdinals) values);
                } else {
                    // brute force
                    for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                        final int valueCount = values.setDocument(docId);
                        for (int i = 0; i < valueCount; ++i) {
                            final BytesRef term = values.nextValue();
                            if (includeExclude == null || includeExclude.accept(term)) {
                                bucketOrds.add(term, values.currentValueHash());
                            }
                        }
                    }
                }
            }

            // With ordinals we can be smarter and add just as many terms as necessary to the hash table
            // For instance, if sorting by term asc, we only need to get the first `requiredSize` terms as other terms would
            // either be excluded by the priority queue or at reduce time.
            if (valuesWithOrdinals.size() > 0) {
                final boolean reverse = order == InternalOrder.TERM_DESC;
                Comparator<BytesRef> comparator = BytesRef.getUTF8SortedAsUnicodeComparator();
                if (reverse) {
                    comparator = Collections.reverseOrder(comparator);
                }
                Iterator<? extends BytesRef>[] iterators = new Iterator[valuesWithOrdinals.size()];
                for (int i = 0; i < valuesWithOrdinals.size(); ++i) {
                    iterators[i] = terms(valuesWithOrdinals.get(i), reverse);
                }
                Iterator<BytesRef> terms = Iterators2.mergeSorted(Arrays.asList(iterators), comparator, true);
                if (includeExclude != null) {
                    terms = Iterators.filter(terms, new Predicate<BytesRef>() {
                        @Override
                        public boolean apply(BytesRef input) {
                            return includeExclude.accept(input);
                        }
                    });
                }
                if (order == InternalOrder.COUNT_ASC) {
                    // let's try to find `shardSize` terms that matched no hit
                    // this one needs shardSize and not requiredSize because even though terms have a count of 0 here,
                    // they might have higher counts on other shards
                    for (int added = 0; added < bucketCountThresholds.getShardSize() && terms.hasNext(); ) {
                        if (bucketOrds.add(terms.next()) >= 0) {
                            ++added;
                        }
                    }
                } else if (order == InternalOrder.COUNT_DESC) {
                    // add terms until there are enough buckets
                    while (bucketOrds.size() < bucketCountThresholds.getRequiredSize() && terms.hasNext()) {
                        bucketOrds.add(terms.next());
                    }
                } else if (order == InternalOrder.TERM_ASC || order == InternalOrder.TERM_DESC) {
                    // add the `requiredSize` least terms
                    for (int i = 0; i < bucketCountThresholds.getRequiredSize() && terms.hasNext(); ++i) {
                        bucketOrds.add(terms.next());
                    }
                } else {
                    // other orders (aggregations) are not optimizable
                    while (terms.hasNext()) {
                        bucketOrds.add(terms.next());
                    }
                }
            }
        }

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        StringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new StringTerms.Bucket(new BytesRef(), 0, null);
            }
            bucketOrds.get(i, spare.termBytes);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = (StringTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final StringTerms.Bucket bucket = (StringTerms.Bucket) ordered.pop();
            // the terms are owned by the BytesRefHash, we need to pull a copy since the BytesRef hash data may be recycled at some point
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Arrays.asList(list));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalTerms.Bucket>emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    /**
     * Extension of StringTermsAggregator that caches bucket ords using terms ordinals.
     */
    public static class WithOrdinals extends StringTermsAggregator {

        private final ValuesSource.Bytes.WithOrdinals valuesSource;
        private BytesValues.WithOrdinals bytesValues;
        private Ordinals.Docs ordinals;
        private LongArray ordinalToBucket;

        public WithOrdinals(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals valuesSource, long esitmatedBucketCount,
                InternalOrder order, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext, Aggregator parent) {
            super(name, factories, valuesSource, esitmatedBucketCount, order, bucketCountThresholds, null, aggregationContext, parent);
            this.valuesSource = valuesSource;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            bytesValues = valuesSource.bytesValues();
            ordinals = bytesValues.ordinals();
            final long maxOrd = ordinals.getMaxOrd();
            if (ordinalToBucket == null || ordinalToBucket.size() < maxOrd) {
                if (ordinalToBucket != null) {
                    ordinalToBucket.close();
                }
                ordinalToBucket = context().bigArrays().newLongArray(BigArrays.overSize(maxOrd), false);
            }
            ordinalToBucket.fill(0, maxOrd, -1L);
        }

        @Override
        public void collect(int doc, long owningBucketOrdinal) throws IOException {
            assert owningBucketOrdinal == 0 : "this is a per_bucket aggregator";
            final int valuesCount = ordinals.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                final long ord = ordinals.nextOrd();
                long bucketOrd = ordinalToBucket.get(ord);
                if (bucketOrd < 0) { // unlikely condition on a low-cardinality field
                    final BytesRef bytes = bytesValues.getValueByOrd(ord);
                    final int hash = bytesValues.currentValueHash();
                    assert hash == bytes.hashCode();
                    bucketOrd = bucketOrds.add(bytes, hash);
                    if (bucketOrd < 0) { // already seen in another segment
                        bucketOrd = - 1 - bucketOrd;
                        collectExistingBucket(doc, bucketOrd);
                    } else {
                        collectBucket(doc, bucketOrd);
                    }
                    ordinalToBucket.set(ord, bucketOrd);
                } else {
                    collectExistingBucket(doc, bucketOrd);
                }
            }
        }

        @Override
        public void doClose() {
            Releasables.close(bucketOrds, ordinalToBucket);
        }
    }

}

