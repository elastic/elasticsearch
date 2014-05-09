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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 */
public class DoubleTermsAggregator extends TermsAggregator {

    private final InternalOrder order;
    private final ValuesSource.Numeric valuesSource;
    private final ValueFormatter formatter;
    private final LongHash bucketOrds;
    private DoubleValues values;

    public DoubleTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, @Nullable ValueFormat format, long estimatedBucketCount,
                               InternalOrder order, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketCount, aggregationContext, parent, bucketCountThresholds);
        this.valuesSource = valuesSource;
        this.formatter = format != null ? format.formatter() : null;
        this.order = InternalOrder.validate(order, this);
        bucketOrds = new LongHash(estimatedBucketCount, aggregationContext.bigArrays());
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final double val = values.nextValue();
            final long bits = Double.doubleToRawLongBits(val);
            long bucketOrdinal = bucketOrds.add(bits);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }
        }
    }

    @Override
    public DoubleTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        if (bucketCountThresholds.getMinDocCount() == 0 && (order != InternalOrder.COUNT_DESC || bucketOrds.size() < bucketCountThresholds.getRequiredSize())) {
            // we need to fill-in the blanks
            for (AtomicReaderContext ctx : context.searchContext().searcher().getTopReaderContext().leaves()) {
                context.setNextReader(ctx);
                final DoubleValues values = valuesSource.doubleValues();
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    final int valueCount = values.setDocument(docId);
                    for (int i = 0; i < valueCount; ++i) {
                        bucketOrds.add(Double.doubleToLongBits(values.nextValue()));
                    }
                }
            }
        }

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        DoubleTerms.Bucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new DoubleTerms.Bucket(0, 0, null);
            }
            spare.term = Double.longBitsToDouble(bucketOrds.get(i));
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = (DoubleTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final DoubleTerms.Bucket bucket = (DoubleTerms.Bucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new DoubleTerms(name, order, formatter, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Arrays.asList(list));
    }

    @Override
    public DoubleTerms buildEmptyAggregation() {
        return new DoubleTerms(name, order, formatter, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalTerms.Bucket>emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}
