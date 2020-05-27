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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class ValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource valuesSource;

    // a count per bucket
    LongArray counts;

    public ValueCountAggregator(
            String name,
            ValuesSource valuesSource,
            SearchContext aggregationContext,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, aggregationContext, parent, metadata);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            counts = context.bigArrays().newLongArray(1, true);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();

        if (valuesSource instanceof ValuesSource.Numeric) {
            final SortedNumericDocValues values = ((ValuesSource.Numeric)valuesSource).longValues(ctx);
            return new LeafBucketCollectorBase(sub, values) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays.grow(counts, bucket + 1);
                    if (values.advanceExact(doc)) {
                        counts.increment(bucket, values.docValueCount());
                    }
                }
            };
        }
        if (valuesSource instanceof ValuesSource.Bytes.GeoPoint) {
            MultiGeoPointValues values = ((ValuesSource.GeoPoint)valuesSource).geoPointValues(ctx);
            return new LeafBucketCollectorBase(sub, null) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays.grow(counts, bucket + 1);
                    if (values.advanceExact(doc)) {
                        counts.increment(bucket, values.docValueCount());
                    }
                }
            };
        }
        // The following is default collector. Including the keyword FieldType
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                if (values.advanceExact(doc)) {
                    counts.increment(bucket, values.docValueCount());
                }
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return (valuesSource == null || owningBucketOrd >= counts.size()) ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0L, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

}
