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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RangeHistogramAggregator extends BucketsAggregator {
    private final ValuesSource.Bytes valuesSource;
    private final DocValueFormat formatter;
    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;

    private final LongHash bucketOrds;

    RangeHistogramAggregator(String name, AggregatorFactories factories, double interval, double offset,
                               BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
                               @Nullable ValuesSource.Bytes valuesSource, DocValueFormat formatter,
                               SearchContext context, Aggregator parent,
                               List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, context, parent, pipelineAggregators, metaData);
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.offset = offset;
        this.order = InternalOrder.validate(order, this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        bucketOrds = new LongHash(1, context.bigArrays());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        // TODO: For prototyping, just hard code this to work with Double ranges
        final BinaryDocValuesRangeQuery.LengthType lengthType = BinaryDocValuesRangeQuery.LengthType.FIXED_8;
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException();
    }
}
