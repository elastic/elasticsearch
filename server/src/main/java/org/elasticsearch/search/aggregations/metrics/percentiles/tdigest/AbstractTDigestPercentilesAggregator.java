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

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AbstractTDigestPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    private static int indexOfKey(double[] keys, double key) {
        return ArrayUtils.binarySearch(keys, key, 0.001);
    }

    protected final double[] keys;
    protected final ValuesSource.Numeric valuesSource;
    protected final DocValueFormat formatter;
    protected ObjectArray<TDigestState> states;
    protected final double compression;
    protected final boolean keyed;

    public AbstractTDigestPercentilesAggregator(String name, ValuesSource.Numeric valuesSource, SearchContext context, Aggregator parent,
            double[] keys, double compression, boolean keyed, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.formatter = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.compression = compression;
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                states = bigArrays.grow(states, bucket + 1);

                TDigestState state = states.get(bucket);
                if (state == null) {
                    state = new TDigestState(compression);
                    states.set(bucket, state);
                }

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        state.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    public boolean hasMetric(String name) {
        return indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected TDigestState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        final TDigestState state = states.get(bucketOrd);
        return state;
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
