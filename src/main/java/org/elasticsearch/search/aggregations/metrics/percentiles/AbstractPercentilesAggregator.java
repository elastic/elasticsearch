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

package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

public abstract class AbstractPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    private static int indexOfKey(double[] keys, double key) {
        return ArrayUtils.binarySearch(keys, key, 0.001);
    }

    protected final double[] keys;
    protected final ValuesSource.Numeric valuesSource;
    private DoubleValues values;
    protected ObjectArray<TDigestState> states;
    protected final double compression;
    protected final boolean keyed;

    public AbstractPercentilesAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, AggregationContext context,
                                 Aggregator parent, double[] keys, double compression, boolean keyed) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.states = bigArrays.newObjectArray(estimatedBucketsCount);
        this.keys = keys;
        this.compression = compression;
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long bucketOrd) throws IOException {
        states = bigArrays.grow(states, bucketOrd + 1);
    
        TDigestState state = states.get(bucketOrd);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucketOrd, state);
        }
    
        final int valueCount = values.setDocument(doc);
        for (int i = 0; i < valueCount; i++) {
            state.add(values.nextValue());
        }
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