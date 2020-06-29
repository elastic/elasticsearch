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

import org.HdrHistogram.DoubleHistogram;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

abstract class AbstractHDRPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    private static int indexOfKey(double[] keys, double key) {
        return ArrayUtils.binarySearch(keys, key, 0.001);
    }

    protected final double[] keys;
    protected final ValuesSource valuesSource;
    protected final DocValueFormat format;
    protected ObjectArray<DoubleHistogram> states;
    protected final int numberOfSignificantValueDigits;
    protected final boolean keyed;

    AbstractHDRPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
            double[] keys, int numberOfSignificantValueDigits, boolean keyed, DocValueFormat formatter,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.format = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();

        final SortedNumericDoubleValues values = ((ValuesSource.Numeric)valuesSource).doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                DoubleHistogram state = getExistingOrNewHistogram(bigArrays, bucket);
                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        state.recordValue(values.nextValue());
                    }
                }
            }
        };

    }

    private DoubleHistogram getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        DoubleHistogram state = states.get(bucket);
        if (state == null) {
            state = new DoubleHistogram(numberOfSignificantValueDigits);
            /* Set the histogram to autosize so it can resize itself as
               the data range increases. Resize operations should be
               rare as the histogram buckets are exponential (on the top
               level). In the future we could expose the range as an
               option on the request so the histogram can be fixed at
               initialisation and doesn't need resizing.
             */
            state.setAutoResize(true);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected DoubleHistogram getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        final DoubleHistogram state = states.get(bucketOrd);
        return state;
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
