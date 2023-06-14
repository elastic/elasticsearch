/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

abstract class AbstractTDigestPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    protected final double[] keys;
    protected final ValuesSource valuesSource;
    protected final DocValueFormat formatter;
    protected ObjectArray<TDigestState> states;
    protected final double compression;
    protected final String executionHint;
    protected final boolean keyed;

    AbstractTDigestPercentilesAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] keys,
        double compression,
        String executionHint,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = config.getValuesSource();
        this.keyed = keyed;
        this.formatter = formatter;
        this.states = context.bigArrays().newObjectArray(1);
        this.keys = keys;
        this.compression = compression;
        this.executionHint = executionHint;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final SortedNumericDoubleValues values = ((ValuesSource.Numeric) valuesSource).doubleValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                TDigestState state = getExistingOrNewHistogram(bigArrays(), bucket);
                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        state.add(values.nextValue());
                    }
                }
            }
        };
    }

    private TDigestState getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        TDigestState state = states.get(bucket);
        if (state == null) {
            state = TDigestState.create(compression, executionHint);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(keys, Double.parseDouble(name)) >= 0;
    }

    protected TDigestState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        return states.get(bucketOrd);
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

}
