/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class InternalHDRPercentileRanks extends AbstractInternalHDRPercentiles implements PercentileRanks {
    public static final String NAME = "hdr_percentile_ranks";

    public InternalHDRPercentileRanks(
        String name,
        double[] cdfValues,
        DoubleHistogram state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, cdfValues, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalHDRPercentileRanks(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static InternalHDRPercentileRanks empty(
        String name,
        double[] keys,
        boolean keyed,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        return new InternalHDRPercentileRanks(name, keys, null, keyed, format, metadata);
    }

    @Override
    public Iterator<Percentile> iterator() {
        if (state == null) {
            return EMPTY_ITERATOR;
        }
        return new Iter(keys, state);
    }

    @Override
    public double percent(double value) {
        return percentileRank(state, value);
    }

    @Override
    public String percentAsString(double value) {
        return valueAsString(String.valueOf(value));
    }

    @Override
    public double value(double key) {
        return percent(key);
    }

    @Override
    protected AbstractInternalHDRPercentiles createReduced(
        String name,
        double[] keys,
        DoubleHistogram merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalHDRPercentileRanks(name, keys, merged, keyed, format, metadata);
    }

    public static double percentileRank(DoubleHistogram state, double value) {
        if (state == null || state.getTotalCount() == 0) {
            return Double.NaN;
        }
        double percentileRank = state.getPercentileAtOrBelowValue(value);
        if (percentileRank < 0) {
            percentileRank = 0;
        } else if (percentileRank > 100) {
            percentileRank = 100;
        }
        return percentileRank;
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] values;
        private final DoubleHistogram state;
        private int i;

        public Iter(double[] values, DoubleHistogram state) {
            this.values = values;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < values.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percentileRank(state, values[i]), values[i]);
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
