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

public class InternalHDRPercentiles extends AbstractInternalHDRPercentiles implements Percentiles {
    public static final String NAME = "hdr_percentiles";

    public InternalHDRPercentiles(
        String name,
        double[] percents,
        DoubleHistogram state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, percents, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalHDRPercentiles(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(keys, state);
    }

    @Override
    public double percentile(double percent) {
        if (state.getTotalCount() == 0) {
            return Double.NaN;
        }
        return state.getValueAtPercentile(percent);
    }

    @Override
    public String percentileAsString(double percent) {
        return valueAsString(String.valueOf(percent));
    }

    @Override
    public double value(double key) {
        return percentile(key);
    }

    @Override
    protected AbstractInternalHDRPercentiles createReduced(
        String name,
        double[] keys,
        DoubleHistogram merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalHDRPercentiles(name, keys, merged, keyed, format, metadata);
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] percents;
        private final DoubleHistogram state;
        private int i;

        public Iter(double[] percents, DoubleHistogram state) {
            this.percents = percents;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < percents.length;
        }

        @Override
        public Percentile next() {
            double percent = percents[i];
            double value = (state.getTotalCount() == 0) ? Double.NaN : state.getValueAtPercentile(percent);
            final Percentile next = new Percentile(percent, value);
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
