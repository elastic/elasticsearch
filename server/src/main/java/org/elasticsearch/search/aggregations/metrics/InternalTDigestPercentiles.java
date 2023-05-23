/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class InternalTDigestPercentiles extends AbstractInternalTDigestPercentiles implements Percentiles {
    public static final String NAME = "tdigest_percentiles";

    public InternalTDigestPercentiles(
        String name,
        double[] percents,
        TDigestState state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, percents, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static InternalTDigestPercentiles empty(
        String name,
        double[] keys,
        boolean keyed,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        return new InternalTDigestPercentiles(name, keys, null, keyed, format, metadata);
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(keys, state);
    }

    @Override
    public double percentile(double percent) {
        return this.state != null ? state.quantile(percent / 100) : Double.NaN;
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
    protected AbstractInternalTDigestPercentiles createReduced(
        String name,
        double[] keys,
        TDigestState merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalTDigestPercentiles(name, keys, merged, keyed, format, metadata);
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] percents;
        private final TDigestState state;
        private int i;

        public Iter(double[] percents, TDigestState state) {
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
            final Percentile next = new Percentile(percents[i], state.quantile(percents[i] / 100));
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
