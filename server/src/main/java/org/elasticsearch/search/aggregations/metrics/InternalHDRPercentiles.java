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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class InternalHDRPercentiles extends AbstractInternalHDRPercentiles implements Percentiles {
    public static final String NAME = "hdr_percentiles";

    public InternalHDRPercentiles(String name, double[] percents, DoubleHistogram state, boolean keyed, DocValueFormat formatter,
                                  Map<String, Object> metadata) {
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
    protected AbstractInternalHDRPercentiles createReduced(String name, double[] keys, DoubleHistogram merged, boolean keyed,
            Map<String, Object> metadata) {
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
