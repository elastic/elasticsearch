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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
*
*/
public class InternalTDigestPercentiles extends AbstractInternalTDigestPercentiles implements Percentiles {

    public final static Type TYPE = new Type(Percentiles.TYPE_NAME, "t_digest_percentiles");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalTDigestPercentiles readResult(StreamInput in) throws IOException {
            InternalTDigestPercentiles result = new InternalTDigestPercentiles();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    InternalTDigestPercentiles() {
    } // for serialization

    public InternalTDigestPercentiles(String name, double[] percents, TDigestState state, boolean keyed, ValueFormatter formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, percents, state, keyed, formatter, pipelineAggregators, metaData);
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(keys, state);
    }

    @Override
    public double percentile(double percent) {
        return state.quantile(percent / 100);
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
    protected AbstractInternalTDigestPercentiles createReduced(String name, double[] keys, TDigestState merged, boolean keyed,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalTDigestPercentiles(name, keys, merged, keyed, valueFormatter, pipelineAggregators, metaData);
    }

    @Override
    public Type type() {
        return TYPE;
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
            final Percentile next = new InternalPercentile(percents[i], state.quantile(percents[i] / 100));
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
