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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalExtendedStatsBucket extends InternalExtendedStats implements ExtendedStatsBucket {

    public final static Type TYPE = new Type("extended_stats_bucket");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalExtendedStatsBucket readResult(StreamInput in) throws IOException {
            InternalExtendedStatsBucket result = new InternalExtendedStatsBucket();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    InternalExtendedStatsBucket(String name, long count, double sum, double min, double max, double sumOfSqrs, double sigma,
                                            ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) {
        super(name, count, sum, min, max, sumOfSqrs, sigma, formatter, pipelineAggregators, metaData);
    }

    InternalExtendedStatsBucket() {
        // for serialization
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats doReduce(
            List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }
}
