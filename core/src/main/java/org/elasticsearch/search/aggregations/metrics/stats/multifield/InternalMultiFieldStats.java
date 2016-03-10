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
package org.elasticsearch.search.aggregations.metrics.stats.multifield;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes distribution statistics over multiple fields
 */
public class InternalMultiFieldStats extends InternalMetricsAggregation implements MultiFieldStats {

    public final static Type TYPE = new Type("multifield_stats");
    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMultiFieldStats readResult(StreamInput in) throws IOException {
            InternalMultiFieldStats result = new InternalMultiFieldStats();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    /** per shard stats needed to compute stats */
    protected MultiFieldStatsResults multiFieldStatsResults;

    protected InternalMultiFieldStats() {
    }

    protected InternalMultiFieldStats(String name, long count, MultiFieldStatsResults multiFieldStatsResults,
                                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert count >= 0;
        this.multiFieldStatsResults = multiFieldStatsResults;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public long getDocCount() {
        return multiFieldStatsResults.docCount;
    }

    @Override
    public long getFieldCount(String field) {
        if (multiFieldStatsResults == null || multiFieldStatsResults.means.containsKey(field) == false) {
            return 0;
        }
        return multiFieldStatsResults.counts.get(field);
    }

    @Override
    public double getMean(String field) {
        if (multiFieldStatsResults == null || multiFieldStatsResults.means.containsKey(field) == false) {
            return Double.NaN;
        }
        return multiFieldStatsResults.means.get(field);
    }

    @Override
    public double getVariance(String field) {
        if (multiFieldStatsResults == null || multiFieldStatsResults.variances.containsKey(field) == false) {
            return Double.NaN;
        }
        return multiFieldStatsResults.variances.get(field);
    }

    @Override
    public double getSkewness(String field) {
        if (multiFieldStatsResults == null || multiFieldStatsResults.skewness.containsKey(field) == false) {
            return Double.NaN;
        }
        return multiFieldStatsResults.skewness.get(field);
    }

    @Override
    public double getKurtosis(String field) {
        if (multiFieldStatsResults == null || multiFieldStatsResults.kurtosis.containsKey(field) == false) {
            return Double.NaN;
        }
        return multiFieldStatsResults.kurtosis.get(field);
    }

    @Override
    public double getCovariance(String fieldX, String fieldY) {
        if (multiFieldStatsResults == null) {
            return Double.NaN;
        }
        return multiFieldStatsResults.getCovariance(fieldX, fieldY);
    }

    @Override
    public HashMap<String, HashMap<String, Double>> getCovariance() {
        return multiFieldStatsResults.covariances;
    }

    @Override
    public double getCorrelation(String fieldX, String fieldY) {
        if (multiFieldStatsResults == null) {
            return Double.NaN;
        }
        return multiFieldStatsResults.getCorrelation(fieldX, fieldY);
    }

    @Override
    public HashMap<String, HashMap<String, Double>> getCorrelation() {
        return multiFieldStatsResults.correlation;
    }

    static class Fields {
        public static final XContentBuilderString COUNTS = new XContentBuilderString("counts");
        public static final XContentBuilderString MEANS = new XContentBuilderString("means");
        public static final XContentBuilderString VARIANCES = new XContentBuilderString("variances");
        public static final XContentBuilderString SKEWNESS = new XContentBuilderString("skewness");
        public static final XContentBuilderString KURTOSIS = new XContentBuilderString("kurtosis");
        public static final XContentBuilderString COVARIANCE = new XContentBuilderString("covariance");
        public static final XContentBuilderString CORRELATION = new XContentBuilderString("correlation");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (multiFieldStatsResults != null && multiFieldStatsResults.correlation != null) {
            builder = doXContentBody(builder, Fields.COUNTS, multiFieldStatsResults.counts);
            builder = doXContentBody(builder, Fields.MEANS, multiFieldStatsResults.means);
            builder = doXContentBody(builder, Fields.VARIANCES, multiFieldStatsResults.variances);
            builder = doXContentBody(builder, Fields.SKEWNESS, multiFieldStatsResults.skewness);
            builder = doXContentBody(builder, Fields.KURTOSIS, multiFieldStatsResults.kurtosis);
            builder = doXContentBody(builder, Fields.COVARIANCE, multiFieldStatsResults.covariances);
            builder = doXContentBody(builder, Fields.CORRELATION, multiFieldStatsResults.correlation);
        }
        return builder;
    }

    private XContentBuilder doXContentBody(XContentBuilder builder, XContentBuilderString field, Map fieldVals) throws IOException {
        builder.startObject(field);
        for (Map.Entry<String, Object> vals : ((HashMap<String, Object>)fieldVals).entrySet()) {
            if (vals.getValue() instanceof HashMap) {
                builder.startObject(vals.getKey());
                for (Map.Entry<String, Double> val : ((HashMap<String, Double>)vals.getValue()).entrySet()) {
                    builder.field(val.getKey(), val.getValue());
                }
                builder.endObject();
            } else {
                builder.field(vals.getKey(), vals.getValue());
            }
        }
        return builder.endObject();
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String coordinate = path.get(0);
            switch (coordinate) {
                case "correlation":
                    return multiFieldStatsResults.correlation;
                default:
                    throw new IllegalArgumentException("Found unknown path element [" + coordinate + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (multiFieldStatsResults == null || multiFieldStatsResults.docCount == 0) {
            out.writeVLong(0);
        } else {
            out.writeVLong(multiFieldStatsResults.docCount);
            multiFieldStatsResults.writeTo(out);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        // read count
        long count = in.readVLong();
        if (count > 0) {
            multiFieldStatsResults = new MultiFieldStatsResults();
            multiFieldStatsResults.docCount = count;
            multiFieldStatsResults.readFrom(in);
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // merge stats across all shards
        aggregations.removeIf(p -> ((InternalMultiFieldStats)p).multiFieldStatsResults == null);

        // return empty result iff all stats are null
        if (aggregations.size() == 0) {
            return new InternalMultiFieldStats(name, 0, new MultiFieldStatsResults(), pipelineAggregators(), getMetaData());
        }

        MultiFieldStatsResults corrStats = ((InternalMultiFieldStats) aggregations.get(0)).multiFieldStatsResults;
        for (int i=1; i < aggregations.size(); ++i) {
            corrStats.merge(((InternalMultiFieldStats) aggregations.get(i)).multiFieldStatsResults);
        }
        corrStats.computeStats();

        return new InternalMultiFieldStats(name, corrStats.docCount, corrStats, pipelineAggregators(), getMetaData());
    }
}
