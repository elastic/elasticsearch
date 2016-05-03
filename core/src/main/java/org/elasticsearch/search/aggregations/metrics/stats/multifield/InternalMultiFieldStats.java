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
    protected RunningStats stats;
    /** final result */
    protected MultiFieldStatsResults results;

    protected InternalMultiFieldStats() {
    }

    /** per shard ctor */
    protected InternalMultiFieldStats(String name, long count, RunningStats multiFieldStatsResults, MultiFieldStatsResults results,
                                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert count >= 0;
        this.stats = multiFieldStatsResults;
        this.results = results;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public long getDocCount() {
        return stats.docCount;
    }

    @Override
    public long getFieldCount(String field) {
        if (results == null) {
            return 0;
        }
        return results.getFieldCount(field);
    }

    @Override
    public Double getMean(String field) {
        if (results == null) {
            return null;
        }
        return results.getMean(field);
    }

    @Override
    public Double getVariance(String field) {
        if (results == null) {
            return null;
        }
        return results.getVariance(field);
    }

    @Override
    public Double getSkewness(String field) {
        if (results == null) {
            return null;
        }
        return results.getSkewness(field);
    }

    @Override
    public Double getKurtosis(String field) {
        if (results == null) {
            return null;
        }
        return results.getKurtosis(field);
    }

    @Override
    public Double getCovariance(String fieldX, String fieldY) {
        if (results == null) {
            return null;
        }
        return results.getCovariance(fieldX, fieldY);
    }

    @Override
    public Map<String, HashMap<String, Double>> getCovariance() {
        return results.getCovariances();
    }

    @Override
    public Double getCorrelation(String fieldX, String fieldY) {
        if (results == null) {
            return null;
        }
        return results.getCorrelation(fieldX, fieldY);
    }

    @Override
    public Map<String, HashMap<String, Double>> getCorrelation() {
        return results.getCorrelations();
    }

    static class Fields {
        public static final String COUNT = "count";
        public static final String MEAN = "mean";
        public static final String VARIANCE = "variance";
        public static final String SKEWNESS = "skewness";
        public static final String KURTOSIS = "kurtosis";
        public static final String COVARIANCE = "covariance";
        public static final String CORRELATION = "correlation";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (results != null) {
            for (String fieldName : results.getFieldCounts().keySet()) {
                builder.startObject(fieldName);
                // count
                builder.field(Fields.COUNT, results.getFieldCount(fieldName));
                // mean
                builder.field(Fields.MEAN, results.getMean(fieldName));
                // variance
                builder.field(Fields.VARIANCE, results.getVariance(fieldName));
                // skewness
                builder.field(Fields.SKEWNESS, results.getSkewness(fieldName));
                // kurtosis
                builder.field(Fields.KURTOSIS, results.getKurtosis(fieldName));
                // covariance
                builder.startObject("covariance_correlation");
                for (String fieldB : results.getFieldCounts().keySet()) {
                    if (fieldB.equals(fieldName) == false) {
                        builder.startObject(fieldB);
                        builder.field(Fields.COVARIANCE, results.getCovariance(fieldName, fieldB));
                        builder.field(Fields.CORRELATION, results.getCorrelation(fieldName, fieldB));
                        builder.endObject();
                    }
                }
                builder.endObject();
                builder.endObject();
            }
        }
        return builder;
    }

    private XContentBuilder doXContentBody(XContentBuilder builder, String field, Map fieldVals) throws IOException {
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
            if (results == null) {
                results = MultiFieldStatsResults.EMPTY();
            }
            switch (coordinate) {
                case "counts":
                    return results.getFieldCounts();
                case "means":
                    return results.getMeans();
                case "variances":
                    return results.getVariances();
                case "skewness":
                    return results.getSkewness();
                case "kurtosis":
                    return results.getKurtosis();
                case "covariance":
                    return results.getCovariances();
                case "correlation":
                    return results.getCorrelations();
                default:
                    throw new IllegalArgumentException("Found unknown path element [" + coordinate + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // write running stats
        if (stats == null || stats.docCount == 0) {
            out.writeVLong(0);
        } else {
            out.writeVLong(stats.docCount);
            stats.writeTo(out);
        }

        // write results
        if (results == null || results.getDocCount() == 0) {
            out.writeVLong(0);
        } else {
            out.writeVLong(results.getDocCount());
            results.writeTo(out);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        // read stats count
        final long statsCount = in.readVLong();
        if (statsCount > 0) {
            stats = new RunningStats();
            stats.docCount = statsCount;
            stats.readFrom(in);
        }

        // read count
        final long count = in.readVLong();
        if (count > 0) {
            results = new MultiFieldStatsResults(in);
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // merge stats across all shards
        aggregations.removeIf(p -> ((InternalMultiFieldStats)p).stats == null);

        // return empty result iff all stats are null
        if (aggregations.isEmpty()) {
            return new InternalMultiFieldStats(name, 0, null, MultiFieldStatsResults.EMPTY(), pipelineAggregators(), getMetaData());
        }

        RunningStats runningStats = ((InternalMultiFieldStats) aggregations.get(0)).stats;
        for (int i=1; i < aggregations.size(); ++i) {
            runningStats.merge(((InternalMultiFieldStats) aggregations.get(i)).stats);
        }
        MultiFieldStatsResults results = new MultiFieldStatsResults(stats);

        return new InternalMultiFieldStats(name, results.getDocCount(), runningStats, results, pipelineAggregators(), getMetaData());
    }
}
