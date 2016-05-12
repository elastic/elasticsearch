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
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

/**
 * Computes distribution statistics over multiple fields
 */
public class InternalMatrixStats extends InternalMetricsAggregation implements MatrixStats {

    public final static Type TYPE = new Type("matrix_stats");
    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMatrixStats readResult(StreamInput in) throws IOException {
            InternalMatrixStats result = new InternalMatrixStats();
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
    protected MatrixStatsResults results;

    protected InternalMatrixStats() {
    }

    /** per shard ctor */
    protected InternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults, MatrixStatsResults results,
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
            Set<String> fieldNames = results.getFieldCounts().keySet();
            builder.field("field", fieldNames);
            builder.field(Fields.COUNT, results.getFieldCounts().values());
            builder.field(Fields.MEAN, results.getMeans().values());
            builder.field(Fields.VARIANCE, results.getVariances().values());
            builder.field(Fields.SKEWNESS, results.getSkewness().values());
            builder.field(Fields.KURTOSIS, results.getKurtosis().values());
            ArrayList<ArrayList<Double>> cov = new ArrayList<>(fieldNames.size());
            ArrayList<ArrayList<Double>> cor = new ArrayList<>(fieldNames.size());
            for (String y : fieldNames) {
                ArrayList<Double> covRow = new ArrayList<>(fieldNames.size());
                ArrayList<Double> corRow = new ArrayList<>(fieldNames.size());
                for (String x : fieldNames) {
                    covRow.add(results.getCovariance(x, y));
                    corRow.add(results.getCorrelation(x, y));
                }
                cov.add(covRow);
                cor.add(corRow);
            }
            builder.field(Fields.COVARIANCE, cov);
            builder.field(Fields.CORRELATION, cor);
        }

        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String coordinate = path.get(0);
            if (results == null) {
                results = MatrixStatsResults.EMPTY();
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
            stats = new RunningStats(in);
            stats.docCount = statsCount;
        }

        // read count
        final long count = in.readVLong();
        if (count > 0) {
            results = new MatrixStatsResults(in);
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // merge stats across all shards
        aggregations.removeIf(p -> ((InternalMatrixStats)p).stats == null);

        // return empty result iff all stats are null
        if (aggregations.isEmpty()) {
            return new InternalMatrixStats(name, 0, null, MatrixStatsResults.EMPTY(), pipelineAggregators(), getMetaData());
        }

        RunningStats runningStats = ((InternalMatrixStats) aggregations.get(0)).stats;
        for (int i=1; i < aggregations.size(); ++i) {
            runningStats.merge(((InternalMatrixStats) aggregations.get(i)).stats);
        }
        MatrixStatsResults results = new MatrixStatsResults(stats);

        return new InternalMatrixStats(name, results.getDocCount(), runningStats, results, pipelineAggregators(), getMetaData());
    }
}
