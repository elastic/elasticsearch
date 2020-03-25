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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;

/**
 * Computes distribution statistics over multiple fields
 */
public class InternalMatrixStats extends InternalAggregation implements MatrixStats {
    /** per shard stats needed to compute stats */
    private final RunningStats stats;
    /** final result */
    private final MatrixStatsResults results;

    /** per shard ctor */
    InternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults, MatrixStatsResults results,
                                  List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert count >= 0;
        this.stats = multiFieldStatsResults;
        this.results = results;
    }

    /**
     * Read from a stream.
     */
    public InternalMatrixStats(StreamInput in) throws IOException {
        super(in);
        stats = in.readOptionalWriteable(RunningStats::new);
        results = in.readOptionalWriteable(MatrixStatsResults::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(stats);
        out.writeOptionalWriteable(results);
    }

    @Override
    public String getWriteableName() {
        return MatrixStatsAggregationBuilder.NAME;
    }

    /** get the number of documents */
    @Override
    public long getDocCount() {
        if (stats == null) {
            return 0;
        }
        return stats.docCount;
    }

    /** get the number of samples for the given field. == docCount - numMissing */
    @Override
    public long getFieldCount(String field) {
        if (results == null) {
            return 0;
        }
        return results.getFieldCount(field);
    }

    /** get the mean for the given field */
    @Override
    public double getMean(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getMean(field);
    }

    /** get the variance for the given field */
    @Override
    public double getVariance(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getVariance(field);
    }

    /** get the distribution skewness for the given field */
    @Override
    public double getSkewness(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getSkewness(field);
    }

    /** get the distribution shape for the given field */
    @Override
    public double getKurtosis(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getKurtosis(field);
    }

    /** get the covariance between the two fields */
    @Override
    public double getCovariance(String fieldX, String fieldY) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getCovariance(fieldX, fieldY);
    }

    /** get the correlation between the two fields */
    @Override
    public double getCorrelation(String fieldX, String fieldY) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getCorrelation(fieldX, fieldY);
    }

    RunningStats getStats() {
        return stats;
    }

    MatrixStatsResults getResults() {
        return results;
    }

    static class Fields {
        public static final String FIELDS = "fields";
        public static final String NAME = "name";
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
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        if (results != null && results.getFieldCounts().keySet().isEmpty() == false) {
            builder.startArray(Fields.FIELDS);
            for (String fieldName : results.getFieldCounts().keySet()) {
                builder.startObject();
                // name
                builder.field(Fields.NAME, fieldName);
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
                builder.startObject(Fields.COVARIANCE);
                for (String fieldB : results.getFieldCounts().keySet()) {
                    builder.field(fieldB, results.getCovariance(fieldName, fieldB));
                }
                builder.endObject();
                // correlation
                builder.startObject(Fields.CORRELATION);
                for (String fieldB : results.getFieldCounts().keySet()) {
                    builder.field(fieldB, results.getCorrelation(fieldName, fieldB));
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String element = path.get(0);
            if (results == null) {
                return emptyMap();
            }
            switch (element) {
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
                    throw new IllegalArgumentException("Found unknown path element [" + element + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // merge stats across all shards
        List<InternalAggregation> aggs = new ArrayList<>(aggregations);
        aggs.removeIf(p -> ((InternalMatrixStats)p).stats == null);

        // return empty result iff all stats are null
        if (aggs.isEmpty()) {
            return new InternalMatrixStats(name, 0, null, new MatrixStatsResults(), pipelineAggregators(), getMetaData());
        }

        RunningStats runningStats = new RunningStats();
        for (InternalAggregation agg : aggs) {
            runningStats.merge(((InternalMatrixStats) agg).stats);
        }

        if (reduceContext.isFinalReduce()) {
            MatrixStatsResults results = new MatrixStatsResults(runningStats);
            return new InternalMatrixStats(name, results.getDocCount(), runningStats, results, pipelineAggregators(), getMetaData());
        }
        return new InternalMatrixStats(name, runningStats.docCount, runningStats, null, pipelineAggregators(), getMetaData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), stats, results);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalMatrixStats other = (InternalMatrixStats) obj;
        return Objects.equals(this.stats, other.stats) &&
            Objects.equals(this.results, other.results);
    }
}
