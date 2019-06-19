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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;

public abstract class BaseInternalMatrixStats extends InternalAggregation implements MatrixStats {
    /** per shard stats needed to compute stats */
    protected final RunningStats stats;
    /** final result */
    protected final MatrixStatsResults results;

    /** per shard ctor */
    public BaseInternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults, MatrixStatsResults results,
                                   List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert count >= 0;
        this.stats = multiFieldStatsResults;
        this.results = results;
    }

    /**
     * Read from a stream.
     */
    public BaseInternalMatrixStats(StreamInput in) throws IOException {
        super(in);
        stats = in.readOptionalWriteable(RunningStats::new);
        results = readOptionalWriteableMatrixStatsResults(in);
    }

    protected MatrixStatsResults readOptionalWriteableMatrixStatsResults(StreamInput in) throws IOException {
        return in.readOptionalWriteable(MatrixStatsResults::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(stats);
        out.writeOptionalWriteable(results);
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

    public RunningStats getStats() {
        return stats;
    }

    public MatrixStatsResults getResults() {
        return results;
    }

    protected static class Fields {
        public static final String FIELDS = "fields";
        public static final String NAME = "name";
        public static final String COUNT = "count";
    }

    public abstract BaseInternalMatrixStats newInternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults,
            MatrixStatsResults results, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    public abstract MatrixStatsResults newMatrixStatsResults();
    public abstract MatrixStatsResults newMatrixStatsResults(RunningStats runningStats);

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
        aggs.removeIf(p -> ((BaseInternalMatrixStats)p).stats == null);

        // return empty result iff all stats are null
        if (aggs.isEmpty()) {
            return newInternalMatrixStats(name, 0, null, newMatrixStatsResults(), pipelineAggregators(), getMetaData());
        }

        RunningStats runningStats = new RunningStats();
        for (InternalAggregation agg : aggs) {
            runningStats.merge(((BaseInternalMatrixStats) agg).stats);
        }

        if (reduceContext.isFinalReduce()) {
            MatrixStatsResults results = newMatrixStatsResults(runningStats);
            return newInternalMatrixStats(name, results.getDocCount(), runningStats, results, pipelineAggregators(), getMetaData());
        }
        return newInternalMatrixStats(name, runningStats.docCount, runningStats, null, pipelineAggregators(), getMetaData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), stats, results);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseInternalMatrixStats)) return false;
        if (!super.equals(o)) return false;
        BaseInternalMatrixStats that = (BaseInternalMatrixStats) o;
        return Objects.equals(stats, that.stats) &&
            Objects.equals(results, that.results);
    }
}
