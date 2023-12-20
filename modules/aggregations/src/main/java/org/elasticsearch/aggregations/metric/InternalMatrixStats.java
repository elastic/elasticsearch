/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.emptyMap;

/**
 * Computes distribution statistics over multiple fields
 */
public class InternalMatrixStats extends InternalAggregation {
    /** per shard stats needed to compute stats */
    private final RunningStats stats;
    /** final result */
    private final MatrixStatsResults results;

    /** per shard ctor */
    InternalMatrixStats(
        String name,
        long count,
        RunningStats multiFieldStatsResults,
        MatrixStatsResults results,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
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

    /** return the total document count */
    public long getDocCount() {
        if (results != null) {
            return results.getDocCount();
        }
        if (stats == null) {
            return 0;
        }
        return stats.docCount;
    }

    /** return total field count (differs from docCount if there are missing values) */
    public long getFieldCount(String field) {
        if (results == null) {
            return 0;
        }
        return results.getFieldCount(field);
    }

    /** return the field mean */
    public double getMean(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getMean(field);
    }

    /** return the field variance */
    public double getVariance(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getVariance(field);
    }

    /** return the skewness of the distribution */
    public double getSkewness(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getSkewness(field);
    }

    /** return the kurtosis of the distribution */
    public double getKurtosis(String field) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getKurtosis(field);
    }

    /** return the covariance between field x and field y */
    public double getCovariance(String fieldX, String fieldY) {
        if (results == null) {
            return Double.NaN;
        }
        return results.getCovariance(fieldX, fieldY);
    }

    /** return the correlation coefficient of field x and field y */
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
        } else if (path.size() == 2) {
            if (results == null) {
                return emptyMap();
            }
            final String field = path.get(0)
                .replaceAll("^\"", "") // remove leading "
                .replaceAll("^'", "") // remove leading '
                .replaceAll("\"$", "") // remove trailing "
                .replaceAll("'$", ""); // remove trailing '
            final String element = path.get(1);
            return switch (element) {
                case "counts" -> results.getFieldCount(field);
                case "means" -> results.getMean(field);
                case "variances" -> results.getVariance(field);
                case "skewness" -> results.getSkewness(field);
                case "kurtosis" -> results.getKurtosis(field);
                case "covariance" -> results.getCovariance(field, field);
                case "correlation" -> results.getCorrelation(field, field);
                default -> throw new IllegalArgumentException("Found unknown path element [" + element + "] in [" + getName() + "]");
            };
        } else if (path.size() == 1) {
            String element = path.get(0);
            if (results == null) {
                return emptyMap();
            }
            return switch (element) {
                case "counts" -> results.getFieldCounts();
                case "means" -> results.getMeans();
                case "variances" -> results.getVariances();
                case "skewness" -> results.getSkewness();
                case "kurtosis" -> results.getKurtosis();
                case "covariance" -> results.getCovariances();
                case "correlation" -> results.getCorrelations();
                default -> throw new IllegalArgumentException("Found unknown path element [" + element + "] in [" + getName() + "]");
            };
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        // merge stats across all shards
        List<InternalAggregation> aggs = new ArrayList<>(aggregations);
        aggs.removeIf(p -> ((InternalMatrixStats) p).stats == null);

        // return empty result iff all stats are null
        if (aggs.isEmpty()) {
            return new InternalMatrixStats(name, 0, null, new MatrixStatsResults(), getMetadata());
        }

        RunningStats runningStats = new RunningStats();
        for (InternalAggregation agg : aggs) {
            final Set<String> missingFields = runningStats.missingFieldNames(((InternalMatrixStats) agg).stats);
            if (missingFields.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Aggregation ["
                        + agg.getName()
                        + "] all fields must exist in all indices, but some indices are missing these fields ["
                        + String.join(", ", new TreeSet<>(missingFields))
                        + "]"
                );
            }
            runningStats.merge(((InternalMatrixStats) agg).stats);
        }

        if (reduceContext.isFinalReduce()) {
            MatrixStatsResults matrixStatsResults = new MatrixStatsResults(runningStats);
            return new InternalMatrixStats(name, matrixStatsResults.getDocCount(), runningStats, matrixStatsResults, getMetadata());
        }
        return new InternalMatrixStats(name, runningStats.docCount, runningStats, null, getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalMatrixStats(
            name,
            samplingContext.scaleUp(getDocCount()),
            stats,
            new MatrixStatsResults(stats, samplingContext),
            getMetadata()
        );
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
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
        return Objects.equals(this.stats, other.stats) && Objects.equals(this.results, other.results);
    }
}
