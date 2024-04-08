/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final pearson product coefficient
 * based on these descriptive stats
 */
class MatrixStatsResults implements Writeable {
    /** object holding results - computes results in place */
    protected final RunningStats results;
    /** pearson product correlation coefficients */
    protected final Map<String, HashMap<String, Double>> correlation;

    /** Base ctor */
    MatrixStatsResults() {
        results = new RunningStats();
        this.correlation = new HashMap<>();
    }

    /** creates and computes result from provided stats */
    MatrixStatsResults(RunningStats stats) {
        this.results = stats.clone();
        this.correlation = new HashMap<>();
        this.compute();
    }

    /** creates and computes the result from the provided stats, scaling as necessary given the sampling context */
    MatrixStatsResults(RunningStats stats, SamplingContext samplingContext) {
        this.results = stats.clone();
        this.correlation = new HashMap<>();
        this.compute();
        // Note: it is important to scale counts AFTER compute as scaling before could introduce bias
        this.results.docCount = samplingContext.scaleUp(this.results.docCount);
        for (String field : this.results.counts.keySet()) {
            this.results.counts.computeIfPresent(field, (k, v) -> samplingContext.scaleUp(v));
        }
    }

    /** creates a results object from the given stream */
    @SuppressWarnings("unchecked")
    protected MatrixStatsResults(StreamInput in) {
        try {
            results = new RunningStats(in);
            correlation = (Map<String, HashMap<String, Double>>) in.readGenericValue();
        } catch (IOException e) {
            throw new ElasticsearchException("Error trying to create multifield_stats results from stream input", e);
        }
    }

    /** Marshalls MatrixStatsResults */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall results
        results.writeTo(out);
        // marshall correlation
        out.writeGenericValue(correlation);
    }

    /** return document count */
    public final long getDocCount() {
        return results.docCount;
    }

    /** return the field counts - not public, used for getProperty() */
    protected Map<String, Long> getFieldCounts() {
        return Collections.unmodifiableMap(results.counts);
    }

    /** return the fied count for the requested field */
    public long getFieldCount(String field) {
        if (results.counts.containsKey(field) == false) {
            return 0;
        }
        return results.counts.get(field);
    }

    /** return the means - not public, used for getProperty() */
    protected Map<String, Double> getMeans() {
        return Collections.unmodifiableMap(results.means);
    }

    /** return the mean for the requested field */
    public double getMean(String field) {
        checkField(field, results.means);
        return results.means.get(field);
    }

    /** return the variances - not public, used for getProperty() */
    protected Map<String, Double> getVariances() {
        return Collections.unmodifiableMap(results.variances);
    }

    /** return the variance for the requested field */
    public double getVariance(String field) {
        checkField(field, results.variances);
        return results.variances.get(field);
    }

    /** return the skewness - not public, used for getProperty() */
    protected Map<String, Double> getSkewness() {
        return Collections.unmodifiableMap(results.skewness);
    }

    /** return the skewness for the requested field */
    public double getSkewness(String field) {
        checkField(field, results.skewness);
        return results.skewness.get(field);
    }

    /** return the kurtosis */
    protected Map<String, Double> getKurtosis() {
        return Collections.unmodifiableMap(results.kurtosis);
    }

    /** return the kurtosis for the requested field */
    public double getKurtosis(String field) {
        checkField(field, results.kurtosis);
        return results.kurtosis.get(field);
    }

    /** return the covariances as a map - not public, used for getProperty() */
    protected Map<String, HashMap<String, Double>> getCovariances() {
        return Collections.unmodifiableMap(results.covariances);
    }

    /** return the covariance between two fields */
    public double getCovariance(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            checkField(fieldX, results.variances);
            return results.variances.get(fieldX);
        }
        return getValFromUpperTriangularMatrix(results.covariances, fieldX, fieldY);
    }

    /** return the correlations as a map - not public, used for getProperty() */
    protected Map<String, HashMap<String, Double>> getCorrelations() {
        return Collections.unmodifiableMap(correlation);
    }

    /** return the correlation coefficient between two fields */
    public Double getCorrelation(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return 1.0;
        }
        return getValFromUpperTriangularMatrix(correlation, fieldX, fieldY);
    }

    /** return the value for two fields in an upper triangular matrix, regardless of row col location. */
    static <M extends Map<String, Double>> double getValFromUpperTriangularMatrix(Map<String, M> map, String fieldX, String fieldY) {
        // for the co-value to exist, one of the two (or both) fields has to be a row key
        if (map.containsKey(fieldX) == false && map.containsKey(fieldY) == false) {
            throw new IllegalArgumentException("neither field " + fieldX + " nor " + fieldY + " exist");
        } else if (map.containsKey(fieldX)) {
            // fieldX exists as a row key
            if (map.get(fieldX).containsKey(fieldY)) {
                // fieldY exists as a col key to fieldX
                return map.get(fieldX).get(fieldY);
            } else {
                // otherwise fieldX is the col key to fieldY
                return map.get(fieldY).get(fieldX);
            }
        } else if (map.containsKey(fieldY)) {
            // fieldX did not exist as a row key, it must be a col key
            return map.get(fieldY).get(fieldX);
        }
        throw new IllegalArgumentException("Coefficient not computed between fields: " + fieldX + " and " + fieldY);
    }

    private static void checkField(String field, Map<String, ?> map) {
        if (field == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }
        if (map.containsKey(field) == false) {
            throw new IllegalArgumentException("field " + field + " does not exist");
        }
    }

    /** Computes final covariance, variance, and correlation */
    private void compute() {
        final double nM1 = results.docCount - 1D;
        // compute final skewness and kurtosis
        for (String fieldName : results.means.keySet()) {
            final double var = results.variances.get(fieldName);
            // update skewness
            results.skewness.put(fieldName, Math.sqrt(results.docCount) * results.skewness.get(fieldName) / Math.pow(var, 1.5D));
            // update kurtosis
            results.kurtosis.put(fieldName, (double) results.docCount * results.kurtosis.get(fieldName) / (var * var));
            // update variances
            results.variances.put(fieldName, results.variances.get(fieldName) / nM1);
        }

        // compute final covariances and correlation
        double cor;
        for (Map.Entry<String, HashMap<String, Double>> row : results.covariances.entrySet()) {
            final String rowName = row.getKey();
            final HashMap<String, Double> covRow = row.getValue();
            final HashMap<String, Double> corRow = new HashMap<>();
            for (Map.Entry<String, Double> col : covRow.entrySet()) {
                final String colName = col.getKey();
                // update covariance
                covRow.put(colName, covRow.get(colName) / nM1);
                // update correlation
                // if there is no variance in the data then correlation is NaN
                if (results.variances.get(rowName) == 0d || results.variances.get(colName) == 0d) {
                    cor = Double.NaN;
                } else {
                    final double corDen = Math.sqrt(results.variances.get(rowName)) * Math.sqrt(results.variances.get(colName));
                    cor = covRow.get(colName) / corDen;
                }
                corRow.put(colName, cor);
            }
            results.covariances.put(rowName, covRow);
            correlation.put(rowName, corRow);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatrixStatsResults that = (MatrixStatsResults) o;
        return Objects.equals(results, that.results) && Objects.equals(correlation, that.correlation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, correlation);
    }
}
