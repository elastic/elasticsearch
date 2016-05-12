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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final pearson product coefficient
 * based on these descriptive stats
 *
 * @internal
 */
class MatrixStatsResults implements Streamable {
    /** object holding results - computes results in place */
    final RunningStats results;
    /** pearson product correlation coefficients */
    protected HashMap<String, HashMap<String, Double>> correlation;

    /** Base ctor */
    private MatrixStatsResults() {
        results = RunningStats.EMPTY();
        this.correlation = new HashMap<>();
    }

    /** creates and computes result from provided stats */
    public MatrixStatsResults(RunningStats stats) {
        try {
            this.results = stats.clone();
            this.correlation = new HashMap<>();
        } catch (CloneNotSupportedException e) {
            throw new ElasticsearchException("Error trying to create multifield_stats results", e);
        }
        this.compute();
    }

    /** creates a results object from the given stream */
    protected MatrixStatsResults(StreamInput in) {
        try {
            results = new RunningStats(in);
            this.readFrom(in);
        } catch (IOException e) {
            throw new ElasticsearchException("Error trying to create multifield_stats results from stream input", e);
        }
    }

    /** create an empty results object **/
    protected static MatrixStatsResults EMPTY() {
        return new MatrixStatsResults();
    }

    /** return document count */
    public final long getDocCount() {
        return results.docCount;
    }

    /** return the field counts */
    public Map<String, Long> getFieldCounts() {
        return Collections.unmodifiableMap(results.counts);
    }

    /** return the fied count for the requested field */
    public long getFieldCount(String field) {
        if (results.counts.containsKey(field) == false) {
            return 0;
        }
        return results.counts.get(field);
    }

    /** return the means */
    public Map<String, Double> getMeans() {
        return Collections.unmodifiableMap(results.means);
    }

    /** return the mean for the requested field */
    public Double getMean(String field) {
        return results.means.get(field);
    }

    /** return the variances */
    public Map<String, Double> getVariances() {
        return Collections.unmodifiableMap(results.variances);
    }

    /** return the variance for the requested field */
    public Double getVariance(String field) {
        return results.variances.get(field);
    }

    /** return the skewness */
    public Map<String, Double> getSkewness() {
        return Collections.unmodifiableMap(results.skewness);
    }

    /** return the skewness for the requested field */
    public Double getSkewness(String field) {
        return results.skewness.get(field);
    }

    /** return the kurtosis */
    public Map<String, Double> getKurtosis() {
        return Collections.unmodifiableMap(results.kurtosis);
    }

    /** return the kurtosis for the requested field */
    public Double getKurtosis(String field) {
        return results.kurtosis.get(field);
    }

    /** return the covariances */
    public Map<String, HashMap<String, Double>> getCovariances() {
        return Collections.unmodifiableMap(results.covariances);
    }

    /** return the covariance between two fields */
    public Double getCovariance(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return results.variances.get(fieldX);
        }
        return getValFromUpperTriangularMatrix(results.covariances, fieldX, fieldY);
    }

    public Map<String, HashMap<String, Double>> getCorrelations() {
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
    private Double getValFromUpperTriangularMatrix(HashMap<String, HashMap<String, Double>> map, String fieldX, String fieldY) {
        // for the co-value to exist, one of the two (or both) fields has to be a row key
        if (map.containsKey(fieldX) == false && map.containsKey(fieldY) == false) {
            return null;
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

    /** Computes final covariance, variance, and correlation */
    private void compute() {
        final double nM1 = results.docCount - 1D;
        // compute final skewness and kurtosis
        for (String fieldName : results.means.keySet()) {
            final double var = results.variances.get(fieldName);
            // update skewness
            results.skewness.put(fieldName, Math.sqrt(results.docCount) * results.skewness.get(fieldName) / Math.pow(var, 1.5D));
            // update kurtosis
            results.kurtosis.put(fieldName, (double)results.docCount * results.kurtosis.get(fieldName) / (var * var));
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

    /** Unmarshalls MatrixStatsResults */
    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            correlation = (HashMap<String, HashMap<String, Double>>) (in.readGenericValue());
        } else {
            correlation = null;
        }
    }

    /** Marshalls MatrixStatsResults */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall results
        results.writeTo(out);
        // marshall correlation
        if (correlation != null) {
            out.writeBoolean(true);
            out.writeGenericValue(correlation);
        } else {
            out.writeBoolean(false);
        }
    }
}
