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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final pearson product coefficient
 * based on these descriptive stats
 *
 * @internal
 */
class MultiFieldStatsResults implements Streamable {
    /** count of observations (same number of observations per field) */
    protected long docCount;
    /** per field sum of observations */
    protected HashMap<String, Double> fieldSum;
    /** counts */
    protected HashMap<String, Long> counts;
    /** mean values */
    protected HashMap<String, Double> means;
    /** variance values */
    protected HashMap<String, Double> variances;
    /** skewness values */
    protected HashMap<String, Double> skewness;
    /** kurtosis values */
    protected HashMap<String, Double> kurtosis;
    /** covariance values */
    protected HashMap<String, HashMap<String, Double>> covariances;
    /** pearson product correlation coefficients */
    protected HashMap<String, HashMap<String, Double>> correlation;

    /** cache flag to avoid recompute if nothing has been added */
    private boolean dirty;

    /**
     * Base ctor
     */
    public MultiFieldStatsResults() {
        docCount = 0;
        dirty = true;
        counts = new HashMap<>();
        fieldSum = new HashMap<>();
        means = new HashMap<>();
        skewness = new HashMap<>();
        kurtosis = new HashMap<>();
        covariances = new HashMap<>();
        variances = new HashMap<>();
    }

    /**
     * Returns covariance between two fields
     */
    public double getCovariance(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return variances.get(fieldX);
        } else if (variances.containsKey(fieldX) == false && variances.containsKey(fieldY) == false) {
            return Double.NaN;
        }
        return getValFromUpperTriangularMatrix(covariances, fieldX, fieldY);
    }

    /**
     * Returns the correlation coefficient between two fields
     */
    public double getCorrelation(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return 1.0;
        } else if (correlation.containsKey(fieldX) == false && correlation.containsKey(fieldY) == false) {
            return Double.NaN;
        }
        return getValFromUpperTriangularMatrix(correlation, fieldX, fieldY);
    }

    /**
     * Gets the value for two fields in an upper triangular matrix, regardless of row col location.
     */
    private double getValFromUpperTriangularMatrix(HashMap<String, HashMap<String, Double>> map, String fieldX, String fieldY) {
        if (map.containsKey(fieldX)) {
            if (map.get(fieldX).containsKey(fieldY)) {
                return map.get(fieldX).get(fieldY);
            } else {
                return map.get(fieldY).get(fieldX);
            }
        } else if (map.containsKey(fieldY)) {
            if (map.get(fieldY).containsKey(fieldX)) {
                return map.get(fieldY).get(fieldX);
            } else {
                return map.get(fieldX).get(fieldY);
            }
        }
        throw new IllegalArgumentException("Coefficient not computed between fields: " + fieldX + " and " + fieldY);
    }

    /**
     * adds a documents fields to the running statistics
     **/
    public void add(Map<String, Double> docFields) {
        // increment count
        docCount++;

        // update total, mean, and variance
        String fieldName;
        double fieldValue;
        final HashMap<String, Double> delta = new HashMap<>();
        for (Map.Entry<String, Double> field : docFields.entrySet()) {
            fieldName = field.getKey();
            fieldValue = field.getValue();

            // update counts
            counts.put(fieldName, 1 + (counts.containsKey(fieldName) ? counts.get(fieldName) : 0));

            // update running sum
            fieldSum.put(fieldName, fieldValue + (fieldSum.containsKey(fieldName) ? fieldSum.get(fieldName) : 0));

            // update running deltas
            delta.put(fieldName, fieldValue * docCount - fieldSum.get(fieldName));

            // update running mean, variance, skewness, kurtosis
            if (means.containsKey(fieldName) == true) {
                // update running means
                final double prevMean = means.get(fieldName);
                final double d = fieldValue - prevMean;
                final double dn = d / docCount;
                final double dn2 = dn * dn;
                final double term1 = d * dn * (docCount - 1);
                final double m2 = variances.get(fieldName);
                means.put(fieldName, prevMean + d / docCount);
                variances.put(fieldName, variances.get(fieldName) + term1);
                final double m3 = skewness.get(fieldName);
                skewness.put(fieldName, m3 + (term1 * dn * (docCount - 2D) - 3D * dn * m2));
                final double newKurt = term1 * dn2 * (docCount * docCount - 3D * docCount + 3D) + 6D * dn2 * m2 - 4D * dn * m3;
                kurtosis.put(fieldName, kurtosis.get(fieldName) + newKurt);
            } else {
                means.put(fieldName, fieldValue);
                variances.put(fieldName, 0.0);
                skewness.put(fieldName, 0.0);
                kurtosis.put(fieldName, 0.0);
            }
        }

        // deep copy of hash keys (field names)
        ArrayList<String> cFieldNames = new ArrayList(docFields.keySet());
        for (Map.Entry<String, Double> field : docFields.entrySet()) {
            fieldName = field.getKey();
            cFieldNames.remove(fieldName);
            // update running covariances
            final double dR = delta.get(fieldName);
            HashMap<String, Double> cFieldVals = (covariances.get(fieldName) != null) ? covariances.get(fieldName) : new HashMap<>();
            for (String cFieldName : cFieldNames) {
                if (cFieldVals.containsKey(cFieldName) == true) {
                    final double newVal = cFieldVals.get(cFieldName) + 1.0/(docCount * (docCount - 1.0)) * dR * delta.get(cFieldName);
                    cFieldVals.put(cFieldName, newVal);
                } else {
                    cFieldVals.put(cFieldName, 0.0);
                }
            }
            if (cFieldVals.size() > 0) {
                covariances.put(fieldName, cFieldVals);
            }
        }
        dirty = true;
    }

    /**
     * Merges the descriptive statistics of a second data set (e.g., per shard)
     */
    public void merge(MultiFieldStatsResults other) {
        // update cache flag
        dirty = true;
        final double countA = docCount;
        final double countB = other.docCount;
        // merge count
        docCount += other.docCount;

        final HashMap<String, Double> delta = new HashMap<>();
        // across fields
        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            final double meanA = means.get(fieldName);
            final double varA = variances.get(fieldName);
            final double skewA = skewness.get(fieldName);
            final double kurtA = kurtosis.get(fieldName);
            final double meanB = other.means.get(fieldName);
            final double varB = other.variances.get(fieldName);
            final double skewB = other.skewness.get(fieldName);
            final double kurtB = other.kurtosis.get(fieldName);

            // merge counts of two sets
            counts.put(fieldName, counts.get(fieldName) + other.counts.get(fieldName));

            // merge means of two sets
            final double newMean = (countA * means.get(fieldName) + countB * other.means.get(fieldName)) / (countA + countB);
            means.put(fieldName, newMean);

            // merge deltas
            delta.put(fieldName, other.fieldSum.get(fieldName) / countB - fieldSum.get(fieldName) / countA);

            // merge totals
            fieldSum.put(fieldName, fieldSum.get(fieldName) + other.fieldSum.get(fieldName));

            // merge variances, skewness, and kurtosis of two sets
            final double d = meanB - meanA;
            final double d2 = d * d;
            final double d3 = d * d2;
            final double d4 = d2 * d2;
            // variance
            variances.put(fieldName, varA + varB + d2 * countA * other.docCount / docCount);
            // skeewness
            final double newSkew = skewA + skewB + d3 * countA * countB * (countA - countB) / (docCount * docCount);
            skewness.put(fieldName, newSkew + 3D * d * (countA * varB - countB * varA) / docCount);
            // kurtosis
            double nk1 = kurtA + kurtB + d4 * countA * countB * (countA * countA - countA * countB + countB * countB) /
                (docCount * docCount * docCount);
            nk1 += 6D * d2 * (countA * countA * varB + countB * countB * varA) / (docCount * docCount)
                + 4D * d * (countA * skewB - countB * skewA) / docCount;
            kurtosis.put(fieldName, nk1);
        }

        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            // merge covariances of two sets
            final double f = countA * other.docCount / (countA + other.docCount);
            final double dR = delta.get(fieldName);
            // merge covariances
            if (covariances.containsKey(fieldName)) {
                HashMap<String, Double> cFieldVals = covariances.get(fieldName);
                for (String cFieldName : cFieldVals.keySet()) {
                    double newVal = cFieldVals.get(cFieldName);
                    if (other.covariances.containsKey(fieldName) && other.covariances.get(fieldName).containsKey(cFieldName)) {
                        newVal += other.covariances.get(fieldName).get(cFieldName) + f * dR * delta.get(cFieldName);
                    } else {
                        newVal += other.covariances.get(cFieldName).get(fieldName) + f * dR * delta.get(cFieldName);
                    }
                    cFieldVals.put(cFieldName, newVal);
                }
                covariances.put(fieldName, cFieldVals);
            }
        }
        dirty = true;
    }


    /**
     * Computes final covariance, variance, and correlation
     */
    public void computeStats() {
        // return if we haven't added anything
        if (dirty == false) {
            return;
        }

        // compute covariance and correlation
        if (correlation == null) {
            correlation = new HashMap<>();
        }

        final double nM1 = docCount - 1D;
        // compute final skewness and kurtosis
        for (String fieldName : means.keySet()) {
            final double var = variances.get(fieldName);
            // update skewness
            skewness.put(fieldName, Math.sqrt(docCount) * skewness.get(fieldName) / Math.pow(var, 1.5D));
            // update kurtosis
            kurtosis.put(fieldName, (double)docCount * kurtosis.get(fieldName) / (var * var));
            // update variances
            variances.put(fieldName, variances.get(fieldName) / nM1);
        }

        // compute final covariances and correlation
        for (Map.Entry<String, HashMap<String, Double>> row : covariances.entrySet()) {
            final String rowName = row.getKey();
            final HashMap<String, Double> covRow = row.getValue();
            final HashMap<String, Double> corRow = new HashMap<>();
            for (Map.Entry<String, Double> col : covRow.entrySet()) {
                final String colName = col.getKey();
                // update covariance
                covRow.put(colName, covRow.get(colName) / nM1);
                // update correlation
                final double corDen = Math.sqrt(variances.get(rowName)) * Math.sqrt(variances.get(colName));
                corRow.put(colName, covRow.get(colName) / corDen);
            }
            covariances.put(rowName, covRow);
            correlation.put(rowName, corRow);
        }
        dirty = false;
    }

    /**
     * Unmarshalls MultiFieldStatsResults
     * NOTE: correlation doesn't need to be serialized since its never passed between shards
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        // read fieldSum
        if (in.readBoolean()) {
            fieldSum = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            fieldSum = null;
        }
        // read correlation
        if (in.readBoolean()) {
            correlation = (HashMap<String, HashMap<String, Double>>) (in.readGenericValue());
        } else {
            correlation = null;
        }
        // counts
        if (in.readBoolean()) {
            counts = (HashMap<String, Long>)(in.readGenericValue());
        } else {
            counts = null;
        }
        // means
        if (in.readBoolean()) {
            means = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            means = null;
        }
        // variances
        if (in.readBoolean()) {
            variances = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            variances = null;
        }
        // skewness
        if (in.readBoolean()) {
            skewness = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            skewness = null;
        }
        // kurtosis
        if (in.readBoolean()) {
            kurtosis = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            kurtosis = null;
        }
        // read covariances
        if (in.readBoolean()) {
            covariances = (HashMap<String, HashMap<String, Double>>) (in.readGenericValue());
        } else {
            covariances = null;
        }
    }

    /**
     * Marshalls MultiFieldStatsResults
     * NOTE: correlation doesn't need to be serialized since its never passed between shards
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall fieldSum
        if (fieldSum != null) {
            out.writeBoolean(true);
            out.writeGenericValue(fieldSum);
        } else {
            out.writeBoolean(false);
        }
        // marshall correlation
        if (correlation != null) {
            out.writeBoolean(true);
            out.writeGenericValue(correlation);
        } else {
            out.writeBoolean(false);
        }
        // counts
        if (counts != null) {
            out.writeBoolean(true);
            out.writeGenericValue(counts);
        } else {
            out.writeBoolean(false);
        }
        // mean
        if (means != null) {
            out.writeBoolean(true);
            out.writeGenericValue(means);
        } else {
            out.writeBoolean(false);
        }
        // variances
        if (variances != null) {
            out.writeBoolean(true);
            out.writeGenericValue(variances);
        } else {
            out.writeBoolean(false);
        }
        // skewness
        if (skewness != null) {
            out.writeBoolean(true);
            out.writeGenericValue(skewness);
        } else {
            out.writeBoolean(false);
        }
        // kurtosis
        if (kurtosis != null) {
            out.writeBoolean(true);
            out.writeGenericValue(kurtosis);
        } else {
            out.writeBoolean(false);
        }
        // covariances
        if (covariances != null) {
            out.writeBoolean(true);
            out.writeGenericValue(covariances);
        } else {
            out.writeBoolean(false);
        }
    }
}
