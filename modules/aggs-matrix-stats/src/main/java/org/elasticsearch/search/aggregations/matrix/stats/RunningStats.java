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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final correlation and covariance stats
 * based on these descriptive stats. This single pass, parallel approach is based on:
 *
 * http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
 *
 * @internal
 */
public class RunningStats implements Streamable, Cloneable {
    /** count of observations (same number of observations per field) */
    protected long docCount = 0;
    /** per field sum of observations */
    protected HashMap<String, Double> fieldSum;
    /** counts */
    protected HashMap<String, Long> counts;
    /** mean values (first moment) */
    protected HashMap<String, Double> means;
    /** variance values (second moment) */
    protected HashMap<String, Double> variances;
    /** skewness values (third moment) */
    protected HashMap<String, Double> skewness;
    /** kurtosis values (fourth moment) */
    protected HashMap<String, Double> kurtosis;
    /** covariance values */
    protected HashMap<String, HashMap<String, Double>> covariances;

    private RunningStats() {
        init();
    }

    /** Ctor to create an instance of running statistics */
    public RunningStats(StreamInput in) throws IOException {
        this();
        this.readFrom(in);
    }

    public RunningStats(Map<String, Double> doc) {
        if (doc != null && doc.isEmpty() == false) {
            init();
            this.add(doc);
        }
    }

    private void init() {
        counts = new HashMap<>();
        fieldSum = new HashMap<>();
        means = new HashMap<>();
        skewness = new HashMap<>();
        kurtosis = new HashMap<>();
        covariances = new HashMap<>();
        variances = new HashMap<>();
    }

    /** create an empty instance */
    protected static RunningStats EMPTY() {
        return new RunningStats();
    }

    /** updates running statistics with a documents field values **/
    public void add(Map<String, Double> doc) {
        if (doc == null || doc.isEmpty()) {
            return;
        }

        // update total, mean, and variance
        ++docCount;
        String fieldName;
        double fieldValue;
        double m1, m2, m3, m4;  // moments
        double d, dn, dn2, t1;
        final HashMap<String, Double> deltas = new HashMap<>();
        for (Map.Entry<String, Double> field : doc.entrySet()) {
            fieldName = field.getKey();
            fieldValue = field.getValue();

            // update counts
            counts.put(fieldName, 1 + (counts.containsKey(fieldName) ? counts.get(fieldName) : 0));
            // update running sum
            fieldSum.put(fieldName, fieldValue + (fieldSum.containsKey(fieldName) ? fieldSum.get(fieldName) : 0));
            // update running deltas
            deltas.put(fieldName, fieldValue * docCount - fieldSum.get(fieldName));

            // update running mean, variance, skewness, kurtosis
            if (means.containsKey(fieldName) == true) {
                // update running means
                m1 = means.get(fieldName);
                d = fieldValue - m1;
                means.put(fieldName, m1 + d / docCount);
                // update running variances
                dn = d / docCount;
                t1 = d * dn * (docCount - 1);
                m2 = variances.get(fieldName);
                variances.put(fieldName, m2 + t1);
                m3 = skewness.get(fieldName);
                skewness.put(fieldName, m3 + (t1 * dn * (docCount - 2D) - 3D * dn * m2));
                dn2 = dn * dn;
                m4 = t1 * dn2 * (docCount * docCount - 3D * docCount + 3D) + 6D * dn2 * m2 - 4D * dn * m3;
                kurtosis.put(fieldName, kurtosis.get(fieldName) + m4);
            } else {
                means.put(fieldName, fieldValue);
                variances.put(fieldName, 0.0);
                skewness.put(fieldName, 0.0);
                kurtosis.put(fieldName, 0.0);
            }
        }

        this.updateCovariance(doc, deltas);
    }

    /** Update covariance matrix */
    private void updateCovariance(final Map<String, Double> doc, final Map<String, Double> deltas) {
        // deep copy of hash keys (field names)
        ArrayList<String> cFieldNames = new ArrayList<>(doc.keySet());
        String fieldName;
        double dR, newVal;
        for (Map.Entry<String, Double> field : doc.entrySet()) {
            fieldName = field.getKey();
            cFieldNames.remove(fieldName);
            // update running covariances
            dR = deltas.get(fieldName);
            HashMap<String, Double> cFieldVals = (covariances.get(fieldName) != null) ? covariances.get(fieldName) : new HashMap<>();
            for (String cFieldName : cFieldNames) {
                if (cFieldVals.containsKey(cFieldName) == true) {
                    newVal = cFieldVals.get(cFieldName) + 1.0 / (docCount * (docCount - 1.0)) * dR * deltas.get(cFieldName);
                    cFieldVals.put(cFieldName, newVal);
                } else {
                    cFieldVals.put(cFieldName, 0.0);
                }
            }
            if (cFieldVals.size() > 0) {
                covariances.put(fieldName, cFieldVals);
            }
        }
    }

    /**
     * Merges the descriptive statistics of a second data set (e.g., per shard)
     *
     * running computations taken from: http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
     **/
    public void merge(final RunningStats other) {
        if (other == null) {
            return;
        }
        final double nA = docCount;
        final double nB = other.docCount;
        // merge count
        docCount += other.docCount;

        final HashMap<String, Double> deltas = new HashMap<>();
        double meanA, varA, skewA, kurtA, meanB, varB, skewB, kurtB;
        double d, d2, d3, d4, n2, nA2, nB2;
        double newSkew, nk;
        // across fields
        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            meanA = means.get(fieldName);
            varA = variances.get(fieldName);
            skewA = skewness.get(fieldName);
            kurtA = kurtosis.get(fieldName);
            meanB = other.means.get(fieldName);
            varB = other.variances.get(fieldName);
            skewB = other.skewness.get(fieldName);
            kurtB = other.kurtosis.get(fieldName);

            // merge counts of two sets
            counts.put(fieldName, counts.get(fieldName) + other.counts.get(fieldName));

            // merge means of two sets
            means.put(fieldName, (nA * means.get(fieldName) + nB * other.means.get(fieldName)) / (nA + nB));

            // merge deltas
            deltas.put(fieldName, other.fieldSum.get(fieldName) / nB - fieldSum.get(fieldName) / nA);

            // merge totals
            fieldSum.put(fieldName, fieldSum.get(fieldName) + other.fieldSum.get(fieldName));

            // merge variances, skewness, and kurtosis of two sets
            d = meanB - meanA;          // delta mean
            d2 = d * d;                 // delta mean squared
            d3 = d * d2;                // delta mean cubed
            d4 = d2 * d2;               // delta mean 4th power
            n2 = docCount * docCount;   // num samples squared
            nA2 = nA * nA;              // doc A num samples squared
            nB2 = nB * nB;              // doc B num samples squared
            // variance
            variances.put(fieldName, varA + varB + d2 * nA * other.docCount / docCount);
            // skeewness
            newSkew = skewA + skewB + d3 * nA * nB * (nA - nB) / n2;
            skewness.put(fieldName, newSkew + 3D * d * (nA * varB - nB * varA) / docCount);
            // kurtosis
            nk = kurtA + kurtB + d4 * nA * nB * (nA2 - nA * nB + nB2) / (n2 * docCount);
            kurtosis.put(fieldName, nk + 6D * d2 * (nA2 * varB + nB2 * varA) / n2 + 4D * d * (nA * skewB - nB * skewA) / docCount);
        }

        this.mergeCovariance(other, deltas);
    }

    /** Merges two covariance matrices */
    private void mergeCovariance(final RunningStats other, final HashMap<String, Double> deltas) {
        final double countA = docCount - other.docCount;
        double f, dR, newVal;
        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            // merge covariances of two sets
            f = countA * other.docCount / this.docCount;
            dR = deltas.get(fieldName);
            // merge covariances
            if (covariances.containsKey(fieldName)) {
                HashMap<String, Double> cFieldVals = covariances.get(fieldName);
                for (String cFieldName : cFieldVals.keySet()) {
                    newVal = cFieldVals.get(cFieldName);
                    if (other.covariances.containsKey(fieldName) && other.covariances.get(fieldName).containsKey(cFieldName)) {
                        newVal += other.covariances.get(fieldName).get(cFieldName) + f * dR * deltas.get(cFieldName);
                    } else {
                        newVal += other.covariances.get(cFieldName).get(fieldName) + f * dR * deltas.get(cFieldName);
                    }
                    cFieldVals.put(cFieldName, newVal);
                }
                covariances.put(fieldName, cFieldVals);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall fieldSum
        if (fieldSum != null) {
            out.writeBoolean(true);
            out.writeGenericValue(fieldSum);
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

    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        // read fieldSum
        if (in.readBoolean()) {
            fieldSum = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            fieldSum = null;
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

    @Override
    public RunningStats clone() throws CloneNotSupportedException {
        return (RunningStats)super.clone();
    }
}
