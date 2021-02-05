/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final correlation and covariance stats
 * based on these descriptive stats. This single pass, parallel approach is based on:
 *
 * http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
 */
public class RunningStats implements Writeable, Cloneable {
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

    RunningStats() {
        init();
    }

    RunningStats(final String[] fieldNames, final double[] fieldVals) {
        if (fieldVals != null && fieldVals.length > 0) {
            init();
            this.add(fieldNames, fieldVals);
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

    /** Ctor to create an instance of running statistics */
    @SuppressWarnings("unchecked")
    public RunningStats(StreamInput in) throws IOException {
        this();
        // read doc count
        docCount = (Long)in.readGenericValue();
        // read fieldSum
        fieldSum = convertIfNeeded((Map<String, Double>)in.readGenericValue());
        // counts
        counts = convertIfNeeded((Map<String, Long>)in.readGenericValue());
        // means
        means = convertIfNeeded((Map<String, Double>)in.readGenericValue());
        // variances
        variances = convertIfNeeded((Map<String, Double>)in.readGenericValue());
        // skewness
        skewness = convertIfNeeded((Map<String, Double>)in.readGenericValue());
        // kurtosis
        kurtosis = convertIfNeeded((Map<String, Double>)in.readGenericValue());
        // read covariances
        covariances = convertIfNeeded((Map<String, HashMap<String, Double>>)in.readGenericValue());
    }

    // Convert Map to HashMap if it isn't
    private static <K, V> HashMap<K, V> convertIfNeeded(Map<K,V> map) {
        if (map instanceof HashMap) {
            return (HashMap<K, V>) map;
        } else {
            return new HashMap<>(map);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall doc count
        out.writeGenericValue(docCount);
        // marshall fieldSum
        out.writeGenericValue(fieldSum);
        // counts
        out.writeGenericValue(counts);
        // mean
        out.writeGenericValue(means);
        // variances
        out.writeGenericValue(variances);
        // skewness
        out.writeGenericValue(skewness);
        // kurtosis
        out.writeGenericValue(kurtosis);
        // covariances
        out.writeGenericValue(covariances);
    }

    /** updates running statistics with a documents field values **/
    public void add(final String[] fieldNames, final double[] fieldVals) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("Cannot add statistics without field names.");
        } else if (fieldVals == null) {
            throw new IllegalArgumentException("Cannot add statistics without field values.");
        } else if (fieldNames.length != fieldVals.length) {
            throw new IllegalArgumentException("Number of field values do not match number of field names.");
        }

        // update total, mean, and variance
        ++docCount;
        String fieldName;
        double fieldValue;
        double m1, m2, m3, m4;  // moments
        double d, dn, dn2, t1;
        final HashMap<String, Double> deltas = new HashMap<>();
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldName = fieldNames[i];
            fieldValue = fieldVals[i];

            // update counts
            counts.put(fieldName, 1 + (counts.containsKey(fieldName) ? counts.get(fieldName) : 0));
            // update running sum
            fieldSum.put(fieldName, fieldValue + (fieldSum.containsKey(fieldName) ? fieldSum.get(fieldName) : 0));
            // update running deltas
            deltas.put(fieldName, fieldValue * docCount - fieldSum.get(fieldName));

            // update running mean, variance, skewness, kurtosis
            if (means.containsKey(fieldName)) {
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

        this.updateCovariance(fieldNames, deltas);
    }

    /** Update covariance matrix */
    private void updateCovariance(final String[] fieldNames, final Map<String, Double> deltas) {
        // deep copy of hash keys (field names)
        ArrayList<String> cFieldNames = new ArrayList<>(Arrays.asList(fieldNames));
        String fieldName;
        double dR, newVal;
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldName = fieldNames[i];
            cFieldNames.remove(fieldName);
            // update running covariances
            dR = deltas.get(fieldName);
            HashMap<String, Double> cFieldVals = (covariances.get(fieldName) != null) ? covariances.get(fieldName) : new HashMap<>();
            for (String cFieldName : cFieldNames) {
                if (cFieldVals.containsKey(cFieldName)) {
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
        } else if (this.docCount == 0) {
            for (Map.Entry<String, Double> fs : other.means.entrySet()) {
                final String fieldName = fs.getKey();
                this.means.put(fieldName, fs.getValue().doubleValue());
                this.counts.put(fieldName, other.counts.get(fieldName).longValue());
                this.fieldSum.put(fieldName, other.fieldSum.get(fieldName).doubleValue());
                this.variances.put(fieldName, other.variances.get(fieldName).doubleValue());
                this.skewness.put(fieldName , other.skewness.get(fieldName).doubleValue());
                this.kurtosis.put(fieldName, other.kurtosis.get(fieldName).doubleValue());
                if (other.covariances.containsKey(fieldName)) {
                    this.covariances.put(fieldName, other.covariances.get(fieldName));
                }
                this.docCount = other.docCount;
            }
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
    private void mergeCovariance(final RunningStats other, final Map<String, Double> deltas) {
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
    public RunningStats clone() {
        try {
            return (RunningStats) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new ElasticsearchException("Error trying to create a copy of RunningStats");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RunningStats that = (RunningStats) o;
        return docCount == that.docCount &&
            Objects.equals(fieldSum, that.fieldSum) &&
            Objects.equals(counts, that.counts) &&
            Objects.equals(means, that.means) &&
            Objects.equals(variances, that.variances) &&
            Objects.equals(skewness, that.skewness) &&
            Objects.equals(kurtosis, that.kurtosis) &&
            Objects.equals(covariances, that.covariances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docCount, fieldSum, counts, means, variances, skewness, kurtosis, covariances);
    }
}
