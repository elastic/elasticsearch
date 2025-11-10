/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

class SimpleLinearRegressionWithTimeseries {
    long count;
    double sumVal;
    long sumTs;
    double sumTsVal;
    long sumTsSq;

    SimpleLinearRegressionWithTimeseries() {
        this.count = 0;
        this.sumVal = 0.0;
        this.sumTs = 0;
        this.sumTsVal = 0.0;
        this.sumTsSq = 0;
    }

    void add(long ts, double val) {
        count++;
        sumVal += val;
        sumTs += ts;
        sumTsVal += ts * val;
        sumTsSq += ts * ts;
    }

    double slope() {
        if (count <= 1) {
            return Double.NaN;
        }
        double numerator = count * sumTsVal - sumTs * sumVal;
        double denominator = count * sumTsSq - sumTs * sumTs;
        if (denominator == 0) {
            return Double.NaN;
        }
        return numerator / denominator;
    }

    double intercept() {
        if (count == 0) {
            return 0.0; // or handle as needed
        }
        var slp = slope();
        if (Double.isNaN(slp)) {
            return Double.NaN;
        }
        return (sumVal - slp * sumTs) / count;
    }

}
