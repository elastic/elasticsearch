/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.search.aggregations.Aggregation;

/**
 * Interface for MatrixStats Metric Aggregation
 */
public interface MatrixStats extends Aggregation {
    /** return the total document count */
    long getDocCount();
    /** return total field count (differs from docCount if there are missing values) */
    long getFieldCount(String field);
    /** return the field mean */
    double getMean(String field);
    /** return the field variance */
    double getVariance(String field);
    /** return the skewness of the distribution */
    double getSkewness(String field);
    /** return the kurtosis of the distribution */
    double getKurtosis(String field);
    /** return the covariance between field x and field y */
    double getCovariance(String fieldX, String fieldY);
    /** return the correlation coefficient of field x and field y */
    double getCorrelation(String fieldX, String fieldY);
}
