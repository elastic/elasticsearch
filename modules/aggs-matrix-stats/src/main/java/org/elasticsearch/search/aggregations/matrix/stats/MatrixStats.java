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
