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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * An aggregation that computes approximate percentiles.
 */
public interface Percentiles extends NumericMetricsAggregation.MultiValue, Iterable<Percentile> {

    public static final String TYPE_NAME = "percentiles";

    /**
     * Return the value associated with the provided percentile.
     */
    double percentile(double percent);

    /**
     * Return the value associated with the provided percentile as a String.
     */
    String percentileAsString(double percent);

}
