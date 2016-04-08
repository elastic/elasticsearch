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
package org.elasticsearch.search.aggregations.metrics.stats;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * Statistics over a set of values (either aggregated over field data or scripts)
 */
public interface Stats extends NumericMetricsAggregation.MultiValue {

    /**
     * @return The number of values that were aggregated.
     */
    long getCount();

    /**
     * @return The minimum value of all aggregated values.
     */
    double getMin();

    /**
     * @return The maximum value of all aggregated values.
     */
    double getMax();

    /**
     * @return The avg value over all aggregated values.
     */
    double getAvg();

    /**
     * @return The sum of aggregated values.
     */
    double getSum();

    /**
     * @return The number of values that were aggregated as a String.
     */
    String getCountAsString();

    /**
     * @return The minimum value of all aggregated values as a String.
     */
    String getMinAsString();

    /**
     * @return The maximum value of all aggregated values as a String.
     */
    String getMaxAsString();

    /**
     * @return The avg value over all aggregated values as a String.
     */
    String getAvgAsString();

    /**
     * @return The sum of aggregated values as a String.
     */
    String getSumAsString();

}
