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

package org.elasticsearch.search.aggregations.metrics;

/**
 * An aggregation that approximates the median absolute deviation of a numeric field
 *
 * @see <a href="https://en.wikipedia.org/wiki/Median_absolute_deviation">https://en.wikipedia.org/wiki/Median_absolute_deviation</a>
 */
public interface MedianAbsoluteDeviation extends NumericMetricsAggregation.SingleValue {

    /**
     * Returns the median absolute deviation statistic computed for this aggregation
     */
    double getMedianAbsoluteDeviation();
}
