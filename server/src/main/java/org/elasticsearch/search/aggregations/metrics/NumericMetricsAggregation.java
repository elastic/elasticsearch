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

import org.elasticsearch.search.aggregations.Aggregation;

public interface NumericMetricsAggregation extends Aggregation {

    interface SingleValue extends NumericMetricsAggregation {

        double value();

        String getValueAsString();

    }

    interface MultiValue extends NumericMetricsAggregation {

        /**
         * Return an iterable over all value names this multi value aggregation provides.
         *
         * The iterable might be created on the fly, if you need to call this multiple times, please
         * cache the result in a variable on caller side..
         *
         * @return iterable over all value names
         */
        Iterable<String> valueNames();

        /**
         * Return the result of 1 value by name
         *
         * @param name of the value
         * @return the value
         */
        double value(String name);

    }
}
