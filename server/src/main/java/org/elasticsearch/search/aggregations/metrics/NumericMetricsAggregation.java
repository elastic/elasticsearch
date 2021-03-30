/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
