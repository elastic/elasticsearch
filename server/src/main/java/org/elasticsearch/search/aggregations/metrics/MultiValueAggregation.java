/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;

public interface MultiValueAggregation extends Aggregation {

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
     * Return a list of all results with the specified name
     *
     * @param name of the value
     * @return list of all values formatted as string
     */
    List<String> getValuesAsStrings(String name);
}
