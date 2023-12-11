/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.collect.Iterators;

import java.util.Iterator;

abstract class ParsedPercentileRanks extends ParsedPercentiles implements PercentileRanks {

    @Override
    public double percent(double value) {
        return getPercentile(value);
    }

    @Override
    public String percentAsString(double value) {
        return getPercentileAsString(value);
    }

    @Override
    public double value(String name) {
        return percent(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return percentiles.keySet().stream().map(Object::toString).toList();
    }

    @Override
    public Iterator<Percentile> iterator() {
        return Iterators.map(super.iterator(), percentile -> new Percentile(percentile.value(), percentile.percent()));
    }
}
