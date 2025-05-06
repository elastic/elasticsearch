/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper.datageneration;

import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class AggregateMetricDoubleFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Map<String, Number>> metrics;
    private final Function<Supplier<Object>, Supplier<Object>> nullWrapper;
    private final Function<Supplier<Object>, Supplier<Object>> nullAndMalformedWrapper;

    public AggregateMetricDoubleFieldDataGenerator(DataSource dataSource) {
        this.metrics = dataSource.get(new DataSourceRequest.AggregateMetricDoubleGenerator()).generator();

        this.nullWrapper = dataSource.get(new DataSourceRequest.NullWrapper()).wrapper();

        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
        var malformed = dataSource.get(new DataSourceRequest.MalformedWrapper(strings::get)).wrapper();
        this.nullAndMalformedWrapper = malformed.andThen(nullWrapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping == null) {
            // this field can't be properly mapped with dynamic mapping
            return null;
        }

        // metrics returned have all metric fields but we only need those that appear in the mapping
        Supplier<Object> metricsAdjustedForMapping = () -> {
            var metric = metrics.get();

            var adjusted = new HashMap<String, Number>();
            for (var metricName : (List<String>) fieldMapping.get("metrics")) {
                adjusted.put(metricName, metric.get(metricName));
            }

            return adjusted;
        };

        if ((Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return nullAndMalformedWrapper.apply(metricsAdjustedForMapping).get();
        }

        return nullWrapper.apply(metricsAdjustedForMapping).get();
    }
}
