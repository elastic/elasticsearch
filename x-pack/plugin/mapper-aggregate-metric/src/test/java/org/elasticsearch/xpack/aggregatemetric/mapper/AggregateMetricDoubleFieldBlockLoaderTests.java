/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.aggregatemetric.mapper.datageneration.AggregateMetricDoubleDataSourceHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AggregateMetricDoubleFieldBlockLoaderTests extends BlockLoaderTestCase {
    public AggregateMetricDoubleFieldBlockLoaderTests(Params params) {
        super("aggregate_metric_double", List.of(new AggregateMetricDoubleDataSourceHandler(), new DataSourceHandler() {
            @Override
            public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
                // aggregate_metric_double does not support multiple values in a document so we can't have object arrays
                return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
            }
        }), params);
    }

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are noop for aggregate_metric_double.
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        if (value instanceof Map<?, ?> map) {
            var expected = new HashMap<String, Object>(map.size());

            // put explicit `null` for metrics that are not present, this is how the block looks like
            Arrays.stream(AggregateMetricDoubleFieldMapper.Metric.values())
                .map(AggregateMetricDoubleFieldMapper.Metric::toString)
                .sorted()
                .forEach(m -> {
                    expected.put(m, map.get(m));
                });

            return expected;
        }

        // malformed or null, return "empty row"
        return new HashMap<>() {
            {
                put("min", null);
                put("max", null);
                put("sum", null);
                put("value_count", null);
            }
        };
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AggregateMetricMapperPlugin());
    }
}
