/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper.datageneration;

import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class AggregateMetricDoubleDataSourceHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.AggregateMetricDoubleGenerator handle(DataSourceRequest.AggregateMetricDoubleGenerator request) {
        return new DataSourceResponse.AggregateMetricDoubleGenerator(() -> {
            var metricContainer = new HashMap<String, Number>();

            // min and max must make sense - max has to be gte min
            double min = ESTestCase.randomDoubleBetween(-Double.MAX_VALUE, 1_000_000_000, false);
            double max = ESTestCase.randomDoubleBetween(min, Double.MAX_VALUE, true);

            metricContainer.put("min", min);
            metricContainer.put("max", max);
            metricContainer.put("sum", ESTestCase.randomDouble());
            metricContainer.put("value_count", ESTestCase.randomIntBetween(1, Integer.MAX_VALUE));

            return metricContainer;
        });
    }

    @Override
    public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        if (request.fieldType().equals("aggregate_metric_double") == false) {
            return null;
        }

        return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
            var map = new HashMap<String, Object>();

            List<AggregateMetricDoubleFieldMapper.Metric> metrics = ESTestCase.randomNonEmptySubsetOf(
                Arrays.asList(AggregateMetricDoubleFieldMapper.Metric.values())
            );

            map.put("metrics", metrics.stream().map(Enum::toString).toList());
            map.put("default_metric", metrics.get(ESTestCase.randomIntBetween(0, metrics.size() - 1)));

            if (ESTestCase.randomBoolean()) {
                map.put("ignore_malformed", ESTestCase.randomBoolean());
            }

            return map;
        });
    }

    @Override
    public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        if (request.fieldType().equals("aggregate_metric_double") == false) {
            return null;
        }

        return new DataSourceResponse.FieldDataGenerator(new AggregateMetricDoubleFieldDataGenerator(request.dataSource()));
    }
}
