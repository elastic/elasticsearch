/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.core.Types;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HistogramFieldBlockLoaderTests extends BlockLoaderTestCase {

    public HistogramFieldBlockLoaderTests(Params params) {
        super(HistogramFieldMapper.CONTENT_TYPE, List.of(DATA_SOURCE_HANDLER), params);
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    private static DataSourceHandler DATA_SOURCE_HANDLER = new DataSourceHandler() {

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            // histogram does not support multiple values in a document so we can't have object arrays
            return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
        }

        @Override
        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            if (request.fieldType().equals(HistogramFieldMapper.CONTENT_TYPE) == false) {
                return null;
            }

            return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                var map = new HashMap<String, Object>();
                if (ESTestCase.randomBoolean()) {
                    map.put("ignore_malformed", ESTestCase.randomBoolean());
                }
                return map;
            });
        }

        @Override
        public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
            if (request.fieldType().equals(HistogramFieldMapper.CONTENT_TYPE) == false) {
                return null;
            }
            return new DataSourceResponse.FieldDataGenerator(mapping -> {
                List<Double> values = randomList(randomIntBetween(1, 1000), ESTestCase::randomDouble);
                values.sort(Double::compareTo);
                return Map.of(
                    "values",
                    values,
                    "counts",
                    // Note - we need the three parameter version of random list here to ensure it's always the same length as values
                    randomList(values.size(), values.size(), ESTestCase::randomNonNegativeLong)
                );
            });
        }
    };

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are not supported
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        Map<String, Object> valueAsMap = Types.forciblyCast(value);
        List<Double> bucketValues = Types.forciblyCast(valueAsMap.get("values"));
        List<Long> counts = Types.forciblyCast(valueAsMap.get("counts"));

        try {
            return HistogramFieldMapper.encodeBytesRef(bucketValues, counts);
        } catch (IOException e) {
            fail("failed to encode histogram field values");
        }
        throw new IllegalStateException("Unreachable");
    }
}
