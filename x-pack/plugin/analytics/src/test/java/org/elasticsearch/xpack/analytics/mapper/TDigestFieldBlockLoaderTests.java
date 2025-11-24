/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Types;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TDigestFieldBlockLoaderTests extends BlockLoaderTestCase {

    public TDigestFieldBlockLoaderTests(Params params) {
        super(TDigestFieldMapper.CONTENT_TYPE, List.of(DATA_SOURCE_HANDLER), params);
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Before
    public void setup() {
        assumeTrue("Only when exponential_histogram feature flag is enabled", TDigestFieldMapper.TDIGEST_FIELD_MAPPER.isEnabled());
    }

    private static DataSourceHandler DATA_SOURCE_HANDLER = new DataSourceHandler() {

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            // tdigest does not support multiple values in a document so we can't have object arrays
            return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
        }

        @Override
        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            if (request.fieldType().equals(TDigestFieldMapper.CONTENT_TYPE) == false) {
                return null;
            }

            return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                var map = new HashMap<String, Object>();
                if (ESTestCase.randomBoolean()) {
                    map.put("ignore_malformed", ESTestCase.randomBoolean());
                    map.put("compression", randomDoubleBetween(1.0, 1000.0, true));
                    map.put("digest_type", randomFrom(TDigestState.Type.values()));
                }
                return map;
            });
        }

        @Override
        public DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
            if (request.fieldType().equals(TDigestFieldMapper.CONTENT_TYPE) == false) {
                return null;
            }
            return new DataSourceResponse.FieldDataGenerator(
                mapping -> TDigestFieldMapperTests.generateRandomFieldValues(randomIntBetween(0, 1_000))
            );
        }
    };

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are not supported
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        Map<String, Object> valueAsMap = Types.forciblyCast(value);
        List<Double> centroids = Types.forciblyCast(valueAsMap.get("centroids"));
        List<Long> counts = Types.forciblyCast(valueAsMap.get("counts"));
        BytesStreamOutput streamOutput = new BytesStreamOutput();

        long totalCount = 0;

        // TODO - refactor this, it's duplicated from the parser
        for (int i = 0; i < centroids.size(); i++) {
            long count = counts.get(i);
            totalCount += count;
            // we do not add elements with count == 0
            try {
                if (count > 0) {
                    streamOutput.writeVLong(count);
                    streamOutput.writeDouble(centroids.get(i));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        long finalTotalCount = totalCount;
        return Map.of(
            "min",
            valueAsMap.get("min"),
            "max",
            valueAsMap.get("max"),
            "sum",
            valueAsMap.get("sum"),
            "value_count",
            finalTotalCount,
            "encoded_digest",
            streamOutput.bytes().toBytesRef()
        );
    }
}
