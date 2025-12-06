/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Types;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramParser;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExponentialHistogramFieldBlockLoaderTests extends BlockLoaderTestCase {

    public ExponentialHistogramFieldBlockLoaderTests(Params params) {
        super(ExponentialHistogramFieldMapper.CONTENT_TYPE, List.of(DATA_SOURCE_HANDLER), params);
    }

    @Before
    public void setup() {
        assumeTrue(
            "Only when exponential_histogram feature flag is enabled",
            ExponentialHistogramParser.EXPONENTIAL_HISTOGRAM_FEATURE.isEnabled()
        );
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new ExponentialHistogramMapperPlugin());
    }

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are not supported
    }

    private static DataSourceHandler DATA_SOURCE_HANDLER = new DataSourceHandler() {

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            // exponential_histogram does not support multiple values in a document so we can't have object arrays
            return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
        }

        @Override
        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            if (request.fieldType().equals(ExponentialHistogramFieldMapper.CONTENT_TYPE) == false) {
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
            if (request.fieldType().equals(ExponentialHistogramFieldMapper.CONTENT_TYPE) == false) {
                return null;
            }
            return new DataSourceResponse.FieldDataGenerator(mapping -> createRandomHistogramJsonValue());
        }
    };

    private static Map<String, ?> createRandomHistogramJsonValue() {
        try {
            ExponentialHistogram histo = ExponentialHistogramTestUtils.randomHistogram();
            return convertToMap(histo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> convertToMap(ExponentialHistogram histo) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            ExponentialHistogramXContent.serialize(builder, histo);
            String json = Strings.toString(builder);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
                return parser.map();
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object parsedHistogram, TestContext testContext) {
        return ExponentialHistogramXContent.parseForTesting(Types.<Map<String, Object>>forciblyCast(parsedHistogram));
    }
}
