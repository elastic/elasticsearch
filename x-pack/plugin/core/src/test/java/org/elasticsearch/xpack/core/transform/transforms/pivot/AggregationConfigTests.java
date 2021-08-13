/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.MockDeprecatedAggregationBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AggregationConfigTests extends AbstractSerializingTransformTestCase<AggregationConfig> {

    private boolean lenient;

    public static AggregationConfig randomAggregationConfig() {

        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
        Map<String, Object> source = null;

        // ensure that the unlikely does not happen: 2 aggs share the same name
        Set<String> names = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            AggregationBuilder aggBuilder = getRandomSupportedAggregation();
            if (names.add(aggBuilder.getName())) {
                builder.addAggregator(aggBuilder);
            }
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            source = XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        } catch (IOException e) {
            fail("failed to create random aggregation config: " + e.getMessage());
        }

        return new AggregationConfig(source, builder);
    }

    public static AggregationConfig randomInvalidAggregationConfig() {
        // create something broken but with a source
        Map<String, Object> source = new LinkedHashMap<>();
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), randomIntBetween(1, 10))) {
            source.put(key, randomAlphaOfLengthBetween(1, 20));
        }

        return new AggregationConfig(source, null);
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected AggregationConfig doParseInstance(XContentParser parser) throws IOException {
        return AggregationConfig.fromXContent(parser, lenient);
    }

    @Override
    protected AggregationConfig createTestInstance() {
        return lenient ? randomBoolean() ? randomAggregationConfig() : randomInvalidAggregationConfig() : randomAggregationConfig();
    }

    @Override
    protected Reader<AggregationConfig> instanceReader() {
        return AggregationConfig::new;
    }

    public void testEmptyAggregation() throws IOException {
        String source = "{}";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            AggregationConfig aggregationConfig = AggregationConfig.fromXContent(parser, true);
            ValidationException validationException = aggregationConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("pivot.aggregations must not be null"));
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(IllegalArgumentException.class, () -> AggregationConfig.fromXContent(parser, false));
        }
    }

    public void testFailOnStrictPassOnLenient() throws IOException {
        String source = "{\n"
            + "          \"avg_rating\": { \"some_removed_agg\": { \"field\": \"rating\" } }\n"
            + "        },\n"
            + "        {\n"
            + "          \"max_rating\": { \"max_rating\" : { \"field\" : \"rating\" } }\n"
            + "        }";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            AggregationConfig aggregationConfig = AggregationConfig.fromXContent(parser, true);
            ValidationException validationException = aggregationConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("pivot.aggregations must not be null"));
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(ParsingException.class, () -> AggregationConfig.fromXContent(parser, false));
        }
    }

    public void testDeprecation() throws IOException {
        String source = "{\"dep_agg\": {\"" + MockDeprecatedAggregationBuilder.NAME + "\" : {}}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            AggregationConfig agg = AggregationConfig.fromXContent(parser, false);
            assertNull(agg.validate(null));
            assertWarnings(MockDeprecatedAggregationBuilder.DEPRECATION_MESSAGE);
        }
    }

    private static AggregationBuilder getRandomSupportedAggregation() {
        final int numberOfSupportedAggs = 4;
        switch (randomIntBetween(1, numberOfSupportedAggs)) {
            case 1:
                return AggregationBuilders.avg("avg_" + randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 2:
                return AggregationBuilders.min("min_" + randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 3:
                return AggregationBuilders.max("max_" + randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 4:
                return AggregationBuilders.sum("sum_" + randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
        }
        return null;
    }
}
