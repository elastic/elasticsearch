/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
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
import org.elasticsearch.xpack.core.dataframe.transforms.AbstractSerializingDataFrameTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.MockDeprecatedAggregationBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class AggregationConfigTests extends AbstractSerializingDataFrameTestCase<AggregationConfig> {

    private boolean lenient;

    public static AggregationConfig randomAggregationConfig() {

        AggregatorFactories.Builder builder = null;
        Map<String, Object> source = new LinkedHashMap<>();
        Map<String, String> specialAggregations = null;

        // ensure that the unlikely does not happen: 2 aggs share the same name
        String[] names = Set.of(generateRandomAlphaArray(4, 20, 1, 10)).toArray(new String[0]);

        int randomMix = randomIntBetween(1, 3);

        if (randomMix <= 2) {
            builder = new AggregatorFactories.Builder();

            for (int i = 1; i < randomIntBetween(2, names.length - 1); ++i) {
                AggregationBuilder aggBuilder = getRandomSupportedAggregation(names[i]);
                    builder.addAggregator(aggBuilder);
            }

            try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
                XContentBuilder content = builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                source = XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
            } catch (IOException e) {
                fail("failed to create random aggregation config: " + e.getMessage());
            }
        }

        if (randomMix >= 2) {
            specialAggregations = getRandomSupportedSpecialAggregation(names[0], source);
        }

        return new AggregationConfig(source, builder, specialAggregations);
    }

    public static AggregationConfig randomInvalidAggregationConfig() {
        // create something broken but with a source
        Map<String, Object> source = new LinkedHashMap<>();
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), randomIntBetween(1, 10))) {
            source.put(key, randomAlphaOfLengthBetween(1, 20));
        }

        return new AggregationConfig(source, null, null);
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
            assertFalse(aggregationConfig.isValid());
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(IllegalArgumentException.class, () -> AggregationConfig.fromXContent(parser, false));
        }
    }

    public void testFailOnStrictPassOnLenient() throws IOException {
        String source = "{\n" +
                "          \"avg_rating\": { \"some_removed_agg\": { \"field\": \"rating\" } }\n" +
                "        },\n" +
                "        {\n" +
                "          \"max_rating\": { \"max_rating\" : { \"field\" : \"rating\" } }\n" +
                "        }";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            AggregationConfig aggregationConfig = AggregationConfig.fromXContent(parser, true);
            assertFalse(aggregationConfig.isValid());
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(NamedObjectNotFoundException.class, () -> AggregationConfig.fromXContent(parser, false));
        }
    }

    public void testDeprecation() throws IOException {
        String source = "{\"dep_agg\": {\"" + MockDeprecatedAggregationBuilder.NAME + "\" : {}}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            AggregationConfig agg = AggregationConfig.fromXContent(parser, false);
            assertTrue(agg.isValid());
            assertWarnings(MockDeprecatedAggregationBuilder.DEPRECATION_MESSAGE);
        }
    }

    public static String[] generateRandomAlphaArray(int minArraySize,
                                                    int maxArraySize,
                                                    int minStringSize,
                                                    int maxStringSize) {
        Set<String> strings = new HashSet<>();
        for (int i = 0; i < randomIntBetween(minArraySize, maxArraySize); ++i) {
            strings.add(randomAlphaOfLengthBetween(minStringSize, maxStringSize));
        }

        return strings.toArray(new String[0]);
    }

    private static AggregationBuilder getRandomSupportedAggregation(String name) {
        final int numberOfSupportedAggs = 4;
        switch (randomIntBetween(1, numberOfSupportedAggs)) {
        case 1:
            return AggregationBuilders.avg(name).field(randomAlphaOfLengthBetween(1, 10));
        case 2:
            return AggregationBuilders.min(name).field(randomAlphaOfLengthBetween(1, 10));
        case 3:
            return AggregationBuilders.max(name).field(randomAlphaOfLengthBetween(1, 10));
        case 4:
            return AggregationBuilders.sum(name).field(randomAlphaOfLengthBetween(1, 10));
        }

        return null;
    }

    private static Map<String, String> getRandomSupportedSpecialAggregation(String name, Map<String, Object> source) {
        source.put(name, Collections.singletonMap("count", Collections.emptyMap()));
        return Collections.singletonMap(name, "count");
    }
}
