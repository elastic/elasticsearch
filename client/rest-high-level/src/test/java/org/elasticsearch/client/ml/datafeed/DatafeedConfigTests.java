/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatafeedConfigTests extends AbstractXContentTestCase<DatafeedConfig> {

    @Override
    protected DatafeedConfig createTestInstance() {
        return createRandom();
    }

    public static DatafeedConfig createRandom() {
        return createRandomBuilder().build();
    }

    public static DatafeedConfig.Builder createRandomBuilder(String datafeedId, String jobId) {
        long bucketSpanMillis = 3600000;
        DatafeedConfig.Builder builder = constructBuilder(datafeedId, jobId);
        builder.setIndices(randomStringList(1, 10));
        if (randomBoolean()) {
            try {
                builder.setQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize query", e);
            }
        }
        boolean addScriptFields = randomBoolean();
        if (addScriptFields) {
            int scriptsSize = randomInt(3);
            List<ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)),
                    randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        Long aggHistogramInterval = null;
        if (randomBoolean()) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list xcontent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggHistogramInterval = randomNonNegativeLong();
            aggHistogramInterval = aggHistogramInterval > bucketSpanMillis ? bucketSpanMillis : aggHistogramInterval;
            aggHistogramInterval = aggHistogramInterval <= 0 ? 1 : aggHistogramInterval;
            MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
            aggs.addAggregator(AggregationBuilders.dateHistogram("buckets")
                .fixedInterval(new DateHistogramInterval(aggHistogramInterval + "ms")).subAggregation(maxTime).field("time"));
            try {
                builder.setAggregations(aggs);
            } catch (IOException e) {
                throw new RuntimeException("failed to serialize aggs", e);
            }
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            if (aggHistogramInterval == null) {
                builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
            } else {
                builder.setFrequency(TimeValue.timeValueMillis(randomIntBetween(1, 5) * aggHistogramInterval));
            }
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean()) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfigTests.createRandomizedConfig());
        }
        if (randomBoolean()) {
            builder.setMaxEmptySearches(randomIntBetween(10, 100));
        }
        if (randomBoolean()) {
            builder.setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()));
        }
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("type", "keyword");
            settings.put("script", "");
            Map<String, Object> field = new HashMap<>();
            field.put("runtime_field_foo", settings);
            builder.setRuntimeMappings(field);
        }
        return builder;
    }

    public static DatafeedConfig.Builder createRandomBuilder() {
        return createRandomBuilder(randomValidDatafeedId(), randomAlphaOfLength(10));
    }

    public static List<String> randomStringList(int min, int max) {
        int size = scaledRandomIntBetween(min, max);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(randomAlphaOfLength(10));
        }
        return list;
    }

    @Override
    protected DatafeedConfig doParseInstance(XContentParser parser) {
        return DatafeedConfig.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    private static final String FUTURE_DATAFEED = "{\n" +
        "    \"datafeed_id\": \"farequote-datafeed\",\n" +
        "    \"job_id\": \"farequote\",\n" +
        "    \"frequency\": \"1h\",\n" +
        "    \"indices\": [\"farequote1\", \"farequote2\"],\n" +
        "    \"tomorrows_technology_today\": \"amazing\",\n" +
        "    \"scroll_size\": 1234\n" +
        "}";

    public void testFutureMetadataParse() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, FUTURE_DATAFEED);
        // Unlike the config version of this test, the metadata parser should tolerate the unknown future field
        assertNotNull(DatafeedConfig.PARSER.apply(parser, null).build());
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            DatafeedConfig datafeedConfig = createTestInstance();
            DatafeedConfig copy = new DatafeedConfig.Builder(datafeedConfig).build();
            assertEquals(datafeedConfig, copy);
        }
    }

    public void testCheckValid_GivenNullIdInConstruction() {
        expectThrows(NullPointerException.class, () -> new DatafeedConfig.Builder(null, null));
    }

    public void testCheckValid_GivenNullJobId() {
        expectThrows(NullPointerException.class, () -> new DatafeedConfig.Builder(randomValidDatafeedId(), null));
    }

    public static String randomValidDatafeedId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static DatafeedConfig.Builder constructBuilder(String datafeedId, String jobId) {
        return new DatafeedConfig.Builder(datafeedId, jobId);
    }

}
