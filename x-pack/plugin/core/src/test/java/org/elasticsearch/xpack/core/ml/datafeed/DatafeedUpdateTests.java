/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig.Mode;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class DatafeedUpdateTests extends AbstractSerializingTestCase<DatafeedUpdate> {

    @Override
    protected DatafeedUpdate createTestInstance() {
        return createRandomized(DatafeedConfigTests.randomValidDatafeedId());
    }

    public static DatafeedUpdate createRandomized(String datafeedId) {
        return createRandomized(datafeedId, null);
    }

    public static DatafeedUpdate createRandomized(String datafeedId, @Nullable DatafeedConfig datafeed) {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(datafeedId);
        if (randomBoolean() && datafeed == null) {
            builder.setJobId(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setIndices(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            builder.setQuery(Collections.singletonMap(TermQueryBuilder.NAME,
                Collections.singletonMap(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
        }
        if (randomBoolean()) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)),
                        randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        if (randomBoolean() && datafeed == null) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list writeable / xcontent logic
            builder.setAggregations(Collections.singletonMap(randomAlphaOfLength(10),
                Collections.singletonMap("avg", Collections.singletonMap("field", randomAlphaOfLength(10)))));
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean()) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfigTests.createRandomizedConfig(randomLongBetween(300_001, 400_000)));
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<DatafeedUpdate> instanceReader() {
        return DatafeedUpdate::new;
    }

    @Override
    protected DatafeedUpdate doParseInstance(XContentParser parser) {
        return DatafeedUpdate.PARSER.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private static final String MULTIPLE_AGG_DEF_DATAFEED = "{\n" +
        "    \"datafeed_id\": \"farequote-datafeed\",\n" +
        "    \"job_id\": \"farequote\",\n" +
        "    \"frequency\": \"1h\",\n" +
        "    \"indices\": [\"farequote1\", \"farequote2\"],\n" +
        "    \"aggregations\": {\n" +
        "    \"buckets\": {\n" +
        "      \"date_histogram\": {\n" +
        "        \"field\": \"time\",\n" +
        "        \"interval\": \"360s\",\n" +
        "        \"time_zone\": \"UTC\"\n" +
        "      },\n" +
        "      \"aggregations\": {\n" +
        "        \"time\": {\n" +
        "          \"max\": {\"field\": \"time\"}\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }," +
        "    \"aggs\": {\n" +
        "    \"buckets2\": {\n" +
        "      \"date_histogram\": {\n" +
        "        \"field\": \"time\",\n" +
        "        \"interval\": \"360s\",\n" +
        "        \"time_zone\": \"UTC\"\n" +
        "      },\n" +
        "      \"aggregations\": {\n" +
        "        \"time\": {\n" +
        "          \"max\": {\"field\": \"time\"}\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";

    public void testMultipleDefinedAggParse() throws IOException {
        try(XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, MULTIPLE_AGG_DEF_DATAFEED)) {
            XContentParseException ex = expectThrows(XContentParseException.class,
                () -> DatafeedUpdate.PARSER.apply(parser, null));
            assertThat(ex.getMessage(), equalTo("[32:3] [datafeed_update] failed to parse field [aggs]"));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause().getMessage(), equalTo("Found two aggregation definitions: [aggs] and [aggregations]"));
        }
    }

    public void testApply_failBecauseTargetDatafeedHasDifferentId() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        expectThrows(IllegalArgumentException.class, () -> createRandomized(datafeed.getId() + "_2").apply(datafeed, null));
    }

    public void testApply_givenEmptyUpdate() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedConfig updatedDatafeed = new DatafeedUpdate.Builder(datafeed.getId()).build().apply(datafeed, Collections.emptyMap());
        assertThat(datafeed, equalTo(updatedDatafeed));
    }

    public void testApply_givenPartialUpdate() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setScrollSize(datafeed.getScrollSize() + 1);

        DatafeedUpdate.Builder updated = new DatafeedUpdate.Builder(datafeed.getId());
        updated.setScrollSize(datafeed.getScrollSize() + 1);
        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        DatafeedConfig.Builder expectedDatafeed = new DatafeedConfig.Builder(datafeed);
        expectedDatafeed.setScrollSize(datafeed.getScrollSize() + 1);
        assertThat(updatedDatafeed, equalTo(expectedDatafeed.build()));
    }

    public void testApply_givenFullUpdateNoAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("foo", "foo-feed");
        datafeedBuilder.setIndices(Collections.singletonList("i_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setJobId("bar");
        update.setIndices(Collections.singletonList("i_2"));
        update.setQueryDelay(TimeValue.timeValueSeconds(42));
        update.setFrequency(TimeValue.timeValueSeconds(142));
        update.setQuery(Collections.singletonMap(TermQueryBuilder.NAME, Collections.singletonMap("a", "b")));
        update.setScriptFields(Collections.singletonList(new SearchSourceBuilder.ScriptField("a", mockScript("b"), false)));
        update.setScrollSize(8000);
        update.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));
        update.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1)));

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getJobId(), equalTo("bar"));
        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_2")));
        assertThat(updatedDatafeed.getQueryDelay(), equalTo(TimeValue.timeValueSeconds(42)));
        assertThat(updatedDatafeed.getFrequency(), equalTo(TimeValue.timeValueSeconds(142)));
        assertThat(updatedDatafeed.getQuery(),
            equalTo(Collections.singletonMap(TermQueryBuilder.NAME, Collections.singletonMap("a", "b"))));
        assertThat(updatedDatafeed.hasAggregations(), is(false));
        assertThat(updatedDatafeed.getScriptFields(),
                equalTo(Collections.singletonList(new SearchSourceBuilder.ScriptField("a", mockScript("b"), false))));
        assertThat(updatedDatafeed.getScrollSize(), equalTo(8000));
        assertThat(updatedDatafeed.getChunkingConfig(), equalTo(ChunkingConfig.newManual(TimeValue.timeValueHours(1))));
        assertThat(updatedDatafeed.getDelayedDataCheckConfig().isEnabled(), equalTo(true));
        assertThat(updatedDatafeed.getDelayedDataCheckConfig().getCheckWindow(), equalTo(TimeValue.timeValueHours(1)));
    }

    public void testApply_givenAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("foo", "foo-feed");
        datafeedBuilder.setIndices(Collections.singletonList("i_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        Map<String, Object> maxTime = Collections.singletonMap("time",
            Collections.singletonMap("max", Collections.singletonMap("field", "time")));
        Map<String, Object> histoDefinition = new HashMap<>();
        histoDefinition.put("interval", 300000);
        histoDefinition.put("field", "time");
        Map<String, Object> aggBody = new HashMap<>();
        aggBody.put("histogram", histoDefinition);
        aggBody.put("aggs", maxTime);
        Map<String, Object> aggMap = Collections.singletonMap("a", aggBody);
        update.setAggregations(aggMap);

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_1")));
        assertThat(updatedDatafeed.getAggregations(), equalTo(aggMap));
    }

    public void testApply_GivenRandomUpdates_AssertImmutability() {
        for (int i = 0; i < 100; ++i) {
            DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig(JobTests.randomValidJobId());
            if (datafeed.getAggregations() != null) {
                DatafeedConfig.Builder withoutAggs = new DatafeedConfig.Builder(datafeed);
                withoutAggs.setAggregations(null);
                datafeed = withoutAggs.build();
            }
            DatafeedUpdate update = createRandomized(datafeed.getId(), datafeed);
            while (update.isNoop(datafeed)) {
                update = createRandomized(datafeed.getId(), datafeed);
            }

            DatafeedConfig updatedDatafeed = update.apply(datafeed, Collections.emptyMap());

            assertThat(datafeed, not(equalTo(updatedDatafeed)));
        }
    }

    public void testEmptyQueryMap() {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder("empty_query_map");
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> builder.setQuery(Collections.emptyMap()));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), equalTo("Datafeed [empty_query_map] query is not parsable"));
    }

    public void testEmptyAggMap() {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder("empty_agg_map");
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> builder.setAggregations(Collections.emptyMap()));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), equalTo("Datafeed [empty_agg_map] aggregations are not parsable"));
    }

    @Override
    protected DatafeedUpdate mutateInstance(DatafeedUpdate instance) {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(instance);
        switch (between(0, 9)) {
        case 0:
            builder.setId(instance.getId() + DatafeedConfigTests.randomValidDatafeedId());
            break;
        case 1:
            builder.setJobId(instance.getJobId() + randomAlphaOfLength(5));
            break;
        case 2:
            if (instance.getQueryDelay() == null) {
                builder.setQueryDelay(new TimeValue(between(100, 100000)));
            } else {
                builder.setQueryDelay(new TimeValue(instance.getQueryDelay().millis() + between(100, 100000)));
            }
            break;
        case 3:
            if (instance.getFrequency() == null) {
                builder.setFrequency(new TimeValue(between(1, 10) * 1000));
            } else {
                builder.setFrequency(new TimeValue(instance.getFrequency().millis() + between(1, 10) * 1000));
            }
            break;
        case 4:
            List<String> indices;
            if (instance.getIndices() == null) {
                indices = new ArrayList<>();
            } else {
                indices = new ArrayList<>(instance.getIndices());
            }
            indices.add(randomAlphaOfLengthBetween(1, 20));
            builder.setIndices(indices);
            break;
        case 5:
            Map<String, Object> boolQuery = new HashMap<>();
            if (instance.getQuery() != null) {
                boolQuery.put("must", instance.getQuery());
            }
            boolQuery.put("filter",
                Collections.singletonList(
                    Collections.singletonMap(TermQueryBuilder.NAME,
                        Collections.singletonMap(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)))));
            builder.setQuery(Collections.singletonMap("bool", boolQuery));
            break;
        case 6:
            if (instance.hasAggregations()) {
                builder.setAggregations(null);
            } else {
                String timeField = randomAlphaOfLength(10);
                Map<String, Object> maxTime = Collections.singletonMap(timeField,
                    Collections.singletonMap("max", Collections.singletonMap("field", timeField)));
                Map<String, Object> histoDefinition = new HashMap<>();
                histoDefinition.put("interval", between(10000, 3600000));
                histoDefinition.put("field", timeField);
                Map<String, Object> aggBody = new HashMap<>();
                aggBody.put("aggs", maxTime);
                aggBody.put("date_histogram", histoDefinition);
                Map<String, Object> aggMap = Collections.singletonMap(timeField, aggBody);
                builder.setAggregations(aggMap);
                if (instance.getScriptFields().isEmpty() == false) {
                    builder.setScriptFields(Collections.emptyList());
                }
            }
            break;
        case 7:
            ArrayList<ScriptField> scriptFields = new ArrayList<>(instance.getScriptFields());
            scriptFields.add(new ScriptField(randomAlphaOfLengthBetween(1, 10), new Script("foo"), true));
            builder.setScriptFields(scriptFields);
            builder.setAggregations(null);
            break;
        case 8:
            if (instance.getScrollSize() == null) {
                builder.setScrollSize(between(1, 100));
            } else {
                builder.setScrollSize(instance.getScrollSize() + between(1, 100));
            }
            break;
        case 9:
            if (instance.getChunkingConfig() == null || instance.getChunkingConfig().getMode() == Mode.AUTO) {
                ChunkingConfig newChunkingConfig = ChunkingConfig.newManual(new TimeValue(randomNonNegativeLong()));
                builder.setChunkingConfig(newChunkingConfig);
            } else {
                builder.setChunkingConfig(null);
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return builder.build();
    }
}
