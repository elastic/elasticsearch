/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig.Mode;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.datafeed.AggProviderTests.createRandomValidAggProvider;
import static org.elasticsearch.xpack.core.ml.utils.QueryProviderTests.createRandomValidQueryProvider;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
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
        TimeValue frequency;
        boolean frequencySet = false;
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            frequencySet = true;
            frequency = TimeValue.timeValueSeconds(randomIntBetween(1, Integer.MAX_VALUE));
            builder.setFrequency(frequency);
        } else {
            frequency = Optional.ofNullable(datafeed).map(DatafeedConfig::getFrequency).orElse(null);
        }
        if (randomBoolean()) {
            builder.setIndices(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            builder.setQuery(createRandomValidQueryProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
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
            builder.setAggregations(createRandomValidAggProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean() || frequencySet) {
            builder.setDelayedDataCheckConfig(
                DelayedDataCheckConfigTests.createRandomizedConfig(
                    randomLongBetween(300_001, 400_000),
                    Optional.ofNullable(frequency).map(TimeValue::millis).orElse(null)
                )
            );
        }
        if (randomBoolean()) {
            builder.setMaxEmptySearches(randomBoolean() ? -1 : randomIntBetween(10, 100));
        }
        if (randomBoolean()) {
            builder.setIndicesOptions(IndicesOptions.fromParameters(
                randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                SearchRequest.DEFAULT_INDICES_OPTIONS));
        }
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("type", "keyword");
            settings.put("script", "");
            Map<String, Object> field = new HashMap<>();
            field.put("runtime_field_foo", settings);
            builder.setRuntimeMappings(field);
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
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
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
        "        \"fixed_interval\": \"360s\",\n" +
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
        "        \"fixed_interval\": \"360s\",\n" +
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
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, MULTIPLE_AGG_DEF_DATAFEED)) {
            XContentParseException ex = expectThrows(XContentParseException.class,
                () -> DatafeedUpdate.PARSER.apply(parser, null));
            assertThat(ex.getMessage(), equalTo("[32:3] [datafeed_update] failed to parse field [aggs]"));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause().getMessage(), equalTo("Found two aggregation definitions: [aggs] and [aggregations]"));
        }
    }

    public void testApply_failBecauseTargetDatafeedHasDifferentId() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        expectThrows(IllegalArgumentException.class, () -> createRandomized(datafeed.getId() + "_2")
            .apply(datafeed, null));
    }

    public void testApply_failBecauseJobIdChanged() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");

        DatafeedUpdate datafeedUpdateWithUnchangedJobId = new DatafeedUpdate.Builder(datafeed.getId())
            .setJobId("foo")
            .build();
        DatafeedConfig updatedDatafeed = datafeedUpdateWithUnchangedJobId.apply(datafeed, Collections.emptyMap());
        assertThat(updatedDatafeed, equalTo(datafeed));

        DatafeedUpdate datafeedUpdateWithChangedJobId = new DatafeedUpdate.Builder(datafeed.getId())
            .setJobId("bar")
            .build();
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> datafeedUpdateWithChangedJobId.apply(datafeed, Collections.emptyMap()));
        assertThat(ex.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ex.getMessage(), equalTo(DatafeedUpdate.ERROR_MESSAGE_ON_JOB_ID_UPDATE));
    }

    public void testApply_givenEmptyUpdate() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedConfig updatedDatafeed = new DatafeedUpdate.Builder(datafeed.getId()).build()
            .apply(datafeed, Collections.emptyMap());
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
        QueryProvider queryProvider = createRandomValidQueryProvider("a", "b");
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setIndices(Collections.singletonList("i_2"));
        update.setQueryDelay(TimeValue.timeValueSeconds(42));
        TimeValue freq = TimeValue.timeValueSeconds(142);
        update.setFrequency(freq);
        update.setQuery(queryProvider);
        update.setScriptFields(Collections.singletonList(new SearchSourceBuilder.ScriptField("a", mockScript("b"), false)));
        update.setScrollSize(8000);
        update.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));
        update.setDelayedDataCheckConfig(
            DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1), freq)
        );
        Map<String, Object> settings = new HashMap<>();
        settings.put("type", "keyword");
        settings.put("script", "");
        Map<String, Object> field = new HashMap<>();
        field.put("updated_runtime_field_foo", settings);
        update.setRuntimeMappings(field);

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getJobId(), equalTo("foo-feed"));
        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_2")));
        assertThat(updatedDatafeed.getQueryDelay(), equalTo(TimeValue.timeValueSeconds(42)));
        assertThat(updatedDatafeed.getFrequency(), equalTo(TimeValue.timeValueSeconds(142)));
        assertThat(updatedDatafeed.getQuery(), equalTo(queryProvider.getQuery()));
        assertThat(updatedDatafeed.hasAggregations(), is(false));
        assertThat(updatedDatafeed.getScriptFields(),
                equalTo(Collections.singletonList(new SearchSourceBuilder.ScriptField("a", mockScript("b"), false))));
        assertThat(updatedDatafeed.getScrollSize(), equalTo(8000));
        assertThat(updatedDatafeed.getChunkingConfig(), equalTo(ChunkingConfig.newManual(TimeValue.timeValueHours(1))));
        assertThat(updatedDatafeed.getDelayedDataCheckConfig().isEnabled(), equalTo(true));
        assertThat(updatedDatafeed.getDelayedDataCheckConfig().getCheckWindow(), equalTo(TimeValue.timeValueHours(1)));
        assertThat(updatedDatafeed.getRuntimeMappings(), hasKey("updated_runtime_field_foo"));
    }

    public void testApply_givenAggregations() throws IOException {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("foo", "foo-feed");
        datafeedBuilder.setIndices(Collections.singletonList("i_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        AggProvider aggProvider = AggProvider.fromParsedAggs(new AggregatorFactories.Builder().addAggregator(
            AggregationBuilders.histogram("a").interval(300000).field("time").subAggregation(maxTime)));
        update.setAggregations(aggProvider);


        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_1")));
        assertThat(updatedDatafeed.getParsedAggregations(xContentRegistry()), equalTo(aggProvider.getParsedAggs()));
        assertThat(updatedDatafeed.getAggregations(), equalTo(aggProvider.getAggs()));
    }

    public void testApply_givenIndicesOptions() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedConfig updatedDatafeed = new DatafeedUpdate.Builder(datafeed.getId())
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .build()
            .apply(datafeed, Collections.emptyMap());
        assertThat(datafeed.getIndicesOptions(), is(not(equalTo(updatedDatafeed.getIndicesOptions()))));
        assertThat(updatedDatafeed.getIndicesOptions(), equalTo(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN));
    }

    public void testApply_GivenRandomUpdates_AssertImmutability() {
        for (int i = 0; i < 100; ++i) {
            DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig(JobTests.randomValidJobId());
            if (datafeed.getAggregations() != null) {
                DatafeedConfig.Builder withoutAggs = new DatafeedConfig.Builder(datafeed);
                withoutAggs.setAggProvider(null);
                datafeed = withoutAggs.build();
            }
            DatafeedUpdate update = createRandomized(datafeed.getId(), datafeed);
            while (update.isNoop(datafeed)) {
                update = createRandomized(datafeed.getId(), datafeed);
            }

            DatafeedConfig updatedDatafeed = update.apply(datafeed, Collections.emptyMap());

            assertThat("update was " + update, datafeed, not(equalTo(updatedDatafeed)));
        }
    }

    public void testSerializationOfComplexAggsBetweenVersions() throws IOException {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("timestamp").field("timestamp");
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("bytes_in_avg").field("system.network.in.bytes");
        DerivativePipelineAggregationBuilder derivativePipelineAggregationBuilder =
            PipelineAggregatorBuilders.derivative("bytes_in_derivative", "bytes_in_avg");
        BucketScriptPipelineAggregationBuilder bucketScriptPipelineAggregationBuilder =
            PipelineAggregatorBuilders.bucketScript("non_negative_bytes",
                Collections.singletonMap("bytes", "bytes_in_derivative"),
                new Script("params.bytes > 0 ? params.bytes : null"));
        DateHistogramAggregationBuilder dateHistogram =
            AggregationBuilders.dateHistogram("histogram_buckets")
                .field("timestamp").fixedInterval(new DateHistogramInterval("300000ms")).timeZone(ZoneOffset.UTC)
                .subAggregation(maxTime)
                .subAggregation(avgAggregationBuilder)
                .subAggregation(derivativePipelineAggregationBuilder)
                .subAggregation(bucketScriptPipelineAggregationBuilder);
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(dateHistogram);
        DatafeedUpdate.Builder datafeedUpdateBuilder = new DatafeedUpdate.Builder("df-update-past-serialization-test");
        datafeedUpdateBuilder.setAggregations(new AggProvider(
            XContentObjectTransformer.aggregatorTransformer(xContentRegistry()).toMap(aggs),
            aggs,
            null,
            false));
        // So equality check between the streamed and current passes
        // Streamed DatafeedConfigs when they are before 6.6.0 require a parsed object for aggs and queries, consequently all the default
        // values are added between them
        datafeedUpdateBuilder.setQuery(
            QueryProvider
                .fromParsedQuery(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)))));
        DatafeedUpdate datafeedUpdate = datafeedUpdateBuilder.build();

        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.CURRENT);
            datafeedUpdate.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(Version.CURRENT);
                DatafeedUpdate streamedDatafeedUpdate = new DatafeedUpdate(in);
                assertEquals(datafeedUpdate, streamedDatafeedUpdate);

                // Assert that the parsed versions of our aggs and queries work as well
                assertEquals(aggs, streamedDatafeedUpdate.getParsedAgg(xContentRegistry()));
                assertEquals(datafeedUpdate.getParsedQuery(xContentRegistry()), streamedDatafeedUpdate.getParsedQuery(xContentRegistry()));
            }
        }
    }

    @Override
    protected DatafeedUpdate mutateInstance(DatafeedUpdate instance) throws IOException {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(instance);
        switch (between(1, 12)) {
        case 1:
            builder.setId(instance.getId() + DatafeedConfigTests.randomValidDatafeedId());
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
            BoolQueryBuilder query = new BoolQueryBuilder();
            if (instance.getQuery() != null) {
                query.must(instance.getParsedQuery(xContentRegistry()));
            }
            query.filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            builder.setQuery(QueryProvider.fromParsedQuery(query));
            break;
        case 6:
            if (instance.hasAggregations()) {
                builder.setAggregations(null);
            } else {
                AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
                String timeField = randomAlphaOfLength(10);
                DateHistogramInterval interval = new DateHistogramInterval(between(10000, 3600000) + "ms");
                aggBuilder.addAggregator(new DateHistogramAggregationBuilder(timeField).field(timeField).fixedInterval(interval)
                    .subAggregation(new MaxAggregationBuilder(timeField).field(timeField)));
                builder.setAggregations(AggProvider.fromParsedAggs(aggBuilder));
                if (instance.getScriptFields().isEmpty() == false) {
                    builder.setScriptFields(Collections.emptyList());
                }
            }
            break;
        case 7:
            builder.setScriptFields(CollectionUtils.appendToCopy(instance.getScriptFields(),
                    new ScriptField(randomAlphaOfLengthBetween(1, 10), new Script("foo"), true)));
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
        case 10:
            if (instance.getMaxEmptySearches() == null) {
                builder.setMaxEmptySearches(randomFrom(-1, 10));
            } else {
                builder.setMaxEmptySearches(instance.getMaxEmptySearches() + 100);
            }
            break;
        case 11:
            if (instance.getIndicesOptions() != null) {
                builder.setIndicesOptions(IndicesOptions.fromParameters(
                    randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                    Boolean.toString(instance.getIndicesOptions().ignoreUnavailable() == false),
                    Boolean.toString(instance.getIndicesOptions().allowNoIndices() == false),
                    Boolean.toString(instance.getIndicesOptions().ignoreThrottled() == false),
                    SearchRequest.DEFAULT_INDICES_OPTIONS));
            } else {
                builder.setIndicesOptions(IndicesOptions.fromParameters(
                    randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                    Boolean.toString(randomBoolean()),
                    Boolean.toString(randomBoolean()),
                    Boolean.toString(randomBoolean()),
                    SearchRequest.DEFAULT_INDICES_OPTIONS));
            }
            break;
        case 12:
                if (instance.getRuntimeMappings() != null) {
                    builder.setRuntimeMappings(null);
                } else {
                    Map<String, Object> settings = new HashMap<>();
                    settings.put("type", "keyword");
                    settings.put("script", "");
                    Map<String, Object> field = new HashMap<>();
                    field.put("runtime_field_foo", settings);
                    builder.setRuntimeMappings(field);
                }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return builder.build();
    }
}
