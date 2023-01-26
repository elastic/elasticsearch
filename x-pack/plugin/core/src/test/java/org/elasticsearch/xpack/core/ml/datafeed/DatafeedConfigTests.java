/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig.Mode;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigBuilderTests.createRandomizedDatafeedConfigBuilder;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO;
import static org.elasticsearch.xpack.core.ml.utils.QueryProviderTests.createRandomValidQueryProvider;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedConfigTests extends AbstractXContentSerializingTestCase<DatafeedConfig> {

    @Override
    protected DatafeedConfig createTestInstance() {
        return createRandomizedDatafeedConfig(randomAlphaOfLength(10));
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId) {
        return createRandomizedDatafeedConfig(jobId, 3600000);
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId, long bucketSpanMillis) {
        return createRandomizedDatafeedConfig(jobId, randomValidDatafeedId(), bucketSpanMillis);
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId, String datafeedId, long bucketSpanMillis) {
        return createRandomizedDatafeedConfigBuilder(jobId, datafeedId, bucketSpanMillis).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of(new AggregationsPlugin()));
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of(new AggregationsPlugin()));
        return new NamedXContentRegistry(searchModule.getNamedXContents());
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
    protected Writeable.Reader<DatafeedConfig> instanceReader() {
        return DatafeedConfig::new;
    }

    @Override
    protected DatafeedConfig doParseInstance(XContentParser parser) {
        return DatafeedConfig.STRICT_PARSER.apply(parser, null).build();
    }

    private static final String FUTURE_DATAFEED = """
        {
            "datafeed_id": "farequote-datafeed",
            "job_id": "farequote",
            "frequency": "1h",
            "indices": ["farequote1", "farequote2"],
            "tomorrows_technology_today": "amazing",
            "scroll_size": 1234
        }""";

    // query:match:type stopped being supported in 6.x
    private static final String ANACHRONISTIC_QUERY_DATAFEED = """
        {
            "datafeed_id": "farequote-datafeed",
            "job_id": "farequote",
            "frequency": "1h",
            "indices": ["farequote1", "farequote2"],
            "query": {"match" : {"query":"fieldName", "type": "phrase"}},
            "scroll_size": 1234
        }""";

    // size: 0 stopped being supported in 6.x
    private static final String ANACHRONISTIC_AGG_DATAFEED = """
        {
            "datafeed_id": "farequote-datafeed",
            "job_id": "farequote",
            "frequency": "1h",
            "indices": ["farequote1", "farequote2"],
            "aggregations": {
            "buckets": {
              "date_histogram": {
                "field": "time",
                "fixed_interval": "360s",
                "time_zone": "UTC"
              },
              "aggregations": {
                "time": {
                  "max": {"field": "time"}
                },
                "airline": {
                  "terms": {
                    "field": "airline",
                    "size": 0
                  }
                }
              }
            }
          }
        }""";

    private static final String AGG_WITH_OLD_DATE_HISTOGRAM_INTERVAL = """
        {
            "datafeed_id": "farequote-datafeed",
            "job_id": "farequote",
            "frequency": "1h",
            "indices": ["farequote1", "farequote2"],
            "aggregations": {
            "buckets": {
              "date_histogram": {
                "field": "time",
                "interval": "360s",
                "time_zone": "UTC"
              },
              "aggregations": {
                "time": {
                  "max": {"field": "time"}
                }
              }
            }
          }
        }""";

    private static final String MULTIPLE_AGG_DEF_DATAFEED = """
        {
            "datafeed_id": "farequote-datafeed",
            "job_id": "farequote",
            "frequency": "1h",
            "indices": ["farequote1", "farequote2"],
            "aggregations": {
            "buckets": {
              "date_histogram": {
                "field": "time",
                "fixed_interval": "360s",
                "time_zone": "UTC"
              },
              "aggregations": {
                "time": {
                  "max": {"field": "time"}
                }
              }
            }
          },    "aggs": {
            "buckets2": {
              "date_histogram": {
                "field": "time",
                "fixed_interval": "360s",
                "time_zone": "UTC"
              },
              "aggregations": {
                "time": {
                  "max": {"field": "time"}
                }
              }
            }
          }
        }""";

    public void testFutureConfigParse() throws IOException {
        XContentParser parser = parser(FUTURE_DATAFEED);
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build()
        );
        assertEquals("[6:5] [datafeed_config] unknown field [tomorrows_technology_today]", e.getMessage());
    }

    public void testPastQueryConfigParse() throws IOException {
        try (XContentParser parser = parser(ANACHRONISTIC_QUERY_DATAFEED)) {

            DatafeedConfig config = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
            assertThat(
                config.getQueryParsingException().getMessage(),
                equalTo("[match] query doesn't support multiple fields, found [query] and [type]")
            );
        }

        try (XContentParser parser = parser(ANACHRONISTIC_QUERY_DATAFEED)) {

            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build()
            );
            assertEquals("[6:64] [datafeed_config] failed to parse field [query]", e.getMessage());
        }
    }

    public void testPastAggConfigParse() throws IOException {
        try (XContentParser parser = parser(ANACHRONISTIC_AGG_DATAFEED)) {

            DatafeedConfig datafeedConfig = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
            assertThat(
                datafeedConfig.getAggParsingException().getMessage(),
                equalTo("[size] must be greater than 0. Found [0] in [airline]")
            );
        }

        try (XContentParser parser = parser(ANACHRONISTIC_AGG_DATAFEED)) {

            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build()
            );
            assertEquals("[25:3] [datafeed_config] failed to parse field [aggregations]", e.getMessage());
        }
    }

    public void testPastAggConfigOldDateHistogramParse() throws IOException {
        try (XContentParser parser = parser(AGG_WITH_OLD_DATE_HISTOGRAM_INTERVAL)) {

            DatafeedConfig datafeedConfig = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
            assertThat(datafeedConfig.getParsedAggregations(xContentRegistry()), is(not(nullValue())));
        }

        try (XContentParser parser = parser(ANACHRONISTIC_AGG_DATAFEED)) {

            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build()
            );
            assertEquals("[25:3] [datafeed_config] failed to parse field [aggregations]", e.getMessage());
        }
    }

    public void testFutureMetadataParse() throws IOException {
        XContentParser parser = parser(FUTURE_DATAFEED);
        // Unlike the config version of this test, the metadata parser should tolerate the unknown future field
        assertNotNull(DatafeedConfig.LENIENT_PARSER.apply(parser, null).build());
    }

    public void testMultipleDefinedAggParse() throws IOException {
        try (XContentParser parser = parser(MULTIPLE_AGG_DEF_DATAFEED)) {
            XContentParseException ex = expectThrows(XContentParseException.class, () -> DatafeedConfig.LENIENT_PARSER.apply(parser, null));
            assertThat(ex.getMessage(), equalTo("[32:3] [datafeed_config] failed to parse field [aggs]"));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause().getMessage(), equalTo("Found two aggregation definitions: [aggs] and [aggregations]"));
        }
        try (XContentParser parser = parser(MULTIPLE_AGG_DEF_DATAFEED)) {
            XContentParseException ex = expectThrows(XContentParseException.class, () -> DatafeedConfig.STRICT_PARSER.apply(parser, null));
            assertThat(ex.getMessage(), equalTo("[32:3] [datafeed_config] failed to parse field [aggs]"));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause().getMessage(), equalTo("Found two aggregation definitions: [aggs] and [aggregations]"));
        }
    }

    public void testToXContentForInternalStorage() throws IOException {
        DatafeedConfig.Builder builder = createRandomizedDatafeedConfigBuilder("foo", randomValidDatafeedId(), 300);

        // headers are only persisted to cluster state
        Map<String, String> headers = new HashMap<>();
        headers.put("header-name", "header-value");
        builder.setHeaders(headers);
        DatafeedConfig config = builder.build();

        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

        BytesReference forClusterstateXContent = XContentHelper.toXContent(config, XContentType.JSON, params, false);
        XContentParser parser = parser(forClusterstateXContent);

        DatafeedConfig parsedConfig = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
        assertThat(parsedConfig.getHeaders(), hasEntry("header-name", "header-value"));

        // headers are not written without the FOR_INTERNAL_STORAGE param
        BytesReference nonClusterstateXContent = XContentHelper.toXContent(config, XContentType.JSON, ToXContent.EMPTY_PARAMS, false);
        parser = parser(nonClusterstateXContent);

        parsedConfig = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
        assertThat(parsedConfig.getHeaders().entrySet(), hasSize(0));
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            DatafeedConfig datafeedConfig = createTestInstance();
            DatafeedConfig copy = new DatafeedConfig.Builder(datafeedConfig).build();
            assertEquals(datafeedConfig, copy);
        }
    }

    public void testDefaults() {
        DatafeedConfig.Builder expectedDatafeedConfig = new DatafeedConfig.Builder("datafeed1", "job1");
        expectedDatafeedConfig.setIndices(Collections.singletonList("index"));
        expectedDatafeedConfig.setQueryDelay(TimeValue.timeValueMinutes(1));
        expectedDatafeedConfig.setScrollSize(1000);
        DatafeedConfig.Builder defaultFeedBuilder = new DatafeedConfig.Builder("datafeed1", "job1");
        defaultFeedBuilder.setIndices(Collections.singletonList("index"));
        DatafeedConfig defaultFeed = defaultFeedBuilder.build();

        assertThat(defaultFeed.getScrollSize(), equalTo(1000));
        assertThat(defaultFeed.getQueryDelay().seconds(), greaterThanOrEqualTo(60L));
        assertThat(defaultFeed.getQueryDelay().seconds(), lessThan(120L));
        assertThat(defaultFeed.getMaxEmptySearches(), is(nullValue()));
    }

    public void testDefaultQueryDelay() {
        DatafeedConfig.Builder feedBuilder1 = new DatafeedConfig.Builder("datafeed1", "job1");
        feedBuilder1.setIndices(Collections.singletonList("foo"));
        DatafeedConfig.Builder feedBuilder2 = new DatafeedConfig.Builder("datafeed2", "job1");
        feedBuilder2.setIndices(Collections.singletonList("foo"));
        DatafeedConfig.Builder feedBuilder3 = new DatafeedConfig.Builder("datafeed3", "job2");
        feedBuilder3.setIndices(Collections.singletonList("foo"));
        DatafeedConfig feed1 = feedBuilder1.build();
        DatafeedConfig feed2 = feedBuilder2.build();
        DatafeedConfig feed3 = feedBuilder3.build();

        // Two datafeeds with the same job id should have the same random query delay
        assertThat(feed1.getQueryDelay(), equalTo(feed2.getQueryDelay()));
        // But the query delay of a datafeed with a different job id should differ too
        assertThat(feed1.getQueryDelay(), not(equalTo(feed3.getQueryDelay())));
    }

    public void testCheckValid_GivenNullIndices() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        expectThrows(IllegalArgumentException.class, () -> conf.setIndices(null));
    }

    public void testCheckValid_GivenInvalidMaxEmptySearches() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> conf.setMaxEmptySearches(randomFrom(-2, 0))
        );
        assertThat(e.getMessage(), containsString("Invalid max_empty_searches value"));
    }

    public void testCheckValid_GivenMaxEmptySearchesMinusOne() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndices(Collections.singletonList("whatever"));
        conf.setMaxEmptySearches(-1);
        assertThat(conf.build().getMaxEmptySearches(), is(nullValue()));
    }

    public void testCheckValid_GivenEmptyIndices() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndices(Collections.emptyList());
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indices", "[]"), e.getMessage());
    }

    public void testCheckValid_GivenIndicesContainsOnlyEmptyStrings() {
        List<String> indices = new ArrayList<>();
        indices.add("");
        indices.add("");
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndices(indices);
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indices", "[, ]"), e.getMessage());
    }

    public void testCheckValid_GivenNegativeQueryDelay() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(
            IllegalArgumentException.class,
            () -> conf.setQueryDelay(TimeValue.timeValueMillis(-1))
        );
        assertEquals("query_delay cannot be less than 0. Value = -1", e.getMessage());
    }

    public void testCheckValid_GivenZeroFrequency() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(TimeValue.ZERO));
        assertEquals("frequency cannot be less or equal than 0. Value = 0s", e.getMessage());
    }

    public void testCheckValid_GivenNegativeFrequency() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(
            IllegalArgumentException.class,
            () -> conf.setFrequency(TimeValue.timeValueMinutes(-1))
        );
        assertEquals("frequency cannot be less or equal than 0. Value = -1", e.getMessage());
    }

    public void testCheckValid_GivenNegativeScrollSize() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> conf.setScrollSize(-1000));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "scroll_size", -1000L), e.getMessage());
    }

    public void testBuild_GivenScriptFieldsAndAggregations() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", "job1");
        datafeed.setIndices(Collections.singletonList("my_index"));
        datafeed.setScriptFields(
            Collections.singletonList(
                new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)), randomBoolean())
            )
        );
        datafeed.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));

        ElasticsearchException e = expectThrows(ElasticsearchException.class, datafeed::build);

        assertThat(e.getMessage(), equalTo("script_fields cannot be used in combination with aggregations"));
    }

    public void testBuild_GivenRuntimeMappingMissingType() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("my_index"));
        Map<String, Object> properties = new HashMap<>();
        properties.put("type_field_is_missing", "");
        Map<String, Object> fields = new HashMap<>();
        fields.put("runtime_field_foo", properties);
        builder.setRuntimeMappings(fields);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);
        assertThat(e.getMessage(), equalTo("No type specified for runtime field [runtime_field_foo]"));
    }

    public void testBuild_GivenInvalidRuntimeMapping() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("my_index"));
        Map<String, Object> fields = new HashMap<>();
        fields.put("field_is_not_an_object", "");
        builder.setRuntimeMappings(fields);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);
        assertThat(e.getMessage(), equalTo("Expected map for runtime field [field_is_not_an_object] definition but got a String"));
    }

    public void testHasAggregations_GivenNull() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(false));
    }

    public void testHasAggregations_NonEmpty() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        builder.setParsedAggregations(
            new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.dateHistogram("time")
                    .fixedInterval(new DateHistogramInterval(300000 + "ms"))
                    .subAggregation(maxTime)
                    .field("time")
            )
        );
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(true));
    }

    public void testBuild_GivenEmptyAggregations() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setParsedAggregations(new AggregatorFactories.Builder());

        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);

        assertThat(e.getMessage(), equalTo("A date_histogram (or histogram) aggregation is required"));
    }

    public void testBuild_GivenHistogramWithDefaultInterval() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        builder.setParsedAggregations(
            new AggregatorFactories.Builder().addAggregator(AggregationBuilders.histogram("time").subAggregation(maxTime).field("time"))
        );

        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);

        assertThat(e.getMessage(), containsString(DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO));
    }

    public void testBuild_GivenDateHistogramWithInvalidTimeZone() {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("bucket")
            .field("time")
            .fixedInterval(new DateHistogramInterval("30000ms"))
            .timeZone(ZoneId.of("CET"))
            .subAggregation(maxTime);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> createDatafeedWithDateHistogram(dateHistogram));

        assertThat(e.getMessage(), equalTo("ML requires date_histogram.time_zone to be UTC"));
    }

    public void testBuild_GivenValidDateHistogram() {
        long millisInDay = 24 * 3600000L;

        assertThat(createDatafeedWithDateHistogram("1s").getHistogramIntervalMillis(xContentRegistry()), equalTo(1000L));
        assertThat(createDatafeedWithDateHistogram("2s").getHistogramIntervalMillis(xContentRegistry()), equalTo(2000L));
        assertThat(createDatafeedWithDateHistogram("1m").getHistogramIntervalMillis(xContentRegistry()), equalTo(60000L));
        assertThat(createDatafeedWithDateHistogram("2m").getHistogramIntervalMillis(xContentRegistry()), equalTo(120000L));
        assertThat(createDatafeedWithDateHistogram("1h").getHistogramIntervalMillis(xContentRegistry()), equalTo(3600000L));
        assertThat(createDatafeedWithDateHistogram("2h").getHistogramIntervalMillis(xContentRegistry()), equalTo(7200000L));
        assertThat(createDatafeedWithDateHistogram("1d").getHistogramIntervalMillis(xContentRegistry()), equalTo(millisInDay));
        assertThat(createDatafeedWithDateHistogram("7d").getHistogramIntervalMillis(xContentRegistry()), equalTo(7 * millisInDay));

        assertThat(
            createDatafeedWithDateHistogram(7 * millisInDay + 1).getHistogramIntervalMillis(xContentRegistry()),
            equalTo(7 * millisInDay + 1)
        );
    }

    public void testBuild_GivenDateHistogramWithMoreThanCalendarWeek() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> createDatafeedWithDateHistogram("month"));

        assertThat(e.getMessage(), containsString("When specifying a date_histogram calendar interval [month]"));
    }

    public void testDefaultChunkingConfig_GivenAggregations() {
        assertThat(
            createDatafeedWithDateHistogram("1s").getChunkingConfig(),
            equalTo(ChunkingConfig.newManual(TimeValue.timeValueSeconds(1000)))
        );
        assertThat(
            createDatafeedWithDateHistogram("2h").getChunkingConfig(),
            equalTo(ChunkingConfig.newManual(TimeValue.timeValueHours(2000)))
        );
    }

    public void testChunkingConfig_GivenExplicitSetting() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(createDatafeedWithDateHistogram("30s"));
        builder.setChunkingConfig(ChunkingConfig.newAuto());
        assertThat(builder.build().getChunkingConfig(), equalTo(ChunkingConfig.newAuto()));
    }

    public void testCheckHistogramAggregationHasChildMaxTimeAgg() {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time_agg").field("max_time");

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> DatafeedConfig.Builder.checkHistogramAggregationHasChildMaxTimeAgg(dateHistogram)
        );

        assertThat(e.getMessage(), containsString("Date histogram must have nested max aggregation for time_field [max_time]"));
    }

    public void testValidateCompositeAggValueSources_MustHaveExactlyOneDateValue() {
        CompositeAggregationBuilder aggregationBuilder = AggregationBuilders.composite(
            "buckets",
            Arrays.asList(new TermsValuesSourceBuilder("foo").field("bar"))
        );
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedConfig.Builder.validateCompositeAggregationSources(aggregationBuilder)
        );
        assertThat(ex.getMessage(), containsString("must have exactly one date_histogram source"));

        CompositeAggregationBuilder aggregationBuilderWithMoreDateHisto = AggregationBuilders.composite(
            "buckets",
            Arrays.asList(
                new TermsValuesSourceBuilder("foo").field("bar"),
                new DateHistogramValuesSourceBuilder("date1").field("time").fixedInterval(DateHistogramInterval.days(1)),
                new DateHistogramValuesSourceBuilder("date2").field("time").fixedInterval(DateHistogramInterval.days(1))
            )
        );
        ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedConfig.Builder.validateCompositeAggregationSources(aggregationBuilderWithMoreDateHisto)
        );
        assertThat(ex.getMessage(), containsString("must have exactly one date_histogram source"));
    }

    public void testValidateCompositeAggValueSources_DateHistoWithMissingBucket() {
        CompositeAggregationBuilder aggregationBuilder = AggregationBuilders.composite(
            "buckets",
            Arrays.asList(
                new TermsValuesSourceBuilder("foo").field("bar"),
                new DateHistogramValuesSourceBuilder("date1").field("time").fixedInterval(DateHistogramInterval.days(1)).missingBucket(true)
            )
        );
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedConfig.Builder.validateCompositeAggregationSources(aggregationBuilder)
        );
        assertThat(ex.getMessage(), containsString("does not support missing_buckets"));
    }

    public void testValidateCompositeAggValueSources_DateHistoBadOrder() {
        CompositeAggregationBuilder aggregationBuilder = AggregationBuilders.composite(
            "buckets",
            Arrays.asList(
                new TermsValuesSourceBuilder("foo").field("bar"),
                new DateHistogramValuesSourceBuilder("date1").field("time").fixedInterval(DateHistogramInterval.days(1)).order("desc")
            )
        );
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedConfig.Builder.validateCompositeAggregationSources(aggregationBuilder)
        );
        assertThat(ex.getMessage(), containsString("must be sorted in ascending order"));
    }

    public void testValidateAggregations_GivenMulitpleHistogramAggs() {
        DateHistogramAggregationBuilder nestedDateHistogram = AggregationBuilders.dateHistogram("nested_time");
        AvgAggregationBuilder avg = AggregationBuilders.avg("avg").subAggregation(nestedDateHistogram);
        TermsAggregationBuilder nestedTerms = AggregationBuilders.terms("nested_terms");

        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time");

        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        dateHistogram.subAggregation(avg).subAggregation(nestedTerms).subAggregation(maxTime).field("time");

        TermsAggregationBuilder toplevelTerms = AggregationBuilders.terms("top_level");
        toplevelTerms.subAggregation(dateHistogram);

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> DatafeedConfig.validateAggregations(new AggregatorFactories.Builder().addAggregator(toplevelTerms))
        );

        assertEquals("Aggregations can only have 1 date_histogram or histogram aggregation", e.getMessage());
    }

    public void testDefaultFrequency_GivenNegative() {
        DatafeedConfig datafeed = createTestInstance();
        ESTestCase.expectThrows(
            IllegalArgumentException.class,
            () -> datafeed.defaultFrequency(TimeValue.timeValueSeconds(-1), xContentRegistry())
        );
    }

    public void testDefaultFrequency_GivenNoAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", "job");
        datafeedBuilder.setIndices(Collections.singletonList("my_index"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(30), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(121), xContentRegistry()));

        assertEquals(TimeValue.timeValueSeconds(61), datafeed.defaultFrequency(TimeValue.timeValueSeconds(122), xContentRegistry()));
        assertEquals(TimeValue.timeValueSeconds(75), datafeed.defaultFrequency(TimeValue.timeValueSeconds(150), xContentRegistry()));
        assertEquals(TimeValue.timeValueSeconds(150), datafeed.defaultFrequency(TimeValue.timeValueSeconds(300), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1200), xContentRegistry()));

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1201), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1800), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(1), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(2), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(12), xContentRegistry()));

        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(12 * 3600 + 1), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(24), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(48), xContentRegistry()));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramOrCompositeInterval_1_Second() {
        DatafeedConfig datafeed = randomBoolean() ? createDatafeedWithDateHistogram("1s") : createDatafeedWithCompositeAgg("1s");

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120), xContentRegistry()));
        assertEquals(TimeValue.timeValueSeconds(125), datafeed.defaultFrequency(TimeValue.timeValueSeconds(250), xContentRegistry()));
        assertEquals(TimeValue.timeValueSeconds(250), datafeed.defaultFrequency(TimeValue.timeValueSeconds(500), xContentRegistry()));

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(1), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13), xContentRegistry()));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramOrCompositeInterval_1_Minute() {
        DatafeedConfig datafeed = randomBoolean() ? createDatafeedWithDateHistogram("1m") : createDatafeedWithCompositeAgg("1m");

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(180), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(2), datafeed.defaultFrequency(TimeValue.timeValueSeconds(240), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(20), xContentRegistry()));

        assertEquals(
            TimeValue.timeValueMinutes(10),
            datafeed.defaultFrequency(TimeValue.timeValueSeconds(20 * 60 + 1), xContentRegistry())
        );
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(6), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(12), xContentRegistry()));

        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(72), xContentRegistry()));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramOrCompositeInterval_10_Minutes() {
        DatafeedConfig datafeed = randomBoolean() ? createDatafeedWithDateHistogram("10m") : createDatafeedWithCompositeAgg("10m");
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(10), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(20), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(30), xContentRegistry()));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(12 * 60), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueMinutes(13 * 60), xContentRegistry()));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramOrCompositeInterval_1_Hour() {
        DatafeedConfig datafeed = randomBoolean() ? createDatafeedWithDateHistogram("1h") : createDatafeedWithCompositeAgg("1h");
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(1), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(3601), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(2), xContentRegistry()));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(12), xContentRegistry()));
    }

    public void testSerializationOfComplexAggs() throws IOException {
        MaxAggregationBuilder maxTime = new MaxAggregationBuilder("timestamp").field("timestamp");
        AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("bytes_in_avg").field("system.network.in.bytes");
        DerivativePipelineAggregationBuilder derivativePipelineAggregationBuilder = new DerivativePipelineAggregationBuilder(
            "bytes_in_derivative",
            "bytes_in_avg"
        );
        BucketScriptPipelineAggregationBuilder bucketScriptPipelineAggregationBuilder = new BucketScriptPipelineAggregationBuilder(
            "non_negative_bytes",
            Collections.singletonMap("bytes", "bytes_in_derivative"),
            new Script("params.bytes > 0 ? params.bytes : null")
        );
        DateHistogramAggregationBuilder dateHistogram = new DateHistogramAggregationBuilder("histogram_buckets").field("timestamp")
            .fixedInterval(new DateHistogramInterval("300000ms"))
            .timeZone(ZoneOffset.UTC)
            .subAggregation(maxTime)
            .subAggregation(avgAggregationBuilder)
            .subAggregation(derivativePipelineAggregationBuilder)
            .subAggregation(bucketScriptPipelineAggregationBuilder);
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilderWithDateHistogram(dateHistogram);
        datafeedConfigBuilder.setQueryProvider(
            createRandomValidQueryProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))
        );
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder().addAggregator(dateHistogram);

        XContentType xContentType = XContentType.JSON;
        BytesReference bytes = XContentHelper.toXContent(datafeedConfig, xContentType, false);
        XContentParser parser = XContentHelper.createParser(
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
            bytes,
            xContentType
        );

        DatafeedConfig parsedDatafeedConfig = doParseInstance(parser);
        assertEquals(datafeedConfig, parsedDatafeedConfig);

        // Assert that the parsed versions of our aggs and queries work as well
        assertEquals(aggBuilder, parsedDatafeedConfig.getParsedAggregations(xContentRegistry()));
        assertEquals(datafeedConfig.getQuery(), parsedDatafeedConfig.getQuery());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            datafeedConfig.writeTo(output);
            try (StreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                DatafeedConfig streamedDatafeedConfig = new DatafeedConfig(streamInput);
                assertEquals(datafeedConfig, streamedDatafeedConfig);

                // Assert that the parsed versions of our aggs and queries work as well
                assertEquals(aggBuilder, streamedDatafeedConfig.getParsedAggregations(xContentRegistry()));
                assertEquals(datafeedConfig.getQuery(), streamedDatafeedConfig.getQuery());
            }
        }
    }

    public void testSerializationOfComplexAggsBetweenVersions() throws IOException {
        MaxAggregationBuilder maxTime = new MaxAggregationBuilder("timestamp").field("timestamp");
        AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("bytes_in_avg").field("system.network.in.bytes");
        DerivativePipelineAggregationBuilder derivativePipelineAggregationBuilder = new DerivativePipelineAggregationBuilder(
            "bytes_in_derivative",
            "bytes_in_avg"
        );
        BucketScriptPipelineAggregationBuilder bucketScriptPipelineAggregationBuilder = new BucketScriptPipelineAggregationBuilder(
            "non_negative_bytes",
            Collections.singletonMap("bytes", "bytes_in_derivative"),
            new Script("params.bytes > 0 ? params.bytes : null")
        );
        DateHistogramAggregationBuilder dateHistogram = new DateHistogramAggregationBuilder("histogram_buckets").field("timestamp")
            .fixedInterval(new DateHistogramInterval("30000ms"))
            .timeZone(ZoneOffset.UTC)
            .subAggregation(maxTime)
            .subAggregation(avgAggregationBuilder)
            .subAggregation(derivativePipelineAggregationBuilder)
            .subAggregation(bucketScriptPipelineAggregationBuilder);
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilderWithDateHistogram(dateHistogram);
        // So equality check between the streamed and current passes
        // Streamed DatafeedConfigs when they are before 6.6.0 require a parsed object for aggs and queries, consequently all the default
        // values are added between them
        datafeedConfigBuilder.setQueryProvider(
            QueryProvider.fromParsedQuery(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)))
            )
        );
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.CURRENT);
            datafeedConfig.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                in.setVersion(Version.CURRENT);
                DatafeedConfig streamedDatafeedConfig = new DatafeedConfig(in);
                assertEquals(datafeedConfig, streamedDatafeedConfig);

                // Assert that the parsed versions of our aggs and queries work as well
                assertEquals(
                    new AggregatorFactories.Builder().addAggregator(dateHistogram),
                    streamedDatafeedConfig.getParsedAggregations(xContentRegistry())
                );
                assertEquals(datafeedConfig.getParsedQuery(xContentRegistry()), streamedDatafeedConfig.getParsedQuery(xContentRegistry()));
            }
        }
    }

    public void testCopyingDatafeedDoesNotCauseStackOverflow() {
        DatafeedConfig datafeed = createTestInstance();
        for (int i = 0; i < 100000; i++) {
            datafeed = new DatafeedConfig.Builder(datafeed).build();
        }
    }

    public static String randomValidDatafeedId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static DatafeedConfig createDatafeedWithCompositeAgg(String interval) {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramValuesSourceBuilder sourceBuilder = new DateHistogramValuesSourceBuilder("time");
        sourceBuilder.field("time");
        if (interval != null) {
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval) != null) {
                sourceBuilder.calendarInterval(new DateHistogramInterval(interval));
            } else {
                sourceBuilder.fixedInterval(new DateHistogramInterval(interval));
            }
        }
        CompositeAggregationBuilder composite = AggregationBuilders.composite("buckets", Arrays.asList(sourceBuilder))
            .subAggregation(maxTime);
        return createDatafeedWithComposite(composite);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(String interval) {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("buckets").subAggregation(maxTime).field("time");
        if (interval != null) {
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval) != null) {
                dateHistogram.calendarInterval(new DateHistogramInterval(interval));
            } else {
                dateHistogram.fixedInterval(new DateHistogramInterval(interval));
            }
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(Long interval) {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("buckets").subAggregation(maxTime).field("time");
        if (interval != null) {
            dateHistogram.fixedInterval(new DateHistogramInterval(interval + "ms"));
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig.Builder createDatafeedBuilderWithDateHistogram(DateHistogramAggregationBuilder dateHistogram) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(dateHistogram);
        DatafeedConfig.validateAggregations(aggs);
        builder.setParsedAggregations(aggs);
        return builder;
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(DateHistogramAggregationBuilder dateHistogram) {
        return createDatafeedBuilderWithDateHistogram(dateHistogram).build();
    }

    private static DatafeedConfig.Builder createDatafeedBuilderWithComposite(CompositeAggregationBuilder compositeAggregationBuilder) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(compositeAggregationBuilder);
        DatafeedConfig.validateAggregations(aggs);
        builder.setParsedAggregations(aggs);
        return builder;
    }

    private static DatafeedConfig createDatafeedWithComposite(CompositeAggregationBuilder dateHistogram) {
        return createDatafeedBuilderWithComposite(dateHistogram).build();
    }

    @Override
    protected DatafeedConfig mutateInstance(DatafeedConfig instance) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(instance);
        switch (between(0, 12)) {
            case 0:
                builder.setId(instance.getId() + randomValidDatafeedId());
                break;
            case 1:
                builder.setJobId(instance.getJobId() + randomAlphaOfLength(5));
                break;
            case 2:
                builder.setQueryDelay(new TimeValue(instance.getQueryDelay().millis() + between(100, 100000)));
                break;
            case 3:
                if (instance.getFrequency() == null) {
                    builder.setFrequency(new TimeValue(between(1, 10) * 1000));
                } else {
                    builder.setFrequency(new TimeValue(instance.getFrequency().millis() + between(1, 10) * 1000));
                }
                break;
            case 4:
                List<String> indices = new ArrayList<>(instance.getIndices());
                indices.add(randomAlphaOfLengthBetween(1, 20));
                builder.setIndices(indices);
                break;
            case 5:
                BoolQueryBuilder query = new BoolQueryBuilder();
                if (instance.getParsedQuery(xContentRegistry()) != null) {
                    query.must(instance.getParsedQuery(xContentRegistry()));
                }
                query.filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
                builder.setParsedQuery(query);
                break;
            case 6:
                if (instance.hasAggregations()) {
                    builder.setAggProvider(null);
                } else {
                    AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
                    String timeField = randomAlphaOfLength(10);
                    long fixedInterval = between(10000, 3600000);

                    aggBuilder.addAggregator(
                        new DateHistogramAggregationBuilder(timeField).field(timeField)
                            .fixedInterval(new DateHistogramInterval(fixedInterval + "ms"))
                            .subAggregation(new MaxAggregationBuilder(timeField).field(timeField))
                    );
                    builder.setParsedAggregations(aggBuilder);
                    if (instance.getScriptFields().isEmpty() == false) {
                        builder.setScriptFields(Collections.emptyList());
                    }
                }
                break;
            case 7:
                builder.setScriptFields(
                    CollectionUtils.appendToCopy(
                        instance.getScriptFields(),
                        new ScriptField(randomAlphaOfLengthBetween(1, 10), new Script("foo"), true)
                    )
                );
                builder.setAggProvider(null);
                break;
            case 8:
                builder.setScrollSize(instance.getScrollSize() + between(1, 100));
                break;
            case 9:
                if (instance.getChunkingConfig() == null || instance.getChunkingConfig().getMode() == Mode.AUTO) {
                    ChunkingConfig newChunkingConfig = ChunkingConfig.newManual(new TimeValue(randomNonNegativeLong()));
                    builder.setChunkingConfig(newChunkingConfig);
                } else {
                    builder.setChunkingConfig(ChunkingConfig.newAuto());
                }
                break;
            case 10:
                if (instance.getMaxEmptySearches() == null) {
                    builder.setMaxEmptySearches(randomIntBetween(10, 100));
                } else {
                    builder.setMaxEmptySearches(instance.getMaxEmptySearches() + 1);
                }
                break;
            case 11:
                builder.setIndicesOptions(
                    IndicesOptions.fromParameters(
                        randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                        Boolean.toString(instance.getIndicesOptions().ignoreUnavailable() == false),
                        Boolean.toString(instance.getIndicesOptions().allowNoIndices() == false),
                        Boolean.toString(instance.getIndicesOptions().ignoreThrottled() == false),
                        SearchRequest.DEFAULT_INDICES_OPTIONS
                    )
                );
                break;
            case 12:
                if (instance.getRuntimeMappings() != null && instance.getRuntimeMappings().isEmpty() == false) {
                    builder.setRuntimeMappings(Collections.emptyMap());
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

    private XContentParser parser(String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
    }

    private XContentParser parser(BytesReference json) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
            json.streamInput()
        );
    }
}
