/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig.Mode;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DatafeedConfigTests extends AbstractSerializingTestCase<DatafeedConfig> {

    @Override
    protected DatafeedConfig createTestInstance() {
        return createRandomizedDatafeedConfig(randomAlphaOfLength(10));
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId) {
        return createRandomizedDatafeedConfig(jobId, 3600000);
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId, long bucketSpanMillis) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(randomValidDatafeedId(), jobId);
        builder.setIndices(randomStringList(1, 10));
        builder.setTypes(randomStringList(0, 10));
        if (randomBoolean()) {
            builder.setParsedQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        boolean addScriptFields = randomBoolean();
        if (addScriptFields) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10), mockScript(randomAlphaOfLength(10)),
                        randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        Long aggHistogramInterval = null;
        if (randomBoolean() && addScriptFields == false) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list writeable / xcontent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggHistogramInterval = randomNonNegativeLong();
            aggHistogramInterval = aggHistogramInterval> bucketSpanMillis ? bucketSpanMillis : aggHistogramInterval;
            aggHistogramInterval = aggHistogramInterval <= 0 ? 1 : aggHistogramInterval;
            MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
            aggs.addAggregator(AggregationBuilders.dateHistogram("buckets")
                    .interval(aggHistogramInterval).subAggregation(maxTime).field("time"));
            builder.setParsedAggregations(aggs);
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            if (aggHistogramInterval == null) {
                builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
            } else {
                builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 5) * aggHistogramInterval));
            }
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        if (randomBoolean()) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfigTests.createRandomizedConfig(bucketSpanMillis));
        }
        return builder.build();
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

    private static final String FUTURE_DATAFEED = "{\n" +
            "    \"datafeed_id\": \"farequote-datafeed\",\n" +
            "    \"job_id\": \"farequote\",\n" +
            "    \"frequency\": \"1h\",\n" +
            "    \"indices\": [\"farequote1\", \"farequote2\"],\n" +
            "    \"tomorrows_technology_today\": \"amazing\",\n" +
            "    \"scroll_size\": 1234\n" +
            "}";

    private static final String ANACHRONISTIC_QUERY_DATAFEED = "{\n" +
            "    \"datafeed_id\": \"farequote-datafeed\",\n" +
            "    \"job_id\": \"farequote\",\n" +
            "    \"frequency\": \"1h\",\n" +
            "    \"indices\": [\"farequote1\", \"farequote2\"],\n" +
                 //query:match:type stopped being supported in 6.x
            "    \"query\": {\"match\" : {\"query\":\"fieldName\", \"type\": \"phrase\"}},\n" +
            "    \"scroll_size\": 1234\n" +
            "}";

    private static final String ANACHRONISTIC_AGG_DATAFEED = "{\n" +
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
        "        },\n" +
        "        \"airline\": {\n" +
        "          \"terms\": {\n" +
        "            \"field\": \"airline\",\n" +
        "            \"size\": 0\n" + //size: 0 stopped being supported in 6.x
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";

    public void testFutureConfigParse() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, FUTURE_DATAFEED);
        XContentParseException e = expectThrows(XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build());
        assertEquals("[6:5] [datafeed_config] unknown field [tomorrows_technology_today], parser not found", e.getMessage());
    }

    public void testPastQueryConfigParse() throws IOException {
        try(XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ANACHRONISTIC_QUERY_DATAFEED)) {

            DatafeedConfig config = DatafeedConfig.LENIENT_PARSER.apply(parser, null).build();
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> config.getParsedQuery());
            assertEquals("[match] query doesn't support multiple fields, found [query] and [type]", e.getMessage());
        }

        try(XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ANACHRONISTIC_QUERY_DATAFEED)) {

            XContentParseException e = expectThrows(XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build());
            assertEquals("[6:25] [datafeed_config] failed to parse field [query]", e.getMessage());
        }
    }

    public void testPastAggConfigParse() throws IOException {
        try(XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ANACHRONISTIC_AGG_DATAFEED)) {

            DatafeedConfig.Builder configBuilder = DatafeedConfig.LENIENT_PARSER.apply(parser, null);
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> configBuilder.build());
            assertEquals(
                "Datafeed [farequote-datafeed] aggregations are not parsable: [size] must be greater than 0. Found [0] in [airline]",
                e.getMessage());
        }

        try(XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ANACHRONISTIC_AGG_DATAFEED)) {

            XContentParseException e = expectThrows(XContentParseException.class,
                () -> DatafeedConfig.STRICT_PARSER.apply(parser, null).build());
            assertEquals("[8:25] [datafeed_config] failed to parse field [aggregations]", e.getMessage());
        }
    }

    public void testFutureMetadataParse() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, FUTURE_DATAFEED);
        // Unlike the config version of this test, the metadata parser should tolerate the unknown future field
        assertNotNull(DatafeedConfig.LENIENT_PARSER.apply(parser, null).build());
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

    public void testCheckValid_GivenEmptyIndices() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndices(Collections.emptyList());
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indices", "[]"), e.getMessage());
    }

    public void testCheckValid_GivenIndicesContainsOnlyNulls() {
        List<String> indices = new ArrayList<>();
        indices.add(null);
        indices.add(null);
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndices(indices);
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indices", "[null, null]"), e.getMessage());
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
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> conf.setQueryDelay(TimeValue.timeValueMillis(-10)));
        assertEquals("query_delay cannot be less than 0. Value = -10", e.getMessage());
    }

    public void testCheckValid_GivenZeroFrequency() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(TimeValue.ZERO));
        assertEquals("frequency cannot be less or equal than 0. Value = 0s", e.getMessage());
    }

    public void testCheckValid_GivenNegativeFrequency() {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> conf.setFrequency(TimeValue.timeValueMinutes(-1)));
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
        datafeed.setTypes(Collections.singletonList("my_type"));
        datafeed.setScriptFields(Collections.singletonList(new SearchSourceBuilder.ScriptField(randomAlphaOfLength(10),
                mockScript(randomAlphaOfLength(10)), randomBoolean())));
        datafeed.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));

        ElasticsearchException e = expectThrows(ElasticsearchException.class, datafeed::build);

        assertThat(e.getMessage(), equalTo("script_fields cannot be used in combination with aggregations"));
    }

    public void testHasAggregations_GivenNull() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(false));
    }

    public void testHasAggregations_NonEmpty() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        builder.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.dateHistogram("time").interval(300000).subAggregation(maxTime).field("time")));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(true));
    }

    public void testBuild_GivenEmptyAggregations() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        builder.setParsedAggregations(new AggregatorFactories.Builder());

        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);

        assertThat(e.getMessage(), equalTo("A date_histogram (or histogram) aggregation is required"));
    }

    public void testBuild_GivenHistogramWithDefaultInterval() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        builder.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.histogram("time").subAggregation(maxTime).field("time"))
        );

        ElasticsearchException e = expectThrows(ElasticsearchException.class, builder::build);

        assertThat(e.getMessage(), containsString("[interval] must be >0 for histogram aggregation [time]"));
    }

    public void testBuild_GivenDateHistogramWithInvalidTimeZone() {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("bucket").field("time")
                .interval(300000L).timeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("EST"))).subAggregation(maxTime);
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> createDatafeedWithDateHistogram(dateHistogram));

        assertThat(e.getMessage(), equalTo("ML requires date_histogram.time_zone to be UTC"));
    }

    public void testBuild_GivenDateHistogramWithDefaultInterval() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> createDatafeedWithDateHistogram((String) null));

        assertThat(e.getMessage(), containsString("Aggregation interval must be greater than 0"));
    }

    public void testBuild_GivenValidDateHistogram() {
        long millisInDay = 24 * 3600000L;

        assertThat(createDatafeedWithDateHistogram("1s").getHistogramIntervalMillis(), equalTo(1000L));
        assertThat(createDatafeedWithDateHistogram("2s").getHistogramIntervalMillis(), equalTo(2000L));
        assertThat(createDatafeedWithDateHistogram("1m").getHistogramIntervalMillis(), equalTo(60000L));
        assertThat(createDatafeedWithDateHistogram("2m").getHistogramIntervalMillis(), equalTo(120000L));
        assertThat(createDatafeedWithDateHistogram("1h").getHistogramIntervalMillis(), equalTo(3600000L));
        assertThat(createDatafeedWithDateHistogram("2h").getHistogramIntervalMillis(), equalTo(7200000L));
        assertThat(createDatafeedWithDateHistogram("1d").getHistogramIntervalMillis(), equalTo(millisInDay));
        assertThat(createDatafeedWithDateHistogram("7d").getHistogramIntervalMillis(), equalTo(7 * millisInDay));

        assertThat(createDatafeedWithDateHistogram(7 * millisInDay + 1).getHistogramIntervalMillis(),
                equalTo(7 * millisInDay + 1));
    }

    public void testBuild_GivenDateHistogramWithMoreThanCalendarWeek() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> createDatafeedWithDateHistogram("8d"));

        assertThat(e.getMessage(), containsString("When specifying a date_histogram calendar interval [8d]"));
    }

    public void testDefaultChunkingConfig_GivenAggregations() {
        assertThat(createDatafeedWithDateHistogram("1s").getChunkingConfig(),
                equalTo(ChunkingConfig.newManual(TimeValue.timeValueSeconds(1000))));
        assertThat(createDatafeedWithDateHistogram("2h").getChunkingConfig(),
                equalTo(ChunkingConfig.newManual(TimeValue.timeValueHours(2000))));
    }

    public void testChunkingConfig_GivenExplicitSetting() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(createDatafeedWithDateHistogram("30s"));
        builder.setChunkingConfig(ChunkingConfig.newAuto());
        assertThat(builder.build().getChunkingConfig(), equalTo(ChunkingConfig.newAuto()));
    }

    public void testCheckHistogramAggregationHasChildMaxTimeAgg() {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time_agg").field("max_time");

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> DatafeedConfig.Builder.checkHistogramAggregationHasChildMaxTimeAgg(dateHistogram));

        assertThat(e.getMessage(), containsString("Date histogram must have nested max aggregation for time_field [max_time]"));
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

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> DatafeedConfig.validateAggregations(new AggregatorFactories.Builder().addAggregator(toplevelTerms)));

        assertEquals("Aggregations can only have 1 date_histogram or histogram aggregation", e.getMessage());
    }

    public void testDefaultFrequency_GivenNegative() {
        DatafeedConfig datafeed = createTestInstance();
        ESTestCase.expectThrows(IllegalArgumentException.class, () -> datafeed.defaultFrequency(TimeValue.timeValueSeconds(-1)));
    }

    public void testDefaultFrequency_GivenNoAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", "job");
        datafeedBuilder.setIndices(Collections.singletonList("my_index"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(30)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(121)));

        assertEquals(TimeValue.timeValueSeconds(61), datafeed.defaultFrequency(TimeValue.timeValueSeconds(122)));
        assertEquals(TimeValue.timeValueSeconds(75), datafeed.defaultFrequency(TimeValue.timeValueSeconds(150)));
        assertEquals(TimeValue.timeValueSeconds(150), datafeed.defaultFrequency(TimeValue.timeValueSeconds(300)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1200)));

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1201)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(1800)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(1)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(2)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(12)));

        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(12 * 3600 + 1)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(24)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(48)));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramInterval_1_Second() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("1s");

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueSeconds(125), datafeed.defaultFrequency(TimeValue.timeValueSeconds(250)));
        assertEquals(TimeValue.timeValueSeconds(250), datafeed.defaultFrequency(TimeValue.timeValueSeconds(500)));

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(1)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13)));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramInterval_1_Minute() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("1m");

        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(60)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(90)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(120)));
        assertEquals(TimeValue.timeValueMinutes(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(180)));
        assertEquals(TimeValue.timeValueMinutes(2), datafeed.defaultFrequency(TimeValue.timeValueSeconds(240)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(20)));

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueSeconds(20 * 60 + 1)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(6)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueHours(12)));

        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(13)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(72)));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramInterval_10_Minutes() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("10m");

        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(10)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(20)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(30)));
        assertEquals(TimeValue.timeValueMinutes(10), datafeed.defaultFrequency(TimeValue.timeValueMinutes(12 * 60)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueMinutes(13 * 60)));
    }

    public void testDefaultFrequency_GivenAggregationsWithHistogramInterval_1_Hour() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("1h");

        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(1)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueSeconds(3601)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(2)));
        assertEquals(TimeValue.timeValueHours(1), datafeed.defaultFrequency(TimeValue.timeValueHours(12)));
    }

    public void testGetAggDeprecations() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("1h");
        String deprecationWarning = "Warning";
        List<String> deprecations = datafeed.getAggDeprecations((map, id, deprecationlist) -> {
            deprecationlist.add(deprecationWarning);
            return new AggregatorFactories.Builder().addAggregator(new MaxAggregationBuilder("field").field("field"));
        });
        assertThat(deprecations, hasItem(deprecationWarning));

        DatafeedConfig spiedConfig = spy(datafeed);
        spiedConfig.getAggDeprecations();
        verify(spiedConfig).getAggDeprecations(DatafeedConfig.lazyAggParser);
    }

    public void testGetQueryDeprecations() {
        DatafeedConfig datafeed = createDatafeedWithDateHistogram("1h");
        String deprecationWarning = "Warning";
        List<String> deprecations = datafeed.getQueryDeprecations((map, id, deprecationlist) -> {
            deprecationlist.add(deprecationWarning);
            return new BoolQueryBuilder();
        });
        assertThat(deprecations, hasItem(deprecationWarning));

        DatafeedConfig spiedConfig = spy(datafeed);
        spiedConfig.getQueryDeprecations();
        verify(spiedConfig).getQueryDeprecations(DatafeedConfig.lazyQueryParser);
    }

    public void testSerializationOfComplexAggs() throws IOException {
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
                .field("timestamp").interval(300000).timeZone(DateTimeZone.UTC)
                .subAggregation(maxTime)
                .subAggregation(avgAggregationBuilder)
                .subAggregation(derivativePipelineAggregationBuilder)
                .subAggregation(bucketScriptPipelineAggregationBuilder);
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilderWithDateHistogram(dateHistogram);
        QueryBuilder terms =
            new BoolQueryBuilder().filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        datafeedConfigBuilder.setParsedQuery(terms);
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder().addAggregator(dateHistogram);


        XContentType xContentType = XContentType.JSON;
        BytesReference bytes = XContentHelper.toXContent(datafeedConfig, xContentType, false);
        XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            bytes,
            xContentType);

        DatafeedConfig parsedDatafeedConfig = doParseInstance(parser);
        assertEquals(datafeedConfig, parsedDatafeedConfig);

        // Assert that the parsed versions of our aggs and queries work as well
        assertEquals(aggBuilder, parsedDatafeedConfig.getParsedAggregations());
        assertEquals(terms, parsedDatafeedConfig.getParsedQuery());

        try(BytesStreamOutput output = new BytesStreamOutput()) {
            datafeedConfig.writeTo(output);
            try(StreamInput streamInput = output.bytes().streamInput()) {
                DatafeedConfig streamedDatafeedConfig = new DatafeedConfig(streamInput);
                assertEquals(datafeedConfig, streamedDatafeedConfig);

                // Assert that the parsed versions of our aggs and queries work as well
                assertEquals(aggBuilder, streamedDatafeedConfig.getParsedAggregations());
                assertEquals(terms, streamedDatafeedConfig.getParsedQuery());
            }
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
                .field("timestamp").interval(300000).timeZone(DateTimeZone.UTC)
                .subAggregation(maxTime)
                .subAggregation(avgAggregationBuilder)
                .subAggregation(derivativePipelineAggregationBuilder)
                .subAggregation(bucketScriptPipelineAggregationBuilder);
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilderWithDateHistogram(dateHistogram);
        QueryBuilder terms =
            new BoolQueryBuilder().filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        datafeedConfigBuilder.setParsedQuery(terms);
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();

        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_6_0_0);
            datafeedConfig.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(Version.V_6_0_0);
                DatafeedConfig streamedDatafeedConfig = new DatafeedConfig(in);
                assertEquals(datafeedConfig, streamedDatafeedConfig);

                // Assert that the parsed versions of our aggs and queries work as well
                assertEquals(new AggregatorFactories.Builder().addAggregator(dateHistogram),
                    streamedDatafeedConfig.getParsedAggregations());
                assertEquals(terms, streamedDatafeedConfig.getParsedQuery());
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
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(String interval) {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("buckets").subAggregation(maxTime).field("time");
        if (interval != null) {
            dateHistogram.dateHistogramInterval(new DateHistogramInterval(interval));
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(Long interval) {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("buckets").subAggregation(maxTime).field("time");
        if (interval != null) {
            dateHistogram.interval(interval);
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig.Builder createDatafeedBuilderWithDateHistogram(DateHistogramAggregationBuilder dateHistogram) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndices(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder().addAggregator(dateHistogram);
        DatafeedConfig.validateAggregations(aggs);
        builder.setParsedAggregations(aggs);
        return builder;
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(DateHistogramAggregationBuilder dateHistogram) {
        return createDatafeedBuilderWithDateHistogram(dateHistogram).build();
    }

    @Override
    protected DatafeedConfig mutateInstance(DatafeedConfig instance) throws IOException {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(instance);
        switch (between(0, 10)) {
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
            List<String> types = new ArrayList<>(instance.getTypes());
            types.add(randomAlphaOfLengthBetween(1, 20));
            builder.setTypes(types);
            break;
        case 6:
            BoolQueryBuilder query = new BoolQueryBuilder();
            if (instance.getParsedQuery() != null) {
               query.must(instance.getParsedQuery());
            }
            query.filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            builder.setParsedQuery(query);
            break;
        case 7:
            if (instance.hasAggregations()) {
                builder.setAggregations(null);
            } else {
                AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
                String timeField = randomAlphaOfLength(10);
                aggBuilder
                        .addAggregator(new DateHistogramAggregationBuilder(timeField).field(timeField).interval(between(10000, 3600000))
                                .subAggregation(new MaxAggregationBuilder(timeField).field(timeField)));
                builder.setParsedAggregations(aggBuilder);
                if (instance.getScriptFields().isEmpty() == false) {
                    builder.setScriptFields(Collections.emptyList());
                }
            }
            break;
        case 8:
            ArrayList<ScriptField> scriptFields = new ArrayList<>(instance.getScriptFields());
            scriptFields.add(new ScriptField(randomAlphaOfLengthBetween(1, 10), new Script("foo"), true));
            builder.setScriptFields(scriptFields);
            builder.setAggregations(null);
            break;
        case 9:
            builder.setScrollSize(instance.getScrollSize() + between(1, 100));
            break;
        case 10:
            if (instance.getChunkingConfig() == null || instance.getChunkingConfig().getMode() == Mode.AUTO) {
                ChunkingConfig newChunkingConfig = ChunkingConfig.newManual(new TimeValue(randomNonNegativeLong()));
                builder.setChunkingConfig(newChunkingConfig);
            } else {
                builder.setChunkingConfig(ChunkingConfig.newAuto());
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return builder.build();
    }
}
