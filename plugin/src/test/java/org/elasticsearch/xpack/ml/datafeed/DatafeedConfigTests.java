/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DatafeedConfigTests extends AbstractSerializingTestCase<DatafeedConfig> {

    @Override
    protected DatafeedConfig createTestInstance() {
        return createRandomizedDatafeedConfig(randomAsciiOfLength(10));
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId) {
        return createRandomizedDatafeedConfig(jobId, 3600000);
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId, long bucketSpanMillis) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(randomValidDatafeedId(), jobId);
        builder.setIndexes(randomStringList(1, 10));
        builder.setTypes(randomStringList(1, 10));
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.termQuery(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        boolean addScriptFields = randomBoolean();
        if (addScriptFields) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10), new Script(randomAsciiOfLength(10)),
                        randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        if (randomBoolean() && addScriptFields == false) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list writeable / xconent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            long interval = randomNonNegativeLong();
            interval = interval > bucketSpanMillis ? bucketSpanMillis : interval;
            interval = interval <= 0 ? 1 : interval;
            aggs.addAggregator(AggregationBuilders.dateHistogram("time").interval(interval));
            builder.setAggregations(aggs);
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setSource(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        return builder.build();
    }

    public static List<String> randomStringList(int min, int max) {
        int size = scaledRandomIntBetween(min, max);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(randomAsciiOfLength(10));
        }
        return list;
    }

    @Override
    protected Writeable.Reader<DatafeedConfig> instanceReader() {
        return DatafeedConfig::new;
    }

    @Override
    protected DatafeedConfig parseInstance(XContentParser parser) {
        return DatafeedConfig.PARSER.apply(parser, null).build();
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TESTQUERIES; i++) {
            DatafeedConfig datafeedConfig = createTestInstance();
            DatafeedConfig copy = new DatafeedConfig.Builder(datafeedConfig).build();
            assertEquals(datafeedConfig, copy);
        }
    }

    public void testFillDefaults() {
        DatafeedConfig.Builder expectedDatafeedConfig = new DatafeedConfig.Builder("datafeed1", "job1");
        expectedDatafeedConfig.setIndexes(Arrays.asList("index"));
        expectedDatafeedConfig.setTypes(Arrays.asList("type"));
        expectedDatafeedConfig.setQueryDelay(TimeValue.timeValueMinutes(1));
        expectedDatafeedConfig.setScrollSize(1000);
        DatafeedConfig.Builder defaultedDatafeedConfig = new DatafeedConfig.Builder("datafeed1", "job1");
        defaultedDatafeedConfig.setIndexes(Arrays.asList("index"));
        defaultedDatafeedConfig.setTypes(Arrays.asList("type"));

        assertEquals(expectedDatafeedConfig.build(), defaultedDatafeedConfig.build());
    }

    public void testCheckValid_GivenNullIndexes() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        expectThrows(IllegalArgumentException.class, () -> conf.setIndexes(null));
    }

    public void testCheckValid_GivenEmptyIndexes() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndexes(Collections.emptyList());
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indexes", "[]"), e.getMessage());
    }

    public void testCheckValid_GivenIndexesContainsOnlyNulls() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add(null);
        indexes.add(null);
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndexes(indexes);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indexes", "[null, null]"), e.getMessage());
    }

    public void testCheckValid_GivenIndexesContainsOnlyEmptyStrings() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add("");
        indexes.add("");
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        conf.setIndexes(indexes);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "indexes", "[, ]"), e.getMessage());
    }

    public void testCheckValid_GivenNegativeQueryDelay() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> conf.setQueryDelay(TimeValue.timeValueMillis(-10)));
        assertEquals("query_delay cannot be less than 0. Value = -10", e.getMessage());
    }

    public void testCheckValid_GivenZeroFrequency() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(TimeValue.ZERO));
        assertEquals("frequency cannot be less or equal than 0. Value = 0s", e.getMessage());
    }

    public void testCheckValid_GivenNegativeFrequency() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> conf.setFrequency(TimeValue.timeValueMinutes(-1)));
        assertEquals("frequency cannot be less or equal than 0. Value = -1", e.getMessage());
    }

    public void testCheckValid_GivenNegativeScrollSize() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setScrollSize(-1000));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "scroll_size", -1000L), e.getMessage());
    }

    public void testBuild_GivenScriptFieldsAndAggregations() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", "job1");
        datafeed.setIndexes(Arrays.asList("my_index"));
        datafeed.setTypes(Arrays.asList("my_type"));
        datafeed.setScriptFields(Arrays.asList(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10),
                new Script(randomAsciiOfLength(10)), randomBoolean())));
        datafeed.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> datafeed.build());

        assertThat(e.getMessage(), equalTo("script_fields cannot be used in combination with aggregations"));
    }

    public void testHasAggregations_GivenNull() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(false));
    }

    public void testHasAggregations_NonEmpty() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.dateHistogram("time").interval(300000)));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(true));
    }

    public void testBuild_GivenEmptyAggregations() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build());

        assertThat(e.getMessage(), equalTo("A top level date_histogram (or histogram) aggregation is required"));
    }

    public void testBuild_GivenTopLevelAggIsTerms() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.terms("foo")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build());

        assertThat(e.getMessage(), equalTo("A top level date_histogram (or histogram) aggregation is required"));
    }

    public void testBuild_GivenHistogramWithDefaultInterval() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.histogram("time")));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build());

        assertThat(e.getMessage(), equalTo("Aggregation interval must be greater than 0"));
    }

    public void testBuild_GivenDateHistogramWithInvalidTimeZone() {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time")
                .interval(300000L).timeZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("EST")));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createDatafeedWithDateHistogram(dateHistogram));

        assertThat(e.getMessage(), equalTo("ML requires date_histogram.time_zone to be UTC"));
    }

    public void testBuild_GivenDateHistogramWithDefaultInterval() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createDatafeedWithDateHistogram((String) null));

        assertThat(e.getMessage(), equalTo("Aggregation interval must be greater than 0"));
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createDatafeedWithDateHistogram("8d"));

        assertThat(e.getMessage(), containsString("When specifying a date_histogram calendar interval [8d]"));
    }

    public static String randomValidDatafeedId() {
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(String interval) {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time");
        if (interval != null) {
            dateHistogram.dateHistogramInterval(new DateHistogramInterval(interval));
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(Long interval) {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("time");
        if (interval != null) {
            dateHistogram.interval(interval);
        }
        return createDatafeedWithDateHistogram(dateHistogram);
    }

    private static DatafeedConfig createDatafeedWithDateHistogram(DateHistogramAggregationBuilder dateHistogram) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder().addAggregator(dateHistogram));
        return builder.build();
    }
}
