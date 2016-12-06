/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.support.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulerConfigTests extends AbstractSerializingTestCase<SchedulerConfig> {

    @Override
    protected SchedulerConfig createTestInstance() {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(randomStringList(1, 10), randomStringList(1, 10));
        if (randomBoolean()) {
            builder.setQuery(Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        boolean retrieveWholeSource = randomBoolean();
        if (retrieveWholeSource) {
            builder.setRetrieveWholeSource(randomBoolean());
        } else if (randomBoolean()) {
            builder.setScriptFields(Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setAggregations(Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        } else if (randomBoolean()) {
            builder.setAggs(Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setFrequency(randomPositiveLong());
        }
        if (randomBoolean()) {
            builder.setQueryDelay(randomPositiveLong());
        }
        return builder.build();
    }

    private static List<String> randomStringList(int min, int max) {
        int size = scaledRandomIntBetween(min, max);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(randomAsciiOfLength(10));
        }
        return list;
    }

    @Override
    protected Writeable.Reader<SchedulerConfig> instanceReader() {
        return SchedulerConfig::new;
    }

    @Override
    protected SchedulerConfig parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return SchedulerConfig.PARSER.apply(parser, () -> matcher).build();
    }

    /**
     * Test parsing of the opaque {@link SchedulerConfig#getQuery()} object
     */
    public void testAnalysisConfigRequiredFields() throws IOException {
        Logger logger = Loggers.getLogger(SchedulerConfigTests.class);

        String jobConfigStr = "{" + "\"job_id\":\"farequote\"," + "\"scheduler_config\" : {"
                + "\"indexes\":[\"farequote\"]," + "\"types\":[\"farequote\"],"
                + "\"query\":{\"match_all\":{} }" + "}," + "\"analysis_config\" : {" + "\"bucket_span\":3600,"
                + "\"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}],"
                + "\"influencers\" :[\"airline\"]" + "}," + "\"data_description\" : {" + "\"format\":\"ELASTICSEARCH\","
                + "\"time_field\":\"@timestamp\"," + "\"time_format\":\"epoch_ms\"" + "}" + "}";

        XContentParser parser = XContentFactory.xContent(jobConfigStr).createParser(jobConfigStr);
        Job jobConfig = Job.PARSER.apply(parser, () -> ParseFieldMatcher.STRICT).build();
        assertNotNull(jobConfig);

        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(jobConfig.getSchedulerConfig());
        assertNotNull(schedulerConfig);

        Map<String, Object> query = schedulerConfig.getQuery();
        assertNotNull(query);

        String queryAsJson = XContentFactory.jsonBuilder().map(query).string();
        logger.info("Round trip of query is: " + queryAsJson);
        assertTrue(query.containsKey("match_all"));
    }

    public void testBuildAggregatedFieldList_GivenNoAggregations() {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        assertTrue(builder.build().buildAggregatedFieldList().isEmpty());
    }

    /**
     * Test parsing of the opaque {@link SchedulerConfig#getAggs()} object
     */

    public void testAggsParse() throws IOException {
        Logger logger = Loggers.getLogger(SchedulerConfigTests.class);

        String jobConfigStr = "{" + "\"job_id\":\"farequote\"," + "\"scheduler_config\" : {"
                + "\"indexes\":[\"farequote\"]," + "\"types\":[\"farequote\"],"
                + "\"query\":{\"match_all\":{} }," + "\"aggs\" : {" + "\"top_level_must_be_time\" : {" + "\"histogram\" : {"
                + "\"field\" : \"@timestamp\"," + "\"interval\" : 3600000" + "}," + "\"aggs\" : {" + "\"by_field_in_the_middle\" : { "
                + "\"terms\" : {" + "\"field\" : \"airline\"," + "\"size\" : 0" + "}," + "\"aggs\" : {" + "\"stats_last\" : {"
                + "\"avg\" : {" + "\"field\" : \"responsetime\"" + "}" + "}" + "} " + "}" + "}" + "}" + "}" + "},"
                + "\"analysis_config\" : {" + "\"summary_count_field_name\":\"doc_count\"," + "\"bucket_span\":3600,"
                + "\"detectors\" :[{\"function\":\"avg\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}],"
                + "\"influencers\" :[\"airline\"]" + "}," + "\"data_description\" : {" + "\"format\":\"ELASTICSEARCH\","
                + "\"time_field\":\"@timestamp\"," + "\"time_format\":\"epoch_ms\"" + "}" + "}";

        XContentParser parser = XContentFactory.xContent(jobConfigStr).createParser(jobConfigStr);
        Job jobConfig = Job.PARSER.parse(parser, () -> ParseFieldMatcher.STRICT).build();
        assertNotNull(jobConfig);

        SchedulerConfig schedulerConfig = jobConfig.getSchedulerConfig();
        assertNotNull(schedulerConfig);

        Map<String, Object> aggs = schedulerConfig.getAggregationsOrAggs();
        assertNotNull(aggs);

        String aggsAsJson = XContentFactory.jsonBuilder().map(aggs).string();
        logger.info("Round trip of aggs is: " + aggsAsJson);
        assertTrue(aggs.containsKey("top_level_must_be_time"));

        List<String> aggregatedFieldList = schedulerConfig.buildAggregatedFieldList();
        assertEquals(3, aggregatedFieldList.size());
        assertEquals("@timestamp", aggregatedFieldList.get(0));
        assertEquals("airline", aggregatedFieldList.get(1));
        assertEquals("responsetime", aggregatedFieldList.get(2));
    }

    public void testFillDefaults_GivenNothingToFill() {
        SchedulerConfig.Builder originalSchedulerConfig = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        originalSchedulerConfig.setQuery(new HashMap<>());
        originalSchedulerConfig.setQueryDelay(30L);
        originalSchedulerConfig.setRetrieveWholeSource(true);
        originalSchedulerConfig.setScrollSize(2000);

        SchedulerConfig.Builder defaultedSchedulerConfig = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        defaultedSchedulerConfig.setQuery(new HashMap<>());
        defaultedSchedulerConfig.setQueryDelay(30L);
        defaultedSchedulerConfig.setRetrieveWholeSource(true);
        defaultedSchedulerConfig.setScrollSize(2000);

        assertEquals(originalSchedulerConfig.build(), defaultedSchedulerConfig.build());
    }

    public void testFillDefaults_GivenDataSourceIsElasticsearchAndDefaultsAreApplied() {
        SchedulerConfig.Builder expectedSchedulerConfig = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        Map<String, Object> defaultQuery = new HashMap<>();
        defaultQuery.put("match_all", new HashMap<String, Object>());
        expectedSchedulerConfig.setQuery(defaultQuery);
        expectedSchedulerConfig.setQueryDelay(60L);
        expectedSchedulerConfig.setRetrieveWholeSource(false);
        expectedSchedulerConfig.setScrollSize(1000);
        SchedulerConfig.Builder defaultedSchedulerConfig = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        assertEquals(expectedSchedulerConfig.build(), defaultedSchedulerConfig.build());
    }

    public void testEquals_GivenDifferentClass() {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        assertFalse(builder.build().equals("a string"));
    }

    public void testEquals_GivenSameRef() {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(Arrays.asList("index"), Arrays.asList("type"));
        SchedulerConfig schedulerConfig = builder.build();
        assertTrue(schedulerConfig.equals(schedulerConfig));
    }

    public void testEquals_GivenEqual() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertTrue(sc1.equals(sc2));
        assertTrue(sc2.equals(sc1));
        assertEquals(sc1.hashCode(), sc2.hashCode());
    }

    public void testEquals_GivenDifferentQueryDelay() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();
        b2.setQueryDelay(120L);

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentScrollSize() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();
        b2.setScrollSize(1);

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentFrequency() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();
        b2.setFrequency(120L);

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentIndexes() {
        SchedulerConfig.Builder sc1 = new SchedulerConfig.Builder(Arrays.asList("myIndex"), Arrays.asList("myType1", "myType2"));
        SchedulerConfig.Builder sc2 = new SchedulerConfig.Builder(Arrays.asList("thisOtherCrazyIndex"),
                Arrays.asList("myType1", "myType2"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentTypes() {
        SchedulerConfig.Builder sc1 = new SchedulerConfig.Builder(Arrays.asList("myIndex"), Arrays.asList("myType1", "myType2"));
        SchedulerConfig.Builder sc2 = new SchedulerConfig.Builder(Arrays.asList("thisOtherCrazyIndex"),
                Arrays.asList("thisOtherCrazyType", "myType2"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentQuery() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();
        Map<String, Object> emptyQuery = new HashMap<>();
        b2.setQuery(emptyQuery);

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentAggregations() {
        SchedulerConfig.Builder sc1 = createFullyPopulated();
        SchedulerConfig.Builder sc2 = createFullyPopulated();
        Map<String, Object> emptyAggs = new HashMap<>();
        sc2.setAggregations(emptyAggs);

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    private static SchedulerConfig.Builder createFullyPopulated() {
        SchedulerConfig.Builder sc = new SchedulerConfig.Builder(Arrays.asList("myIndex"), Arrays.asList("myType1", "myType2"));
        sc.setFrequency(60L);
        sc.setScrollSize(5000);
        Map<String, Object> query = new HashMap<>();
        query.put("foo", new HashMap<>());
        sc.setQuery(query);
        Map<String, Object> aggs = new HashMap<>();
        aggs.put("bar", new HashMap<>());
        sc.setAggregations(aggs);
        sc.setQueryDelay(90L);
        return sc;
    }

    public void testCheckValidElasticsearch_AllOk() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        conf.setQueryDelay(90L);
        String json = "{ \"match_all\" : {} }";
        XContentParser parser = XContentFactory.xContent(json).createParser(json);
        conf.setQuery(parser.map());
        conf.setScrollSize(2000);
        conf.build();
    }

    public void testCheckValidElasticsearch_NoQuery() {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        assertEquals(Collections.singletonMap("match_all", new HashMap<>()), conf.build().getQuery());
    }

    public void testCheckValidElasticsearch_GivenScriptFieldsNotWholeSource() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        String json = "{ \"twiceresponsetime\" : { \"script\" : { \"lang\" : \"expression\", "
                + "\"inline\" : \"doc['responsetime'].value * 2\" } } }";
        XContentParser parser = XContentFactory.xContent(json).createParser(json);
        conf.setScriptFields(parser.map());
        conf.setRetrieveWholeSource(false);
        assertEquals(1, conf.build().getScriptFields().size());
    }

    public void testCheckValidElasticsearch_GivenScriptFieldsAndWholeSource() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        String json = "{ \"twiceresponsetime\" : { \"script\" : { \"lang\" : \"expression\", "
                + "\"inline\" : \"doc['responsetime'].value * 2\" } } }";
        XContentParser parser = XContentFactory.xContent(json).createParser(json);
        conf.setScriptFields(parser.map());
        conf.setRetrieveWholeSource(true);
        expectThrows(IllegalArgumentException.class, conf::build);
    }

    public void testCheckValidElasticsearch_GivenNullIndexes() throws IOException {
        expectThrows(NullPointerException.class, () -> new SchedulerConfig.Builder(null, Arrays.asList("mytype")));
    }

    public void testCheckValidElasticsearch_GivenEmptyIndexes() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Collections.emptyList(), Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "indexes", "[]"), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenIndexesContainsOnlyNulls() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add(null);
        indexes.add(null);
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(indexes, Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "indexes", "[null, null]"), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenIndexesContainsOnlyEmptyStrings() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add("");
        indexes.add("");
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(indexes, Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "indexes", "[, ]"), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenNegativeQueryDelay() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setQueryDelay(-10L));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "query_delay", -10L), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenZeroFrequency() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(0L));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "frequency", 0L), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenNegativeFrequency() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(-600L));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "frequency", -600L), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenNegativeScrollSize() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setScrollSize(-1000));
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, "scroll_size", -1000L), e.getMessage());
    }

    public void testCheckValidElasticsearch_GivenBothAggregationsAndAggsAreSet() {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder(Arrays.asList("myindex"), Arrays.asList("mytype"));
        conf.setScrollSize(1000);
        Map<String, Object> aggs = new HashMap<>();
        conf.setAggregations(aggs);
        conf.setAggs(aggs);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_MULTIPLE_AGGREGATIONS), e.getMessage());
    }
}
