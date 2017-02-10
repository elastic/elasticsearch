/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DatafeedConfigTests extends AbstractSerializingTestCase<DatafeedConfig> {

    @Override
    protected DatafeedConfig createTestInstance() {
        return createRandomizedDatafeedConfig(randomAsciiOfLength(10));
    }

    public static DatafeedConfig createRandomizedDatafeedConfig(String jobId) {
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
            aggs.addAggregator(AggregationBuilders.avg(randomAsciiOfLength(10)).field(randomAsciiOfLength(10)));
            builder.setAggregations(aggs);
        }
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setFrequency(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setQueryDelay(randomNonNegativeLong());
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

    public void testFillDefaults() {
        DatafeedConfig.Builder expectedDatafeedConfig = new DatafeedConfig.Builder("datafeed1", "job1");
        expectedDatafeedConfig.setIndexes(Arrays.asList("index"));
        expectedDatafeedConfig.setTypes(Arrays.asList("type"));
        expectedDatafeedConfig.setQueryDelay(60L);
        expectedDatafeedConfig.setScrollSize(1000);
        DatafeedConfig.Builder defaultedDatafeedConfig = new DatafeedConfig.Builder("datafeed1", "job1");
        defaultedDatafeedConfig.setIndexes(Arrays.asList("index"));
        defaultedDatafeedConfig.setTypes(Arrays.asList("type"));

        assertEquals(expectedDatafeedConfig.build(), defaultedDatafeedConfig.build());
    }

    public void testEquals_GivenDifferentQueryDelay() {
        DatafeedConfig.Builder b1 = createFullyPopulated();
        DatafeedConfig.Builder b2 = createFullyPopulated();
        b2.setQueryDelay(120L);

        DatafeedConfig sc1 = b1.build();
        DatafeedConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentScrollSize() {
        DatafeedConfig.Builder b1 = createFullyPopulated();
        DatafeedConfig.Builder b2 = createFullyPopulated();
        b2.setScrollSize(1);

        DatafeedConfig sc1 = b1.build();
        DatafeedConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentFrequency() {
        DatafeedConfig.Builder b1 = createFullyPopulated();
        DatafeedConfig.Builder b2 = createFullyPopulated();
        b2.setFrequency(120L);

        DatafeedConfig sc1 = b1.build();
        DatafeedConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentIndexes() {
        DatafeedConfig.Builder sc1 = createFullyPopulated();
        DatafeedConfig.Builder sc2 = createFullyPopulated();
        sc2.setIndexes(Arrays.asList("blah", "di", "blah"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentTypes() {
        DatafeedConfig.Builder sc1 = createFullyPopulated();
        DatafeedConfig.Builder sc2 = createFullyPopulated();
        sc2.setTypes(Arrays.asList("blah", "di", "blah"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentQuery() {
        DatafeedConfig.Builder b1 = createFullyPopulated();
        DatafeedConfig.Builder b2 = createFullyPopulated();
        b2.setQuery(QueryBuilders.termQuery("foo", "bar"));

        DatafeedConfig sc1 = b1.build();
        DatafeedConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentAggregations() {
        DatafeedConfig.Builder sc1 = createFullyPopulated();
        DatafeedConfig.Builder sc2 = createFullyPopulated();
        sc2.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.count("foo")));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    private static DatafeedConfig.Builder createFullyPopulated() {
        DatafeedConfig.Builder sc = new DatafeedConfig.Builder("datafeed1", "job1");
        sc.setIndexes(Arrays.asList("myIndex"));
        sc.setTypes(Arrays.asList("myType1", "myType2"));
        sc.setFrequency(60L);
        sc.setScrollSize(5000);
        sc.setQuery(QueryBuilders.matchAllQuery());
        sc.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));
        sc.setQueryDelay(90L);
        return sc;
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
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setQueryDelay(-10L));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "query_delay", -10L), e.getMessage());
    }

    public void testCheckValid_GivenZeroFrequency() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(0L));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "frequency", 0L), e.getMessage());
    }

    public void testCheckValid_GivenNegativeFrequency() throws IOException {
        DatafeedConfig.Builder conf = new DatafeedConfig.Builder("datafeed1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(-600L));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, "frequency", -600L), e.getMessage());
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

    public void testHasAggregations_GivenEmpty() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder());
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(false));
    }

    public void testHasAggregations_NonEmpty() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed1", "job1");
        builder.setIndexes(Arrays.asList("myIndex"));
        builder.setTypes(Arrays.asList("myType"));
        builder.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));
        DatafeedConfig datafeedConfig = builder.build();

        assertThat(datafeedConfig.hasAggregations(), is(true));
    }

    public void testDomainSplitInjection() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", "job1");
        datafeed.setIndexes(Arrays.asList("my_index"));
        datafeed.setTypes(Arrays.asList("my_type"));

        SearchSourceBuilder.ScriptField withoutSplit = new SearchSourceBuilder.ScriptField(
                "script1", new Script("return 1+1;"), false);
        SearchSourceBuilder.ScriptField withSplit = new SearchSourceBuilder.ScriptField(
                "script2", new Script("return domainSplit('foo.com', params);"), false);
        datafeed.setScriptFields(Arrays.asList(withoutSplit, withSplit));

        DatafeedConfig config = datafeed.build();
        List<SearchSourceBuilder.ScriptField> scriptFields = config.getScriptFields();

        assertThat(scriptFields.size(), equalTo(2));
        assertThat(scriptFields.get(0).fieldName(), equalTo("script1"));
        assertThat(scriptFields.get(0).script().getIdOrCode(), equalTo("return 1+1;"));
        assertFalse(scriptFields.get(0).script().getParams().containsKey("exact"));

        assertThat(scriptFields.get(1).fieldName(), equalTo("script2"));
        assertThat(scriptFields.get(1).script().getIdOrCode(), containsString("List domainSplit(String host, Map params)"));
        assertTrue(scriptFields.get(1).script().getParams().containsKey("exact"));
    }

    public static String randomValidDatafeedId() {
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }
}
