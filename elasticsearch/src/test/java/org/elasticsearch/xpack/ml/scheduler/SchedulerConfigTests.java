/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

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

public class SchedulerConfigTests extends AbstractSerializingTestCase<SchedulerConfig> {

    @Override
    protected SchedulerConfig createTestInstance() {
        return createRandomizedSchedulerConfig(randomAsciiOfLength(10));
    }

    public static SchedulerConfig createRandomizedSchedulerConfig(String jobId) {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(randomValidSchedulerId(), jobId);
        builder.setIndexes(randomStringList(1, 10));
        builder.setTypes(randomStringList(1, 10));
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.termQuery(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        if (randomBoolean()) {
            // can only test with a single agg as the xcontent order gets randomized by test base class and then
            // the actual xcontent isn't the same and test fail.
            // Testing with a single agg is ok as we don't have special list writeable / xconent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggs.addAggregator(AggregationBuilders.avg(randomAsciiOfLength(10)).field(randomAsciiOfLength(10)));
            builder.setAggregations(aggs);
        }
        int scriptsSize = randomInt(3);
        List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
        for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
            scriptFields.add(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10), new Script(randomAsciiOfLength(10)),
                    randomBoolean()));
        }
        builder.setScriptFields(scriptFields);
        if (randomBoolean()) {
            builder.setScrollSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setFrequency(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setQueryDelay(randomNonNegativeLong());
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
    protected SchedulerConfig parseInstance(XContentParser parser) {
        return SchedulerConfig.PARSER.apply(parser, null).build();
    }

    public void testFillDefaults() {
        SchedulerConfig.Builder expectedSchedulerConfig = new SchedulerConfig.Builder("scheduler1", "job1");
        expectedSchedulerConfig.setIndexes(Arrays.asList("index"));
        expectedSchedulerConfig.setTypes(Arrays.asList("type"));
        expectedSchedulerConfig.setQueryDelay(60L);
        expectedSchedulerConfig.setScrollSize(1000);
        SchedulerConfig.Builder defaultedSchedulerConfig = new SchedulerConfig.Builder("scheduler1", "job1");
        defaultedSchedulerConfig.setIndexes(Arrays.asList("index"));
        defaultedSchedulerConfig.setTypes(Arrays.asList("type"));

        assertEquals(expectedSchedulerConfig.build(), defaultedSchedulerConfig.build());
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
        SchedulerConfig.Builder sc1 = createFullyPopulated();
        SchedulerConfig.Builder sc2 = createFullyPopulated();
        sc2.setIndexes(Arrays.asList("blah", "di", "blah"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentTypes() {
        SchedulerConfig.Builder sc1 = createFullyPopulated();
        SchedulerConfig.Builder sc2 = createFullyPopulated();
        sc2.setTypes(Arrays.asList("blah", "di", "blah"));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    public void testEquals_GivenDifferentQuery() {
        SchedulerConfig.Builder b1 = createFullyPopulated();
        SchedulerConfig.Builder b2 = createFullyPopulated();
        b2.setQuery(QueryBuilders.termQuery("foo", "bar"));

        SchedulerConfig sc1 = b1.build();
        SchedulerConfig sc2 = b2.build();
        assertFalse(sc1.equals(sc2));
        assertFalse(sc2.equals(sc1));
    }

    public void testEquals_GivenDifferentAggregations() {
        SchedulerConfig.Builder sc1 = createFullyPopulated();
        SchedulerConfig.Builder sc2 = createFullyPopulated();
        sc2.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.count("foo")));

        assertFalse(sc1.build().equals(sc2.build()));
        assertFalse(sc2.build().equals(sc1.build()));
    }

    private static SchedulerConfig.Builder createFullyPopulated() {
        SchedulerConfig.Builder sc = new SchedulerConfig.Builder("scheduler1", "job1");
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
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        expectThrows(IllegalArgumentException.class, () -> conf.setIndexes(null));
    }

    public void testCheckValid_GivenEmptyIndexes() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        conf.setIndexes(Collections.emptyList());
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "indexes", "[]"), e.getMessage());
    }

    public void testCheckValid_GivenIndexesContainsOnlyNulls() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add(null);
        indexes.add(null);
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        conf.setIndexes(indexes);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "indexes", "[null, null]"), e.getMessage());
    }

    public void testCheckValid_GivenIndexesContainsOnlyEmptyStrings() throws IOException {
        List<String> indexes = new ArrayList<>();
        indexes.add("");
        indexes.add("");
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        conf.setIndexes(indexes);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, conf::build);
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "indexes", "[, ]"), e.getMessage());
    }

    public void testCheckValid_GivenNegativeQueryDelay() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setQueryDelay(-10L));
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "query_delay", -10L), e.getMessage());
    }

    public void testCheckValid_GivenZeroFrequency() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(0L));
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "frequency", 0L), e.getMessage());
    }

    public void testCheckValid_GivenNegativeFrequency() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setFrequency(-600L));
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "frequency", -600L), e.getMessage());
    }

    public void testCheckValid_GivenNegativeScrollSize() throws IOException {
        SchedulerConfig.Builder conf = new SchedulerConfig.Builder("scheduler1", "job1");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, () -> conf.setScrollSize(-1000));
        assertEquals(Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, "scroll_size", -1000L), e.getMessage());
    }

    public static String randomValidSchedulerId() {
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }
}
