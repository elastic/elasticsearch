/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DatafeedUpdateTests extends AbstractSerializingTestCase<DatafeedUpdate> {

    @Override
    protected DatafeedUpdate createTestInstance() {
        return createRandomized(DatafeedConfigTests.randomValidDatafeedId());
    }

    public static DatafeedUpdate createRandomized(String datafeedId) {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(datafeedId);
        if (randomBoolean()) {
            builder.setJobId(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            builder.setQueryDelay(TimeValue.timeValueMillis(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setFrequency(TimeValue.timeValueSeconds(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        if (randomBoolean()) {
            builder.setIndexes(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            builder.setTypes(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.termQuery(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        if (randomBoolean()) {
            int scriptsSize = randomInt(3);
            List<SearchSourceBuilder.ScriptField> scriptFields = new ArrayList<>(scriptsSize);
            for (int scriptIndex = 0; scriptIndex < scriptsSize; scriptIndex++) {
                scriptFields.add(new SearchSourceBuilder.ScriptField(randomAsciiOfLength(10), new Script(randomAsciiOfLength(10)),
                        randomBoolean()));
            }
            builder.setScriptFields(scriptFields);
        }
        if (randomBoolean()) {
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
            builder.setSource(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setChunkingConfig(ChunkingConfigTests.createRandomizedChunk());
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<DatafeedUpdate> instanceReader() {
        return DatafeedUpdate::new;
    }

    @Override
    protected DatafeedUpdate parseInstance(XContentParser parser) {
        return DatafeedUpdate.PARSER.apply(parser, null).build();
    }

    public void testApply_failBecauseTargetDatafeedHasDifferentId() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        expectThrows(IllegalArgumentException.class, () -> createRandomized(datafeed.getId() + "_2").apply(datafeed));
    }

    public void testApply_givenEmptyUpdate() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedConfig updatedDatafeed = new DatafeedUpdate.Builder(datafeed.getId()).build().apply(datafeed);
        assertThat(datafeed, equalTo(updatedDatafeed));
    }

    public void testApply_givenPartialUpdate() {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandomizedDatafeedConfig("foo");
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setScrollSize(datafeed.getScrollSize() + 1);

        DatafeedUpdate.Builder updated = new DatafeedUpdate.Builder(datafeed.getId());
        updated.setScrollSize(datafeed.getScrollSize() + 1);
        DatafeedConfig updatedDatafeed = update.build().apply(datafeed);

        DatafeedConfig.Builder expectedDatafeed = new DatafeedConfig.Builder(datafeed);
        expectedDatafeed.setScrollSize(datafeed.getScrollSize() + 1);
        assertThat(updatedDatafeed, equalTo(expectedDatafeed.build()));
    }

    public void testApply_givenFullUpdateNoAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("foo", "foo-feed");
        datafeedBuilder.setIndexes(Arrays.asList("i_1"));
        datafeedBuilder.setTypes(Arrays.asList("t_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setJobId("bar");
        update.setIndexes(Arrays.asList("i_2"));
        update.setTypes(Arrays.asList("t_2"));
        update.setQueryDelay(TimeValue.timeValueSeconds(42));
        update.setFrequency(TimeValue.timeValueSeconds(142));
        update.setQuery(QueryBuilders.termQuery("a", "b"));
        update.setScriptFields(Arrays.asList(new SearchSourceBuilder.ScriptField("a", new Script("b"), false)));
        update.setScrollSize(8000);
        update.setSource(true);
        update.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed);

        assertThat(updatedDatafeed.getJobId(), equalTo("bar"));
        assertThat(updatedDatafeed.getIndexes(), equalTo(Arrays.asList("i_2")));
        assertThat(updatedDatafeed.getTypes(), equalTo(Arrays.asList("t_2")));
        assertThat(updatedDatafeed.getQueryDelay(), equalTo(TimeValue.timeValueSeconds(42)));
        assertThat(updatedDatafeed.getFrequency(), equalTo(TimeValue.timeValueSeconds(142)));
        assertThat(updatedDatafeed.getQuery(), equalTo(QueryBuilders.termQuery("a", "b")));
        assertThat(updatedDatafeed.hasAggregations(), is(false));
        assertThat(updatedDatafeed.getScriptFields(),
                equalTo(Arrays.asList(new SearchSourceBuilder.ScriptField("a", new Script("b"), false))));
        assertThat(updatedDatafeed.getScrollSize(), equalTo(8000));
        assertThat(updatedDatafeed.isSource(), is(true));
        assertThat(updatedDatafeed.getChunkingConfig(), equalTo(ChunkingConfig.newManual(TimeValue.timeValueHours(1))));
    }

    public void testApply_givenAggregations() {
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("foo", "foo-feed");
        datafeedBuilder.setIndexes(Arrays.asList("i_1"));
        datafeedBuilder.setTypes(Arrays.asList("t_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("a")));

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed);

        assertThat(updatedDatafeed.getIndexes(), equalTo(Arrays.asList("i_1")));
        assertThat(updatedDatafeed.getTypes(), equalTo(Arrays.asList("t_1")));
        assertThat(updatedDatafeed.getAggregations(),
                equalTo(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("a"))));
    }
}