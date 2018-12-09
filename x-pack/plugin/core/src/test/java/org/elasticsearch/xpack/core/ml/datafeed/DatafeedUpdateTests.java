/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig.Mode;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
            builder.setTypes(DatafeedConfigTests.randomStringList(1, 10));
        }
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
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
            // Testing with a single agg is ok as we don't have special list writeable / xconent logic
            AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
            aggs.addAggregator(AggregationBuilders.avg(randomAlphaOfLength(10)).field(randomAlphaOfLength(10)));
            builder.setAggregations(aggs);
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
        datafeedBuilder.setTypes(Collections.singletonList("t_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        update.setJobId("bar");
        update.setIndices(Collections.singletonList("i_2"));
        update.setTypes(Collections.singletonList("t_2"));
        update.setQueryDelay(TimeValue.timeValueSeconds(42));
        update.setFrequency(TimeValue.timeValueSeconds(142));
        update.setQuery(QueryBuilders.termQuery("a", "b"));
        update.setScriptFields(Collections.singletonList(new SearchSourceBuilder.ScriptField("a", mockScript("b"), false)));
        update.setScrollSize(8000);
        update.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));
        update.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1)));

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getJobId(), equalTo("bar"));
        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_2")));
        assertThat(updatedDatafeed.getTypes(), equalTo(Collections.singletonList("t_2")));
        assertThat(updatedDatafeed.getQueryDelay(), equalTo(TimeValue.timeValueSeconds(42)));
        assertThat(updatedDatafeed.getFrequency(), equalTo(TimeValue.timeValueSeconds(142)));
        assertThat(updatedDatafeed.getParsedQuery(), equalTo(QueryBuilders.termQuery("a", "b")));
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
        datafeedBuilder.setTypes(Collections.singletonList("t_1"));
        DatafeedConfig datafeed = datafeedBuilder.build();

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeed.getId());
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        update.setAggregations(new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.histogram("a").interval(300000).field("time").subAggregation(maxTime)));

        DatafeedConfig updatedDatafeed = update.build().apply(datafeed, Collections.emptyMap());

        assertThat(updatedDatafeed.getIndices(), equalTo(Collections.singletonList("i_1")));
        assertThat(updatedDatafeed.getTypes(), equalTo(Collections.singletonList("t_1")));
        assertThat(updatedDatafeed.getParsedAggregations(),
                equalTo(new AggregatorFactories.Builder().addAggregator(
                        AggregationBuilders.histogram("a").interval(300000).field("time").subAggregation(maxTime))));
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

    @Override
    protected DatafeedUpdate mutateInstance(DatafeedUpdate instance) {
        DatafeedUpdate.Builder builder = new DatafeedUpdate.Builder(instance);
        switch (between(0, 10)) {
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
            List<String> types;
            if (instance.getTypes() == null) {
                types = new ArrayList<>();
            } else {
                types = new ArrayList<>(instance.getTypes());
            }
            types.add(randomAlphaOfLengthBetween(1, 20));
            builder.setTypes(types);
            break;
        case 6:
            BoolQueryBuilder query = new BoolQueryBuilder();
            if (instance.getQuery() != null) {
                query.must(instance.getQuery());
            }
            query.filter(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            builder.setQuery(query);
            break;
        case 7:
            if (instance.hasAggregations()) {
                builder.setAggregations(null);
            } else {
                AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
                String timeField = randomAlphaOfLength(10);
                aggBuilder.addAggregator(new DateHistogramAggregationBuilder(timeField).field(timeField).interval(between(10000, 3600000))
                        .subAggregation(new MaxAggregationBuilder(timeField).field(timeField)));
                builder.setAggregations(aggBuilder);
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
            if (instance.getScrollSize() == null) {
                builder.setScrollSize(between(1, 100));
            } else {
                builder.setScrollSize(instance.getScrollSize() + between(1, 100));
            }
            break;
        case 10:
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
