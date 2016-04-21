/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.Schedules;
import org.elasticsearch.watcher.trigger.schedule.support.MonthTimes;
import org.elasticsearch.watcher.trigger.schedule.support.WeekTimes;
import org.elasticsearch.watcher.watch.WatchStore;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.condition.ConditionBuilders.compareCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.hourly;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.monthly;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.weekly;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class BasicWatcherTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return true;
    }

    public void testIndexWatch() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                        .addAction("_logger", loggingAction("_logging")
                                        .setCategory("_category")))
                .get();
        timeWarp().scheduler().trigger("_name");
        assertWatchWithMinimumPerformedActionsCount("_name", 1);

        GetWatchResponse getWatchResponse = watcherClient().prepareGetWatch().setId("_name").get();
        assertThat(getWatchResponse.isFound(), is(true));
        assertThat(getWatchResponse.getSource(), notNullValue());
    }

    public void testIndexWatchRegisterWatchBeforeTargetIndex() throws Exception {
        WatcherClient watcherClient = watcherClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();
        timeWarp().scheduler().trigger("_name");
        // The watch's condition won't meet because there is no data that matches with the query
        assertWatchWithNoActionNeeded("_name", 1);

        // Index sample doc after we register the watch and the watch's condition should meet
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();

        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().trigger("_name");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_name", 1);
    }

    public void testDeleteWatch() throws Exception {
        WatcherClient watcherClient = watcherClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(matchAllQuery()));
        PutWatchResponse indexResponse = watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/1 * * * * ? 2020")))
                        .input(searchInput(searchRequest))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();
        assertThat(indexResponse.isCreated(), is(true));
        DeleteWatchResponse deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse, notNullValue());
        assertThat(deleteWatchResponse.isFound(), is(true));

        refresh();
        assertHitCount(client().prepareSearch(WatchStore.INDEX).setSize(0).get(), 0L);

        // Deleting the same watch for the second time
        deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse, notNullValue());
        assertThat(deleteWatchResponse.isFound(), is(false));
    }

    public void testMalformedWatch() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        XContentBuilder watchSource = jsonBuilder();

        watchSource.startObject();
        watchSource.field("unknown_field", "x");
        watchSource.startObject("schedule").field("cron", "0/5 * * * * ? *").endObject();

        watchSource.startObject("condition").startObject("script").field("script", "return true").field("request");
        WatcherUtils.writeSearchRequest(newInputSearchRequest(), watchSource, ToXContent.EMPTY_PARAMS);
        watchSource.endObject();

        watchSource.endObject();
        try {
            watcherClient.preparePutWatch("_name")
                    .setSource(watchSource.bytes())
                    .get();
            fail();
        } catch (ElasticsearchParseException e) {
            // In watch store we fail parsing if an watch contains undefined fields.
        }
        try {
            client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_name")
                    .setSource(watchSource)
                    .get();
            fail();
        } catch (Exception e) {
            // The watch index template the mapping is defined as strict
        }
    }

    public void testModifyWatches() throws Exception {
        SearchRequest searchRequest = newInputSearchRequest("idx")
                .source(searchSource().query(matchAllQuery()));

        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(searchInput(searchRequest))
                .addAction("_id", indexAction("idx", "action"));

        watcherClient().preparePutWatch("_name")
                .setSource(source.condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        assertWatchWithMinimumPerformedActionsCount("_name", 0, false);

        watcherClient().preparePutWatch("_name")
                .setSource(source.condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L)))
                .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        refresh();
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);

        watcherClient().preparePutWatch("_name")
                .setSource(source
                        .trigger(schedule(Schedules.cron("0/1 * * * * ? 2020")))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L)))
                .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        long count =  findNumberOfPerformedActions("_name");

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        assertThat(count, equalTo(findNumberOfPerformedActions("_name")));
    }

    public void testModifyWatchWithSameUnit() throws Exception {
        if (timeWarped()) {
            logger.info("Skipping testModifyWatches_ because timewarp is enabled");
            return;
        }

        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("1s")))
                .input(simpleInput("key", "value"))
                .defaultThrottlePeriod(TimeValue.timeValueSeconds(0))
                .addAction("_id", loggingAction("_logging"));
        watcherClient().preparePutWatch("_name")
                .setSource(source)
                .get();

        Thread.sleep(5000);
        assertWatchWithMinimumPerformedActionsCount("_name", 5, false);

        source = watchBuilder()
                .trigger(schedule(interval("100s")))
                .defaultThrottlePeriod(TimeValue.timeValueSeconds(0))
                .input(simpleInput("key", "value"))
                .addAction("_id", loggingAction("_logging"));
        watcherClient().preparePutWatch("_name")
                .setSource(source)
                .get();

        // Wait one second to be sure that the scheduler engine has executed any previous job instance of the watch
        Thread.sleep(1000);
        long before = historyRecordsCount("_name");
        Thread.sleep(5000);
        assertThat("Watch has been updated to 100s interval, so no new records should have been added.", historyRecordsCount("_name"),
                equalTo(before));
    }

    public void testConditionSearchWithSource() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        testConditionSearch(newInputSearchRequest("events").source(searchSourceBuilder));
    }

    public void testConditionSearchWithIndexedTemplate() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        client().admin().cluster().preparePutStoredScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject().bytes())
                .get();

        Template template = new Template("my-template", ScriptType.STORED, null, null, null);
        SearchRequest searchRequest = newInputSearchRequest("events");
        // TODO (2.0 upgrade): move back to BytesReference instead of coverting to a string
        searchRequest.template(template);
        testConditionSearch(searchRequest);
    }

    public void testInputFiltering() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_name1")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest).extractKeys("hits.total"))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();
        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        watcherClient.preparePutWatch("_name2")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest).extractKeys("hits.total"))
                        .condition(compareCondition("ctx.payload.hits.max_score", CompareCondition.Op.GTE, 0L)))
                .get();

        timeWarp().scheduler().trigger("_name1");
        timeWarp().scheduler().trigger("_name2");
        assertWatchWithMinimumPerformedActionsCount("_name1", 1);
        assertWatchWithNoActionNeeded("_name2", 1);

        // Check that the input result payload has been filtered
        refresh();
        SearchResponse searchResponse = searchWatchRecords(new Callback<SearchRequestBuilder>() {
            @Override
            public void handle(SearchRequestBuilder builder) {
                builder.setQuery(matchQuery("watch_id", "_name1"));
            }
        });
        assertHitCount(searchResponse, 1);
        XContentSource source = xContentSource(searchResponse.getHits().getAt(0).getSourceRef());
        assertThat(source.getValue("result.input.payload.hits.total"), equalTo((Object) 1));
    }

    public void testPutWatchWithNegativeSchedule() throws Exception {
        try {
            watcherClient().preparePutWatch("_name")
                    .setSource(watchBuilder()
                            .trigger(schedule(interval(-5, IntervalSchedule.Interval.Unit.SECONDS)))
                            .input(simpleInput("key", "value"))
                            .condition(alwaysCondition())
                            .addAction("_logger", loggingAction("executed!")))
                    .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("interval can't be lower than 1000 ms, but [-5s] was specified"));
        }

        try {
            watcherClient().preparePutWatch("_name")
                    .setSource(watchBuilder()
                            .trigger(schedule(hourly().minutes(-10).build()))
                            .input(simpleInput("key", "value"))
                            .condition(alwaysCondition())
                            .addAction("_logger", loggingAction("executed!")))
                    .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("invalid hourly minute [-10]. minute must be between 0 and 59 incl."));
        }

        try {
            watcherClient().preparePutWatch("_name")
                    .setSource(watchBuilder()
                            .trigger(schedule(daily().atRoundHour(-10).build()))
                            .input(simpleInput("key", "value"))
                            .condition(alwaysCondition())
                            .addAction("_logger", loggingAction("executed!")))
                    .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl."));
        }

        try {
            watcherClient().preparePutWatch("_name")
                    .setSource(watchBuilder()
                            .trigger(schedule(weekly().time(WeekTimes.builder().atRoundHour(-10).build()).build()))
                                    .input(simpleInput("key", "value"))
                                    .condition(alwaysCondition())
                                    .addAction("_logger", loggingAction("executed!")))
                            .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl."));
        }

        try {
            watcherClient().preparePutWatch("_name")
                    .setSource(watchBuilder()
                            .trigger(schedule(monthly().time(MonthTimes.builder().atRoundHour(-10).build()).build()))
                            .input(simpleInput("key", "value"))
                            .condition(alwaysCondition())
                            .addAction("_logger", loggingAction("executed!")))
                    .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl."));
        }
    }

    private void testConditionSearch(SearchRequest request) throws Exception {
        // reset, so we don't miss event docs when we filter over the _timestamp field.
        timeWarp().clock().setTime(SystemClock.INSTANCE.nowUTC());

        String watchName = "_name";
        assertAcked(prepareCreate("events").addMapping("event", "_timestamp", "enabled=true", "level", "type=text"));

        watcherClient().preparePutWatch(watchName)
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(request))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GTE, 3L)))
                .get();

        logger.info("created watch [{}] at [{}]", watchName, SystemClock.INSTANCE.nowUTC());

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();

        refresh();
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 1);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "b")
                .get();
        refresh();
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 2);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        refresh();
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithMinimumPerformedActionsCount(watchName, 1);
    }
}
