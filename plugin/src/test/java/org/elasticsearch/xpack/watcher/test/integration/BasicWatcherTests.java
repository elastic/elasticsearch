/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedules;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.MonthTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.hourly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.monthly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.weekly;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BasicWatcherTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean enableSecurity() {
        return false;
    }

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
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(request))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
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
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
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
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(matchAllQuery()), "idx");
        PutWatchResponse indexResponse = watcherClient.preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/1 * * * * ? 2020")))
                        .input(searchInput(searchRequest))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();
        assertThat(indexResponse.isCreated(), is(true));
        DeleteWatchResponse deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse, notNullValue());
        assertThat(deleteWatchResponse.isFound(), is(true));

        refresh();
        assertHitCount(client().prepareSearch(Watch.INDEX).setSize(0).get(), 0L);

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

        watchSource.startObject("condition").startObject("script").field("script", "return true");
        watchSource.field("request", templateRequest(searchSource().query(matchAllQuery())));
        watchSource.endObject().endObject();

        watchSource.endObject();
        try {
            watcherClient.preparePutWatch("_name")
                    .setSource(watchSource.bytes(), watchSource.contentType())
                    .get();
            fail();
        } catch (ElasticsearchParseException e) {
            // In watch store we fail parsing if an watch contains undefined fields.
        }
        try {
            client().prepareIndex(Watch.INDEX, Watch.DOC_TYPE, "_name")
                    .setSource(watchSource)
                    .get();
            fail();
        } catch (Exception e) {
            // The watch index template the mapping is defined as strict
        }
    }

    public void testModifyWatches() throws Exception {
        WatcherSearchTemplateRequest searchRequest = templateRequest(searchSource().query(matchAllQuery()), "idx");

        WatchSourceBuilder source = watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(searchInput(searchRequest))
                .addAction("_id", indexAction("idx", "action"));

        watcherClient().preparePutWatch("_name")
                .setSource(source.condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        assertWatchWithMinimumPerformedActionsCount("_name", 0, false);

        watcherClient().preparePutWatch("_name")
                .setSource(source.condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L)))
                .get();

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        refresh();
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);

        watcherClient().preparePutWatch("_name")
                .setSource(source
                        .trigger(schedule(Schedules.cron("0/1 * * * * ? 2020")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 0L)))
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

    @TestLogging("org.elasticsearch.xpack.watcher.trigger:DEBUG")
    public void testConditionSearchWithSource() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        testConditionSearch(templateRequest(searchSourceBuilder, "events"));
    }

    public void testConditionSearchWithIndexedTemplate() throws Exception {
        SearchSourceBuilder searchSourceBuilder = searchSource().query(matchQuery("level", "a"));
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setLang("mustache")
                .setId("my-template")
                .setContent(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject().bytes(), XContentType.JSON)
                .get());

        Script template = new Script(ScriptType.STORED, "mustache", "my-template", Collections.emptyMap());
        WatcherSearchTemplateRequest searchRequest = new WatcherSearchTemplateRequest(new String[]{"events"}, new String[0],
                SearchType.DEFAULT, WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS, template);
        testConditionSearch(searchRequest);
    }

    public void testInputFiltering() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "idx");
        watcherClient.preparePutWatch("_name1")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(request).extractKeys("hits.total"))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();
        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        watcherClient.preparePutWatch("_name2")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(request).extractKeys("hits.total"))
                        .condition(new CompareCondition("ctx.payload.hits.max_score", CompareCondition.Op.GTE, 0L)))
                .get();

        timeWarp().scheduler().trigger("_name1");
        timeWarp().scheduler().trigger("_name2");
        assertWatchWithMinimumPerformedActionsCount("_name1", 1);
        assertWatchWithNoActionNeeded("_name2", 1);

        // Check that the input result payload has been filtered
        refresh();
        SearchResponse searchResponse = searchWatchRecords(builder -> builder.setQuery(matchQuery("watch_id", "_name1")));
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
                            .condition(AlwaysCondition.INSTANCE)
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
                            .condition(AlwaysCondition.INSTANCE)
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
                            .condition(AlwaysCondition.INSTANCE)
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
                                    .condition(AlwaysCondition.INSTANCE)
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
                            .condition(AlwaysCondition.INSTANCE)
                            .addAction("_logger", loggingAction("executed!")))
                    .get();
            fail("put watch should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    equalTo("invalid time [0-10:00]. invalid time hour value [-10]. time hours must be between 0 and 23 incl."));
        }
    }

    private void testConditionSearch(WatcherSearchTemplateRequest request) throws Exception {
        // reset, so we don't miss event docs when we filter over the _timestamp field.
        timeWarp().clock().setTime(new DateTime(Clock.systemUTC().millis()));

        String watchName = "_name";
        assertAcked(prepareCreate("events").addMapping("event", "level", "type=text"));

        watcherClient().preparePutWatch(watchName)
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(request))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GTE, 3L)))
                .get();

        logger.info("created watch [{}] at [{}]", watchName, new DateTime(Clock.systemUTC().millis()));

        client().prepareIndex("events", "event")
                .setSource("level", "a")
                .get();
        client().prepareIndex("events", "event")
                .setSource("level", "a")
                .get();

        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 1);

        client().prepareIndex("events", "event")
                .setSource("level", "b")
                .get();
        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithNoActionNeeded(watchName, 2);

        client().prepareIndex("events", "event")
                .setSource("level", "a")
                .get();
        refresh();
        timeWarp().clock().fastForwardSeconds(1);
        timeWarp().scheduler().trigger(watchName);
        assertWatchWithMinimumPerformedActionsCount(watchName, 1);
    }
}
