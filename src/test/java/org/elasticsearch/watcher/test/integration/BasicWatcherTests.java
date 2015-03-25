/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.scheduler.schedule.IntervalSchedule;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.WatchStore;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilder.watchSourceBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.scheduler.schedule.Schedules.cron;
import static org.elasticsearch.watcher.scheduler.schedule.Schedules.interval;
import static org.elasticsearch.watcher.support.Variables.*;
import static org.elasticsearch.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.hamcrest.Matchers.*;

/**
 */
public class BasicWatcherTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testIndexWatch() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_name")
                .source(watchSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_name", 1);

        GetWatchResponse getWatchResponse = watcherClient().prepareGetWatch().setWatchName("_name").get();
        assertThat(getWatchResponse.getResponse().isExists(), is(true));
        assertThat(getWatchResponse.getResponse().isSourceEmpty(), is(false));
    }

    @Test
    public void testIndexWatch_registerWatchBeforeTargetIndex() throws Exception {
        WatcherClient watcherClient = watcherClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_name")
                .source(watchSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        // The watch's condition won't meet because there is no data that matches with the query
        assertWatchWithNoActionNeeded("_name", 1);

        // Index sample doc after we register the watch and the watch's condition should meet
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_name", 1);
    }

    @Test
    public void testDeleteWatch() throws Exception {
        WatcherClient watcherClient = watcherClient();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(matchAllQuery()));
        PutWatchResponse indexResponse = watcherClient.preparePutWatch("_name")
                .source(watchSourceBuilder()
                        .schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS))
                        .input(searchInput(searchRequest))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();
        assertThat(indexResponse.indexResponse().isCreated(), is(true));

        if (!timeWarped()) {
            // Although there is no added benefit in this test for waiting for the watch to fire, however
            // we need to wait here because of a test timing issue. When we tear down a test we delete the watch and delete all
            // indices, but there may still be inflight fired watches, which may trigger the watch history to be created again, before
            // we finished the tear down phase.
            assertWatchWithNoActionNeeded("_name", 1);
        }

        DeleteWatchResponse deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse.deleteResponse(), notNullValue());
        assertThat(deleteWatchResponse.deleteResponse().isFound(), is(true));

        refresh();
        assertHitCount(client().prepareCount(WatchStore.INDEX).get(), 0l);

        // Deleting the same watch for the second time
        deleteWatchResponse = watcherClient.prepareDeleteWatch("_name").get();
        assertThat(deleteWatchResponse.deleteResponse(), notNullValue());
        assertThat(deleteWatchResponse.deleteResponse().isFound(), is(false));
    }

    @Test
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
                    .source(watchSource.bytes())
                    .get();
            fail();
        } catch (WatcherException e) {
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

    @Test
    public void testModifyWatches() throws Exception {
        SearchRequest searchRequest = newInputSearchRequest("idx")
                .source(searchSource().query(matchAllQuery()));

        WatchSourceBuilder source = watchSourceBuilder()
                .schedule(interval("5s"))
                .input(searchInput(searchRequest))
                .addAction(indexAction("idx", "action"));

        watcherClient().preparePutWatch("_name")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }
        assertWatchWithMinimumPerformedActionsCount("_name", 0, false);

        watcherClient().preparePutWatch("_name")
                .source(source.condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        }
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);

        watcherClient().preparePutWatch("_name")
                .source(source.schedule(cron("0/1 * * * * ? 2020")).condition(scriptCondition("ctx.payload.hits.total == 0")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        } else {
            Thread.sleep(1000);
        }

        long count =  findNumberOfPerformedActions("_name");

        if (timeWarped()) {
            timeWarp().scheduler().fire("_name");
            refresh();
        } else {
            Thread.sleep(1000);
        }

        assertThat(count, equalTo(findNumberOfPerformedActions("_name")));
    }

    @Test
    public void testConditionSearchWithSource() throws Exception {
        String variable = randomFrom(EXECUTION_TIME, SCHEDULED_FIRE_TIME, FIRE_TIME);
        SearchSourceBuilder searchSourceBuilder = searchSource().query(filteredQuery(
                matchQuery("level", "a"),
                rangeFilter("_timestamp")
                        .from("{{" + variable + "}}||-30s")
                        .to("{{" + variable + "}}")));

        testConditionSearch(newInputSearchRequest("events").source(searchSourceBuilder));
    }

    @Test
    public void testConditionSearchWithIndexedTemplate() throws Exception {
        String variable = randomFrom(EXECUTION_TIME, SCHEDULED_FIRE_TIME, FIRE_TIME);
        SearchSourceBuilder searchSourceBuilder = searchSource().query(filteredQuery(
                matchQuery("level", "a"),
                rangeFilter("_timestamp")
                        .from("{{" + variable + "}}||-30s")
                        .to("{{" + variable + "}}")));

        client().preparePutIndexedScript()
                .setScriptLang("mustache")
                .setId("my-template")
                .setSource(jsonBuilder().startObject().field("template").value(searchSourceBuilder).endObject())
                .get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("events");
        searchRequest.templateName("my-template");
        searchRequest.templateType(ScriptService.ScriptType.INDEXED);
        testConditionSearch(searchRequest);
    }

    private void testConditionSearch(SearchRequest request) throws Exception {
        String watchName = "_name";
        assertAcked(prepareCreate("events").addMapping("event", "_timestamp", "enabled=true", "level", "type=string"));

        watcherClient().prepareDeleteWatch(watchName).get();
        watcherClient().preparePutWatch(watchName)
                .source(createWatchSource(interval("5s"), request, "return ctx.payload.hits.total >= 3"))
                .get();

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(watchName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertWatchWithNoActionNeeded(watchName, 1);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "b")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(watchName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertWatchWithNoActionNeeded(watchName, 2);

        client().prepareIndex("events", "event")
                .setCreate(true)
                .setSource("level", "a")
                .get();
        refresh();
        if (timeWarped()) {
            timeWarp().clock().fastForwardSeconds(5);
            timeWarp().scheduler().fire(watchName);
            refresh();
        } else {
            Thread.sleep(5000);
        }
        assertWatchWithMinimumPerformedActionsCount(watchName, 1);
    }
}
