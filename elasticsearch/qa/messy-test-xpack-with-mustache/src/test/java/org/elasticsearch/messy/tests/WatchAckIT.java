/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;


import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.compareCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.matchAllRequest;
import static org.elasticsearch.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class WatchAckIT extends AbstractWatcherIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        // TODO remove dependency on mustache
        types.add(MustachePlugin.class);
        return types;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        // remove the mock because we use mustache here...
        plugins.remove(MockMustacheScriptEngine.TestPlugin.class);
        return plugins;
    }

    private IndexResponse indexTestDoc() {
        createIndex("actions", "events");
        ensureGreen("actions", "events");

        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setSource("level", "error")
                .get();
        assertThat(eventIndexResponse.isCreated(), is(true));
        refresh();
        return eventIndexResponse;
    }

    public void testAckSingleAction() throws Exception {
        WatcherClient watcherClient = watcherClient();
        IndexResponse eventIndexResponse = indexTestDoc();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .addAction("_a2", indexAction("actions", "action2"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        AckWatchResponse ackResponse = watcherClient.prepareAckWatch("_id").setActionIds("_a1").get();
        assertThat(ackResponse.getStatus().actionStatus("_a1").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        assertThat(ackResponse.getStatus().actionStatus("_a2").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKABLE));

        refresh();
        long a1CountAfterAck = docCount("actions", "action1", matchAllQuery());
        long a2CountAfterAck = docCount("actions", "action2", matchAllQuery());
        assertThat(a1CountAfterAck, greaterThanOrEqualTo((long) 1));
        assertThat(a2CountAfterAck, greaterThanOrEqualTo((long) 1));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions", "action1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There should be more a2 actions in the index after we ack the watch
        long a2CountAfterPostAckFires = docCount("actions", "action2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, greaterThan(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", eventIndexResponse.getId()).get();
        assertThat(response.isFound(), is(true));
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(getWatchResponse.getId(), true, getWatchResponse.getSource().getBytes());
        assertThat(parsedWatch.status().actionStatus("_a1").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        assertThat(parsedWatch.status().actionStatus("_a2").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.THROTTLED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckAllActions() throws Exception {
        WatcherClient watcherClient = watcherClient();
        IndexResponse eventIndexResponse = indexTestDoc();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .addAction("_a2", indexAction("actions", "action2"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        AckWatchRequestBuilder ackWatchRequestBuilder = watcherClient.prepareAckWatch("_id");
        if (randomBoolean()) {
            ackWatchRequestBuilder.setActionIds("_all");
        } else if (randomBoolean()) {
            ackWatchRequestBuilder.setActionIds("_all", "a1");
        }
        AckWatchResponse ackResponse = ackWatchRequestBuilder.get();

        assertThat(ackResponse.getStatus().actionStatus("_a1").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        assertThat(ackResponse.getStatus().actionStatus("_a2").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));

        refresh();
        long a1CountAfterAck = docCount("actions", "action1", matchAllQuery());
        long a2CountAfterAck = docCount("actions", "action2", matchAllQuery());
        assertThat(a1CountAfterAck, greaterThanOrEqualTo((long) 1));
        assertThat(a2CountAfterAck, greaterThanOrEqualTo((long) 1));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions", "action1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There shouldn't be more a2 actions in the index after we ack the watch, even though the watch was triggered
        long a2CountAfterPostAckFires = docCount("actions", "action2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, equalTo(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", eventIndexResponse.getId()).get();
        assertThat(response.isFound(), is(true));
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(getWatchResponse.getId(), true, getWatchResponse.getSource().getBytes());
        assertThat(parsedWatch.status().actionStatus("_a1").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        assertThat(parsedWatch.status().actionStatus("_a2").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.THROTTLED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckWithRestart() throws Exception {
        WatcherClient watcherClient = watcherClient();
        indexTestDoc();
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction("_id", indexAction("actions", "action")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        if (randomBoolean()) {
            stopWatcher();
            ensureWatcherStopped();
            startWatcher();
            ensureWatcherStarted();
        }

        AckWatchResponse ackResponse = watcherClient.prepareAckWatch("_name").get();
        assertThat(ackResponse.getStatus().actionStatus("_id").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));

        refresh();
        long countAfterAck = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterAck, greaterThanOrEqualTo((long) 1));

        if (randomBoolean()) {
            stopWatcher();
            ensureWatcherStopped();
            startWatcher();
            ensureWatcherStarted();
        }

        GetWatchResponse watchResponse = watcherClient.getWatch(new GetWatchRequest("_name")).actionGet();
        assertThat(watchResponse.getStatus().actionStatus("_id").ackStatus().state(), Matchers.equalTo(ActionStatus.AckStatus.State.ACKED));

        refresh();
        GetResponse getResponse = client().get(new GetRequest(WatchStore.INDEX, WatchStore.DOC_TYPE, "_name")).actionGet();
        Watch indexedWatch = watchParser().parse("_name", true, getResponse.getSourceAsBytesRef());
        assertThat(watchResponse.getStatus().actionStatus("_id").ackStatus().state(),
                equalTo(indexedWatch.status().actionStatus("_id").ackStatus().state()));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        refresh();

        // There shouldn't be more actions in the index after we ack the watch, even though the watch was triggered
        long countAfterPostAckFires = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterPostAckFires, equalTo(countAfterAck));
    }

    public void testAckInvalidWatchId() throws Exception {
        WatcherClient watcherClient = watcherClient();
        try {
            watcherClient.prepareAckWatch("id with whitespaces").get();
            fail("Expected ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("Watch id cannot have white spaces"));
        }
    }

    public void testAckInvalidActionId() throws Exception {
        WatcherClient watcherClient = watcherClient();
        try {
            watcherClient.prepareAckWatch("_id").setActionIds("id with whitespaces").get();
            fail("Expected ActionRequestValidationException");
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("Action id cannot have white spaces"));
        }
    }
}
