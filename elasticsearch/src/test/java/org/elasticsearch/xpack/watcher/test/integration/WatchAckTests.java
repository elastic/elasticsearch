/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;


import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class WatchAckTests extends AbstractWatcherIntegrationTestCase {

    private WatcherClient watcherClient;
    private UUID id = UUID.randomUUID();

    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    @Before
    public void indexTestDocument() {
        watcherClient = watcherClient();
        IndexResponse eventIndexResponse = client().prepareIndex("events", "event", id.toString())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("level", "error")
                .get();
        assertEquals(DocWriteResponse.Result.CREATED, eventIndexResponse.getResult());
    }

    public void testAckSingleAction() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(templateRequest(searchSource(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(searchSource(), "events")))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .addAction("_a2", indexAction("actions", "action2"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        AckWatchResponse ackResponse = watcherClient.prepareAckWatch("_id").setActionIds("_a1").get();
        assertThat(ackResponse.getStatus().actionStatus("_a1").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        assertThat(ackResponse.getStatus().actionStatus("_a2").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKABLE));

        refresh();
        long a1CountAfterAck = docCount("actions", "action1", matchAllQuery());
        long a2CountAfterAck = docCount("actions", "action2", matchAllQuery());
        assertThat(a1CountAfterAck, greaterThan(0L));
        assertThat(a2CountAfterAck, greaterThan(0L));

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions", "action1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There should be more a2 actions in the index after we ack the watch
        long a2CountAfterPostAckFires = docCount("actions", "action2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, greaterThan(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", id.toString()).get();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        refresh();

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(getWatchResponse.getId(), true, getWatchResponse.getSource().getBytes(), XContentType.JSON);
        assertThat(parsedWatch.status().actionStatus("_a1").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        assertThat(parsedWatch.status().actionStatus("_a2").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.ACKNOWLEDGED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckAllActions() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(templateRequest(searchSource(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(searchSource(), "events")))
                        .addAction("_a1", indexAction("actions", "action1"))
                        .addAction("_a2", indexAction("actions", "action2"))
                        .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS)))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));

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

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions", "action1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There shouldn't be more a2 actions in the index after we ack the watch, even though the watch was triggered
        long a2CountAfterPostAckFires = docCount("actions", "action2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, equalTo(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", id.toString()).get();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        refresh();

        timeWarp().scheduler().trigger("_id", 4, TimeValue.timeValueSeconds(5));

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(getWatchResponse.getId(), true, getWatchResponse.getSource().getBytes(), XContentType.JSON);
        assertThat(parsedWatch.status().actionStatus("_a1").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        assertThat(parsedWatch.status().actionStatus("_a2").ackStatus().state(),
                is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.ACKNOWLEDGED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckWithRestart() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(templateRequest(searchSource(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(searchSource(), "events")))
                        .addAction("_id", indexAction("actions", "action")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        restartWatcherRandomly();

        AckWatchResponse ackResponse = watcherClient.prepareAckWatch("_name").get();
        assertThat(ackResponse.getStatus().actionStatus("_id").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));

        refresh("actions");
        long countAfterAck = client().prepareSearch("actions").setTypes("action").setQuery(matchAllQuery()).get().getHits().totalHits();
        assertThat(countAfterAck, greaterThanOrEqualTo(1L));

        restartWatcherRandomly();

        GetWatchResponse watchResponse = watcherClient.getWatch(new GetWatchRequest("_name")).actionGet();
        assertThat(watchResponse.getStatus().actionStatus("_id").ackStatus().state(), Matchers.equalTo(ActionStatus.AckStatus.State.ACKED));

        refresh();
        GetResponse getResponse = client().get(new GetRequest(Watch.INDEX, Watch.DOC_TYPE, "_name")).actionGet();
        Watch indexedWatch = watchParser().parse("_name", true, getResponse.getSourceAsBytesRef(), XContentType.JSON);
        assertThat(watchResponse.getStatus().actionStatus("_id").ackStatus().state(),
                equalTo(indexedWatch.status().actionStatus("_id").ackStatus().state()));

        timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        refresh("actions");

        // There shouldn't be more actions in the index after we ack the watch, even though the watch was triggered
        long countAfterPostAckFires = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterPostAckFires, equalTo(countAfterAck));
    }

    private void restartWatcherRandomly() throws Exception {
        if (randomBoolean()) {
            stopWatcher();
            ensureWatcherStopped();
            startWatcher();
            ensureWatcherStarted();
        }
    }
}
