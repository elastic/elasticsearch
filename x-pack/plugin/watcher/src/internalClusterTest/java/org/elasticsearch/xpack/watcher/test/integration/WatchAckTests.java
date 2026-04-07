/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

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

    private String id = randomAlphaOfLength(10);

    @Before
    public void indexTestDocument() {
        DocWriteResponse eventIndexResponse = prepareIndex("events").setId(id)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("level", "error")
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, eventIndexResponse.getResult());
    }

    public void testAckSingleAction() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? *")))
                    .input(searchInput(templateRequest(searchSource(), "events")))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                    .transform(searchTransform(templateRequest(searchSource(), "events")))
                    .addAction("_a1", indexAction("actions1"))
                    .addAction("_a2", indexAction("actions2"))
                    .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        assertBusy(() -> assertThat(new WatcherStatsRequestBuilder(client()).get().getWatchesCount(), is(1L)));

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        AckWatchResponse ackResponse = new AckWatchRequestBuilder(client(), "_id").setActionIds("_a1").get();
        assertThat(ackResponse.getStatus().actionStatus("_a1").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        assertThat(ackResponse.getStatus().actionStatus("_a2").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKABLE));

        refresh();
        long a1CountAfterAck = docCount("actions1", matchAllQuery());
        long a2CountAfterAck = docCount("actions2", matchAllQuery());
        assertThat(a1CountAfterAck, greaterThan(0L));
        assertThat(a2CountAfterAck, greaterThan(0L));

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There should be more a2 actions in the index after we ack the watch
        long a2CountAfterPostAckFires = docCount("actions2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, greaterThan(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete().setIndex("events").setId(id).get();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        refresh();

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client()).setId("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(
            getWatchResponse.getId(),
            true,
            getWatchResponse.getSource().getBytes(),
            XContentType.JSON,
            getWatchResponse.getSeqNo(),
            getWatchResponse.getPrimaryTerm()
        );
        assertThat(
            parsedWatch.status().actionStatus("_a1").ackStatus().state(),
            is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION)
        );
        assertThat(
            parsedWatch.status().actionStatus("_a2").ackStatus().state(),
            is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION)
        );

        long throttledCount = docCount(
            HistoryStoreField.DATA_STREAM + "*",
            matchQuery(WatchRecord.STATE.getPreferredName(), ExecutionState.ACKNOWLEDGED.id())
        );
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckAllActions() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? *")))
                    .input(searchInput(templateRequest(searchSource(), "events")))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                    .transform(searchTransform(templateRequest(searchSource(), "events")))
                    .addAction("_a1", indexAction("actions1"))
                    .addAction("_a2", indexAction("actions2"))
                    .defaultThrottlePeriod(new TimeValue(0, TimeUnit.SECONDS))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));
        assertBusy(() -> assertThat(new WatcherStatsRequestBuilder(client()).get().getWatchesCount(), is(1L)));

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));

        AckWatchRequestBuilder ackWatchRequestBuilder = new AckWatchRequestBuilder(client(), "_id");
        if (randomBoolean()) {
            ackWatchRequestBuilder.setActionIds("_all");
        } else if (randomBoolean()) {
            ackWatchRequestBuilder.setActionIds("_all", "a1");
        }
        AckWatchResponse ackResponse = ackWatchRequestBuilder.get();

        assertThat(ackResponse.getStatus().actionStatus("_a1").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        assertThat(ackResponse.getStatus().actionStatus("_a2").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));

        refresh();
        long a1CountAfterAck = docCount("actions1", matchAllQuery());
        long a2CountAfterAck = docCount("actions2", matchAllQuery());
        assertThat(a1CountAfterAck, greaterThanOrEqualTo((long) 1));
        assertThat(a2CountAfterAck, greaterThanOrEqualTo((long) 1));

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));
        flush();
        refresh();

        // There shouldn't be more a1 actions in the index after we ack the watch, even though the watch was triggered
        long a1CountAfterPostAckFires = docCount("actions1", matchAllQuery());
        assertThat(a1CountAfterPostAckFires, equalTo(a1CountAfterAck));

        // There shouldn't be more a2 actions in the index after we ack the watch, even though the watch was triggered
        long a2CountAfterPostAckFires = docCount("actions2", matchAllQuery());
        assertThat(a2CountAfterPostAckFires, equalTo(a2CountAfterAck));

        // Now delete the event and the ack states should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete().setIndex("events").setId(id).get();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        refresh();

        timeWarp().trigger("_id", 4, TimeValue.timeValueSeconds(5));

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client()).setId("_id").get();
        assertThat(getWatchResponse.isFound(), is(true));

        Watch parsedWatch = watchParser().parse(
            getWatchResponse.getId(),
            true,
            getWatchResponse.getSource().getBytes(),
            XContentType.JSON,
            getWatchResponse.getSeqNo(),
            getWatchResponse.getPrimaryTerm()
        );
        assertThat(
            parsedWatch.status().actionStatus("_a1").ackStatus().state(),
            is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION)
        );
        assertThat(
            parsedWatch.status().actionStatus("_a2").ackStatus().state(),
            is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION)
        );

        long throttledCount = docCount(
            HistoryStoreField.DATA_STREAM + "*",
            matchQuery(WatchRecord.STATE.getPreferredName(), ExecutionState.ACKNOWLEDGED.id())
        );
        assertThat(throttledCount, greaterThan(0L));
    }

    public void testAckWithRestart() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("_name")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? *")))
                    .input(searchInput(templateRequest(searchSource(), "events")))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                    .transform(searchTransform(templateRequest(searchSource(), "events")))
                    .addAction("_id", indexAction("actions"))
            )
            .get();
        assertThat(putWatchResponse.isCreated(), is(true));
        assertBusy(() -> assertThat(new WatcherStatsRequestBuilder(client()).get().getWatchesCount(), is(1L)));

        timeWarp().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        restartWatcherRandomly();

        AckWatchResponse ackResponse = new AckWatchRequestBuilder(client(), "_name").get();
        assertThat(ackResponse.getStatus().actionStatus("_id").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));

        refresh("actions");
        long countAfterAck = SearchResponseUtils.getTotalHitsValue(prepareSearch("actions").setQuery(matchAllQuery()));
        assertThat(countAfterAck, greaterThanOrEqualTo(1L));

        restartWatcherRandomly();

        GetWatchResponse watchResponse = new GetWatchRequestBuilder(client()).setId("_name").get();
        assertThat(watchResponse.getStatus().actionStatus("_id").ackStatus().state(), Matchers.equalTo(ActionStatus.AckStatus.State.ACKED));

        refresh();
        GetResponse getResponse = client().get(new GetRequest(Watch.INDEX, "_name")).actionGet();
        Watch indexedWatch = watchParser().parse(
            "_name",
            true,
            getResponse.getSourceAsBytesRef(),
            XContentType.JSON,
            getResponse.getSeqNo(),
            getResponse.getPrimaryTerm()
        );
        assertThat(
            watchResponse.getStatus().actionStatus("_id").ackStatus().state(),
            equalTo(indexedWatch.status().actionStatus("_id").ackStatus().state())
        );

        timeWarp().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        refresh("actions");

        // There shouldn't be more actions in the index after we ack the watch, even though the watch was triggered
        long countAfterPostAckFires = docCount("actions", matchAllQuery());
        assertThat(countAfterPostAckFires, equalTo(countAfterAck));
    }

    private void restartWatcherRandomly() throws Exception {
        if (randomBoolean()) {
            stopWatcher();
            startWatcher();
        }
    }
}
