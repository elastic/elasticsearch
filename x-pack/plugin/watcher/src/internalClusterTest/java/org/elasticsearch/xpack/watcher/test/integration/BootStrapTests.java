/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.hamcrest.Matchers;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class BootStrapTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return false;
    }

    public void testLoadMalformedWatchRecord() throws Exception {
        client().prepareIndex()
            .setIndex(Watch.INDEX)
            .setId("_id")
            .setSource(
                jsonBuilder().startObject()
                    .startObject(WatchField.TRIGGER.getPreferredName())
                    .startObject("schedule")
                    .field("cron", "0/5 * * * * ? 2050")
                    .endObject()
                    .endObject()
                    .startObject(WatchField.ACTIONS.getPreferredName())
                    .endObject()
                    .endObject()
            )
            .get();

        // valid watch record:
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Wid wid = new Wid("_id", now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        ExecutableCondition condition = InternalAlwaysCondition.INSTANCE;
        client().prepareIndex()
            .setIndex(HistoryStoreField.DATA_STREAM)
            .setId(wid.value())
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(
                jsonBuilder().startObject()
                    .field("@timestamp", ZonedDateTime.now())
                    .startObject(WatchRecord.TRIGGER_EVENT.getPreferredName())
                    .field(event.type(), event)
                    .endObject()
                    .startObject(WatchField.CONDITION.getPreferredName())
                    .field(condition.type(), condition)
                    .endObject()
                    .startObject(WatchField.INPUT.getPreferredName())
                    .startObject("none")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // unknown condition:
        wid = new Wid("_id", now);
        client().prepareIndex()
            .setIndex(HistoryStoreField.DATA_STREAM)
            .setId(wid.value())
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(
                jsonBuilder().startObject()
                    .field("@timestamp", ZonedDateTime.now())
                    .startObject(WatchRecord.TRIGGER_EVENT.getPreferredName())
                    .field(event.type(), event)
                    .endObject()
                    .startObject(WatchField.CONDITION.getPreferredName())
                    .startObject("unknown")
                    .endObject()
                    .endObject()
                    .startObject(WatchField.INPUT.getPreferredName())
                    .startObject("none")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // unknown trigger:
        wid = new Wid("_id", now);
        client().prepareIndex()
            .setIndex(HistoryStoreField.DATA_STREAM)
            .setId(wid.value())
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(
                jsonBuilder().startObject()
                    .field("@timestamp", ZonedDateTime.now())
                    .startObject(WatchRecord.TRIGGER_EVENT.getPreferredName())
                    .startObject("unknown")
                    .endObject()
                    .endObject()
                    .startObject(WatchField.CONDITION.getPreferredName())
                    .field(condition.type(), condition)
                    .endObject()
                    .startObject(WatchField.INPUT.getPreferredName())
                    .startObject("none")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        stopWatcher();
        startWatcher();

        assertBusy(() -> {
            WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).get();
            assertThat(response.getWatchesCount(), equalTo(1L));
        });
    }

    public void testLoadExistingWatchesUponStartup() throws Exception {
        stopWatcher();

        int numWatches = scaledRandomIntBetween(16, 128);
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "my-index");

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < numWatches; i++) {
            bulkRequestBuilder.add(
                client().prepareIndex()
                    .setIndex(Watch.INDEX)
                    .setId("_id" + i)
                    .setSource(
                        watchBuilder().trigger(schedule(cron("0 0/5 * * * ? 2050")))
                            .input(searchInput(request))
                            .condition(new CompareCondition("ctx.payload.hits.total.value", CompareCondition.Op.EQ, 1L))
                            .buildAsBytes(XContentType.JSON),
                        XContentType.JSON
                    )
                    .setWaitForActiveShards(ActiveShardCount.ALL)
            );
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertHitCount(client().prepareSearch(Watch.INDEX).setSize(0).get(), numWatches);

        startWatcher();

        assertBusy(() -> {
            WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).get();
            assertThat(response.getWatchesCount(), equalTo((long) numWatches));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/82420")
    public void testMixedTriggeredWatchLoading() throws Exception {
        createIndex("output");
        client().prepareIndex()
            .setIndex("my-index")
            .setId("bar")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("field", "value")
            .get();

        WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).get();
        assertThat(response.getWatchesCount(), equalTo(0L));

        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "my-index");

        ensureGreen("output", "my-index");
        int numWatches = 8;
        for (int i = 0; i < numWatches; i++) {
            String watchId = "_id" + i;
            new PutWatchRequestBuilder(client()).setId(watchId)
                .setSource(
                    watchBuilder().trigger(schedule(cron("0/5 * * * * ? 2050")))
                        .input(searchInput(request))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_id", indexAction("output"))
                        .defaultThrottlePeriod(TimeValue.timeValueMillis(0))
                )
                .get();
        }

        stopWatcher();

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        final int numRecords = scaledRandomIntBetween(numWatches, 128);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < numRecords; i++) {
            String watchId = "_id" + (i % numWatches);
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, now);
            TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
            bulkRequestBuilder.add(
                client().prepareIndex()
                    .setIndex(TriggeredWatchStoreField.INDEX_NAME)
                    .setId(triggeredWatch.id().value())
                    .setSource(jsonBuilder().value(triggeredWatch))
                    .request()
            );
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        logger.info("Added [{}] triggered watches for [{}] different watches, starting watcher again", numRecords, numWatches);
        startWatcher();
        assertSingleExecutionAndCompleteWatchHistory(numWatches, numRecords);
    }

    public void testTriggeredWatchLoading() throws Exception {
        cluster().wipeIndices(TriggeredWatchStoreField.INDEX_NAME);
        createIndex("output");
        client().prepareIndex()
            .setIndex("my-index")
            .setId("bar")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("field", "value")
            .get();

        WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).get();
        assertThat(response.getWatchesCount(), equalTo(0L));

        String watchId = "_id";
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "value")), "my-index");
        new PutWatchRequestBuilder(client()).setId(watchId)
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? 2050")))
                    .input(searchInput(request))
                    .condition(InternalAlwaysCondition.INSTANCE)
                    .addAction("_id", indexAction("output"))
                    .defaultThrottlePeriod(TimeValue.timeValueMillis(0))
            )
            .get();

        stopWatcher();

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        final int numRecords = scaledRandomIntBetween(2, 12);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < numRecords; i++) {
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, now);
            TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
            bulkRequestBuilder.add(
                client().prepareIndex()
                    .setIndex(TriggeredWatchStoreField.INDEX_NAME)
                    .setId(triggeredWatch.id().value())
                    .setSource(jsonBuilder().value(triggeredWatch))
                    .setWaitForActiveShards(ActiveShardCount.ALL)
            );
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        startWatcher();

        assertSingleExecutionAndCompleteWatchHistory(1, numRecords);
    }

    private void assertSingleExecutionAndCompleteWatchHistory(final long numberOfWatches, final int expectedWatchHistoryCount)
        throws Exception {
        assertBusy(() -> {
            // We need to wait until all the records are processed from the internal execution queue, only then we can assert
            // that numRecords watch records have been processed as part of starting up.
            WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).setIncludeCurrentWatches(true).get();
            long maxSize = response.getNodes().stream().map(WatcherStatsResponse.Node::getSnapshots).mapToLong(List::size).sum();
            assertThat(maxSize, equalTo(0L));

            refresh();
            SearchResponse searchResponse = client().prepareSearch("output").get();
            assertThat(searchResponse.getHits().getTotalHits().value, is(greaterThanOrEqualTo(numberOfWatches)));
            long successfulWatchExecutions = searchResponse.getHits().getTotalHits().value;

            // the watch history should contain entries for each triggered watch, which a few have been marked as not executed
            SearchResponse historySearchResponse = client().prepareSearch(HistoryStoreField.INDEX_PREFIX + "*").setSize(10000).get();
            assertHitCount(historySearchResponse, expectedWatchHistoryCount);
            long notExecutedCount = Arrays.stream(historySearchResponse.getHits().getHits())
                .filter(hit -> hit.getSourceAsMap().get("state").equals(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED.id()))
                .count();
            logger.info(
                "Watches not executed: [{}]: expected watch history count [{}] - [{}] successful watch exections",
                notExecutedCount,
                expectedWatchHistoryCount,
                successfulWatchExecutions
            );
            assertThat(notExecutedCount, is(expectedWatchHistoryCount - successfulWatchExecutions));
        }, 20, TimeUnit.SECONDS);
    }

    public void testManuallyStopped() throws Exception {
        WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).get();
        assertThat(response.watcherMetadata().manuallyStopped(), is(false));
        stopWatcher();
        response = new WatcherStatsRequestBuilder(client()).get();
        assertThat(response.watcherMetadata().manuallyStopped(), is(true));
        startWatcher();
        response = new WatcherStatsRequestBuilder(client()).get();
        assertThat(response.watcherMetadata().manuallyStopped(), is(false));
    }

    public void testWatchRecordSavedTwice() throws Exception {
        // Watcher could prevent to start if a watch record tried to executed twice or more and the watch didn't exist
        // for that watch record or the execution threadpool rejected the watch record.
        // A watch record without a watch is the easiest to simulate, so that is what this test does.
        if (indexExists(Watch.INDEX) == false) {
            // we rarely create an .watches alias in the base class
            assertAcked(indicesAdmin().prepareCreate(Watch.INDEX));
        }
        LocalDateTime localDateTime = LocalDateTime.of(2015, 11, 5, 0, 0, 0, 0);
        ZonedDateTime triggeredTime = ZonedDateTime.of(localDateTime, ZoneOffset.UTC);

        logger.info("Stopping watcher");
        stopWatcher();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        int numRecords = scaledRandomIntBetween(8, 32);
        for (int i = 0; i < numRecords; i++) {
            String watchId = Integer.toString(i);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, triggeredTime, triggeredTime);
            Wid wid = new Wid(watchId, triggeredTime);
            TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
            bulkRequestBuilder.add(
                client().prepareIndex()
                    .setIndex(TriggeredWatchStoreField.INDEX_NAME)
                    .setId(triggeredWatch.id().value())
                    .setSource(jsonBuilder().value(triggeredWatch))
            );

            String id = internalCluster().getInstance(ClusterService.class).localNode().getId();
            WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(wid, event, ExecutionState.EXECUTED, "executed", id);
            bulkRequestBuilder.add(
                client().prepareIndex()
                    .setIndex(HistoryStoreField.DATA_STREAM)
                    .setId(watchRecord.id().value())
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setSource(jsonBuilder().value(watchRecord))
            );
        }
        assertNoFailures(bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get());

        logger.info("Starting watcher");
        startWatcher();

        assertBusy(() -> {
            // We need to wait until all the records are processed from the internal execution queue, only then we can assert
            // that numRecords watch records have been processed as part of starting up.
            WatcherStatsResponse response = new WatcherStatsRequestBuilder(client()).setIncludeCurrentWatches(true).get();
            long maxSize = response.getNodes().stream().map(WatcherStatsResponse.Node::getSnapshots).mapToLong(List::size).sum();
            assertThat(maxSize, equalTo(0L));

            // but even then since the execution of the watch record is async it may take a little bit before
            // the actual documents are in the output index
            refresh();
            SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM).setSize(numRecords).get();
            assertThat(searchResponse.getHits().getTotalHits().value, Matchers.equalTo((long) numRecords));
            for (int i = 0; i < numRecords; i++) {
                assertThat(searchResponse.getHits().getAt(i).getSourceAsMap().get("state"), is(ExecutionState.EXECUTED.id()));
            }
        });
    }
}
