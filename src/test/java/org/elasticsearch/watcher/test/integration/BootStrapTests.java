/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.WatcherState;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.execution.TriggeredWatch;
import org.elasticsearch.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.history.*;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class BootStrapTests extends AbstractWatcherIntegrationTests {

    @Override
    protected boolean timeWarped() {
        // timewarping isn't necessary here, because we aren't testing triggering or throttling
        return false;
    }

    @Test
    public void testLoadMalformedWatch() throws Exception {
        // valid watch
        client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id0")
                .setSource(jsonBuilder().startObject()
                            .startObject(Watch.Field.TRIGGER.getPreferredName())
                                .startObject("schedule")
                                    .field("interval", "1s")
                                .endObject()
                            .endObject()
                            .startObject(Watch.Field.ACTIONS.getPreferredName())
                            .endObject()
                        .endObject())
                .get();

        // invalid interval
        client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id2")
                .setSource(jsonBuilder().startObject()
                        .startObject(Watch.Field.TRIGGER.getPreferredName())
                            .startObject("schedule")
                                .field("interval", true)
                            .endObject()
                        .endObject()
                        .startObject(Watch.Field.ACTIONS.getPreferredName())
                        .endObject()
                        .endObject())
                .get();

        // illegal top level field
        client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id3")
                .setSource(jsonBuilder().startObject()
                        .startObject(Watch.Field.TRIGGER.getPreferredName())
                            .startObject("schedule")
                                .field("interval", "1s")
                            .endObject()
                            .startObject("illegal_field").endObject()
                        .endObject()
                        .startObject(Watch.Field.ACTIONS.getPreferredName()).endObject()
                        .endObject())
                .get();

        stopWatcher();
        startWatcher();

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
        // Only the valid watch should been loaded
        assertThat(response.getWatchesCount(), equalTo(1l));
        assertThat(watcherClient().prepareGetWatch("_id0").get().getId(), Matchers.equalTo("_id0"));
    }

    @Test
    public void testLoadMalformedWatchRecord() throws Exception {
        client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id")
                .setSource(jsonBuilder().startObject()
                        .startObject(Watch.Field.TRIGGER.getPreferredName())
                            .startObject("schedule")
                                .field("cron", "0/5 * * * * ? 2050")
                            .endObject()
                        .endObject()
                        .startObject(Watch.Field.ACTIONS.getPreferredName())
                        .endObject()
                        .endObject())
                .get();

        // valid watch record:
        DateTime now = DateTime.now(UTC);
        Wid wid = new Wid("_id", 1, now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        Condition condition = new AlwaysCondition();
        String index = HistoryStore.getHistoryIndexNameForTime(now);
        client().prepareIndex(index, HistoryStore.DOC_TYPE, wid.value())
                .setSource(jsonBuilder().startObject()
                        .startObject(WatchRecord.Field.TRIGGER_EVENT.getPreferredName())
                            .field(event.type(), event)
                        .endObject()
                        .startObject(Watch.Field.CONDITION.getPreferredName())
                            .field(condition.type(), condition)
                        .endObject()
                        .startObject(Watch.Field.INPUT.getPreferredName())
                            .startObject("none").endObject()
                        .endObject()
                        .endObject())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setRefresh(true)
                .get();

        // unknown condition:
        wid = new Wid("_id", 2, now);
        client().prepareIndex(index, HistoryStore.DOC_TYPE, wid.value())
                .setSource(jsonBuilder().startObject()
                        .startObject(WatchRecord.Field.TRIGGER_EVENT.getPreferredName())
                            .field(event.type(), event)
                        .endObject()
                        .startObject(Watch.Field.CONDITION.getPreferredName())
                            .startObject("unknown").endObject()
                        .endObject()
                        .startObject(Watch.Field.INPUT.getPreferredName())
                            .startObject("none").endObject()
                        .endObject()
                        .endObject())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setRefresh(true)
                .get();

        // unknown trigger:
        wid = new Wid("_id", 2, now);
        client().prepareIndex(index, HistoryStore.DOC_TYPE, wid.value())
                .setSource(jsonBuilder().startObject()
                        .startObject(WatchRecord.Field.TRIGGER_EVENT.getPreferredName())
                            .startObject("unknown").endObject()
                        .endObject()
                        .startObject(Watch.Field.CONDITION.getPreferredName())
                            .field(condition.type(), condition)
                        .endObject()
                        .startObject(Watch.Field.INPUT.getPreferredName())
                            .startObject("none").endObject()
                        .endObject()
                        .endObject())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setRefresh(true)
                .get();

        stopWatcher();
        startWatcher();

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
        assertThat(response.getWatchesCount(), equalTo(1l));
    }

    @Test
    public void testDeletedWhileQueued() throws Exception {
        DateTime now = DateTime.now(UTC);
        Wid wid = new Wid("_id", 1, now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        Condition condition = new AlwaysCondition();

        client().prepareIndex(TriggeredWatchStore.INDEX_NAME, TriggeredWatchStore.DOC_TYPE, wid.value())
                .setSource(jsonBuilder().startObject()
                        .startObject(WatchRecord.Field.TRIGGER_EVENT.getPreferredName())
                            .field(event.type(), event)
                        .endObject()
                        .endObject())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setRefresh(true)
                .get();

        stopWatcher();
        startWatcher();

        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*").get();
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).id(), Matchers.equalTo(wid.value()));
        assertThat(searchResponse.getHits().getAt(0).sourceAsMap().get(WatchRecord.Field.STATE.getPreferredName()).toString(), Matchers.equalTo(ExecutionState.NOT_EXECUTED_WATCH_MISSING.toString()));
    }


    @Test
    public void testLoadExistingWatchesUponStartup() throws Exception {
        int numWatches = scaledRandomIntBetween(16, 128);
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        for (int i = 0; i < numWatches; i++) {
            client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id" + i)
                    .setSource(watchBuilder()
                                    .trigger(schedule(cron("0 0/5 * * * ? 2050")))
                                    .input(searchInput(searchRequest))
                                    .condition(scriptCondition("ctx.payload.hits.total == 1"))
                                    .buildAsBytes(XContentType.JSON)
                    )
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .get();
        }

        refresh();
        stopWatcher();
        startWatcher();

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
        assertThat(response.getWatchesCount(), equalTo((long) numWatches));
    }

    @Test
    @TestLogging("watcher.actions:DEBUG")
    public void testTriggeredWatchLoading() throws Exception {
        createIndex("output");
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
        assertThat(response.getWatchesCount(), equalTo(0L));

        String watchId = "_id";
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        watcherClient().preparePutWatch(watchId).setSource(watchBuilder()
                .trigger(schedule(cron("0/5 * * * * ? 2050")))
                .input(searchInput(searchRequest))
                .condition(alwaysCondition())
                .addAction("_id", indexAction("output", "test"))
                .defaultThrottlePeriod(TimeValue.timeValueMillis(0))
        ).get();

        DateTime now = DateTime.now(UTC);
        final int numRecords = scaledRandomIntBetween(2, 128);
        for (int i = 0; i < numRecords; i++) {
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, randomLong(), now);
            TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
            client().prepareIndex(TriggeredWatchStore.INDEX_NAME, TriggeredWatchStore.DOC_TYPE, triggeredWatch.id().value())
                    .setSource(jsonBuilder().value(triggeredWatch))
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .get();
        }

        stopWatcher();
        startWatcher();

        assertBusy(new Runnable() {

            @Override
            public void run() {
                // We need to wait until all the records are processed from the internal execution queue, only then we can assert
                // that numRecords watch records have been processed as part of starting up.
                WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
                assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
                assertThat(response.getExecutionQueueSize(), equalTo(0l));

                // but even then since the execution of the watch record is async it may take a little bit before
                // the actual documents are in the output index
                refresh();
                SearchResponse searchResponse = client().prepareSearch("output").get();
                assertHitCount(searchResponse, numRecords);
            }
        });
    }

    @Test
    public void testMixedTriggeredWatchLoading() throws Exception {
        createIndex("output");
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
        assertThat(response.getWatchesCount(), equalTo(0L));

        String watchId = "_id";
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        watcherClient().preparePutWatch(watchId).setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? 2050")))
                        .input(searchInput(searchRequest))
                        .condition(alwaysCondition())
                        .addAction("_id", indexAction("output", "test"))
                        .defaultThrottlePeriod(TimeValue.timeValueMillis(0))
        ).get();

        DateTime now = DateTime.now(UTC);
        final int numRecords = scaledRandomIntBetween(2, 128);
        for (int i = 0; i < numRecords; i++) {
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, randomLong(), now);
            TriggeredWatch triggeredWatch = new TriggeredWatch(wid, event);
            client().prepareIndex(TriggeredWatchStore.INDEX_NAME, TriggeredWatchStore.DOC_TYPE, triggeredWatch.id().value())
                    .setSource(jsonBuilder().value(triggeredWatch))
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .get();
        }

        stopWatcher();
        startWatcher();

        assertBusy(new Runnable() {

            @Override
            public void run() {
                // We need to wait until all the records are processed from the internal execution queue, only then we can assert
                // that numRecords watch records have been processed as part of starting up.
                WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
                assertThat(response.getWatcherState(), equalTo(WatcherState.STARTED));
                assertThat(response.getExecutionQueueSize(), equalTo(0l));

                // but even then since the execution of the watch record is async it may take a little bit before
                // the actual documents are in the output index
                refresh();
                SearchResponse searchResponse = client().prepareSearch("output").get();
                assertHitCount(searchResponse, numRecords);
            }
        });
    }


}
