/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
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
import static org.elasticsearch.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.is;
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
    public void testDeletedWhileQueued() throws Exception {
        DateTime now = DateTime.now(UTC);
        Wid wid = new Wid("_id", 1, now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        Condition condition = new AlwaysCondition();

        String index = HistoryStore.getHistoryIndexNameForTime(now);
        client().prepareIndex(index, HistoryStore.DOC_TYPE, wid.value())
                .setSource(jsonBuilder().startObject()
                        .field(WatchRecord.Parser.WATCH_ID_FIELD.getPreferredName(), wid.value())
                        .startObject(WatchRecord.Parser.TRIGGER_EVENT_FIELD.getPreferredName())
                            .field(event.type(), event)
                        .endObject()
                        .startObject(Watch.Parser.CONDITION_FIELD.getPreferredName())
                            .field(condition.type(), condition)
                        .endObject()
                        .field(WatchRecord.Parser.STATE_FIELD.getPreferredName(), WatchRecord.State.AWAITS_EXECUTION)
                        .endObject())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setRefresh(true)
                .get();

        stopWatcher();
        startWatcher();

        refresh();
        SearchResponse searchResponse = client().prepareSearch(index).get();
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).sourceAsMap().get(WatchRecord.Parser.WATCH_ID_FIELD.getPreferredName()).toString(), Matchers.equalTo(wid.value()));
        assertThat(searchResponse.getHits().getAt(0).sourceAsMap().get(WatchRecord.Parser.STATE_FIELD.getPreferredName()).toString(), Matchers.equalTo(WatchRecord.State.DELETED_WHILE_QUEUED.toString()));
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
        assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo((long) numWatches));
    }

    @Test
    @TestLogging("watcher.actions:DEBUG")
    public void testWatchRecordLoading() throws Exception {
        createIndex("output");
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo(0L));

        String watchId = "_id";
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        watcherClient().preparePutWatch(watchId).setSource(watchBuilder()
                .trigger(schedule(cron("0/5 * * * * ? 2050")))
                .input(searchInput(searchRequest))
                .condition(alwaysCondition())
                .addAction("_id", indexAction("output", "test"))
                .throttlePeriod(TimeValue.timeValueMillis(0))
        ).get();

        DateTime now = DateTime.now(UTC);
        final int numRecords = scaledRandomIntBetween(2, 128);
        for (int i = 0; i < numRecords; i++) {
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, randomLong(), now);
            WatchRecord watchRecord = new WatchRecord(wid, watchService().getWatch(watchId), event);
            String index = HistoryStore.getHistoryIndexNameForTime(now);
            client().prepareIndex(index, HistoryStore.DOC_TYPE, watchRecord.id().value())
                    .setSource(jsonBuilder().value(watchRecord))
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
                assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
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
    public void testMixedWatchRecordLoading() throws Exception {
        createIndex("output");
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo(0L));

        String watchId = "_id";
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        watcherClient().preparePutWatch(watchId).setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? 2050")))
                        .input(searchInput(searchRequest))
                        .condition(alwaysCondition())
                        .addAction("_id", indexAction("output", "test"))
                        .throttlePeriod(TimeValue.timeValueMillis(0))
        ).get();

        DateTime now = DateTime.now(UTC);
        int numRecords = scaledRandomIntBetween(2, 128);
        int awaitsExecution = 0;
        for (int i = 0; i < numRecords; i++) {
            now = now.plusMinutes(1);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, now, now);
            Wid wid = new Wid(watchId, randomLong(), now);
            WatchRecord watchRecord = new WatchRecord(wid, watchService().getWatch(watchId), event);
            String index = HistoryStore.getHistoryIndexNameForTime(now);
            client().prepareIndex(index, HistoryStore.DOC_TYPE, watchRecord.id().value())
                    .setSource(jsonBuilder().value(watchRecord))
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .get();

            final WatchRecord.State state;
            if (i == 0) {
                // at least have one record that we need to execute (otherwise the output index doesn't exist)
                awaitsExecution++;
                continue;
            } else {
                // update to a random state:
                state = randomFrom(WatchRecord.State.AWAITS_EXECUTION, WatchRecord.State.CHECKING, WatchRecord.State.EXECUTION_NOT_NEEDED, WatchRecord.State.EXECUTED);
            }
            client().prepareUpdate(index, HistoryStore.DOC_TYPE, watchRecord.id().value())
                    .setDoc(WatchRecord.Parser.STATE_FIELD.getPreferredName(), state.id())
                    .get();
            if (state == WatchRecord.State.AWAITS_EXECUTION) {
                awaitsExecution++;
            }
        }

        stopWatcher();
        startWatcher();

        final int finalAwaitsExecution = awaitsExecution;
        assertBusy(new Runnable() {

            @Override
            public void run() {
                // We need to wait until all the records are processed from the internal execution queue, only then we can assert
                // that numRecords watch records have been processed as part of starting up.
                WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
                assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
                assertThat(response.getExecutionQueueSize(), equalTo(0l));

                // but even then since the execution of the watch record is async it may take a little bit before
                // the actual documents are in the output index
                refresh();
                SearchResponse searchResponse = client().prepareSearch("output").get();
                assertHitCount(searchResponse, finalAwaitsExecution);
            }
        });
    }

    @Test
    @TestLogging("watcher.actions:DEBUG")
    public void testBootStrapManyHistoryIndices() throws Exception {
        DateTime now = new DateTime(UTC);
        long numberOfWatchHistoryIndices = randomIntBetween(2, 8);
        long numberOfWatchRecordsPerIndex = randomIntBetween(5, 10);
        SearchRequest searchRequest = newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));

        for (int i = 0; i < numberOfWatchHistoryIndices; i++) {
            DateTime historyIndexDate = now.minus((new TimeValue(i, TimeUnit.DAYS)).getMillis());
            String actionHistoryIndex = HistoryStore.getHistoryIndexNameForTime(historyIndexDate);
            createIndex(actionHistoryIndex);
            ensureGreen(actionHistoryIndex);
            logger.info("Created index {}", actionHistoryIndex);

            for (int j = 0; j < numberOfWatchRecordsPerIndex; j++) {
                String watchId = "_id" + i + "-" + j;
                WatchSourceBuilder watchSource = watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? 2050")))
                        .input(searchInput(searchRequest))
                        .condition(alwaysCondition())
                        .transform(searchTransform(searchRequest));

                PutWatchResponse putWatchResponse = watcherClient().preparePutWatch(watchId).setSource(watchSource).get();
                assertThat(putWatchResponse.isCreated(), is(true));

                ScheduleTriggerEvent event = new ScheduleTriggerEvent(watchId, historyIndexDate, historyIndexDate);
                Wid wid = new Wid(watchId, randomLong(), DateTime.now(UTC));
                WatchRecord watchRecord = new WatchRecord(wid, watchService().getWatch(watchId), event);

                XContentBuilder jsonBuilder2 = jsonBuilder();
                watchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

                IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.DOC_TYPE, watchRecord.id().value())
                        .setConsistencyLevel(WriteConsistencyLevel.ALL)
                        .setSource(jsonBuilder2.bytes())
                        .get();
                assertThat(indexResponse.isCreated(), is(true));
            }
            client().admin().indices().prepareRefresh(actionHistoryIndex).get();
        }

        stopWatcher();
        startWatcher();
        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();

        assertThat(response.getWatchServiceState(), equalTo(WatcherService.State.STARTED));
        final long totalHistoryEntries = numberOfWatchRecordsPerIndex * numberOfWatchHistoryIndices;

        assertBusy(new Runnable() {
            @Override
            public void run() {
                long count = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE,
                        termQuery(WatchRecord.Parser.STATE_FIELD.getPreferredName(), WatchRecord.State.EXECUTED.id()));
                assertThat(count, is(totalHistoryEntries));
            }
        }, 30, TimeUnit.SECONDS);

    }


}
