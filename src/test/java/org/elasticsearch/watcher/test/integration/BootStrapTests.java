/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Actions;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.search.SearchInput;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.transform.SearchTransform;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchService;
import org.elasticsearch.watcher.watch.WatchStore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class BootStrapTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testBootStrapWatcher() throws Exception {
        ensureWatcherStarted();

        SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference watchSource = createWatchSource("0 0/5 * * * ? *", searchRequest, "ctx.payload.hits.total == 1");
        client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, "my-first-watch")
                .setSource(watchSource)
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .get();

        client().admin().indices().prepareRefresh(WatchStore.INDEX).get();
        stopWatcher();
        startWatcher();

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatchServiceState(), equalTo(WatchService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo(1L));
    }

    @Test
    @TestLogging("watcher.actions:DEBUG")
    public void testBootstrapHistory() throws Exception {
        ensureWatcherStarted();

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatchServiceState(), equalTo(WatchService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo(0L));

        SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        Watch watch = new Watch(
                "test-serialization",
                SystemClock.INSTANCE,
                new ScheduleTrigger(new CronSchedule("0/5 * * * * ? 2035")), //Set this into the future so we don't get any extra runs
                new SearchInput(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                new ScriptCondition(logger, scriptService(), new Script("return true")),
                new SearchTransform(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                new Actions(new ArrayList<Action>()),
                null, // metadata
                new TimeValue(0),
                new Watch.Status());

        XContentBuilder builder = jsonBuilder().value(watch);
        IndexResponse indexResponse = client().prepareIndex(WatchStore.INDEX, WatchStore.DOC_TYPE, watch.name())
                .setSource(builder).get();
        ensureGreen(WatchStore.INDEX);
        refresh();
        assertThat(indexResponse.isCreated(), is(true));

        DateTime now = DateTime.now(DateTimeZone.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(now, now);
        WatchRecord watchRecord = new WatchRecord(watch, event);
        String actionHistoryIndex = HistoryStore.getHistoryIndexNameForTime(now);

        createIndex(actionHistoryIndex);
        ensureGreen(actionHistoryIndex);
        logger.info("Created index {}", actionHistoryIndex);

        indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.DOC_TYPE, watchRecord.id())
                .setConsistencyLevel(WriteConsistencyLevel.ALL)
                .setSource(jsonBuilder().value(watchRecord))
                .get();
        assertThat(indexResponse.isCreated(), is(true));

        stopWatcher();
        startWatcher();

        response = watcherClient().prepareWatcherStats().get();
        assertThat(response.getWatchServiceState(), equalTo(WatchService.State.STARTED));
        assertThat(response.getWatchesCount(), equalTo(1L));
        assertThat(response.getWatchExecutionQueueMaxSize(), greaterThanOrEqualTo(1l));
    }

    @Test
    @TestLogging("watcher.actions:DEBUG")
    public void testBootStrapManyHistoryIndices() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        long numberOfWatchHistoryIndices = randomIntBetween(2, 8);
        long numberOfWatchRecordsPerIndex = randomIntBetween(5, 10);
        SearchRequest searchRequest = WatcherTestUtils.newInputSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));

        for (int i = 0; i < numberOfWatchHistoryIndices; i++) {
            DateTime historyIndexDate = now.minus((new TimeValue(i, TimeUnit.DAYS)).getMillis());
            String actionHistoryIndex = HistoryStore.getHistoryIndexNameForTime(historyIndexDate);
            createIndex(actionHistoryIndex);
            ensureGreen(actionHistoryIndex);
            logger.info("Created index {}", actionHistoryIndex);

            for (int j = 0; j < numberOfWatchRecordsPerIndex; j++) {

                Watch watch = new Watch(
                        "action-test-" + i + " " + j,
                        SystemClock.INSTANCE,
                        new ScheduleTrigger(new CronSchedule("0/5 * * * * ? 2035")), //Set a cron schedule far into the future so this watch is never scheduled
                        new SearchInput(logger, scriptService(), ClientProxy.of(client()),
                                searchRequest),
                        new ScriptCondition(logger, scriptService(), new Script("return true")),
                        new SearchTransform(logger, scriptService(), ClientProxy.of(client()), searchRequest),
                        new Actions(new ArrayList<Action>()),
                        null, // metatdata
                        new TimeValue(0),
                        new Watch.Status());
                XContentBuilder jsonBuilder = jsonBuilder();
                watch.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

                PutWatchResponse putWatchResponse = watcherClient().preparePutWatch(watch.name()).source(jsonBuilder.bytes()).get();
                assertThat(putWatchResponse.indexResponse().isCreated(), is(true));

                ScheduleTriggerEvent event = new ScheduleTriggerEvent(historyIndexDate, historyIndexDate);
                WatchRecord watchRecord = new WatchRecord(watch, event);

                XContentBuilder jsonBuilder2 = jsonBuilder();
                watchRecord.toXContent(jsonBuilder2, ToXContent.EMPTY_PARAMS);

                IndexResponse indexResponse = client().prepareIndex(actionHistoryIndex, HistoryStore.DOC_TYPE, watchRecord.id())
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

        assertThat(response.getWatchServiceState(), equalTo(WatchService.State.STARTED));
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
