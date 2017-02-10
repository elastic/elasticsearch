/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;

public class TimeThrottleIntegrationTests extends AbstractWatcherIntegrationTestCase {
    
    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Before
    public void indexTestDocument() {
        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("level", "error")
                .get();
        assertEquals(DocWriteResponse.Result.CREATED, eventIndexResponse.getResult());
    }

    public void testTimeThrottle() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch()
                .setId("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(templateRequest(new SearchSourceBuilder(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(new SearchSourceBuilder(), "events")))
                        .addAction("_id", indexAction("actions", "action"))
                        .defaultThrottlePeriod(TimeValue.timeValueSeconds(30)))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().clock().setTime(DateTime.now(DateTimeZone.UTC));

        timeWarp().scheduler().trigger("_name");
        refresh();

        // the first fire should work
        long actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(1L));

        timeWarp().clock().fastForwardSeconds(5);
        timeWarp().scheduler().trigger("_name");
        refresh();

        // the last fire should have been throttled, so number of actions shouldn't change
        actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(1L));

        timeWarp().clock().fastForwardSeconds(30);
        timeWarp().scheduler().trigger("_name");
        refresh();

        // the last fire occurred passed the throttle period, so a new action should have been added
        actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(2L));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.THROTTLED.id()));
        assertThat(throttledCount, is(1L));
    }

    public void testTimeThrottleDefaults() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch()
                .setId("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(searchInput(templateRequest(new SearchSourceBuilder(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(new SearchSourceBuilder(), "events")))
                        .addAction("_id", indexAction("actions", "action")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().clock().setTime(DateTime.now(DateTimeZone.UTC));

        timeWarp().scheduler().trigger("_name");
        refresh();

        // the first trigger should work
        long actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(1L));

        timeWarp().clock().fastForwardSeconds(2);
        timeWarp().scheduler().trigger("_name");
        refresh();

        // the last fire should have been throttled, so number of actions shouldn't change
        actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(1L));

        timeWarp().clock().fastForwardSeconds(10);
        timeWarp().scheduler().trigger("_name");
        refresh();

        // the last fire occurred passed the throttle period, so a new action should have been added
        actionsCount = docCount("actions", "action", matchAllQuery());
        assertThat(actionsCount, is(2L));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", null,
                matchQuery(WatchRecord.Field.STATE.getPreferredName(), ExecutionState.THROTTLED.id()));
        assertThat(throttledCount, is(1L));
    }
}
