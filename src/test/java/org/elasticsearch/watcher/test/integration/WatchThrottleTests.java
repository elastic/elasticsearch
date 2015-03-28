/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.watcher.actions.ActionBuilders;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.watcher.client.WatchSourceBuilder.watchSourceBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.matchAllRequest;
import static org.elasticsearch.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class WatchThrottleTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testAckThrottle() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("actions", "events");
        ensureGreen("actions", "events");

        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setSource("level", "error")
                .get();
        assertThat(eventIndexResponse.isCreated(), is(true));
        refresh();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .watchName("_name")
                .source(watchSourceBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? *")))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(scriptCondition("ctx.payload.hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction(ActionBuilders.indexAction("actions", "action")))
                .get();

        assertThat(putWatchResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        AckWatchResponse ackResponse = watcherClient.prepareAckWatch("_name").get();
        assertThat(ackResponse.getStatus().ackStatus().state(), is(Watch.Status.AckStatus.State.ACKED));

        refresh();
        long countAfterAck = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterAck, greaterThanOrEqualTo((long) 1));

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }
        refresh();

        // There shouldn't be more actions in the index after we ack the watch, even though the watch was triggered
        long countAfterPostAckFires = docCount("actions", "action", matchAllQuery());
        assertThat(countAfterPostAckFires, equalTo(countAfterAck));

        //Now delete the event and the ack state should change to AWAITS_EXECUTION
        DeleteResponse response = client().prepareDelete("events", "event", eventIndexResponse.getId()).get();
        assertThat(response.isFound(), is(true));
        refresh();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name", 4, TimeValue.timeValueSeconds(5));
        } else {
            Thread.sleep(20000);
        }

        GetWatchResponse getWatchResponse = watcherClient.prepareGetWatch("_name").get();
        assertThat(getWatchResponse.getResponse().isExists(), is(true));

        Watch parsedWatch = watchParser().parse(getWatchResponse.getResponse().getId(), true,
                getWatchResponse.getResponse().getSourceAsBytesRef());
        assertThat(parsedWatch.status().ackStatus().state(), is(Watch.Status.AckStatus.State.AWAITS_EXECUTION));

        long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", null,
                matchQuery(WatchRecord.Parser.STATE_FIELD.getPreferredName(), WatchRecord.State.THROTTLED.id()));
        assertThat(throttledCount, greaterThan(0L));
    }


    @Test
    public void testTimeThrottle() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("actions", "events");
        ensureGreen("actions", "events");

        IndexResponse eventIndexResponse = client().prepareIndex("events", "event")
                .setSource("level", "error")
                .get();
        assertTrue(eventIndexResponse.isCreated());
        refresh();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .watchName("_name")
                .source(watchSourceBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(searchInput(matchAllRequest().indices("events")))
                        .condition(scriptCondition("ctx.payload.hits.total > 0"))
                        .transform(searchTransform(matchAllRequest().indices("events")))
                        .addAction(ActionBuilders.indexAction("actions", "action"))
                        .throttlePeriod(TimeValue.timeValueSeconds(10)))
                .get();
        assertThat(putWatchResponse.indexResponse().isCreated(), is(true));

        if (timeWarped()) {
            timeWarp().clock().setTime(DateTime.now());

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

            timeWarp().clock().fastForwardSeconds(10);
            timeWarp().scheduler().trigger("_name");
            refresh();

            // the last fire occurred passed the throttle period, so a new action should have been added
            actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(2L));

            long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", null,
                    matchQuery(WatchRecord.Parser.STATE_FIELD.getPreferredName(), WatchRecord.State.THROTTLED.id()));
            assertThat(throttledCount, is(1L));

        } else {

            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            refresh();

            // the first fire should work so we should have a single action in the actions index
            long actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            // we should still be within the throttling period... so the number of actions shouldn't change
            actionsCount = docCount("actions", "action", matchAllQuery());
            assertThat(actionsCount, is(1L));

            long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", null,
                    matchQuery(WatchRecord.Parser.STATE_FIELD.getPreferredName(), WatchRecord.State.THROTTLED.id()));
            assertThat(throttledCount, greaterThanOrEqualTo(1L));
        }
    }

}
