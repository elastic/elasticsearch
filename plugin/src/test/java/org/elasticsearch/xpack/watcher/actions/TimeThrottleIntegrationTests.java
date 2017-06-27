/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.searchTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;

@TestLogging("org.elasticsearch.xpack.watcher:DEBUG," +
        "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail:WARN," +
        "org.elasticsearch.xpack.watcher.WatcherLifeCycleService:DEBUG," +
        "org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerMock:TRACE," +
        "org.elasticsearch.xpack.watcher.WatcherIndexingListener:TRACE")
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
        String id = randomAlphaOfLength(20);
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch()
                .setId(id)
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

        timeWarp().trigger(id);
        refresh();

        // the first fire should work
        assertHitCount(client().prepareSearch("actions").setTypes("action").get(), 1);

        timeWarp().clock().fastForward(TimeValue.timeValueMillis(4000));
        timeWarp().trigger(id);
        refresh();

        // the last fire should have been throttled, so number of actions shouldn't change
        assertHitCount(client().prepareSearch("actions").setTypes("action").get(), 1);

        timeWarp().clock().fastForwardSeconds(30);
        timeWarp().trigger(id);
        refresh();

        // the last fire occurred passed the throttle period, so a new action should have been added
        assertHitCount(client().prepareSearch("actions").setTypes("action").get(), 2);

        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setSource(new SearchSourceBuilder().query(QueryBuilders.boolQuery()
                        .must(matchQuery(WatchRecord.STATE.getPreferredName(), ExecutionState.THROTTLED.id()))
                        .must(termQuery("watch_id", id))))
                .get();
        List<Map<String, Object>> hits = Arrays.stream(response.getHits().getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());

        String message = String.format(Locale.ROOT, "Expected single throttled hits, but was %s", hits);
        assertThat(message, response.getHits().getTotalHits(), is(1L));
    }

    public void testTimeThrottleDefaults() throws Exception {
        String id = randomAlphaOfLength(30);
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch()
                .setId(id)
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(searchInput(templateRequest(new SearchSourceBuilder(), "events")))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                        .transform(searchTransform(templateRequest(new SearchSourceBuilder(), "events")))
                        .addAction("_id", indexAction("actions", "action")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().clock().setTime(DateTime.now(DateTimeZone.UTC));

        timeWarp().trigger(id);
        refresh();

        // the first trigger should work
        SearchResponse response = client().prepareSearch("actions").setTypes("action").get();
        assertHitCount(response, 1);

        timeWarp().clock().fastForwardSeconds(2);
        timeWarp().trigger(id);
        refresh("actions");

        // the last fire should have been throttled, so number of actions shouldn't change
        response = client().prepareSearch("actions").setTypes("action").get();
        assertHitCount(response, 1);

        timeWarp().clock().fastForwardSeconds(10);
        timeWarp().trigger(id);
        refresh();

        // the last fire occurred passed the throttle period, so a new action should have been added
        response = client().prepareSearch("actions").setTypes("action").get();
        assertHitCount(response, 2);

        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setSource(new SearchSourceBuilder().query(QueryBuilders.boolQuery()
                        .must(matchQuery(WatchRecord.STATE.getPreferredName(), ExecutionState.THROTTLED.id()))
                        .must(termQuery("watch_id", id))))
                .get();
        assertHitCount(searchResponse, 1);
    }
}
