/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class TimeThrottleIntegrationTests extends AbstractWatcherIntegrationTestCase {

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/97518")
    public void testTimeThrottle() throws Exception {
        String id = randomAlphaOfLength(20);
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId(id)
            .setSource(
                watchBuilder().trigger(schedule(interval("5s")))
                    .input(simpleInput())
                    .addAction("my-logging-action", loggingAction("foo"))
                    .defaultThrottlePeriod(TimeValue.timeValueSeconds(30))
            )
            .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().trigger(id);
        assertHistoryEntryExecuted(id);

        timeWarp().clock().fastForward(TimeValue.timeValueMillis(4000));
        timeWarp().trigger(id);
        assertHistoryEntryThrottled(id);

        timeWarp().clock().fastForwardSeconds(30);
        timeWarp().trigger(id);
        assertHistoryEntryExecuted(id);

        assertTotalHistoryEntries(id, 3);
    }

    public void testTimeThrottleDefaults() throws Exception {
        String id = randomAlphaOfLength(30);
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId(id)
            .setSource(
                watchBuilder().trigger(schedule(interval("1s")))
                    .input(simpleInput())
                    .addAction("my-logging-action", indexAction("my_watcher_index"))
            )
            .get();
        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().trigger(id);
        assertHistoryEntryExecuted(id);

        timeWarp().clock().fastForwardSeconds(2);
        timeWarp().trigger(id);
        assertHistoryEntryThrottled(id);

        timeWarp().clock().fastForwardSeconds(10);
        timeWarp().trigger(id);
        assertHistoryEntryExecuted(id);

        assertTotalHistoryEntries(id, 3);
    }

    private void assertHistoryEntryExecuted(String id) throws Exception {
        assertLatestHistoryEntry(id, "success");
    }

    private void assertHistoryEntryThrottled(String id) throws Exception {
        assertLatestHistoryEntry(id, "throttled");
    }

    private void assertLatestHistoryEntry(String id, String expectedValue) throws Exception {
        assertBusy(() -> {
            refresh(HistoryStoreField.DATA_STREAM + "*");

            SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                .setSize(1)
                .setSource(new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(termQuery("watch_id", id))))
                .addSort(SortBuilders.fieldSort("result.execution_time").order(SortOrder.DESC))
                .get();
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
            Map<String, Object> map = searchResponse.getHits().getHits()[0].getSourceAsMap();
            assertNotNull(map);
            String actionId = ObjectPath.eval("result.actions.0.id", map);
            assertThat(actionId, is("my-logging-action"));
            String actionStatus = ObjectPath.eval("result.actions.0.status", map);
            assertThat(actionStatus, is(expectedValue));
        });
    }

    private void assertTotalHistoryEntries(String id, long expectedCount) {
        SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
            .setSize(0)
            .setSource(new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(termQuery("watch_id", id))))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, is(oneOf(expectedCount, expectedCount + 1)));
    }
}
