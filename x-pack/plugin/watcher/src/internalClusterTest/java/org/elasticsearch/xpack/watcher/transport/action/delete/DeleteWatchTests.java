/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.action.delete;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.sleep;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class DeleteWatchTests extends AbstractWatcherIntegrationTestCase {

    // This is a special case, since locking is removed
    // Deleting a watch while it is being executed is possible now
    // This test ensures that there are no leftovers, like a watch status without a watch in the watch store
    // Also the watch history is checked, that the error has been marked as deleted
    // The mock webserver does not support count down latches, so we have to use sleep - sorry!
    public void testWatchDeletionDuringExecutionWorks() throws Exception {
        MockResponse response = new MockResponse();
        response.setBody("foo");
        response.setResponseCode(200);
        response.setBodyDelay(TimeValue.timeValueSeconds(5));

        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(response);
            server.start();
            HttpRequestTemplate template = HttpRequestTemplate.builder(server.getHostName(), server.getPort()).path("/").build();

            PutWatchResponse responseFuture = new PutWatchRequestBuilder(client(), "_name").setSource(
                watchBuilder().trigger(schedule(interval("6h"))).input(httpInput(template)).addAction("_action1", loggingAction("anything"))
            ).get();
            assertThat(responseFuture.isCreated(), is(true));

            ActionFuture<ExecuteWatchResponse> executeWatchFuture = new ExecuteWatchRequestBuilder(client(), "_name").setRecordExecution(
                true
            ).execute();

            // without this sleep the delete operation might overtake the watch execution
            sleep(1000);
            DeleteWatchResponse deleteWatchResponse = new DeleteWatchRequestBuilder(client(), "_name").get();
            assertThat(deleteWatchResponse.isFound(), is(true));

            executeWatchFuture.get();

            // the watch is gone, no leftovers
            GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client(), "_name").get();
            assertThat(getWatchResponse.isFound(), is(false));

            // the watch history shows a successful execution, even though the watch was deleted
            // during execution
            refresh(HistoryStoreField.INDEX_PREFIX + "*");

            SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.INDEX_PREFIX + "*").setQuery(matchAllQuery()).get();
            assertHitCount(searchResponse, 1);

            Map<String, Object> source = searchResponse.getHits().getAt(0).getSourceAsMap();
            // watch has been executed successfully
            String state = ObjectPath.eval("state", source);
            assertThat(state, is("executed"));
            // no exception occurred
            assertThat(source, not(hasKey("exception")));
        }
    }
}
