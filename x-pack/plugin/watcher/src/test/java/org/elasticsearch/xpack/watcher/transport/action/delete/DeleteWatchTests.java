/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.delete;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.sleep;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class DeleteWatchTests extends AbstractWatcherIntegrationTestCase {

    public void testDelete() throws Exception {
        PutWatchResponse putResponse = watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("anything")))
                .get();

        assertThat(putResponse, notNullValue());
        assertThat(putResponse.isCreated(), is(true));

        DeleteWatchResponse deleteResponse = watcherClient().deleteWatch(new DeleteWatchRequest("_name")).get();
        assertThat(deleteResponse, notNullValue());
        assertThat(deleteResponse.getId(), is("_name"));
        assertThat(deleteResponse.getVersion(), is(putResponse.getVersion() + 1));
        assertThat(deleteResponse.isFound(), is(true));
    }

    public void testDeleteNotFound() throws Exception {
        DeleteWatchResponse response = watcherClient().deleteWatch(new DeleteWatchRequest("_name")).get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_name"));
        assertThat(response.getVersion(), is(1L));
        assertThat(response.isFound(), is(false));
    }

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

            PutWatchResponse responseFuture = watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                    .trigger(schedule(interval("6h")))
                    .input(httpInput(template))
                    .addAction("_action1", loggingAction("anything")))
                    .get();
            assertThat(responseFuture.isCreated(), is(true));

            ActionFuture<ExecuteWatchResponse> executeWatchFuture =
                    watcherClient().prepareExecuteWatch("_name").setRecordExecution(true).execute();

            // without this sleep the delete operation might overtake the watch execution
            sleep(1000);
            DeleteWatchResponse deleteWatchResponse = watcherClient().prepareDeleteWatch("_name").get();
            assertThat(deleteWatchResponse.isFound(), is(true));

            executeWatchFuture.get();

            // the watch is gone, no leftovers
            GetWatchResponse getWatchResponse = watcherClient().prepareGetWatch("_name").get();
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
