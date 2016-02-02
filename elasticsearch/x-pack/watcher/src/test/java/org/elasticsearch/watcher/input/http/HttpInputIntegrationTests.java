/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.http;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;

import java.net.InetSocketAddress;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.compareCondition;
import static org.elasticsearch.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;

public class HttpInputIntegrationTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @TestLogging("watcher.support.http:TRACE")
    public void testHttpInput() throws Exception {
        createIndex("index");
        client().prepareIndex("index", "type", "id").setSource("{}").setRefresh(true).get();

        InetSocketAddress address = internalCluster().httpAddresses()[0];
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(httpInput(HttpRequestTemplate.builder(address.getHostString(), address.getPort())
                                .path("/index/_search")
                                .body(jsonBuilder().startObject().field("size", 1).endObject())
                                .auth(shieldEnabled() ? new BasicAuth("test", "changeme".toCharArray()) : null)))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                        .addAction("_id", loggingAction("watch [{{ctx.watch_id}}] matched")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name");
            refresh();
        }
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

    public void testHttpInputClusterStats() throws Exception {
        InetSocketAddress address = internalCluster().httpAddresses()[0];
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(httpInput(HttpRequestTemplate.builder(address.getHostString(), address.getPort())
                                .path("/_cluster/stats")
                                .auth(shieldEnabled() ? new BasicAuth("test", "changeme".toCharArray()) : null)))
                        .condition(compareCondition("ctx.payload.nodes.count.total", CompareCondition.Op.GTE, 1L))
                        .addAction("_id", loggingAction("watch [{{ctx.watch_id}}] matched")))
                .get();

        assertTrue(putWatchResponse.isCreated());
        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name");
            refresh();
        }
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

    @TestLogging("watcher.support.http:TRACE")
    public void testInputFiltering() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();

        InetSocketAddress address = internalCluster().httpAddresses()[0];
        XContentBuilder body = jsonBuilder().prettyPrint().startObject()
                    .field("query").value(termQuery("field", "value"))
                .endObject();
        HttpRequestTemplate.Builder requestBuilder = HttpRequestTemplate.builder(address.getHostString(), address.getPort())
                .path(TextTemplate.inline("/idx/_search"))
                .body(body);
        if (shieldEnabled()) {
            requestBuilder.auth(new BasicAuth("test", "changeme".toCharArray()));
        }

        watcherClient.preparePutWatch("_name1")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(10, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).extractKeys("hits.total"))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();

        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        watcherClient.preparePutWatch("_name2")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(10, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).extractKeys("hits.total"))
                        .condition(compareCondition("ctx.payload.hits.max_score", CompareCondition.Op.GTE, 0L)))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name1");
            timeWarp().scheduler().trigger("_name2");
            refresh();
        } else {
            Thread.sleep(10000);
        }

        assertWatchWithMinimumPerformedActionsCount("_name1", 1, false);
        assertWatchWithNoActionNeeded("_name2", 1);

        // Check that the input result payload has been filtered
        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(matchQuery("watch_id", "_name1"))
                .setSize(1)
                .get();
        assertHitCount(searchResponse, 1);
        XContentSource source = xContentSource(searchResponse.getHits().getAt(0).getSourceRef());
        assertThat(source.getValue("result.input.payload.hits.total"), equalTo((Object) 1));
    }
}
