/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.http;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("org.elasticsearch.xpack.watcher:DEBUG,org.elasticsearch.xpack.watcher.WatcherIndexingListener:TRACE")
public class HttpInputIntegrationTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(Netty4Plugin.class); // for http
        return plugins;
    }

    public void testHttpInput() throws Exception {
        createIndex("index");
        client().prepareIndex("index", "type", "id").setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        InetSocketAddress address = internalCluster().httpAddresses()[0];
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(httpInput(HttpRequestTemplate.builder(address.getHostString(), address.getPort())
                                .path("/index/_search")
                                .body(Strings.toString(jsonBuilder().startObject().field("size", 1).endObject()))
                                .putHeader("Content-Type", new TextTemplate("application/json"))))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                        .addAction("_id", loggingAction("anything")))
                .get();

        timeWarp().trigger("_name");
        refresh();
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40682")
    public void testHttpInputClusterStats() throws Exception {
        InetSocketAddress address = internalCluster().httpAddresses()[0];
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("1s")))
                        .input(httpInput(HttpRequestTemplate.builder(address.getHostString(), address.getPort()).path("/_cluster/stats")))
                        .condition(new CompareCondition("ctx.payload.nodes.count.total", CompareCondition.Op.GTE, 1L))
                        .addAction("_id", loggingAction("anything")))
                .get();

        assertTrue(putWatchResponse.isCreated());
        timeWarp().trigger("_name");
        refresh();
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

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
                .path(new TextTemplate("/idx/_search"))
                .body(Strings.toString(body));

        watcherClient.preparePutWatch("_name1")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(10, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).extractKeys("hits.total"))
                        .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L)))
                .get();

        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        watcherClient.preparePutWatch("_name2")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(10, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).extractKeys("hits.total"))
                        .condition(new CompareCondition("ctx.payload.hits.max_score", CompareCondition.Op.GTE, 0L)))
                .get();

        timeWarp().trigger("_name1");
        timeWarp().trigger("_name2");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_name1", 1, false);
        assertWatchWithNoActionNeeded("_name2", 1);

        // Check that the input result payload has been filtered
        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(matchQuery("watch_id", "_name1"))
                .setSize(1)
                .get();
        assertHitCount(searchResponse, 1);
        XContentSource source = xContentSource(searchResponse.getHits().getAt(0).getSourceRef());
        assertThat(source.getValue("result.input.payload.hits.total"), equalTo((Object) 1));
    }
}
