/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;
import org.elasticsearch.watcher.support.http.auth.BasicAuth;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.template;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.watcher.support.http.TemplatedHttpRequest.sourceBuilder;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class HttpInputIntegrationTest extends AbstractWatcherIntegrationTests {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(InternalNode.HTTP_ENABLED, true)
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Test
    public void testHttpInput() throws Exception {
        createIndex("index");
        client().prepareIndex("index", "type", "id").setSource("{}").setRefresh(true).get();

        InetSocketAddress address = internalTestCluster().httpAddresses()[0];
        watcherClient().preparePutWatch("_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(httpInput(sourceBuilder(address.getHostName(), address.getPort())
                                .setPath("/index/_search")
                                .setBody(jsonBuilder().startObject().field("size", 1).endObject())
                                .setAuth(shieldEnabled() ? new BasicAuth("test", "changeme") : null)))
                        .condition(scriptCondition("ctx.payload.hits.total == 1"))
                        .addAction(indexAction("_id", "idx", "action")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name");
            refresh();
        }
        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

    @Test
    public void testInputFiltering() throws Exception {
        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();

        ScriptServiceProxy sc = scriptService();
        InetSocketAddress address = internalTestCluster().httpAddresses()[0];
        XContentBuilder body = jsonBuilder().prettyPrint().startObject()
                    .field("query").value(termQuery("field", "value"))
                .endObject();
        TemplatedHttpRequest.SourceBuilder requestBuilder = new TemplatedHttpRequest.SourceBuilder(address.getHostName(), address.getPort())
                .setPath(template("/idx/_search"))
                .setBody(body);
        if (shieldEnabled()) {
            requestBuilder.setAuth(new BasicAuth("test", "changeme"));
        }

        watcherClient.preparePutWatch("_name1")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).addExtractKey("hits.total"))
                        .condition(scriptCondition("ctx.payload.hits.total == 1")))
                .get();

        // in this watcher the condition will fail, because max_score isn't extracted, only total:
        watcherClient.preparePutWatch("_name2")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(httpInput(requestBuilder).addExtractKey("hits.total"))
                        .condition(scriptCondition("ctx.payload.hits.max_score >= 0")))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_name1");
            timeWarp().scheduler().trigger("_name2");
            refresh();
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
        Map payload = (Map) ((Map)((Map)((Map) searchResponse.getHits().getAt(0).sourceAsMap().get("watch_execution")).get("input_result")).get("http")).get("payload");
        assertThat(payload.size(), equalTo(1));
        assertThat(((Map) payload.get("hits")).size(), equalTo(1));
        assertThat((Integer) ((Map) payload.get("hits")).get("total"), equalTo(1));
    }

}
