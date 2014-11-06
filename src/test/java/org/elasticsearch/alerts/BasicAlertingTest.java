/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 1)
public class BasicAlertingTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.mandatory", "alerts")
                .put("plugin.types", AlertsPlugin.class.getName())
                .put("node.mode", "network")
                .put("http.enabled", true)
                .put("plugins.load_classpath_plugins", false)
                .build();
    }

    @Test
    public void testIndexAlert() throws Exception {
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.prepareCreateAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));

                SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                        .setQuery(termQuery("state", AlertActionState.ACTION_PERFORMED.toString()))
                        .addField("response.hits.total")
                        .setSize(1)
                        .get();
                assertThat(searchResponse.getHits().getHits().length, equalTo(1));
                assertThat((Integer) searchResponse.getHits().getAt(0).field("response.hits.total").getValue(), equalTo(1));
            }
        }, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteAlert() throws Exception {
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(matchAllQuery()));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.prepareCreateAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();

        DeleteAlertRequest deleteAlertRequest = new DeleteAlertRequest("my-first-alert");
        DeleteAlertResponse deleteAlertResponse = alertsClient.deleteAlert(deleteAlertRequest).actionGet();
        assertNotNull(deleteAlertResponse.deleteResponse());
        assertTrue(deleteAlertResponse.deleteResponse().isFound());

        assertHitCount(client().prepareCount(AlertsStore.ALERT_INDEX).get(), 0l);
    }

    @After
    public void clearAlerts() {
        // Clear all in-memory alerts on all nodes, perhaps there isn't an elected master at this point
        for (AlertManager manager : internalCluster().getInstances(AlertManager.class)) {
            manager.clear();
        }
    }

    private BytesReference createAlertSource(String cron, SearchRequest request, String scriptTrigger) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("schedule", cron);
        builder.field("enable", true);

        builder.startObject("request");
        XContentHelper.writeRawField("body", request.source(), builder, ToXContent.EMPTY_PARAMS);
        builder.startArray("indices");
        for (String index : request.indices()) {
            builder.value(index);
        }
        builder.endArray();
        builder.endObject();

        builder.startObject("trigger");
        builder.startObject("script");
        builder.field("script", scriptTrigger);
        builder.endObject();
        builder.endObject();

        builder.startObject("actions");
        builder.startObject("index");
        builder.field("index", "my-index");
        builder.field("type", "trail");
        builder.endObject();
        builder.endObject();

        return builder.endObject().bytes();
    }

    private AlertsClient alertClient() {
        return internalCluster().getInstance(AlertsClient.class, internalCluster().getMasterName());
    }

}
