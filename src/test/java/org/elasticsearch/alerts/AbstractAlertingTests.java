/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 */
public abstract class AbstractAlertingTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types", AlertsPlugin.class.getName())
                .put("node.mode", "network")
                .build();
    }

    @After
    public void clearAlerts() {
        // Clear all in-memory alerts on all nodes, perhaps there isn't an elected master at this point
        for (AlertManager manager : internalCluster().getInstances(AlertManager.class)) {
            manager.clear();
        }
    }

    protected BytesReference createAlertSource(String cron, SearchRequest request, String scriptTrigger) throws IOException {
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

    protected AlertsClient alertClient() {
        return internalCluster().getInstance(AlertsClient.class);
    }

    protected void assertAlertTriggered(final String alertName) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                // The alerthistory index gets created in the background when the first alert fires, so we to check first is this index is created and shards are started
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                IndexRoutingTable routingTable = state.getRoutingTable().index(AlertActionManager.ALERT_HISTORY_INDEX);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));

                SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("alert_name", alertName)).must(matchQuery("state", AlertActionState.ACTION_PERFORMED.toString())))
                        .addField("response.hits.total")
                        .setSize(1)
                        .get();
                assertThat(searchResponse.getHits().getHits().length, equalTo(1));
                assertThat((Integer) searchResponse.getHits().getAt(0).field("response.hits.total").getValue(), equalTo(1));
            }
        });
    }

    protected void assertNoAlertTrigger(final String alertName) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                // The alerthistory index gets created in the background when the first alert fires, so we to check first is this index is created and shards are started
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                IndexRoutingTable routingTable = state.getRoutingTable().index(AlertActionManager.ALERT_HISTORY_INDEX);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));

                SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("alert_name", alertName)).must(matchQuery("state", AlertActionState.NO_ACTION_NEEDED.toString())))
                        .setSize(1)
                        .get();
                assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            }
        });
    }

}
