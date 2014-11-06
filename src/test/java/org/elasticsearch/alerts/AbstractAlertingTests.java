/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
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

/**
 */
public class AbstractAlertingTests extends ElasticsearchIntegrationTest {

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

}
