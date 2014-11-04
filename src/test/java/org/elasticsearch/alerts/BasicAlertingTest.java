/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.alerts.actions.*;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.alerts.triggers.ScriptedAlertTrigger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0, maxNumDataNodes = 1, minNumDataNodes = 1, numDataNodes = 1)
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
    // TODO: add request, response & request builder etc.
    public void testAlerSchedulerStartsProperly() throws Exception {
        createIndex("my-index");
        createIndex(AlertsStore.ALERT_INDEX);
        createIndex(AlertActionManager.ALERT_HISTORY_INDEX);
        ensureGreen("my-index", AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX);

        final AlertManager alertManager = internalCluster().getInstance(AlertManager.class, internalCluster().getMasterName());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(alertManager.isStarted(), is(true));
            }
        });
        final AtomicBoolean alertActionInvoked = new AtomicBoolean(false);
        final AlertAction alertAction = new AlertAction() {
            @Override
            public String getActionName() {
                return "test";
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public void readFrom(StreamInput in) throws IOException {

            }

            @Override
            public boolean doAction(Alert alert, AlertActionEntry actionEntry) {
                logger.info("Alert {} invoked: {}", alert.alertName(), actionEntry);
                alertActionInvoked.set(true);
                return true;
            }
        };
        AlertActionRegistry alertActionRegistry = internalCluster().getInstance(AlertActionRegistry.class, internalCluster().getMasterName());
        alertActionRegistry.registerAction("test", new AlertActionFactory() {
            @Override
            public AlertAction createAction(XContentParser parser) throws IOException {
                parser.nextToken();
                return alertAction;
            }

            @Override
            public AlertAction readFrom(StreamInput in) throws IOException {
                return alertAction;
            }
        });
        AlertTrigger alertTrigger = new AlertTrigger(new ScriptedAlertTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"));
        Alert alert = new Alert(
                "my-first-alert",
                client().prepareSearch("my-index").setQuery(QueryBuilders.matchAllQuery()).request(),
                alertTrigger,
                Arrays.asList(alertAction),
                "0/5 * * * * ? *",
                null,
                1,
                true
        );
        alertManager.addAlert("my-first-alert", jsonBuilder().value(alert).bytes());
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(alertActionInvoked.get(), is(true));
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));
            }
        }, 30, TimeUnit.SECONDS);
    }

}
