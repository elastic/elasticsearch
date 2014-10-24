/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugin.alerting.AlertingPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 1)
public class BasicAlertingTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.mandatory", "alerting-plugin")
                .put("plugin.types", AlertingPlugin.class.getName())
                .put("node.mode", "network")
                .put("plugins.load_classpath_plugins", false)
                .build();
    }

    @Test
    // TODO: add request, response & request builder etc.
    public void testAlerSchedulerStartsProperly() throws Exception {
        createIndex("my-index");
        createIndex(ScriptService.SCRIPT_INDEX);

        client().prepareIndex(ScriptService.SCRIPT_INDEX, "mustache", "query")
                .setSource(jsonBuilder().startObject().startObject("template").startObject("match_all").endObject().endObject().endObject())
                .get();

        /*client().admin().indices().preparePutTemplate("query")
                .setTemplate("*")
                .setSource(jsonBuilder().startObject().startObject("query").startObject("match_all").endObject().endObject().endObject())
                .get();

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().prepareGetTemplates("query").get();
        assertThat(templatesResponse.getIndexTemplates().size(), equalTo(1));
        assertThat(templatesResponse.getIndexTemplates().get(0).getName(), equalTo("query"));*/

        AlertScheduler alertScheduler = internalCluster().getInstance(AlertScheduler.class, internalCluster().getMasterName());
        assertThat(alertScheduler.isRunning(), is(true));

        AlertManager alertManager = internalCluster().getInstance(AlertManager.class, internalCluster().getMasterName());
        final CountDownLatch latch = new CountDownLatch(1);
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
            public boolean doAction(String alertName, AlertResult alert) {
                logger.info("Alert {} invoked: {}", alertName, alert);
                latch.countDown();
                return true;
            }
        };
        AlertActionManager alertActionManager = internalCluster().getInstance(AlertActionManager.class, internalCluster().getMasterName());
        alertActionManager.registerAction("test", new AlertActionFactory() {
            @Override
            public AlertAction createAction(Object parameters) {
                return alertAction;
            }
        });
        AlertTrigger alertTrigger = new AlertTrigger(new ScriptedAlertTrigger("return true", ScriptService.ScriptType.INLINE, "groovy"));
        Alert alert = new Alert(
                "my-first-alert",
                "/mustache/query",
                alertTrigger,
                TimeValue.timeValueSeconds(1),
                Arrays.asList(alertAction),
                "* * * * * ? *",
                null,
                Arrays.asList("my-index"),
                null,
                1,
                true,
                true
                );
        alertManager.addAlert("my-first-alert", alert, true);
        latch.await();
    }

}
