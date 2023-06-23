/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.actions.webhook;

import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.notification.WebhookService;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class WebhookTokenIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private MockWebServer webServer = new MockWebServer();

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), Netty4Plugin.class); // for http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(WebhookService.SETTING_WEBHOOK_TOKEN_ENABLED.getKey(), true);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(WebhookService.SETTING_WEBHOOK_HOST_TOKEN_PAIRS.getKey(), "localhost:0=oldtoken");
        builder.setSecureSettings(secureSettings);
        return builder.build();
    }

    @Before
    public void startWebservice() throws Exception {
        webServer.start();
    }

    @After
    public void stopWebservice() throws Exception {
        webServer.close();
    }

    public void testWebhook() throws Exception {
        assumeFalse(
            "Cannot run in FIPS mode since the keystore will be password protected and sending a password in the reload"
                + "settings api call, require TLS to be configured for the transport layer",
            inFipsJvm()
        );
        String localServer = "localhost:" + webServer.getPort();
        logger.info("--> updating keystore token hosts to: {}", localServer);
        Path configPath = internalCluster().configPaths().stream().findFirst().orElseThrow();
        try (KeyStoreWrapper ksw = KeyStoreWrapper.create()) {
            ksw.setString(WebhookService.SETTING_WEBHOOK_HOST_TOKEN_PAIRS.getKey(), (localServer + "=token1234").toCharArray());
            ksw.save(configPath, "".toCharArray(), false);
        }
        // Reload the keystore to load the new settings
        NodesReloadSecureSettingsRequest reloadReq = new NodesReloadSecureSettingsRequest();
        reloadReq.setSecureStorePassword(new SecureString("".toCharArray()));
        client().execute(NodesReloadSecureSettingsAction.INSTANCE, reloadReq).get();

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webServer.getPort())
            .path(new TextTemplate("/test/_id"))
            .putParam("param1", new TextTemplate("value1"))
            .putParam("watch_id", new TextTemplate("_id"))
            .body(new TextTemplate("_body"))
            .auth(new BasicAuth("user", "pass".toCharArray()))
            .method(HttpMethod.POST);

        new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput("key", "value"))
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_id", ActionBuilders.webhookAction(builder))
        ).get();

        timeWarp().trigger("_id");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);
        assertThat(webServer.requests(), hasSize(1));
        MockRequest req = webServer.requests().get(0);
        assertThat(
            webServer.requests().get(0).getUri().getQuery(),
            anyOf(equalTo("watch_id=_id&param1=value1"), equalTo("param1=value1&watch_id=_id"))
        );
        assertThat("token header should be set", req.getHeader(WebhookService.TOKEN_HEADER_NAME), equalTo("token1234"));

        assertThat(webServer.requests().get(0).getBody(), is("_body"));

        SearchResponse response = searchWatchRecords(b -> QueryBuilders.termQuery(WatchRecord.STATE.getPreferredName(), "executed"));

        assertNoFailures(response);
        XContentSource source = xContentSource(response.getHits().getAt(0).getSourceRef());
        String body = source.getValue("result.actions.0.webhook.response.body");
        assertThat(body, notNullValue());
        assertThat(body, is("body"));
        Number status = source.getValue("result.actions.0.webhook.response.status");
        assertThat(status, notNullValue());
        assertThat(status.intValue(), is(200));
    }
}
