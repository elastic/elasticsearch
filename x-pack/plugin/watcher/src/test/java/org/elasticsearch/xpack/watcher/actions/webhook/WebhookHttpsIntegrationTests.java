/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.webhook;

import com.sun.net.httpserver.HttpsServer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class WebhookHttpsIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private MockWebServer webServer;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path keyPath = getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.pem");
        Path certPath = getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.crt");
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("xpack.http.ssl.key", keyPath)
            .put("xpack.http.ssl.certificate", certPath)
            .put("xpack.http.ssl.keystore.password", "testnode")
            .putList("xpack.http.ssl.supported_protocols", getProtocols())
            .build();
    }

    @Before
    public void startWebservice() throws Exception {
        Settings settings = getInstanceFromMaster(Settings.class);
        TestsSSLService sslService = new TestsSSLService(settings, getInstanceFromMaster(Environment.class));
        webServer = new MockWebServer(sslService.sslContext("xpack.http.ssl"), false);
        webServer.start();
    }

    @After
    public void stopWebservice() throws Exception {
        webServer.close();
    }

    public void testHttps() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webServer.getPort())
                .scheme(Scheme.HTTPS)
                .path(new TextTemplate("/test/_id"))
                .body(new TextTemplate("{key=value}"))
                .method(HttpMethod.POST);

        new PutWatchRequestBuilder(client(), "_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_id", ActionBuilders.webhookAction(builder)))
                .get();

        timeWarp().trigger("_id");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);
        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getUri().getPath(), equalTo("/test/_id"));
        assertThat(webServer.requests().get(0).getBody(), equalTo("{key=value}"));

        SearchResponse response =
                searchWatchRecords(b -> b.setQuery(QueryBuilders.termQuery(WatchRecord.STATE.getPreferredName(), "executed")));

        assertNoFailures(response);
        XContentSource source = xContentSource(response.getHits().getAt(0).getSourceRef());
        String body = source.getValue("result.actions.0.webhook.response.body");
        assertThat(body, notNullValue());
        assertThat(body, is("body"));

        Number status = source.getValue("result.actions.0.webhook.response.status");
        assertThat(status, notNullValue());
        assertThat(status.intValue(), is(200));
    }

    public void testHttpsAndBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webServer.getPort())
                .scheme(Scheme.HTTPS)
                .auth(new BasicAuth("_username", "_password".toCharArray()))
                .path(new TextTemplate("/test/_id"))
                .body(new TextTemplate("{key=value}"))
                .method(HttpMethod.POST);

        new PutWatchRequestBuilder(client(), "_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(simpleInput("key", "value"))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_id", ActionBuilders.webhookAction(builder)))
                .get();

        timeWarp().trigger("_id");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);
        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getUri().getPath(), equalTo("/test/_id"));
        assertThat(webServer.requests().get(0).getBody(), equalTo("{key=value}"));
        assertThat(webServer.requests().get(0).getHeader("Authorization"), equalTo("Basic X3VzZXJuYW1lOl9wYXNzd29yZA=="));
    }

    /**
     * The {@link HttpsServer} in the JDK has issues with TLSv1.3 when running in a JDK prior to
     * 12.0.1 so we pin to TLSv1.2 when running on an earlier JDK
     */
    private static List<String> getProtocols() {
        if (JavaVersion.current().compareTo(JavaVersion.parse("12")) < 0) {
            return List.of("TLSv1.2");
        } else {
            JavaVersion full =
                AccessController.doPrivileged(
                    (PrivilegedAction<JavaVersion>) () -> JavaVersion.parse(System.getProperty("java.version")));
            if (full.compareTo(JavaVersion.parse("12.0.1")) < 0) {
                return List.of("TLSv1.2");
            }
        }
        return XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS;
    }
}
