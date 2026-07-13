/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.notification.slack.message.SlackMessageDefaultsTests;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PagerDutyAccountsTests extends ESTestCase {

    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testProxy() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        PagerDutyService service = new PagerDutyService(
            builder.build(),
            httpClient,
            new ClusterSettings(Settings.EMPTY, new HashSet<>(PagerDutyService.getSettings()))
        );
        PagerDutyAccount account = service.getAccount("account1");

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        HttpProxy proxy = new HttpProxy("localhost", 8080);
        IncidentEvent event = new IncidentEvent("foo", null, null, null, null, account.getName(), true, null, proxy);
        account.send(event, Payload.EMPTY, null);

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
    }

    // in earlier versions of the PD action the wrong JSON was sent, because the contexts field was named context
    // the pagerduty API accepts any JSON, thus this was never caught
    public void testContextIsSentCorrect() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        PagerDutyService service = new PagerDutyService(
            builder.build(),
            httpClient,
            new ClusterSettings(Settings.EMPTY, new HashSet<>(PagerDutyService.getSettings()))
        );
        PagerDutyAccount account = service.getAccount("account1");

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        IncidentEventContext[] contexts = {
            IncidentEventContext.link("https://www.elastic.co/products/x-pack/alerting", "Go to the Elastic.co Alerting website"),
            IncidentEventContext.image(
                "https://www.elastic.co/assets/blte5d899fd0b0e6808/icon-alerting-bb.svg",
                "https://www.elastic.co/products/x-pack/alerting",
                "X-Pack-Alerting website link with log"
            ) };
        IncidentEvent event = new IncidentEvent("foo", null, null, null, null, account.getName(), true, contexts, HttpProxy.NO_PROXY);
        account.send(event, Payload.EMPTY, null);

        HttpRequest request = argumentCaptor.getValue();
        ObjectPath source = ObjectPath.createFromXContent(JsonXContent.jsonXContent, new BytesArray(request.body()));
        assertThat(source.evaluate("contexts"), nullValue());
        assertThat(source.evaluate("links"), notNullValue());
        assertThat(source.evaluate("images"), notNullValue());
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            "xpack.notification.pagerduty.account." + name + "." + PagerDutyAccount.SECURE_SERVICE_API_KEY_SETTING.getKey(),
            randomAlphaOfLength(50)
        );
        builder.setSecureSettings(secureSettings);
        Settings defaults = SlackMessageDefaultsTests.randomSettings();
        for (String setting : defaults.keySet()) {
            builder.copy("xpack.notification.pagerduty.message_defaults." + setting, setting, defaults);
        }
    }
}
