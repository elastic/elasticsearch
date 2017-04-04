/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpProxy;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.notification.slack.message.SlackMessageDefaultsTests;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PagerDutyAccountsTests extends ESTestCase {

    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        PagerDutyAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        PagerDutyAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        PagerDutyAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccounts_NoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        PagerDutyAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = service.getAccount(null);
        assertThat(account, notNullValue());
        assertThat(account.name, isOneOf("account1", "account2"));
    }

    public void testMultipleAccounts_UnknownDefault() throws Exception {
        expectThrows(SettingsException.class, () -> {
            Settings.Builder builder = Settings.builder()
                    .put("xpack.notification.pagerduty.default_account", "unknown");
            addAccountSettings("account1", builder);
            addAccountSettings("account2", builder);
            new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        });
    }

    public void testNoAccount() throws Exception {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings.Builder builder = Settings.builder();
            PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
            service.getAccount(null);
        });
    }

    public void testNoAccount_WithDefaultAccount() throws Exception {
        try {
            Settings.Builder builder = Settings.builder()
                    .put("xpack.notification.pagerduty.default_account", "unknown");
            new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
            fail("Expected a SettingsException to happen");
        } catch (SettingsException e) {}
    }

    public void testProxy() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.pagerduty.default_account", "account1");
        addAccountSettings("account1", builder);
        PagerDutyService service = new PagerDutyService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(PagerDutyService.PAGERDUTY_ACCOUNT_SETTING)));
        PagerDutyAccount account = service.getAccount("account1");

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        HttpProxy proxy = new HttpProxy("localhost", 8080);
        IncidentEvent event = new IncidentEvent("foo", null, null, null, null, account.getName(), true, null, proxy);
        account.send(event, Payload.EMPTY);

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        builder.put("xpack.notification.pagerduty.account." + name + ".service_api_key", randomAlphaOfLength(50));
        Settings defaults = SlackMessageDefaultsTests.randomSettings();
        for (Map.Entry<String, String> setting : defaults.getAsMap().entrySet()) {
            builder.put("xpack.notification.pagerduty.message_defaults." + setting.getKey(), setting.getValue());
        }
    }
}
