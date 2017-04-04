/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpProxy;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HipChatAccountsTests extends ESTestCase {
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "account1");
        addAccountSettings("account1", builder);
        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY, 
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        HipChatAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        HipChatAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        HipChatAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccountsNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        HipChatAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = service.getAccount(null);
        assertThat(account, notNullValue());
        assertThat(account.name, isOneOf("account1", "account2"));
    }

    public void testMultipleAccountsUnknownDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        try {
            new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default account [unknown]"));
        }
    }

    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        try {
            service.getAccount(null);
            fail("no accounts are configured so trying to get the default account should throw an IllegalStateException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no account found for name: [null]"));
        }
    }

    public void testNoAccountWithDefaultAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "unknown");
        try {
            new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default account [unknown]"));
        }
    }

    public void testProxy() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "account1");
        addAccountSettings("account1", builder);
        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(HipChatService.HIPCHAT_ACCOUNT_SETTING)));
        HipChatAccount account = service.getAccount("account1");

        HipChatMessage.Template template = new HipChatMessage.Template.Builder(new TextTemplate("foo"))
                .addRooms(new TextTemplate("room"))
                .setFrom("from")
                .build();
        HipChatMessage hipChatMessage = template.render(new MockTextTemplateEngine(), new HashMap());

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        HttpProxy proxy = new HttpProxy("localhost", 8080);
        account.send(hipChatMessage, proxy);

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        builder.put("xpack.notification.hipchat.account." + name + ".profile", profile.value());
        builder.put("xpack.notification.hipchat.account." + name + ".auth_token", randomAlphaOfLength(50));
        if (profile == HipChatAccount.Profile.INTEGRATION) {
            builder.put("xpack.notification.hipchat.account." + name + ".room", randomAlphaOfLength(10));
        }
    }
}
