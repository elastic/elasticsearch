/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.notification.slack.message.SlackMessageDefaultsTests;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class SlackAccountsTests extends ESTestCase {
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.slack.default_account", "account1");
        addAccountSettings("account1", builder);

        SlackService service = new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
        SlackAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        SlackService service = new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
        SlackAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.slack.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        SlackService service = new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
        SlackAccount account = service.getAccount("account1");
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
                .put("xpack.notification.slack.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        SlackService service = new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
        SlackAccount account = service.getAccount("account1");
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
                .put("xpack.notification.slack.default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        try {
            new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default account [unknown]"));
        }
    }

    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        SlackService service = new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
        try {
            service.getAccount(null);
            fail("no accounts are configured so trying to get the default account should throw an IllegalStateException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no account found for name: [null]"));
        }
    }

    public void testNoAccountWithDefaultAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.slack.default_account", "unknown");
        try {
            new SlackService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                    Collections.singleton(SlackService.SLACK_ACCOUNT_SETTING)));
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default account [unknown]"));
        }
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        builder.put("xpack.notification.slack.account." + name + ".url", "https://hooks.slack.com/services/" + randomAlphaOfLength(50));
        Settings defaults = SlackMessageDefaultsTests.randomSettings();
        for (Map.Entry<String, String> setting : defaults.getAsMap().entrySet()) {
            builder.put("xpack.notification.slack.message_defaults." + setting.getKey(), setting.getValue());
        }
    }
}
