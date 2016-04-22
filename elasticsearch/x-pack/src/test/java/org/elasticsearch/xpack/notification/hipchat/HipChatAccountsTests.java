/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 *
 */
public class HipChatAccountsTests extends ESTestCase {
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);

        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        HipChatAccount account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        HipChatAccount account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        HipChatAccount account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = accounts.account("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
    }

    public void testMultipleAccountsNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        HipChatAccount account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account1"));
        account = accounts.account("account2");
        assertThat(account, notNullValue());
        assertThat(account.name, equalTo("account2"));
        account = accounts.account(null);
        assertThat(account, notNullValue());
        assertThat(account.name, isOneOf("account1", "account2"));
    }

    public void testMultipleAccountsUnknownDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        try {
            new HipChatAccounts(builder.build(), httpClient, logger);
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default hipchat account [unknown]"));
        }
    }

    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        try {
            accounts.account(null);
            fail("no accounts are configured so trying to get the default account should throw an IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("cannot find default hipchat account as no accounts have been configured"));
        }
    }

    public void testNoAccountWithDefaultAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "unknown");
        try {
            new HipChatAccounts(builder.build(), httpClient, logger);
            fail("Expected SettingsException");
        } catch (SettingsException e) {
            assertThat(e.getMessage(), is("could not find default hipchat account [unknown]"));
        }
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        builder.put("account." + name + ".profile", profile.value());
        builder.put("account." + name + ".auth_token", randomAsciiOfLength(50));
        if (profile == HipChatAccount.Profile.INTEGRATION) {
            builder.put("account." + name + ".room", randomAsciiOfLength(10));
        }
    }
}
