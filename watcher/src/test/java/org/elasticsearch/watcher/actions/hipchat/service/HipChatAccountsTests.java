/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat.service;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
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

    @Test
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

    @Test
    public void testSingleAccount_NoExplicitDefault() throws Exception {
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

    @Test
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

    @Test
    public void testMultipleAccounts_NoExplicitDefault() throws Exception {
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

    @Test(expected = SettingsException.class)
    public void testMultipleAccounts_UnknownDefault() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        new HipChatAccounts(builder.build(), httpClient, logger);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        HipChatAccounts accounts = new HipChatAccounts(builder.build(), httpClient, logger);
        accounts.account(null);
        fail("no accounts are configured so trying to get the default account should throw an IllegalStateException");
    }

    @Test(expected = SettingsException.class)
    public void testNoAccount_WithDefaultAccount() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("default_account", "unknown");
        new HipChatAccounts(builder.build(), httpClient, logger);
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
