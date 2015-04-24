/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

/**
 *
 */
public class AccountsTests extends ElasticsearchTestCase {

    @Test
    public void testSingleAccount() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);

        Accounts accounts = new Accounts(builder.build(), new SecretService.PlainText(), logger);
        Account account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    @Test
    public void testSingleAccount_NoExplicitDefault() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        addAccountSettings("account1", builder);

        Accounts accounts = new Accounts(builder.build(), new SecretService.PlainText(), logger);
        Account account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    @Test
    public void testMultipleAccounts() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        Accounts accounts = new Accounts(builder.build(), new SecretService.PlainText(), logger);
        Account account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = accounts.account("account2");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account2"));
        account = accounts.account(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    @Test
    public void testMultipleAccounts_NoExplicitDefault() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        Accounts accounts = new Accounts(builder.build(), new SecretService.PlainText(), logger);
        Account account = accounts.account("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = accounts.account("account2");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account2"));
        account = accounts.account(null);
        assertThat(account, notNullValue());
        assertThat(account.name(), isOneOf("account1", "account2"));
    }

    @Test(expected = EmailSettingsException.class)
    public void testMultipleAccounts_UnknownDefault() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        new Accounts(builder.build(), new SecretService.PlainText(), logger);
    }

    @Test
    public void testNoAccount() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        Accounts accounts = new Accounts(builder.build(), new SecretService.PlainText(), logger);
        Account account = accounts.account(null);
        assertThat(account, nullValue());
    }

    @Test(expected = EmailSettingsException.class)
    public void testNoAccount_WithDefaultAccount() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("default_account", "unknown");
        new Accounts(builder.build(), new SecretService.PlainText(), logger);
    }

    private void addAccountSettings(String name, ImmutableSettings.Builder builder) {
        builder.put("account." + name + ".smtp.host", "_host");
    }
}
