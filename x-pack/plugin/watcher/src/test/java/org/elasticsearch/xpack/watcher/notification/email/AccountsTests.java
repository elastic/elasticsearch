/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;

public class AccountsTests extends ESTestCase {
    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder().put("default_account", "account1");
        addAccountSettings("account1", builder);
        EmailService service = new EmailService(
            builder.build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );
        Account account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);
        EmailService service = new EmailService(
            builder.build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );
        Account account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.email.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        EmailService service = new EmailService(
            builder.build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );
        Account account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account2"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
    }

    public void testMultipleAccountsNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder().put("default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        EmailService service = new EmailService(
            builder.build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );
        Account account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.name(), equalTo("account2"));
        account = service.getAccount(null);
        assertThat(account, notNullValue());
        assertThat(account.name(), is(oneOf("account1", "account2")));
    }

    public void testMultipleAccountsUnknownDefault() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.email.default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()));
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> new EmailService(builder.build(), null, mock(SSLService.class), clusterSettings)
        );
        assertThat(e.getMessage(), is("could not find default account [unknown]"));
    }

    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        EmailService service = new EmailService(
            builder.build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );
        expectThrows(IllegalArgumentException.class, () -> service.getAccount(null));
    }

    public void testNoAccountWithDefaultAccount() throws Exception {
        Settings settings = Settings.builder().put("xpack.notification.email.default_account", "unknown").build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()));
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> new EmailService(settings, null, mock(SSLService.class), clusterSettings)
        );
        assertThat(e.getMessage(), is("could not find default account [unknown]"));
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        builder.put("xpack.notification.email.account." + name + ".smtp.host", "_host");
    }
}
