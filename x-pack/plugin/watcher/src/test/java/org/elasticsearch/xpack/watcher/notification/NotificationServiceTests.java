/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.util.Collections;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

public class NotificationServiceTests extends ESTestCase {

    public void testSingleAccount() {
        String accountName = randomAlphaOfLength(10);
        Settings settings = Settings.builder().put("xpack.notification.test.account." + accountName, "bar").build();

        TestNotificationService service = new TestNotificationService(settings);
        assertThat(service.getAccount(accountName), is(accountName));
        // single account, this will also be the default
        assertThat(service.getAccount("non-existing"), is(accountName));
    }

    public void testMultipleAccountsWithExistingDefault() {
        String accountName = randomAlphaOfLength(10);
        Settings settings = Settings.builder()
                .put("xpack.notification.test.account." + accountName, "bar")
                .put("xpack.notification.test.account.second", "bar")
                .put("xpack.notification.test.default_account", accountName)
                .build();

        TestNotificationService service = new TestNotificationService(settings);
        assertThat(service.getAccount(accountName), is(accountName));
        assertThat(service.getAccount("second"), is("second"));
        assertThat(service.getAccount("non-existing"), is(accountName));
    }

    public void testMultipleAccountsWithNoDefault() {
        String accountName = randomAlphaOfLength(10);
        Settings settings = Settings.builder()
                .put("xpack.notification.test.account." + accountName, "bar")
                .put("xpack.notification.test.account.second", "bar")
                .put("xpack.notification.test.account.third", "bar")
                .build();

        TestNotificationService service = new TestNotificationService(settings);
        assertThat(service.getAccount(null), anyOf(is(accountName), is("second"), is("third")));
    }

    public void testMultipleAccountsUnknownDefault() {
        String accountName = randomAlphaOfLength(10);
        Settings settings = Settings.builder()
                .put("xpack.notification.test.account." + accountName, "bar")
                .put("xpack.notification.test.account.second", "bar")
                .put("xpack.notification.test.default_account", "non-existing")
                .build();

        SettingsException e = expectThrows(SettingsException.class, () -> new TestNotificationService(settings));
        assertThat(e.getMessage(), is("could not find default account [non-existing]"));
    }

    public void testNoSpecifiedDefaultAccount() {
        String accountName = randomAlphaOfLength(10);
        Settings settings = Settings.builder().put("xpack.notification.test.account." + accountName, "bar").build();

        TestNotificationService service = new TestNotificationService(settings);
        assertThat(service.getAccount(null), is(accountName));
    }

    public void testAccountDoesNotExist() throws Exception{
        TestNotificationService service = new TestNotificationService(Settings.EMPTY);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.getAccount(null));
        assertThat(e.getMessage(),
                is("no accounts of type [test] configured. Please set up an account using the [xpack.notification.test] settings"));
    }

    private static class TestNotificationService extends NotificationService<String> {

        TestNotificationService(Settings settings) {
            super("test", settings, Collections.emptyList());
            reload(settings);
        }

        @Override
        protected String createAccount(String name, Settings accountSettings) {
            return name;
        }
    }
}
