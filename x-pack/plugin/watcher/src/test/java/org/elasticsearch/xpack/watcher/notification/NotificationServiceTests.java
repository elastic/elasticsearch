/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

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

    public void testAccountWithSecureSettings() throws Exception {
        final Setting<SecureString> secureSetting1 = SecureSetting.secureString("xpack.notification.test.account.secure_only", null);
        final Setting<SecureString> secureSetting2 = SecureSetting.secureString("xpack.notification.test.account.mixed.secure", null);
        final Map<String, char[]> secureSettingsMap = new HashMap<>();
        secureSettingsMap.put(secureSetting1.getKey(), "secure_only".toCharArray());
        secureSettingsMap.put(secureSetting2.getKey(), "mixed_secure".toCharArray());
        Settings settings = Settings.builder()
                .put("xpack.notification.test.account.unsecure_only", "bar")
                .put("xpack.notification.test.account.mixed.unsecure", "mixed_unsecure")
                .setSecureSettings(secureSettingsFromMap(secureSettingsMap))
                .build();
        TestNotificationService service = new TestNotificationService(settings, Arrays.asList(secureSetting1, secureSetting2));
        assertThat(service.getAccount("secure_only"), is("secure_only"));
        assertThat(service.getAccount("unsecure_only"), is("unsecure_only"));
        assertThat(service.getAccount("mixed"), is("mixed"));
        assertThat(service.getAccount(null), anyOf(is("secure_only"), is("unsecure_only"), is("mixed")));
    }

    public void testAccountUpdateSettings() throws Exception {
        final Setting<SecureString> secureSetting = SecureSetting.secureString("xpack.notification.test.account.x.secure", null);
        final Setting<String> setting = Setting.simpleString("xpack.notification.test.account.x.dynamic", Setting.Property.Dynamic,
                Setting.Property.NodeScope);
        final AtomicReference<String> secureSettingValue = new AtomicReference<String>(randomAlphaOfLength(4));
        final AtomicReference<String> settingValue = new AtomicReference<String>(randomAlphaOfLength(4));
        final Map<String, char[]> secureSettingsMap = new HashMap<>();
        final AtomicInteger validationInvocationCount = new AtomicInteger(0);
        secureSettingsMap.put(secureSetting.getKey(), secureSettingValue.get().toCharArray());
        final Settings.Builder settingsBuilder = Settings.builder()
                .put(setting.getKey(), settingValue.get())
                .setSecureSettings(secureSettingsFromMap(secureSettingsMap));
        final TestNotificationService service = new TestNotificationService(settingsBuilder.build(), Arrays.asList(secureSetting),
                (String name, Settings accountSettings) -> {
                    assertThat(accountSettings.get("dynamic"), is(settingValue.get()));
                    assertThat(SecureSetting.secureString("secure", null).get(accountSettings), is(secureSettingValue.get()));
                    validationInvocationCount.incrementAndGet();
                });
        assertThat(validationInvocationCount.get(), is(0));
        service.getAccount(null);
        assertThat(validationInvocationCount.get(), is(1));
        // update secure setting only
        secureSettingValue.set(randomAlphaOfLength(4));
        secureSettingsMap.put(secureSetting.getKey(), secureSettingValue.get().toCharArray());
        service.reload(settingsBuilder.build());
        assertThat(validationInvocationCount.get(), is(1));
        service.getAccount(null);
        assertThat(validationInvocationCount.get(), is(2));
        // update dynamic cluster setting only
        settingValue.set(randomAlphaOfLength(4));
        settingsBuilder.put(setting.getKey(), settingValue.get());
        service.clusterSettingsConsumer(settingsBuilder.build());
        assertThat(validationInvocationCount.get(), is(2));
        service.getAccount(null);
        assertThat(validationInvocationCount.get(), is(3));
        // update both
        if (randomBoolean()) {
            // update secure first
            secureSettingValue.set(randomAlphaOfLength(4));
            secureSettingsMap.put(secureSetting.getKey(), secureSettingValue.get().toCharArray());
            service.reload(settingsBuilder.build());
            // update cluster second
            settingValue.set(randomAlphaOfLength(4));
            settingsBuilder.put(setting.getKey(), settingValue.get());
            service.clusterSettingsConsumer(settingsBuilder.build());
        } else {
            // update cluster first
            settingValue.set(randomAlphaOfLength(4));
            settingsBuilder.put(setting.getKey(), settingValue.get());
            service.clusterSettingsConsumer(settingsBuilder.build());
            // update secure second
            secureSettingValue.set(randomAlphaOfLength(4));
            secureSettingsMap.put(secureSetting.getKey(), secureSettingValue.get().toCharArray());
            service.reload(settingsBuilder.build());
        }
        assertThat(validationInvocationCount.get(), is(3));
        service.getAccount(null);
        assertThat(validationInvocationCount.get(), is(4));
    }

    private static class TestNotificationService extends NotificationService<String> {

        private final BiConsumer<String, Settings> validator;

        TestNotificationService(Settings settings, List<Setting<?>> secureSettings, BiConsumer<String, Settings> validator) {
            super("test", settings, secureSettings);
            this.validator = validator;
            reload(settings);
        }

        TestNotificationService(Settings settings, List<Setting<?>> secureSettings) {
            this(settings, secureSettings, (x, y) -> {});
        }

        TestNotificationService(Settings settings) {
            this(settings, Collections.emptyList(), (x, y) -> {});
        }

        @Override
        protected String createAccount(String name, Settings accountSettings) {
            validator.accept(name, accountSettings);
            return name;
        }
    }

    private static SecureSettings secureSettingsFromMap(Map<String, char[]> secureSettingsMap) {
        return new SecureSettings() {

            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) throws GeneralSecurityException {
                return new SecureString(secureSettingsMap.get(setting));
            }

            @Override
            public Set<String> getSettingNames() {
                return secureSettingsMap.keySet();
            }

            @Override
            public InputStream getFile(String setting) throws GeneralSecurityException {
                return null;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }
}
