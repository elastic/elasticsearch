/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Basic notification service
 */
public abstract class NotificationService<Account> extends AbstractComponent {

    private final String type;
    // all are guarded by this
    private volatile Map<String, Account> accounts;
    private volatile Account defaultAccount;
    private volatile Settings cachedClusterSettings = null;
    private volatile SecureSettings cachedSecureSettings = null;

    public NotificationService(String type, ClusterSettings clusterSettings, List<Setting<?>> pluginSettings) {
        this.type = type;
        clusterSettings.addSettingsUpdateConsumer(this::clusterSettingsConsumer, pluginSettings);
    }

    private synchronized void clusterSettingsConsumer(Settings settings) {
        // update cached settings
        this.cachedClusterSettings = settings;
        final Set<String> accountNames = getAccountNames(settings);
        // build new settings from the previously cached secure settings
        final Settings.Builder completeSettingsBuilder = Settings.builder().put(settings, false);
        if (cachedSecureSettings != null) {
            completeSettingsBuilder.setSecureSettings(cachedSecureSettings);
        }
        final Settings completeSettings = completeSettingsBuilder.build();
        // create new accounts from the latest
        this.accounts = createAccounts(completeSettings, accountNames, this::createAccount);
        this.defaultAccount = selectDefaultAccount(completeSettings, this.accounts);
    }

    // Used for testing only
    NotificationService(String type) {
        this.type = type;
    }

    public synchronized void reload(Settings settings) {
        Tuple<Map<String, Account>, Account> accounts = buildAccounts(settings, type, this::createAccount);
        this.accounts = accounts.v1();
        this.defaultAccount = accounts.v2();
    }

    protected abstract Account createAccount(String name, Settings accountSettings);

    public Account getAccount(String name) {
        // note this is not final since we mock it in tests and that causes
        // trouble since final methods can't be mocked...
        final Map<String, Account> accounts;
        final Account defaultAccount;
        synchronized (this) { // must read under sync block otherwise it might be inconsistent
            accounts = this.accounts;
            defaultAccount = this.defaultAccount;
        }
        Account theAccount = accounts.getOrDefault(name, defaultAccount);
        if (theAccount == null && name == null) {
            throw new IllegalArgumentException("no accounts of type [" + type + "] configured. " +
                    "Please set up an account using the [xpack.notification." + type +"] settings");
        }
        if (theAccount == null) {
            throw new IllegalArgumentException("no account found for name: [" + name + "]");
        }
        return theAccount;
    }

    private String getNotificationsAccountPrefix() {
        return "xpack.notification." + type + ".account.";
    }

    private Set<String> getAccountNames(Settings settings) {
        return settings.getByPrefix(getNotificationsAccountPrefix()).names();
    }

    private @Nullable String getDefaultAccountName(Settings settings) {
        return settings.get("xpack.notification." + type + ".default_account");
    }

    private Map<String, Account> createAccounts(Settings settings, Set<String> accountNames,
            BiFunction<String, Settings, Account> accountFactory) {
        final Map<String, Account> accounts = new HashMap<>();
        for (final String accountName : accountNames) {
            final Settings accountSettings = settings.getAsSettings(getNotificationsAccountPrefix() + accountName);
            final Account account = accountFactory.apply(accountName, accountSettings);
            accounts.put(accountName, account);
        }
        return Collections.unmodifiableMap(accounts);
    }

    private @Nullable Account selectDefaultAccount(Settings settings, Map<String, Account> accounts) {
        final String defaultAccountName = getDefaultAccountName(settings);
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                return null;
            } else {
                return accounts.values().iterator().next();
            }
        } else {
            final Account account = accounts.get(defaultAccountName);
            if (account == null) {
                throw new SettingsException("could not find default account [" + defaultAccountName + "]");
            }
            return account;
        }
    }

    private static <A> Tuple<Map<String, A>, A> buildAccounts(Settings settings, String type,
            BiFunction<String, Settings, A> accountFactory) {
        Settings accountsSettings = settings.getByPrefix("xpack.notification." + type + ".account.");
        Map<String, A> accounts = new HashMap<>();
        for (String name : accountsSettings.names()) {
            Settings accountSettings = accountsSettings.getAsSettings(name);
            A account = accountFactory.apply(name, accountSettings);
            accounts.put(name, account);
        }
        final String defaultAccountName = settings.get("xpack.notification." + type + ".default_account");
        A defaultAccount;
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                defaultAccount = null;
            } else {
                A account = accounts.values().iterator().next();
                defaultAccount = account;
            }
        } else {
            defaultAccount = accounts.get(defaultAccountName);
            if (defaultAccount == null) {
                throw new SettingsException("could not find default account [" + defaultAccountName + "]");
            }
        }
        return new Tuple<>(Collections.unmodifiableMap(accounts), defaultAccount);
    }

    private SecureSettings cacheSecureSettings(Settings source, List<SecureSetting<?>> secureSettingsList) {
        for (final SecureSetting<?> secureSetting : secureSettingsList) {
            secureSetting.getA
        }
        source.get("a");
        source.filter(settingKey -> {
            for (final SecureSetting secureSetting : secureSettingsList) {
                if (secureSetting.match(settingKey)) {
                    return true;
                }
            }
            return false;
        });
        return new SecureSettings() {

            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) throws GeneralSecurityException {
                return null;
            }

            @Override
            public Set<String> getSettingNames() {
                return null;
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
