/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ClusterSettings;
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

/**
 * Basic notification service
 */
public abstract class NotificationService<Account> {

    private final String type;
    // all are guarded by this
    private volatile Map<String, Account> accounts;
    private volatile Account defaultAccount;
    // cached cluster setting, required when recreating the notification clients
    // using the new "reloaded" secure settings
    private volatile Settings cachedClusterSettings;
    // cached secure settings, required when recreating the notification clients
    // using the new updated cluster settings
    private volatile SecureSettings cachedSecureSettings;

    public NotificationService(String type, ClusterSettings clusterSettings, List<Setting<?>> pluginClusterSettings) {
        this.type = type;
        clusterSettings.addSettingsUpdateConsumer(this::clusterSettingsConsumer, pluginClusterSettings);
    }

    private synchronized void clusterSettingsConsumer(Settings settings) {
        // update cached cluster settings
        this.cachedClusterSettings = settings;
        // build new settings from the previously cached secure settings
        final Settings.Builder completeSettingsBuilder = Settings.builder().put(settings, false);
        if (cachedSecureSettings != null) {
            completeSettingsBuilder.setSecureSettings(cachedSecureSettings);
        }
        final Settings completeSettings = completeSettingsBuilder.build();
        // create new accounts from the latest
        final Set<String> accountNames = getAccountNames(completeSettings);
        this.accounts = createAccounts(completeSettings, accountNames, this::createAccount);
        this.defaultAccount = selectDefaultAccount(completeSettings, this.accounts);
    }

    // Used for testing only
    NotificationService(String type) {
        this.type = type;
    }

    public synchronized void reload(Settings settings) {
        // `SecureSettings` are available here!
        // cache the `SecureSettings`
        try {
            this.cachedSecureSettings = cacheSecureSettings(settings);
        } catch (GeneralSecurityException e) {
            logger.error("Keystore exception while reloading watcher notification service", e);
            return;
        }
        // build new settings from the previously cached cluster settings
        final Settings.Builder completeSettingsBuilder = Settings.builder().put(settings, true);
        if (cachedClusterSettings != null) {
            completeSettingsBuilder.put(cachedClusterSettings);
        }
        final Settings completeSettings = completeSettingsBuilder.build();
        // create new accounts from the latest
        final Set<String> accountNames = getAccountNames(completeSettings);
        this.accounts = createAccounts(completeSettings, accountNames, this::createAccount);
        this.defaultAccount = selectDefaultAccount(completeSettings, this.accounts);
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
        // secure settings don't account for the client names
        final Settings noSecureSettings = Settings.builder().put(settings, false).build();
        return noSecureSettings.getByPrefix(getNotificationsAccountPrefix()).names();
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

    private SecureSettings cacheSecureSettings(Settings source) throws GeneralSecurityException {
        // get the secure settings out
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        // cache them...
        final Map<String, SecureString> cache = new HashMap<>();
        for (final String settingKey : sourceSecureSettings.getSettingNames()) {
            cache.put(settingKey, sourceSecureSettings.getString(settingKey));
        }
        return new SecureSettings() {

            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) throws GeneralSecurityException {
                return cache.get(setting);
            }

            @Override
            public Set<String> getSettingNames() {
                return cache.keySet();
            }

            @Override
            public InputStream getFile(String setting) throws GeneralSecurityException {
                throw new IllegalStateException("A NotificationService setting cannot be File.");
            }

            @Override
            public void close() throws IOException {
            }
        };
    }
}
