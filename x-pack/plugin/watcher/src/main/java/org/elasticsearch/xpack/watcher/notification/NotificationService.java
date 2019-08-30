/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.LazyInitializable;

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
    private final Logger logger;
    private final Settings bootSettings;
    private final List<Setting<?>> pluginSecureSettings;
    // all are guarded by this
    private volatile Map<String, LazyInitializable<Account, SettingsException>> accounts;
    private volatile LazyInitializable<Account, SettingsException> defaultAccount;
    // cached cluster setting, required when recreating the notification clients
    // using the new "reloaded" secure settings
    private volatile Settings cachedClusterSettings;
    // cached secure settings, required when recreating the notification clients
    // using the new updated cluster settings
    private volatile SecureSettings cachedSecureSettings;

    public NotificationService(String type, Settings settings, ClusterSettings clusterSettings, List<Setting<?>> pluginDynamicSettings,
            List<Setting<?>> pluginSecureSettings) {
        this(type, settings, pluginSecureSettings);
        // register a grand updater for the whole group, as settings are usable together
        clusterSettings.addSettingsUpdateConsumer(this::clusterSettingsConsumer, pluginDynamicSettings);
    }

    // Used for testing only
    NotificationService(String type, Settings settings, List<Setting<?>> pluginSecureSettings) {
        this.type = type;
        this.logger = LogManager.getLogger();
        this.bootSettings = settings;
        this.pluginSecureSettings = pluginSecureSettings;
    }

    protected synchronized void clusterSettingsConsumer(Settings settings) {
        // update cached cluster settings
        this.cachedClusterSettings = settings;
        // use these new dynamic cluster settings together with the previously cached
        // secure settings
        buildAccounts();
    }

    public synchronized void reload(Settings settings) {
        // `SecureSettings` are available here! cache them as they will be needed
        // whenever dynamic cluster settings change and we have to rebuild the accounts
        try {
            this.cachedSecureSettings = extractSecureSettings(settings, pluginSecureSettings);
        } catch (GeneralSecurityException e) {
            logger.error("Keystore exception while reloading watcher notification service", e);
            return;
        }
        // use these new secure settings together with the previously cached dynamic
        // cluster settings
        buildAccounts();
    }

    private void buildAccounts() {
        // build complete settings combining cluster and secure settings
        final Settings.Builder completeSettingsBuilder = Settings.builder().put(bootSettings, false);
        if (this.cachedClusterSettings != null) {
            completeSettingsBuilder.put(this.cachedClusterSettings, false);
        }
        if (this.cachedSecureSettings != null) {
            completeSettingsBuilder.setSecureSettings(this.cachedSecureSettings);
        }
        final Settings completeSettings = completeSettingsBuilder.build();
        // obtain account names and create accounts
        final Set<String> accountNames = getAccountNames(completeSettings);
        this.accounts = createAccounts(completeSettings, accountNames, (name, accountSettings) -> createAccount(name, accountSettings));
        this.defaultAccount = findDefaultAccountOrNull(completeSettings, this.accounts);
    }

    protected abstract Account createAccount(String name, Settings accountSettings);

    public Account getAccount(String name) {
        // note this is not final since we mock it in tests and that causes
        // trouble since final methods can't be mocked...
        final Map<String, LazyInitializable<Account, SettingsException>> accounts;
        final LazyInitializable<Account, SettingsException> defaultAccount;
        synchronized (this) { // must read under sync block otherwise it might be inconsistent
            accounts = this.accounts;
            defaultAccount = this.defaultAccount;
        }
        LazyInitializable<Account, SettingsException> theAccount = accounts.getOrDefault(name, defaultAccount);
        if (theAccount == null && name == null) {
            throw new IllegalArgumentException("no accounts of type [" + type + "] configured. " +
                    "Please set up an account using the [xpack.notification." + type +"] settings");
        }
        if (theAccount == null) {
            throw new IllegalArgumentException("no account found for name: [" + name + "]");
        }
        return theAccount.getOrCompute();
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

    private Map<String, LazyInitializable<Account, SettingsException>> createAccounts(Settings settings, Set<String> accountNames,
            BiFunction<String, Settings, Account> accountFactory) {
        final Map<String, LazyInitializable<Account, SettingsException>> accounts = new HashMap<>();
        for (final String accountName : accountNames) {
            final Settings accountSettings = settings.getAsSettings(getNotificationsAccountPrefix() + accountName);
            accounts.put(accountName, new LazyInitializable<>(() -> {
                return accountFactory.apply(accountName, accountSettings);
            }));
        }
        return Collections.unmodifiableMap(accounts);
    }

    private @Nullable LazyInitializable<Account, SettingsException> findDefaultAccountOrNull(Settings settings,
            Map<String, LazyInitializable<Account, SettingsException>> accounts) {
        final String defaultAccountName = getDefaultAccountName(settings);
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                return null;
            } else {
                return accounts.values().iterator().next();
            }
        } else {
            final LazyInitializable<Account, SettingsException> account = accounts.get(defaultAccountName);
            if (account == null) {
                throw new SettingsException("could not find default account [" + defaultAccountName + "]");
            }
            return account;
        }
    }

    /**
     * Extracts the {@link SecureSettings}` out of the passed in {@link Settings} object. The {@code Setting} argument has to have the
     * {@code SecureSettings} open/available. Normally {@code SecureSettings} are available only under specific callstacks (eg. during node
     * initialization or during a `reload` call). The returned copy can be reused freely as it will never be closed (this is a bit of
     * cheating, but it is necessary in this specific circumstance). Only works for secure settings of type string (not file).
     * 
     * @param source
     *            A {@code Settings} object with its {@code SecureSettings} open/available.
     * @param securePluginSettings
     *            The list of settings to copy.
     * @return A copy of the {@code SecureSettings} of the passed in {@code Settings} argument.
     */
    private static SecureSettings extractSecureSettings(Settings source, List<Setting<?>> securePluginSettings)
            throws GeneralSecurityException {
        // get the secure settings out
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        // filter and cache them...
        final Map<String, Tuple<SecureString, byte[]>> cache = new HashMap<>();
        if (sourceSecureSettings != null && securePluginSettings != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : securePluginSettings) {
                    if (secureSetting.match(settingKey)) {
                        cache.put(settingKey,
                                new Tuple<>(sourceSecureSettings.getString(settingKey), sourceSecureSettings.getSHA256Digest(settingKey)));
                    }
                }
            }
        }
        return new SecureSettings() {

            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) {
                return cache.get(setting).v1();
            }

            @Override
            public Set<String> getSettingNames() {
                return cache.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new IllegalStateException("A NotificationService setting cannot be File.");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return cache.get(setting).v2();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }
}
