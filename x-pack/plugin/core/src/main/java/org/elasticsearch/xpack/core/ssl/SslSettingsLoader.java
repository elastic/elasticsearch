/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslConfigurationLoader;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A configuration loader for SSL Settings
 */
public class SslSettingsLoader extends SslConfigurationLoader {

    private final Settings settings;
    private final Map<String, Setting<? extends SecureString>> secureSettings;
    private final Map<String, Setting<?>> standardSettings;
    private final Map<String, Setting<?>> disabledSettings;

    public SslSettingsLoader(Settings settings, String settingPrefix, boolean acceptNonSecurePasswords) {
        super(settingPrefix);
        this.settings = settings;
        final SSLConfigurationSettings sslConfigurationSettings = settingPrefix == null
            ? SSLConfigurationSettings.withoutPrefix(acceptNonSecurePasswords)
            : SSLConfigurationSettings.withPrefix(settingPrefix, acceptNonSecurePasswords);
        this.secureSettings = mapOf(sslConfigurationSettings.getSecureSettings());
        this.standardSettings = mapOf(sslConfigurationSettings.getEnabledSettings());
        this.disabledSettings = mapOf(sslConfigurationSettings.getDisabledSettings());
        setDefaultClientAuth(SslClientAuthenticationMode.REQUIRED);
    }

    private static <T> Map<String, Setting<? extends T>> mapOf(List<Setting<? extends T>> settingList) {
        return settingList.stream().collect(Collectors.toMap(Setting::getKey, Function.identity()));
    }

    @Override
    protected boolean hasSettings(String prefix) {
        Settings group = settings;
        if (Strings.hasLength(prefix)) {
            if (prefix.endsWith(".")) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            group = settings.getAsSettings(prefix);
        }
        return group.isEmpty() == false;
    }

    @Override
    protected String getSettingAsString(String key) {
        checkSetting(key);
        final String val = settings.get(key);
        return val == null ? "" : val;
    }

    @Override
    protected List<String> getSettingAsList(String key) throws Exception {
        checkSetting(key);
        return settings.getAsList(key);
    }

    private void checkSetting(String key) {
        final Setting<?> setting = standardSettings.get(key);
        if (setting != null) {
            // This triggers deprecation warnings
            setting.get(settings);
        } else if (disabledSettings.containsKey(key) == false) {
            throw new SslConfigException(
                "The setting ["
                    + key
                    + "] is not supported, valid SSL settings are: ["
                    + Strings.collectionToCommaDelimitedString(standardSettings.keySet())
                    + "]"
            );
        }
    }

    @Override
    protected char[] getSecureSetting(String key) {
        final Setting<? extends SecureString> setting = secureSettings.get(key);
        if (setting == null) {
            throw new SslConfigException(
                "The secure setting ["
                    + key
                    + "] is not supported, valid secure SSL settings are: ["
                    + Strings.collectionToCommaDelimitedString(secureSettings.keySet())
                    + "]"
            );
        }
        return setting.exists(settings) ? setting.get(settings).getChars() : null;
    }

    @Override
    protected SslTrustConfig buildTrustConfig(Path basePath, SslVerificationMode verificationMode, SslKeyConfig keyConfig) {
        final SslTrustConfig trustConfig = super.buildTrustConfig(basePath, verificationMode, keyConfig);
        final Path trustRestrictions = super.resolvePath("trust_restrictions.path", basePath);
        if (trustRestrictions == null) {
            return trustConfig;
        }
        return new RestrictedTrustConfig(trustRestrictions, trustConfig);
    }

    public SslConfiguration load(Environment env) {
        return load(env.configFile());
    }

    public static SslConfiguration load(Settings settings, String prefix, Environment env) {
        return load(settings, prefix, env, null);
    }

    public static SslConfiguration load(
        Settings settings,
        String prefix,
        Environment env,
        @Nullable Function<KeyStore, KeyStore> keyStoreFilter
    ) {
        final SslSettingsLoader settingsLoader = new SslSettingsLoader(settings, prefix, true);
        settingsLoader.setKeyStoreFilter(keyStoreFilter);
        return settingsLoader.load(env);
    }

}
