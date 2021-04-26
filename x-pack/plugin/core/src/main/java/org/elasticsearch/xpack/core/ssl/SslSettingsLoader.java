/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslConfigurationLoader;
import org.elasticsearch.env.Environment;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A configuration loader for SSL Settings
 */
public class SslSettingsLoader extends SslConfigurationLoader {

    private final Settings settings;
    private final Map<String, Setting<SecureString>> secureSettings;

    public SslSettingsLoader(Settings settings, String settingPrefix) {
        super(settingPrefix);
        this.settings = settings;
        final SSLConfigurationSettings sslConfigurationSettings = settingPrefix == null ?
            SSLConfigurationSettings.withoutPrefix() : SSLConfigurationSettings.withPrefix(settingPrefix);
        this.secureSettings = sslConfigurationSettings.getSecureSettings()
            .stream()
            .collect(Collectors.toMap(Setting::getKey, Function.identity()));
        setDefaultClientAuth(SslClientAuthenticationMode.REQUIRED);
    }

    @Override
    protected boolean hasSettings(String prefix) {
        return settings.getAsSettings(prefix).isEmpty() == false;
    }

    @Override
    protected String getSettingAsString(String key) {
        return settings.get(key);
    }

    @Override
    protected char[] getSecureSetting(String key) {
        final Setting<SecureString> setting = secureSettings.get(key);
        if (setting == null) {
            throw new IllegalArgumentException("The secure setting [" + key + "] is not registered");
        }
        return setting.get(settings).getChars();
    }

    @Override
    protected List<String> getSettingAsList(String key) throws Exception {
        return settings.getAsList(key);
    }

    public SslConfiguration load(Environment env) {
        return load(env.configFile());
    }

    public static SslConfiguration load(Settings settings, String prefix, Environment env) {
        return new SslSettingsLoader(settings, prefix).load(env);
    }

}
