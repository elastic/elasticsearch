/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.ssl.SslConfigurationLoader;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;

/**
 * Loads "reindex.ssl.*" configuration from Settings, and makes the applicable configuration (trust manager / key manager / hostname
 * verification / cipher-suites) available for reindex-from-remote.
 */
public class ReindexSslConfig {

    private static final Map<String, Setting<?>> SETTINGS = new HashMap<>();
    private static final Map<String, Setting<SecureString>> SECURE_SETTINGS = new HashMap<>();

    static {
        Setting.Property[] defaultProperties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Filtered };
        Setting.Property[] deprecatedProperties = new Setting.Property[] {
            Setting.Property.DeprecatedWarning,
            Setting.Property.NodeScope,
            Setting.Property.Filtered };
        for (String key : SslConfigurationKeys.getStringKeys()) {
            String settingName = "reindex.ssl." + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, simpleString(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getListKeys()) {
            String settingName = "reindex.ssl." + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, listSetting(settingName, Collections.emptyList(), Function.identity(), properties));
        }
        for (String key : SslConfigurationKeys.getSecureStringKeys()) {
            String settingName = "reindex.ssl." + key;
            SECURE_SETTINGS.put(settingName, SecureSetting.secureString(settingName, null));
        }
    }

    private final SslConfiguration configuration;
    private volatile SSLContext context;

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(SETTINGS.values());
        settings.addAll(SECURE_SETTINGS.values());
        return settings;
    }

    public ReindexSslConfig(Settings settings, Environment environment, ResourceWatcherService resourceWatcher) {
        final SslConfigurationLoader loader = new SslConfigurationLoader("reindex.ssl.") {
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
                final Setting<SecureString> setting = SECURE_SETTINGS.get(key);
                if (setting == null) {
                    throw new IllegalArgumentException("The secure setting [" + key + "] is not registered");
                }
                return setting.get(settings).getChars();
            }

            @Override
            protected List<String> getSettingAsList(String key) throws Exception {
                return settings.getAsList(key);
            }
        };
        configuration = loader.load(environment.configFile());
        reload();

        final FileChangesListener listener = new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileDeleted(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                ReindexSslConfig.this.reload();
            }
        };
        for (Path file : configuration.getDependentFiles()) {
            try {
                final FileWatcher watcher = new FileWatcher(file);
                watcher.addListener(listener);
                resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot watch file [" + file + "]", e);
            }
        }
    }

    private void reload() {
        this.context = configuration.createSslContext();
    }

    /**
     * Encapsulate the loaded SSL configuration as a HTTP-client {@link SSLIOSessionStrategy}.
     * The returned strategy is immutable, but successive calls will return different objects that may have different
     * configurations if the underlying key/certificate files are modified.
     */
    SSLIOSessionStrategy getStrategy() {
        final HostnameVerifier hostnameVerifier = configuration.verificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();
        final String[] protocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(Strings.EMPTY_ARRAY);
        return new SSLIOSessionStrategy(context, protocols, cipherSuites, hostnameVerifier);
    }
}
