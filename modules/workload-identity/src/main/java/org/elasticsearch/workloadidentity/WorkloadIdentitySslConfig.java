/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.common.settings.Setting.simpleString;
import static org.elasticsearch.common.settings.Setting.stringListSetting;

/**
 * Loads {@code workload_identity.ssl.*} configuration from {@link Settings} and exposes the
 * resulting {@link SSLIOSessionStrategy} for the Apache HC-based issuer client.
 *
 * <p>The underlying {@link SSLContext} is rebuilt in place when any of the files referenced by
 * the loaded {@link SslConfiguration} (see {@link SslConfiguration#getDependentFiles()}) change on
 * disk. File monitoring is delegated to {@link ResourceWatcherService}, which polls at the
 * frequency configured by the global {@code resource.reload.interval.high} setting (default 5s).
 * Operators can therefore rotate the workload-identity client certificate and key material by
 * overwriting the referenced files in place, without restarting the node, matching the behavior
 * of {@code xpack.security.*.ssl.*} (see {@code SSLConfigurationReloader}). The value-type
 * settings under {@code workload_identity.ssl.*} ({@code verification_mode}, {@code
 * supported_protocols}, {@code cipher_suites}, {@code client_authentication}) remain
 * {@link Setting.Property#NodeScope NodeScope} and still require a node restart, consistent with
 * {@code SSLService}.
 *
 * <p>Consumers wire up reload reactions via {@link #addReloadListener(Runnable)}. Listeners are
 * invoked on the {@link ResourceWatcherService} polling thread after the new {@link SSLContext}
 * has been published, and after every other listener has had a chance to observe the swap; a
 * listener that throws is logged and isolated so it cannot prevent later listeners from running.
 *
 * @see SslConfigurationLoader
 */
public final class WorkloadIdentitySslConfig {

    private static final Logger logger = LogManager.getLogger(WorkloadIdentitySslConfig.class);

    /** Setting prefix shared by all SSL configuration values for this module. */
    public static final String SETTING_PREFIX = WorkloadIdentityIssuerSettings.SETTING_PREFIX + "ssl.";

    private static final Map<String, Setting<?>> SETTINGS = new HashMap<>();
    private static final Map<String, Setting<SecureString>> SECURE_SETTINGS = new HashMap<>();

    static {
        Setting.Property[] defaultProperties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Filtered };
        Setting.Property[] deprecatedProperties = new Setting.Property[] {
            Setting.Property.DeprecatedWarning,
            Setting.Property.NodeScope,
            Setting.Property.Filtered };
        for (String key : SslConfigurationKeys.getStringKeys()) {
            String settingName = SETTING_PREFIX + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, simpleString(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getListKeys()) {
            String settingName = SETTING_PREFIX + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, stringListSetting(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getSecureStringKeys()) {
            String settingName = SETTING_PREFIX + key;
            SECURE_SETTINGS.put(settingName, SecureSetting.secureString(settingName, null));
        }
    }

    private final SslConfiguration configuration;
    /**
     * Reassigned in-place by {@link #reload()} when a watched file changes. Reads from
     * {@link #getStrategy()} always sees the most-recently-successfully-loaded context; readers
     * performing a single field read are sufficient because the strategy and engine derived from
     * a context capture it by reference.
     */
    private volatile SSLContext context;
    private final CopyOnWriteArrayList<Runnable> reloadListeners = new CopyOnWriteArrayList<>();

    /**
     * @return all SSL settings registered by this module. Combine with the rest of the module's
     *         settings via {@link WorkloadIdentityIssuerSettings#getSettings()} when registering
     *         with the plugin.
     */
    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(SETTINGS.values());
        settings.addAll(SECURE_SETTINGS.values());
        return settings;
    }

    public WorkloadIdentitySslConfig(Settings settings, Environment environment, ResourceWatcherService resourceWatcher) {
        final SslConfigurationLoader loader = new SslConfigurationLoader(SETTING_PREFIX) {
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
            protected List<String> getSettingAsList(String key) {
                return settings.getAsList(key);
            }
        };
        this.configuration = loader.load(environment.configDir());
        this.context = configuration.createSslContext();
        registerFileWatchers(resourceWatcher);
    }

    /**
     * Registers a {@link FileWatcher} at {@link ResourceWatcherService.Frequency#HIGH HIGH}
     * frequency for every path returned by {@link SslConfiguration#getDependentFiles()}. The
     * watcher list is fixed at construction time; changing <em>which</em> files are referenced
     * requires updating {@code workload_identity.ssl.*} and restarting the node (those settings
     * are {@link Setting.Property#NodeScope NodeScope}).
     */
    private void registerFileWatchers(ResourceWatcherService resourceWatcher) {
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
                WorkloadIdentitySslConfig.this.reload();
            }
        };
        for (Path file : configuration.getDependentFiles()) {
            try {
                final FileWatcher watcher = new FileWatcher(file);
                watcher.addListener(listener);
                resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot watch workload-identity SSL file [" + file + "]", e);
            }
        }
    }

    /**
     * Rebuild the {@link SSLContext} from the current contents of the watched files and republish
     * it for subsequent {@link #getStrategy()} callers, then notify reload listeners.
     */
    private void reload() {
        final SSLContext newContext;
        try {
            newContext = configuration.createSslContext();
        } catch (Exception e) {
            logger.warn("failed to reload workload-identity SSL context; continuing with the previously loaded material", e);
            return;
        }
        this.context = newContext;
        logger.info("reloaded workload-identity SSL context");
        for (Runnable l : reloadListeners) {
            try {
                l.run();
            } catch (Exception e) {
                // Isolate listener failures: one badly-behaved consumer must not prevent the
                // others from observing the swap, and the watcher thread must not propagate.
                logger.warn("workload-identity SSL reload listener threw", e);
            }
        }
    }

    /**
     * Subscribe to reload events. Listeners run on the {@link ResourceWatcherService} polling
     * thread, after the volatile {@link SSLContext} field has been swapped, so a listener that
     * calls {@link #getStrategy()} sees the new context. Listeners are not removable; the plugin
     * owns the {@code WorkloadIdentitySslConfig} for its lifetime so there is no de-registration
     * path to maintain.
     */
    public void addReloadListener(Runnable listener) {
        this.reloadListeners.add(listener);
    }

    /**
     * @return an {@link SSLIOSessionStrategy} for Apache HC's async client. A fresh strategy is
     *         returned each call over whatever {@link SSLContext} is currently published; callers
     *         that need to react to rotation should fetch a new strategy via a
     *         {@link #addReloadListener(Runnable) reload listener} rather than caching the
     *         returned object.
     */
    public SSLIOSessionStrategy getStrategy() {
        final HostnameVerifier hostnameVerifier = configuration.verificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();
        final String[] protocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(Strings.EMPTY_ARRAY);
        return new SSLIOSessionStrategy(context, protocols, cipherSuites, hostnameVerifier);
    }
}
