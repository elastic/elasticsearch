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
import java.util.concurrent.atomic.AtomicBoolean;

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
 * has been published; a listener that throws is logged and isolated.
 *
 * <p>Lifecycle: construct &rarr; {@link #addReloadListener(Runnable) addReloadListener} &rarr;
 * {@link #start()}. The constructor parses settings only. {@code start()} is one-shot (a second
 * call throws); it registers the file watchers <em>before</em> loading the initial
 * {@link SSLContext}, so a concurrent cert change is either reflected in the initial load or
 * detected as a diff against the watcher baseline on the next poll.
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
    private final ResourceWatcherService resourceWatcher;
    /**
     * Reassigned in-place by {@link #loadAndPublish(boolean)}; {@code null} until {@link #start()}
     * runs. A single volatile read is sufficient because consumers capture the context by
     * reference into the strategy they hold.
     */
    private volatile SSLContext context;
    private final AtomicBoolean started = new AtomicBoolean(false);
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
        this.resourceWatcher = resourceWatcher;
    }

    /**
     * Register file watchers, then load and publish the initial {@link SSLContext}. Valid only
     * once; a second call throws {@link IllegalStateException}.
     */
    public void start() {
        if (started.compareAndSet(false, true) == false) {
            throw new IllegalStateException("workload-identity SSL config has already been started");
        }
        registerFileWatchers();
        loadAndPublish(true);
    }

    /**
     * Registers a {@link FileWatcher} at {@link ResourceWatcherService.Frequency#HIGH HIGH}
     * frequency for every path returned by {@link SslConfiguration#getDependentFiles()}. The
     * watcher list is fixed at construction time; changing <em>which</em> files are referenced
     * requires updating {@code workload_identity.ssl.*} and restarting the node (those settings
     * are {@link Setting.Property#NodeScope NodeScope}).
     */
    private void registerFileWatchers() {
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
                WorkloadIdentitySslConfig.this.loadAndPublish(false);
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
     * Rebuild the {@link SSLContext} from the current contents of the watched files, republish
     * it, and notify reload listeners.
     *
     * @param initial {@code true} for the {@link #start()}-driven load (propagate failures so a
     *                bad initial configuration fails the node); {@code false} for watcher-driven
     *                reloads (log {@code WARN} and retain the previous context).
     */
    private void loadAndPublish(boolean initial) {
        final SSLContext newContext;
        try {
            newContext = configuration.createSslContext();
        } catch (RuntimeException e) {
            if (initial) {
                throw e;
            }
            logger.warn("failed to reload workload-identity SSL context; continuing with the previously loaded material", e);
            return;
        }
        this.context = newContext;
        logger.debug("loaded workload-identity SSL context");
        for (Runnable l : reloadListeners) {
            try {
                l.run();
            } catch (Exception e) {
                // Isolate listener failures so one consumer cannot block others or the watcher thread.
                logger.warn("workload-identity SSL reload listener threw", e);
            }
        }
    }

    /**
     * Subscribe to reload events. Listeners run after the volatile {@link SSLContext} field has
     * been swapped, on the {@link ResourceWatcherService} polling thread or, for the initial
     * load, on the {@link #start()} thread. Listeners registered before {@code start()} are
     * invoked once during it; listeners registered later miss the initial publish. Listeners
     * are not removable; the plugin owns this object for its lifetime.
     */
    public void addReloadListener(Runnable listener) {
        this.reloadListeners.add(listener);
    }

    /**
     * @return an {@link SSLIOSessionStrategy} over the currently-published {@link SSLContext}. A
     *         fresh strategy is returned each call; rotation-aware callers should re-fetch via
     *         {@link #addReloadListener(Runnable)} rather than cache the result.
     * @throws IllegalStateException if {@link #start()} has not yet completed.
     */
    public SSLIOSessionStrategy getStrategy() {
        if (started.get() == false) {
            throw new IllegalStateException("workload-identity SSL config has not been started");
        }
        final HostnameVerifier hostnameVerifier = configuration.verificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();
        final String[] protocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(Strings.EMPTY_ARRAY);
        return new SSLIOSessionStrategy(context, protocols, cipherSuites, hostnameVerifier);
    }
}
