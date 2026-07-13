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
import org.elasticsearch.watcher.WatcherHandle;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>Lifecycle: {@code INIT} (constructed) &rarr; {@code STARTED} ({@link #start()}) &rarr;
 * {@code CLOSED} ({@link #close()}). The constructor parses settings only;
 * {@link #addReloadListener(Runnable)} must be called before {@code start()}. {@code start()} is
 * valid only from {@code INIT} (a second call while {@code STARTED}, or a call from {@code CLOSED},
 * throws) and registers the watchers before the initial {@link SSLContext} load. {@code close()}
 * from {@code STARTED} unregisters the watchers and is terminal; {@code close()} from {@code INIT}
 * or {@code CLOSED} is a no-op that leaves the state unchanged.
 *
 * @see SslConfigurationLoader
 */
public final class WorkloadIdentitySslConfig implements Closeable {

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

    /** Lifecycle states; see the class Javadoc for transitions. */
    private enum State {
        /** Constructed; settings parsed but no watchers registered and no {@link SSLContext} published. */
        INIT,
        /** {@link #start()} has registered the watchers and published the initial {@link SSLContext}. */
        STARTED,
        /** {@link #close()} has been called; watchers unregistered. Terminal. */
        CLOSED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    /**
     * Reload callbacks, fixed before {@link #start()} (see {@link #addReloadListener(Runnable)}).
     * Written only on the setup thread, before {@code start()} registers the file watchers. The
     * initial publish runs on that same thread; a watcher-driven reload runs only after the watcher
     * is registered, and that registration with the {@link ResourceWatcherService} (added after the
     * listeners) establishes the happens-before that publishes them to the watcher thread. A plain
     * {@link ArrayList} therefore suffices.
     */
    private final List<Runnable> reloadListeners = new ArrayList<>();
    /**
     * Watcher handles retained so {@link #close()} can unregister them. Written by {@link #start()}
     * and read by {@link #close()}.
     */
    private final List<WatcherHandle<FileWatcher>> watcherHandles = new ArrayList<>();

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
     * from {@code INIT}; a second call, or a call after {@link #close()}, throws
     * {@link IllegalStateException}.
     */
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTED) == false) {
            throw new IllegalStateException("cannot start workload-identity SSL config in state [" + state.get() + "]");
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
                watcherHandles.add(resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH));
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
        if (state.get() == State.CLOSED) {
            return;
        }
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
     * Subscribe to reload events. Must be called before {@link #start()} (while in {@code INIT});
     * calling it later throws {@link IllegalStateException}. Fixing the set before the initial
     * publish lets every listener observe it and keeps the list lock-free. Listeners run after the
     * {@link SSLContext} swap &mdash; on the {@code start()} thread for the initial load, the
     * {@link ResourceWatcherService} polling thread thereafter &mdash; and are not removable.
     */
    public void addReloadListener(Runnable listener) {
        if (state.get() != State.INIT) {
            throw new IllegalStateException("cannot add a reload listener to workload-identity SSL config in state [" + state.get() + "]");
        }
        this.reloadListeners.add(listener);
    }

    /**
     * @return an {@link SSLIOSessionStrategy} over the currently-published {@link SSLContext}. A
     *         fresh strategy is returned each call; rotation-aware callers should re-fetch via
     *         {@link #addReloadListener(Runnable)} rather than cache the result. Remains usable
     *         after {@link #close()} (which only stops future reloads) over the last-published
     *         context.
     * @throws IllegalStateException if {@link #start()} has not yet completed.
     */
    public SSLIOSessionStrategy getStrategy() {
        if (state.get() == State.INIT) {
            throw new IllegalStateException("workload-identity SSL config has not been started");
        }
        final HostnameVerifier hostnameVerifier = configuration.verificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();
        final String[] protocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(Strings.EMPTY_ARRAY);
        return new SSLIOSessionStrategy(context, protocols, cipherSuites, hostnameVerifier);
    }

    // Visible for testing
    void reloadNow() {
        loadAndPublish(false);
    }

    /**
     * Unregisters file watchers registered by {@link #start()}. Only the {@code STARTED → CLOSED}
     * transition does work; calls from any other state silently return. {@link #getStrategy()}
     * remains usable over the last-published context.
     */
    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED) == false) {
            return;
        }
        for (WatcherHandle<FileWatcher> handle : watcherHandles) {
            handle.stop();
        }
        watcherHandles.clear();
    }
}
