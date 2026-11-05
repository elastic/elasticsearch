/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.allocator.ContinuousComputation;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Reloads {@link ReloadablePlugin}s with a merged view of every secure settings source on the node: reserved state
 * ({@link ClusterSecrets}) over API-set values ({@link SecureSettingsMetadata}) over the keystore. Plugins rebuild their clients from
 * the settings they are given, so a reload that left out a source would drop its values.
 * <p>Reloads triggered by cluster state changes and by {@code POST _nodes/reload_secure_settings} run one at a time. Cluster state
 * reloads run on the given executor, never on the cluster applier thread, and only the latest pending state is reloaded.
 * <p>The keystore is closed after node startup, so its values are copied when the node starts and replaced on each
 * {@code reload_secure_settings} call. As without the API, a change to the keystore file takes effect only through
 * {@code reload_secure_settings} or a restart.
 */
final class SecureSettingsReloader {

    private static final Logger logger = LogManager.getLogger(SecureSettingsReloader.class);

    /**
     * The cluster state sources of secure settings to reload with.
     */
    record Sources(@Nullable ClusterSecrets clusterSecrets, @Nullable SecureSettingsMetadata apiSettings) {

        @FixForMultiProject(description = "secure settings are node-level, but API-set values are stored in the default project")
        static Sources of(ClusterState state) {
            Metadata metadata = state.metadata();
            SecureSettingsMetadata apiSettings = metadata.hasProject(ProjectId.DEFAULT)
                ? metadata.getProject(ProjectId.DEFAULT).custom(SecureSettingsMetadata.TYPE)
                : null;
            return new Sources(state.custom(ClusterSecrets.TYPE), apiSettings);
        }
    }

    private final Settings nodeSettings;
    private final EncryptionService encryptionService;
    private final ContinuousComputation<Sources> clusterStateReloads;
    private final Object mutex = new Object();
    private volatile boolean retryNeeded;
    private ReloadablePlugin reloadCallback;
    // guarded by mutex
    private MergedSecureSettings.InMemory keystore;

    /**
     * @param keystore the node's keystore while it is still open during startup, or {@code null} if it has none
     */
    SecureSettingsReloader(
        Settings nodeSettings,
        @Nullable SecureSettings keystore,
        EncryptionService encryptionService,
        Executor executor
    ) {
        this.nodeSettings = Settings.builder().put(nodeSettings, false).build();
        this.encryptionService = encryptionService;
        this.clusterStateReloads = new ContinuousComputation<>(executor) {
            @Override
            protected void processInput(Sources sources) {
                reloadFromClusterState(sources);
            }
        };
        try {
            this.keystore = keystore == null ? MergedSecureSettings.InMemory.empty() : MergedSecureSettings.InMemory.copyOf(keystore);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException("failed to read the keystore", e);
        }
    }

    void setReloadCallback(ReloadablePlugin reloadCallback) {
        if (this.reloadCallback != null) {
            throw new IllegalStateException("Cannot set reload callback twice");
        }
        this.reloadCallback = reloadCallback;
    }

    /**
     * Whether the last reload could not decrypt the API-set values because encryption was unavailable, so a later cluster state
     * should reload again.
     */
    boolean retryNeeded() {
        return retryNeeded;
    }

    /**
     * Reloads with {@code sources} on the executor. If a reload is already pending, it reloads with these sources instead.
     */
    void reloadAsync(Sources sources) {
        assert reloadCallback != null : "reload callback has not been set";
        clusterStateReloads.onNewInput(sources);
    }

    /**
     * If the API-set values can't be decrypted, plugins are not reloaded, so they keep the values they have.
     */
    private void reloadFromClusterState(Sources sources) {
        synchronized (mutex) {
            Set<String> wiped = new HashSet<>();
            MergedSecureSettings.InMemory apiValues;
            try {
                apiValues = decrypt(sources.apiSettings(), wiped);
            } catch (RuntimeException e) {
                onDecryptionFailure(sources, e);
                return;
            }
            try (apiValues) {
                reloadCallback.reload(mergedSettings(sources, apiValues, wiped, keystore));
            } catch (Exception e) {
                logger.warn("Failed to reload secure settings from cluster state", e);
            }
        }
    }

    /**
     * Replaces the keystore copy with {@code newKeystore} and reloads {@code reloadablePlugins} with it and {@code sources}.
     * <p>If the API-set values can't be decrypted, plugins are still reloaded, without those values, and a cluster state reload is
     * queued to add them back. The new keystore may hold the password the encryption service needs to decrypt them, and plugins only
     * see it through this reload.
     */
    void reloadWithKeystore(SecureSettings newKeystore, Sources sources, ReloadablePlugin reloadablePlugins) throws Exception {
        MergedSecureSettings.InMemory copy = MergedSecureSettings.InMemory.copyOf(newKeystore);
        boolean decrypted = true;
        synchronized (mutex) {
            MergedSecureSettings.InMemory previous = keystore;
            keystore = copy;
            previous.close();
            Set<String> hidden = new HashSet<>();
            MergedSecureSettings.InMemory apiValues;
            try {
                apiValues = decrypt(sources.apiSettings(), hidden);
            } catch (RuntimeException e) {
                onDecryptionFailure(sources, e);
                decrypted = false;
                apiValues = MergedSecureSettings.InMemory.empty();
                hidden.addAll(sources.apiSettings().settings().keySet());
            }
            try (MergedSecureSettings.InMemory values = apiValues) {
                reloadablePlugins.reload(mergedSettings(sources, values, hidden, copy));
            }
        }
        if (decrypted == false && reloadCallback != null) {
            reloadAsync(sources);
        }
    }

    private void onDecryptionFailure(Sources sources, RuntimeException e) {
        Set<String> names = sources.apiSettings().settings().keySet();
        if (e instanceof EncryptionKeyNotYetAvailableException || e instanceof EncryptionServiceUnavailableException) {
            if (retryNeeded) {
                logger.debug("Still cannot decrypt secure settings {} set through the API: {}", names, e.getMessage());
            } else {
                logger.warn(
                    "Cannot decrypt secure settings {} set through the API, will retry on the next cluster state update: {}",
                    names,
                    e.getMessage()
                );
            }
            retryNeeded = true;
        } else {
            retryNeeded = false;
            logger.warn("Failed to decrypt secure settings {} set through the API", names, e);
        }
    }

    private MergedSecureSettings.InMemory decrypt(@Nullable SecureSettingsMetadata apiSettings, Set<String> wiped) {
        if (apiSettings == null) {
            retryNeeded = false;
            return MergedSecureSettings.InMemory.empty();
        }
        Map<String, byte[]> decrypted = new HashMap<>();
        try {
            for (Map.Entry<String, EncryptedData> entry : apiSettings.settings().entrySet()) {
                if (entry.getValue() == null) {
                    wiped.add(entry.getKey());
                } else {
                    decrypted.put(entry.getKey(), encryptionService.decrypt(entry.getValue()));
                }
            }
        } catch (RuntimeException e) {
            new MergedSecureSettings.InMemory(decrypted).close();
            throw e;
        }
        retryNeeded = false;
        return new MergedSecureSettings.InMemory(decrypted);
    }

    private Settings mergedSettings(Sources sources, SecureSettings apiValues, Set<String> hidden, SecureSettings keystoreValues) {
        List<SecureSettings> layers = new ArrayList<>();
        Set<String> hiddenNames = new HashSet<>(hidden);
        if (sources.clusterSecrets() != null) {
            SecureSettings reserved = sources.clusterSecrets().getSettings();
            layers.add(reserved);
            hiddenNames.removeAll(reserved.getSettingNames());
        }
        layers.add(apiValues);
        layers.add(keystoreValues);
        return Settings.builder().put(nodeSettings, false).setSecureSettings(new MergedSecureSettings(layers, hiddenNames)).build();
    }
}
