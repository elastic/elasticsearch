/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.InMemoryClonedSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SETTINGS_PART_SIGNING;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.getDynamicSigningSettings;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.getDynamicTrustSettings;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.getSecureSettings;

/**
 * Responsible for reloading a provided {@link CrossClusterApiKeySignatureManager} when updates are received from the following
 * sources:
 * - Dynamic cluster settings
 * - Reloadable secure settings
 * - File changes in any of the files pointed to by the cluster settings
 */
public final class CrossClusterApiKeySigningConfigReloader implements ReloadableSecurityComponent {

    private static final Logger logger = LogManager.getLogger(CrossClusterApiKeySigningConfigReloader.class);
    private final Map<Path, ChangeListener> signingConfigPathsToChangeListener = new ConcurrentHashMap<>();
    private final ResourceWatcherService resourceWatcherService;
    private final AtomicReference<Settings> trustSettings = new AtomicReference<>();
    private final Map<String, Settings> signingSettingsByClusterAlias = new ConcurrentHashMap<>();

    private final PlainActionFuture<CrossClusterApiKeySignatureManager> apiKeySignatureManagerFuture = new PlainActionFuture<>() {
        @Override
        protected boolean blockingAllowed() {
            return true; // waits on the scheduler thread, once, and not for long
        }
    };

    public CrossClusterApiKeySigningConfigReloader(
        Environment environment,
        ResourceWatcherService resourceWatcherService,
        ClusterSettings clusterSettings
    ) {
        this.resourceWatcherService = resourceWatcherService;
        initTrustConfig(clusterSettings, environment);
        initSigningConfig(clusterSettings, environment);
    }

    public void setSigningConfigLoader(CrossClusterApiKeySignatureManager apiKeySignatureManager) {
        assert apiKeySignatureManagerFuture.isDone() == false : "apiKeySignatureManager already set";
        apiKeySignatureManagerFuture.onResponse(apiKeySignatureManager);
    }

    private void initTrustConfig(ClusterSettings clusterSettings, Environment environment) {
        var allRemoteClusterSettings = environment.settings().getByPrefix("cluster.remote.");
        trustSettings.set(allRemoteClusterSettings.getByPrefix(SETTINGS_PART_SIGNING + "."));
        var sslSettingsLoader = SslSettingsLoader.load(allRemoteClusterSettings, SETTINGS_PART_SIGNING + ".", environment);
        watchDependentFiles(new HashSet<>(sslSettingsLoader.getDependentFiles()));

        clusterSettings.addSettingsUpdateConsumer((val) -> {
            reloadConsumer(val.getByPrefix("cluster.remote."), false);
            logger.info("Updated trust configuration due to updated cluster settings");
        }, getDynamicTrustSettings(), val -> this.validateUpdate(val.getByPrefix("cluster.remote.")));

    }

    private void initSigningConfig(ClusterSettings clusterSettings, Environment environment) {
        var clusterSettingsByClusterAlias = environment.settings().getGroups("cluster.remote.", true);
        clusterSettingsByClusterAlias.forEach((clusterAlias, settingsForCluster) -> {
            var sslSettingsLoader = SslSettingsLoader.load(settingsForCluster, SETTINGS_PART_SIGNING + ".", environment);
            signingSettingsByClusterAlias.put(clusterAlias, settingsForCluster);
            watchDependentFiles(clusterAlias, new HashSet<>(sslSettingsLoader.getDependentFiles()));
        });

        clusterSettings.addAffixGroupUpdateConsumer(getDynamicSigningSettings(), (key, val) -> {
            reloadConsumer(key, val.getByPrefix("cluster.remote." + key + "."), false);
            logger.info("Updated signing configuration for [{}] due to updated cluster settings", key);
        }, (key, val) -> this.validateUpdate(val.getByPrefix("cluster.remote." + key + ".")));
    }

    private void validateUpdate(Settings settings) {
        try {
            var apiKeySignatureManager = apiKeySignatureManagerFuture.get();
            apiKeySignatureManager.validate(settings);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new ElasticsearchException("Failed to obtain crossClusterApiKeySigner", e);
        } catch (Exception e) {
            logger.debug(Strings.format("Failed to update signing settings [%s] due validation error [%s]", settings, e));
            throw e;
        }
    }

    private void reloadConsumer(@Nullable Settings settings, boolean updateSecureSettings) {
        reloadConsumer(null, settings, updateSecureSettings);
    }

    private void reloadConsumer(@Nullable String clusterAlias, @Nullable Settings settings, boolean updateSecureSettings) {
        try {
            var apiKeySignatureManager = apiKeySignatureManagerFuture.get();
            if (clusterAlias == null) {
                trustSettings.updateAndGet(
                    (currentSettings) -> this.reloadAndWatch(apiKeySignatureManager, currentSettings, settings, updateSecureSettings)
                );
            } else {
                signingSettingsByClusterAlias.compute(
                    clusterAlias,
                    (alias, currentSettings) -> this.reloadAndWatch(
                        alias,
                        apiKeySignatureManager,
                        currentSettings,
                        settings,
                        updateSecureSettings
                    )
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new ElasticsearchException("Failed to obtain apiKeySignatureManager", e);
        }
    }

    private Settings reloadAndWatch(
        CrossClusterApiKeySignatureManager signatureManager,
        Settings currentSettings,
        Settings newSettings,
        boolean updateSecureSettings
    ) {
        return reloadAndWatch(null, signatureManager, currentSettings, newSettings, updateSecureSettings);
    }

    private Settings reloadAndWatch(
        @Nullable String clusterAlias,
        CrossClusterApiKeySignatureManager signatureManager,
        Settings currentSettings,
        Settings newSettings,
        boolean updateSecureSettings
    ) {
        var effectiveSettings = buildEffectiveSettings(currentSettings, newSettings, updateSecureSettings);
        try {
            if (clusterAlias == null) {
                signatureManager.reload(effectiveSettings);
            } else {
                signatureManager.reload(clusterAlias, effectiveSettings);
            }
        } catch (IllegalStateException e) {
            logger.error(Strings.format("Failed to load trust config"), e);
        } finally {
            var configFiles = clusterAlias == null
                ? signatureManager.getDependentTrustFiles()
                : signatureManager.getDependentSigningFiles(clusterAlias);
            watchDependentFiles(clusterAlias, new HashSet<>(configFiles));
        }
        return effectiveSettings;
    }

    private void watchDependentFiles(Set<Path> filesToMonitor) {
        watchDependentFiles(null, filesToMonitor);
    }

    private void watchDependentFiles(@Nullable String clusterAlias, Set<Path> filesToMonitor) {
        filesToMonitor.forEach(path -> {
            signingConfigPathsToChangeListener.compute(path, (monitoredPath, existingChangeListener) -> {
                var changeListener = existingChangeListener != null ? existingChangeListener : new ChangeListener(path);
                changeListener.addChangeCallback(new ChangeCallback(clusterAlias, () -> this.reloadConsumer(clusterAlias, null, false)));

                FileWatcher fileWatcher = new FileWatcher(path);
                fileWatcher.addListener(changeListener);
                try {
                    resourceWatcherService.add(fileWatcher, Frequency.HIGH);
                    return changeListener;
                } catch (IOException | SecurityException e) {
                    logger.error(Strings.format("failed to start watching file [%s]", path), e);
                }
                return changeListener;
            });
        });
    }

    private record ChangeCallback(@Nullable String clusterAlias, Runnable callback) {}

    private record ChangeListener(Path file, List<ChangeCallback> changeCallbacks) implements FileChangesListener {
        ChangeListener(Path file) {
            this(file, new ArrayList<>()); // delegate to canonical
        }

        public void addChangeCallback(ChangeCallback changeCallback) {
            if (changeCallbacks.stream()
                .anyMatch(existingChangeCallback -> Objects.equals(existingChangeCallback.clusterAlias, changeCallback.clusterAlias))) {
                return;
            }
            this.changeCallbacks.add(changeCallback);
        }

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
            if (this.file.equals(file)) {
                this.changeCallbacks.forEach(callback -> callback.callback.run());
                logger.info("Updated signing configuration due to update of file [{}]", file);
            }
        }
    }

    /**
     * Build the effective remote cluster settings by merging the currently configured (if any) and new/updated settings
     * <p>
     * - If newSettings is null - use existing settings, used to refresh the dependent files
     * - If newSettings is empty - return empty settings, used for resetting signing config
     * - If updateSecureSettings is true - merge secure settings from newSettings with current settings, used by secure settings refresh
     * - If updateSecureSettings is false - merge new settings with existing secure settings, used for regular settings update
     */
    private Settings buildEffectiveSettings(
        @Nullable Settings currentSettings,
        @Nullable Settings newSettings,
        boolean updateSecureSettings
    ) {
        if (currentSettings == null) {
            return newSettings == null ? Settings.EMPTY : newSettings;
        }
        if (newSettings == null) {
            return currentSettings;
        }
        if (newSettings.isEmpty()) {
            return Settings.EMPTY;
        }

        Settings secureSettingsSource = updateSecureSettings ? newSettings : currentSettings;
        Settings settingsSource = updateSecureSettings ? currentSettings : newSettings;
        SecureSettings secureSettings = Settings.builder().put(secureSettingsSource, true).getSecureSettings();

        var builder = Settings.builder().put(settingsSource, false);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return builder.build();
    }

    @Override
    public void reload(Settings settings) {
        try {
            // The secure settings provided to reload are only available in the scope of this method call since after that the keystore is
            // closed. Since the secure settings will potentially be used later when the signing config is used to sign headers, the
            // settings need to be retrieved from the keystore and cached
            Settings cachedSettings = Settings.builder()
                .setSecureSettings(InMemoryClonedSecureSettings.cloneSecureSettings(settings, getSecureSettings()))
                .build();
            cachedSettings.getGroups("cluster.remote.", true).forEach((clusterAlias, settingsForCluster) -> {
                if (settingsForCluster.getByPrefix(SETTINGS_PART_SIGNING).isEmpty() == false) {
                    reloadConsumer(clusterAlias, settingsForCluster, true);
                    logger.info("Updated signing configuration for [{}] due to reload of secure settings", clusterAlias);
                }
            });

            if (cachedSettings.getByPrefix("cluster.remote." + SETTINGS_PART_SIGNING + ".").isEmpty() == false) {
                reloadConsumer(cachedSettings.getByPrefix("cluster.remote."), true);
                logger.info("Updated trust configuration for cluster due to reload of secure settings");
            }
        } catch (GeneralSecurityException e) {
            logger.error("Keystore exception while reloading signing configuration after reload of secure settings", e);
        }
    }
}
