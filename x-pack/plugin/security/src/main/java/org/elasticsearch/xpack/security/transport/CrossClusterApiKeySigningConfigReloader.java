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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SETTINGS_PART_SIGNING;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.getDynamicSettings;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.getSecureSettings;

/**
 * Responsible for reloading a provided {@link CrossClusterApiKeySigner} when updates are received from the following sources:
 * - Dynamic cluster settings
 * - Reloadable secure settings
 * - File changes in any of the files pointed to by the cluster settings
 */
public final class CrossClusterApiKeySigningConfigReloader implements ReloadableSecurityComponent {

    private static final Logger logger = LogManager.getLogger(CrossClusterApiKeySigningConfigReloader.class);
    private final Map<Path, ChangeListener> monitoredPathToChangeListener = new ConcurrentHashMap<>();
    private final ResourceWatcherService resourceWatcherService;
    private final Map<String, Settings> settingsByClusterAlias = new ConcurrentHashMap<>();

    private final PlainActionFuture<CrossClusterApiKeySigner> crossClusterApiKeySignerFuture = new PlainActionFuture<>() {
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
        settingsByClusterAlias.putAll(environment.settings().getGroups("cluster.remote.", true));
        watchDependentFilesForClusterAliases(resourceWatcherService, getInitialFilesToMonitor(environment));
        clusterSettings.addAffixGroupUpdateConsumer(getDynamicSettings(), (key, val) -> {
            reloadConsumer(key, val.getByPrefix("cluster.remote." + key + "."), false);
            logger.info("Updated signing configuration for [{}] due to updated cluster settings", key);
        });
    }

    private void reloadConsumer(String clusterAlias, @Nullable Settings settings, boolean updateSecureSettings) {
        try {
            var apiKeySigner = crossClusterApiKeySignerFuture.get();
            settingsByClusterAlias.compute(clusterAlias, (key, val) -> {
                var effectiveSettings = buildEffectiveSettings(val, settings, updateSecureSettings);
                try {
                    var signingConfig = apiKeySigner.loadSigningConfig(clusterAlias, effectiveSettings);
                    if (signingConfig != null) {
                        watchDependentFilesForClusterAliases(
                            resourceWatcherService,
                            signingConfig.dependentFiles().stream().collect(Collectors.toMap(file -> file, (file) -> Set.of(clusterAlias)))
                        );
                    }
                } catch (IllegalStateException e) {
                    logger.error(Strings.format("Failed to load signing config for cluster [%s]", clusterAlias), e);
                }
                return effectiveSettings;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new ElasticsearchException("Failed to obtain crossClusterApiKeySigner", e);
        }
    }

    public void setApiKeySigner(CrossClusterApiKeySigner apiKeySigner) {
        assert crossClusterApiKeySignerFuture.isDone() == false : "apiKeySigner already set";
        crossClusterApiKeySignerFuture.onResponse(apiKeySigner);
    }

    private Map<Path, Set<String>> getInitialFilesToMonitor(Environment environment) {
        Map<Path, Set<String>> filesToMonitor = new HashMap<>();
        this.settingsByClusterAlias.forEach((clusterAlias, settingsForCluster) -> {
            SslKeyConfig keyConfig = CertParsingUtils.createKeyConfig(settingsForCluster, SETTINGS_PART_SIGNING + ".", environment, false);
            for (Path path : keyConfig.getDependentFiles()) {
                filesToMonitor.compute(
                    path,
                    (p, aliases) -> aliases == null ? Set.of(clusterAlias) : Sets.addToCopy(aliases, clusterAlias)
                );
            }
        });
        return filesToMonitor;
    }

    private void watchDependentFilesForClusterAliases(
        ResourceWatcherService resourceWatcherService,
        Map<Path, Set<String>> dependentFilesToClusterAliases
    ) {
        dependentFilesToClusterAliases.forEach((path, clusterAliases) -> {
            monitoredPathToChangeListener.compute(path, (monitoredPath, existingChangeListener) -> {
                if (existingChangeListener != null) {
                    logger.trace("Found existing listener for file [{}], adding clusterAliases {}", path, clusterAliases);
                    existingChangeListener.addClusterAliases(clusterAliases);
                    return existingChangeListener;
                }

                logger.trace("Adding listener for file [{}] for clusters {}", path, clusterAliases);
                ChangeListener changeListener = new ChangeListener(
                    new HashSet<>(clusterAliases),
                    path,
                    (clusterAlias) -> this.reloadConsumer(clusterAlias, null, false)
                );
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

    private record ChangeListener(Set<String> clusterAliases, Path file, Consumer<String> reloadConsumer) implements FileChangesListener {
        public void addClusterAliases(Set<String> clusterAliases) {
            this.clusterAliases.addAll(clusterAliases);
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
                this.clusterAliases.forEach(reloadConsumer);
                logger.info("Updated signing configuration for [{}] config(s) due to update of file [{}]", clusterAliases.size(), file);
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
            Settings cachedSettings = Settings.builder().setSecureSettings(extractSecureSettings(settings, getSecureSettings())).build();
            cachedSettings.getGroups("cluster.remote.", true).forEach((clusterAlias, settingsForCluster) -> {
                // Only update signing config if settings were found, since empty config means config deletion
                if (settingsForCluster.isEmpty() == false) {
                    reloadConsumer(clusterAlias, settingsForCluster, true);
                    logger.info("Updated signing configuration for [{}] due to reload of secure settings", clusterAlias);
                }
            });
        } catch (GeneralSecurityException e) {
            logger.error("Keystore exception while reloading signing configuration after reload of secure settings", e);
        }
    }

    /**
     * Extracts the {@link SecureSettings}` out of the passed in {@link Settings} object. The {@code Setting} argument has to have the
     * {@code SecureSettings} open/available. Normally {@code SecureSettings} are available only under specific callstacks (eg. during node
     * initialization or during a `reload` call). The returned copy can be reused freely as it will never be closed (this is a bit of
     * cheating, but it is necessary in this specific circumstance). Only works for secure settings of type string (not file).
     *
     * @param source               A {@code Settings} object with its {@code SecureSettings} open/available.
     * @param settingsToCopy The list of settings to copy.
     * @return A copy of the {@code SecureSettings} of the passed in {@code Settings} argument.
     */
    private static SecureSettings extractSecureSettings(Settings source, List<Setting.AffixSetting<?>> settingsToCopy)
        throws GeneralSecurityException {
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        final Map<String, SecureSettingValue> copiedSettings = new HashMap<>();

        if (sourceSecureSettings != null && settingsToCopy != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : settingsToCopy) {
                    if (secureSetting.match(settingKey)) {
                        copiedSettings.put(
                            settingKey,
                            new SecureSettingValue(
                                sourceSecureSettings.getString(settingKey),
                                sourceSecureSettings.getSHA256Digest(settingKey)
                            )
                        );
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
                return copiedSettings.get(setting).value();
            }

            @Override
            public Set<String> getSettingNames() {
                return copiedSettings.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new UnsupportedOperationException("A cached SecureSetting cannot be a file");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return copiedSettings.get(setting).sha256Digest();
            }

            @Override
            public void close() throws IOException {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new UnsupportedOperationException("A cached SecureSetting cannot be serialized");
            }
        };
    }

    private record SecureSettingValue(SecureString value, byte[] sha256Digest) {}

}
