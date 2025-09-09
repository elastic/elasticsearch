/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.getDynamicSettings;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.getSecureSettings;

/**
 * Responsible for reloading a provided {@link CrossClusterApiKeySigner} when updates are received from the following sources:
 * - Dynamic cluster settings
 * - Reloadable secure settings
 * - File changes in any of the files pointed to by the cluster settings
 */
public final class CrossClusterApiKeySignerReloader implements ReloadableSecurityComponent {

    private static final Logger logger = LogManager.getLogger(CrossClusterApiKeySignerReloader.class);
    private final CrossClusterApiKeySigner apiKeySigner;
    private final Map<Path, ChangeListener> monitoredPathToChangeListener = new HashMap<>();

    public CrossClusterApiKeySignerReloader(
        ResourceWatcherService resourceWatcherService,
        ClusterSettings clusterSettings,
        CrossClusterApiKeySigner apiKeySigner
    ) {
        this.apiKeySigner = apiKeySigner;
        clusterSettings.addAffixGroupUpdateConsumer(getDynamicSettings(), (key, val) -> {
            apiKeySigner.loadSigningConfig(key, val.getByPrefix("cluster.remote." + key + "."), false);
            logger.info("Updated signing configuration for [{}] due to updated cluster settings", key);
            watchDependentFilesForClusterAliases(
                apiKeySigner::reloadSigningConfigs,
                resourceWatcherService,
                apiKeySigner.getDependentFilesToClusterAliases()
            );
        });

        watchDependentFilesForClusterAliases(
            apiKeySigner::reloadSigningConfigs,
            resourceWatcherService,
            apiKeySigner.getDependentFilesToClusterAliases()
        );
    }

    private void watchDependentFilesForClusterAliases(
        Consumer<Set<String>> reloadConsumer,
        ResourceWatcherService resourceWatcherService,
        Map<Path, Set<String>> dependentFilesToClusterAliases
    ) {
        dependentFilesToClusterAliases.forEach((path, clusterAliases) -> {
            var existingChangeListener = monitoredPathToChangeListener.get(path);
            if (existingChangeListener != null) {
                logger.trace("Found existing listener for file [{}], adding clusterAliases {}", path, clusterAliases);
                existingChangeListener.addClusterAliases(clusterAliases);
                return;
            }

            logger.trace("Adding listener for file [{}] for clusters {}", path, clusterAliases);

            ChangeListener changeListener = new ChangeListener(clusterAliases, path, reloadConsumer);
            FileWatcher fileWatcher = new FileWatcher(path);
            fileWatcher.addListener(changeListener);
            try {
                resourceWatcherService.add(fileWatcher, Frequency.HIGH);
                monitoredPathToChangeListener.put(path, changeListener);
            } catch (IOException | SecurityException e) {
                logger.error(Strings.format("failed to start watching file [%s]", path), e);
            }
        });
    }

    private record ChangeListener(Set<String> clusterAliases, Path file, Consumer<Set<String>> reloadConsumer)
        implements
            FileChangesListener {

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
                reloadConsumer.accept(this.clusterAliases);
                logger.info("Updated signing configuration for [{}] config(s) due to update of file [{}]", clusterAliases.size(), file);
            }
        }
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
                    apiKeySigner.loadSigningConfig(clusterAlias, settingsForCluster, true);
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
