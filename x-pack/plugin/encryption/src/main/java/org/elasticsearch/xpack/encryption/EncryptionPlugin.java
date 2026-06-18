/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Plugin for the project encryption key (PEK) lifecycle. Wires up the key cache, rotation coordinator, AES-GCM encryption service, and
 * health indicator. Loads {@link EncryptedDataHandlerProvider} contributions from other plugins and forwards secure-settings reloads.
 */
public class EncryptionPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, ReloadablePlugin, HealthPlugin {

    private final Settings settings;
    private final List<EncryptedDataHandlerProvider> encryptedDataHandlerProviders = new ArrayList<>();

    private final SetOnce<ProjectEncryptionKeyService> pekService = new SetOnce<>();
    private final SetOnce<KeyRotationCoordinator> coordinator = new SetOnce<>();
    private final SetOnce<ProjectEncryptionKeyHealthIndicatorService> healthIndicatorService = new SetOnce<>();

    private volatile Settings pekSettings;

    private final ProjectEncryptionKeyMetadata.PekEncryption pekEncryption = new ProjectEncryptionKeyMetadata.PekEncryption() {
        @Override
        public byte[] wrap(byte[] plaintextPek, String passwordId) {
            try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(pekSettings, passwordId)) {
                if (password == null) {
                    throw new ElasticsearchException(
                        "cannot wrap PEK for disk: secure setting ["
                            + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                            + passwordId
                            + "] is not configured"
                    );
                }
                return PasswordBasedEncryption.wrap(plaintextPek, passwordId, password.getChars()).payload();
            }
        }

        @Override
        public byte[] unwrap(byte[] wrappedPek, String passwordId) {
            try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(pekSettings, passwordId)) {
                if (password == null) {
                    throw new ElasticsearchException(
                        "cannot unwrap PEK from disk: secure setting ["
                            + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                            + passwordId
                            + "] is not configured"
                    );
                }
                return PasswordBasedEncryption.unwrap(new EncryptedData(passwordId, wrappedPek), password.getChars());
            }
        }
    };

    public EncryptionPlugin(Settings settings) {
        this.settings = settings;
        this.pekSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings, settings);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return;
        }
        encryptedDataHandlerProviders.addAll(loader.loadExtensions(EncryptedDataHandlerProvider.class));
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        ProjectEncryptionKeyService pekService = ProjectEncryptionKeyService.create(
            services.clusterService(),
            services.projectResolver(),
            () -> this.pekSettings
        );
        AesGcmEncryptionService encryptionService = new AesGcmEncryptionService(pekService);
        List<EncryptedDataHandler<?>> handlers = encryptedDataHandlerProviders.stream().flatMap(p -> p.getHandlers().stream()).toList();
        EncryptedDataHandlerRegistry handlerRegistry = new EncryptedDataHandlerRegistry(handlers);
        KeyRotationCoordinator coordinator = KeyRotationCoordinator.create(
            services.clusterService(),
            services.threadPool(),
            services.projectResolver(),
            services.featureService(),
            encryptionService,
            handlers,
            () -> this.pekSettings,
            pekEncryption
        );
        ProjectEncryptionKeyHealthIndicatorService healthIndicator = new ProjectEncryptionKeyHealthIndicatorService(
            services.clusterService(),
            services.projectResolver(),
            pekService
        );

        this.pekService.set(pekService);
        this.coordinator.set(coordinator);
        this.healthIndicatorService.set(healthIndicator);

        List<Object> components = new ArrayList<>();
        components.add(new PluginComponentBinding<>(EncryptionService.class, encryptionService));
        components.add(pekService);
        components.add(coordinator);
        components.add(healthIndicator);
        components.add(handlerRegistry);
        return components;
    }

    @Override
    public List<Setting<?>> getSettings() {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        List<Setting<?>> all = new ArrayList<>();
        all.add(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING);
        all.add(KeyRotationCoordinator.CHECK_INTERVAL_SETTING);
        all.addAll(ProjectEncryptionKeyPasswordSettings.getSettings());
        return all;
    }

    @Override
    public void reload(Settings settings) {
        this.pekSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(this.settings, settings);
        KeyRotationCoordinator localCoordinator = this.coordinator.get();
        if (localCoordinator != null) {
            localCoordinator.reload();
        }
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        ProjectEncryptionKeyHealthIndicatorService local = this.healthIndicatorService.get();
        return local == null ? List.of() : List.of(local);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(this.coordinator.get(), this.pekService.get());
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(new ActionHandler(TransportEncryptionResetAction.TYPE, TransportEncryptionResetAction.class));
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(new RestEncryptionResetAction(clusterSupportsFeature));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Metadata.ProjectCustom.class,
                ProjectEncryptionKeyMetadata.TYPE,
                in -> new ProjectEncryptionKeyMetadata(in, pekEncryption)
            ),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ProjectEncryptionKeyMetadata.TYPE, ProjectEncryptionKeyMetadata::readDiffFrom)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(ProjectEncryptionKeyMetadata.TYPE),
                parser -> ProjectEncryptionKeyMetadata.fromXContent(parser, pekEncryption)
            )
        );
    }
}
