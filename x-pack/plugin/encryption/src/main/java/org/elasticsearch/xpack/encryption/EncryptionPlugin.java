/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Plugin for the project encryption key (PEK) lifecycle. Wires up the in-memory key cache, the AES-GCM encryption
 * service, and the rotation coordinator. Loads {@link EncryptedDataHandlerProvider} contributions from other plugins via
 * {@link ExtensiblePlugin#loadExtensions(ExtensionLoader)}.
 */
public class EncryptionPlugin extends Plugin implements ExtensiblePlugin {

    private final Settings settings;
    private final List<EncryptedDataHandlerProvider> encryptedDataHandlerProviders = new ArrayList<>();

    public EncryptionPlugin(Settings settings) {
        this.settings = settings;
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
        ProjectEncryptionKeyService pekService = ProjectEncryptionKeyService.create(services.clusterService(), services.projectResolver());
        AesGcmEncryptionService encryptionService = new AesGcmEncryptionService(pekService);
        encryptedDataHandlerProviders.forEach(p -> p.onEncryptionServiceAvailable(encryptionService));
        List<EncryptedDataHandler<?>> handlers = encryptedDataHandlerProviders.stream().flatMap(p -> p.getHandlers().stream()).toList();
        KeyRotationCoordinator coordinator = KeyRotationCoordinator.create(
            services.clusterService(),
            services.threadPool(),
            services.projectResolver(),
            services.featureService(),
            encryptionService,
            handlers,
            settings
        );

        List<Object> components = new ArrayList<>();
        components.add(new PluginComponentBinding<>(EncryptionService.class, encryptionService));
        components.add(pekService);
        components.add(coordinator);
        return components;
    }

    @Override
    public List<Setting<?>> getSettings() {
        if (ProjectEncryptionKeyService.PROJECT_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING, KeyRotationCoordinator.CHECK_INTERVAL_SETTING);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Metadata.ProjectCustom.class,
                ProjectEncryptionKeyMetadata.TYPE,
                ProjectEncryptionKeyMetadata::new
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
                ProjectEncryptionKeyMetadata::fromXContent
            )
        );
    }
}
