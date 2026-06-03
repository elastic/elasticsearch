/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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
 * Plugin for the project encryption key (PEK) lifecycle. Wires up the in-memory key cache, the AES-GCM encryption
 * service, the rotation coordinator, and the health indicator. Loads {@link EncryptedDataHandlerProvider} contributions from other
 * plugins via {@link ExtensiblePlugin#loadExtensions(ExtensionLoader)}. Forwards secure-settings reloads to the components so the
 * password material is picked up on the next access.
 */
public class EncryptionPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, ReloadablePlugin, HealthPlugin {

    private final Settings settings;
    private final List<EncryptedDataHandlerProvider> encryptedDataHandlerProviders = new ArrayList<>();

    // Captured at createComponents time so we can route reload + health-indicator calls to them. Unset when the feature flag is off.
    private final SetOnce<ProjectEncryptionKeyService> pekService = new SetOnce<>();
    private final SetOnce<KeyRotationCoordinator> coordinator = new SetOnce<>();
    private final SetOnce<ProjectEncryptionKeyHealthIndicatorService> healthIndicatorService = new SetOnce<>();

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
        ProjectEncryptionKeyService pekService = ProjectEncryptionKeyService.create(
            services.clusterService(),
            services.projectResolver(),
            settings
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
            settings
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
        ProjectEncryptionKeyService localPekService = this.pekService.get();
        if (localPekService != null) {
            localPekService.reload(settings);
        }
        KeyRotationCoordinator localCoordinator = this.coordinator.get();
        if (localCoordinator != null) {
            localCoordinator.reload(settings);
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
