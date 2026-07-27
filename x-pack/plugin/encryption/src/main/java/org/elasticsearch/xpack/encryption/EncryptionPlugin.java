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
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

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

    private final List<EncryptedDataHandlerProvider> encryptedDataHandlerProviders = new ArrayList<>();
    private final SetOnce<ProjectEncryptionKeyService> pekService = new SetOnce<>();
    private final SetOnce<KeyRotationCoordinator> coordinator = new SetOnce<>();
    private final SetOnce<ProjectEncryptionKeyHealthIndicatorService> healthIndicatorService = new SetOnce<>();

    private volatile Settings pekSettings;

    private final ProjectEncryptionKeyMetadata.PekEncryption pekEncryption = new PasswordPekEncryption(() -> this.pekSettings);
    private final ProjectEncryptionKeyMetadata.DegradedBlobHolder degradedBlobHolder =
        new ProjectEncryptionKeyMetadata.DegradedBlobHolder();

    public EncryptionPlugin(Settings settings) {
        this.pekSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
        // Clear any service left in the registry by a previously constructed plugin instance (e.g. a prior node in the same test JVM),
        // so this node's createComponents publishes into a clean slot.
        EncryptionServiceRegistry.reset();
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        encryptedDataHandlerProviders.addAll(loader.loadExtensions(EncryptedDataHandlerProvider.class));
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ProjectEncryptionKeyService pekService = ProjectEncryptionKeyService.create(
            services.clusterService(),
            services.projectResolver(),
            () -> this.pekSettings
        );
        AesGcmEncryptionService encryptionService = new AesGcmEncryptionService(
            pekService,
            pekService::state,
            pekService::isEncryptionRequired
        );
        EncryptionServiceRegistry.setEncryptionService(encryptionService);
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
        List<Setting<?>> all = new ArrayList<>();
        all.add(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING);
        all.add(KeyRotationCoordinator.CHECK_INTERVAL_SETTING);
        all.addAll(ProjectEncryptionKeyPasswordSettings.getSettings());
        return all;
    }

    @Override
    public void reload(Settings settings) {
        this.pekSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
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
        return List.of(new ActionHandler(TransportEncryptionResetAction.TYPE, TransportEncryptionResetAction.class));
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestEncryptionResetAction(clusterSupportsFeature));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Metadata.ProjectCustom.class,
                ProjectEncryptionKeyMetadata.TYPE,
                in -> new ProjectEncryptionKeyMetadata(in, pekEncryption, degradedBlobHolder)
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
                parser -> ProjectEncryptionKeyMetadata.fromXContent(parser, pekEncryption, degradedBlobHolder)
            )
        );
    }
}
