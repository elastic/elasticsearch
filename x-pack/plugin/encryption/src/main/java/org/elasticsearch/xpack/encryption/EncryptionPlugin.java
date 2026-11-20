/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
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
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Plugin for the project encryption key (PEK) lifecycle. Wires up the key service, rotation coordinator, AES-GCM encryption service, and
 * health indicator. Loads {@link EncryptedDataHandlerProvider} contributions from other plugins and forwards secure-settings reloads.
 */
public class EncryptionPlugin extends Plugin
    implements
        ActionPlugin,
        ExtensiblePlugin,
        ReloadablePlugin,
        HealthPlugin,
        ClusterCoordinationPlugin {

    private final List<EncryptedDataHandlerProvider> encryptedDataHandlerProviders = new ArrayList<>();
    private final SetOnce<KeyRotationCoordinator> coordinator = new SetOnce<>();
    private final SetOnce<ProjectEncryptionKeyHealthIndicatorService> healthIndicatorService = new SetOnce<>();
    private final SetOnce<SnapshotEncryptedDataFilter> snapshotEncryptedDataFilter = new SetOnce<>();
    private final SetOnce<ProjectEncryptionKeyService> pekService = new SetOnce<>();
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();

    private final boolean stateless;
    private final boolean persistsClusterState;
    private volatile Settings pekSettings;

    private final PasswordPekEncryption pekEncryption = new PasswordPekEncryption(() -> this.pekSettings);

    public EncryptionPlugin(Settings settings) {
        this.stateless = DiscoveryNode.isStateless(settings);
        this.persistsClusterState = PasswordPekEncryption.persistsClusterState(DiscoveryNode.getRolesFromSettings(settings), stateless);
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
        EncryptedDataHandlerRegistry.setInstance(handlerRegistry);
        snapshotEncryptedDataFilter.set(
            new SnapshotEncryptedDataFilter(services.clusterService(), services.projectResolver(), handlerRegistry, pekService::state)
        );
        KeyRotationCoordinator coordinator = KeyRotationCoordinator.create(
            services.clusterService(),
            services.threadPool(),
            services.projectResolver(),
            services.featureService(),
            encryptionService,
            handlers,
            () -> this.pekSettings,
            pekEncryption,
            services.client()
        );
        ProjectEncryptionKeyHealthIndicatorService healthIndicator = new ProjectEncryptionKeyHealthIndicatorService(
            services.clusterService(),
            services.projectResolver(),
            pekService,
            pekEncryption,
            handlerRegistry,
            coordinator::nodesWithoutPassword,
            persistsClusterState
        );

        this.coordinator.set(coordinator);
        this.healthIndicatorService.set(healthIndicator);
        this.pekService.set(pekService);
        this.clusterService.set(services.clusterService());

        List<Object> components = new ArrayList<>();
        components.add(new PluginComponentBinding<>(EncryptionService.class, encryptionService));
        components.add(pekService);
        components.add(pekEncryption);
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

    // synchronized so concurrent reloads can't each validate against the same stale settings
    @Override
    public synchronized void reload(Settings settings) {
        Settings updated = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
        if (shouldValidateReload()) {
            ProjectEncryptionKeyPasswordSettings.validateReload(
                pekEncryption.canWrapForGateway(),
                pekSettings,
                updated,
                pekEncryption::canReadGatewayKeysWith,
                pekEncryption.gatewayKeys().passwordId()
            );
        }
        this.pekSettings = updated;
        KeyRotationCoordinator localCoordinator = this.coordinator.get();
        if (localCoordinator != null) {
            localCoordinator.reload();
        }
    }

    // while the cluster has a key, including while the elected master holds none, since the key can still be restored
    private boolean shouldValidateReload() {
        if (persistsClusterState == false) {
            return false;
        }
        ProjectEncryptionKeyService service = pekService.get();
        ClusterService localClusterService = clusterService.get();
        if (service == null || localClusterService == null) {
            return false;
        }
        ClusterState state = localClusterService.state();
        return PasswordPekEncryption.passwordsFromClusterSecrets(state, stateless) == false && service.metadataIn(state) != null;
    }

    /**
     * Rejects this node joining a cluster that uses a cluster state encryption key while the node cannot persist it, since it could
     * not persist any cluster state. While the elected master holds no key, nodes may join so that the cluster can form and recover.
     */
    @Override
    public BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
        return persistsClusterState ? this::rejectIfThisNodeCannotPersistKey : null;
    }

    private void rejectIfThisNodeCannotPersistKey(DiscoveryNode node, ClusterState state) {
        ClusterService localClusterService = clusterService.get();
        ProjectEncryptionKeyService service = pekService.get();
        // the master also runs join validators, for the joining node, which this node's settings say nothing about
        if (localClusterService == null || service == null || node.getId().equals(localClusterService.localNode().getId()) == false) {
            return;
        }
        // the node gets the password from cluster secrets once it joins, whatever it loaded at startup
        if (PasswordPekEncryption.passwordsFromClusterSecrets(state, stateless)) {
            return;
        }
        ProjectEncryptionKeyMetadata metadata = service.metadataIn(state);
        // no key to persist: none created yet, or the elected master holds none and the cluster must still form
        if (metadata == null || metadata.isUnwrapFailed()) {
            return;
        }
        String wrapFailure = pekEncryption.wrapForGatewayFailure();
        if (wrapFailure != null) {
            throw new IllegalStateException(
                "this node cannot join the cluster: the cluster uses a cluster state encryption key and this node cannot persist it ("
                    + wrapFailure
                    + "). Configure ["
                    + ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY
                    + "] and the matching ["
                    + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                    + "<id>] secure setting in this node's keystore, then restart it or reload its secure settings"
            );
        }
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        ProjectEncryptionKeyHealthIndicatorService local = this.healthIndicatorService.get();
        return local == null ? List.of() : List.of(local);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(this.coordinator.get());
    }

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(TransportEncryptionResetAction.TYPE, TransportEncryptionResetAction.class),
            new ActionHandler(TransportNodesCanPersistKeyAction.TYPE, TransportNodesCanPersistKeyAction.class)
        );
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return singletonList(snapshotEncryptedDataFilter.get());
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
