/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandler;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.security.spi.encryption.EncryptionService;

import java.util.ArrayList;
import java.util.List;

/**
 * Wires up the primary encryption key (PEK) lifecycle components on plugin startup.
 */
public final class EncryptionComponents {

    private EncryptionComponents() {}

    /**
     * Returns the node-scoped settings exposed by the PEK lifecycle
     */
    public static List<Setting<?>> settings() {
        return List.of(KeyRotationCoordinator.ROTATION_INTERVAL_SETTING, KeyRotationCoordinator.CHECK_INTERVAL_SETTING);
    }

    /**
     * Creates the PEK service, encryption service, and rotation coordinator
     */
    public static List<Object> create(
        ClusterService clusterService,
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        FeatureService featureService,
        Settings settings,
        List<EncryptedDataHandlerProvider> providers
    ) {
        PrimaryEncryptionKeyService pekService = PrimaryEncryptionKeyService.create(clusterService, projectResolver);
        AesGcmEncryptionService encryptionService = new AesGcmEncryptionService(pekService);
        List<EncryptedDataHandler<?>> handlers = providers.stream().flatMap(p -> p.getHandlers().stream()).toList();
        KeyRotationCoordinator coordinator = KeyRotationCoordinator.create(
            clusterService,
            threadPool,
            projectResolver,
            featureService,
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
}
