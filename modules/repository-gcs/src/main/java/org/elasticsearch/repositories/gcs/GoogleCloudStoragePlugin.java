/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleCloudStoragePlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private final Settings settings;
    // package-private for tests
    final SetOnce<GoogleCloudStorageService> storageService = new SetOnce<>();

    public GoogleCloudStoragePlugin(final Settings settings) {
        this.settings = settings;
    }

    // overridable for tests
    protected GoogleCloudStorageService createStorageService(ClusterService clusterService, ProjectResolver projectResolver) {
        return new GoogleCloudStorageService(clusterService, projectResolver);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics,
        SnapshotMetrics snapshotMetrics
    ) {
        return Collections.singletonMap(
            GoogleCloudStorageRepository.TYPE,
            (projectId, metadata) -> new GoogleCloudStorageRepository(
                projectId,
                metadata,
                namedXContentRegistry,
                this.storageService.get(),
                clusterService,
                bigArrays,
                recoverySettings,
                new GcsRepositoryStatsCollector(clusterService.threadPool(), metadata, repositoriesMetrics),
                snapshotMetrics
            )
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final ClusterService clusterService = services.clusterService();
        storageService.set(createStorageService(clusterService, services.projectResolver()));
        // eagerly load client settings so that secure settings are readable (not closed)
        reload(settings);
        return List.of(storageService.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING,
            GoogleCloudStorageClientSettings.ENDPOINT_SETTING,
            GoogleCloudStorageClientSettings.PROJECT_ID_SETTING,
            GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING,
            GoogleCloudStorageClientSettings.TOKEN_URI_SETTING,
            GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING,
            GoogleCloudStorageClientSettings.PROXY_HOST_SETTING,
            GoogleCloudStorageClientSettings.PROXY_PORT_SETTING,
            GoogleCloudStorageClientSettings.MAX_RETRIES_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // Secure settings should be readable inside this method. Duplicate client
        // settings in a format (`GoogleCloudStorageClientSettings`) that does not
        // require for the `SecureSettings` to be open. Pass that around (the
        // `GoogleCloudStorageClientSettings` instance) instead of the `Settings`
        // instance.
        final Map<String, GoogleCloudStorageClientSettings> clientsSettings = GoogleCloudStorageClientSettings.load(settings);
        this.storageService.get().refreshAndClearCache(clientsSettings);
    }
}
