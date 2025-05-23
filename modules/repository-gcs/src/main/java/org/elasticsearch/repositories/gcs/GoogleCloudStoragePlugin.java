/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleCloudStoragePlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    // package-private for tests
    final GoogleCloudStorageService storageService;

    @SuppressWarnings("this-escape")
    public GoogleCloudStoragePlugin(final Settings settings) {
        var isServerless = DiscoveryNode.isStateless(settings);
        this.storageService = createStorageService(isServerless);
        // eagerly load client settings so that secure settings are readable (not closed)
        reload(settings);
    }

    // overridable for tests
    protected GoogleCloudStorageService createStorageService(boolean isServerless) {
        return new GoogleCloudStorageService(isServerless);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics
    ) {
        return Collections.singletonMap(
            GoogleCloudStorageRepository.TYPE,
            (projectId, metadata) -> new GoogleCloudStorageRepository(
                metadata,
                namedXContentRegistry,
                this.storageService,
                clusterService,
                bigArrays,
                recoverySettings,
                new GcsRepositoryStatsCollector(clusterService.threadPool(), metadata, repositoriesMetrics)
            )
        );
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
            GoogleCloudStorageClientSettings.PROXY_PORT_SETTING
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
        this.storageService.refreshAndClearCache(clientsSettings);
    }
}
