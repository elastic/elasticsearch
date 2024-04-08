/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.util.json.Jackson;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
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
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the AWS S3.
 */
public class S3RepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    static {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // kick jackson to do some static caching of declared members info
                Jackson.jsonNodeOf("{}");
                // ClientConfiguration clinit has some classloader problems
                // TODO: fix that
                Class.forName("com.amazonaws.ClientConfiguration");
                // Pre-load region metadata to avoid looking them up dynamically without privileges enabled
                RegionUtils.initialize();
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private final SetOnce<S3Service> service = new SetOnce<>();
    private final Settings settings;

    public S3RepositoryPlugin(Settings settings) {
        this.settings = settings;
    }

    S3Service getService() {
        return service.get();
    }

    // proxy method for testing
    protected S3Repository createRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final S3RepositoriesMetrics s3RepositoriesMetrics
    ) {
        return new S3Repository(metadata, registry, service.get(), clusterService, bigArrays, recoverySettings, s3RepositoriesMetrics);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        service.set(s3Service(services.environment(), services.clusterService().getSettings(), services.resourceWatcherService()));
        this.service.get().refreshAndClearCache(S3ClientSettings.load(settings));
        return List.of(service);
    }

    S3Service s3Service(Environment environment, Settings nodeSettings, ResourceWatcherService resourceWatcherService) {
        return new S3Service(environment, nodeSettings, resourceWatcherService);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        final Environment env,
        final NamedXContentRegistry registry,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final RepositoriesMetrics repositoriesMetrics
    ) {
        final S3RepositoriesMetrics s3RepositoriesMetrics = new S3RepositoriesMetrics(repositoriesMetrics);
        return Collections.singletonMap(
            S3Repository.TYPE,
            metadata -> createRepository(metadata, registry, clusterService, bigArrays, recoverySettings, s3RepositoriesMetrics)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // named s3 client configuration settings
            S3ClientSettings.ACCESS_KEY_SETTING,
            S3ClientSettings.SECRET_KEY_SETTING,
            S3ClientSettings.SESSION_TOKEN_SETTING,
            S3ClientSettings.ENDPOINT_SETTING,
            S3ClientSettings.PROTOCOL_SETTING,
            S3ClientSettings.PROXY_HOST_SETTING,
            S3ClientSettings.PROXY_PORT_SETTING,
            S3ClientSettings.PROXY_SCHEME_SETTING,
            S3ClientSettings.PROXY_USERNAME_SETTING,
            S3ClientSettings.PROXY_PASSWORD_SETTING,
            S3ClientSettings.READ_TIMEOUT_SETTING,
            S3ClientSettings.MAX_RETRIES_SETTING,
            S3ClientSettings.USE_THROTTLE_RETRIES_SETTING,
            S3ClientSettings.USE_PATH_STYLE_ACCESS,
            S3ClientSettings.SIGNER_OVERRIDE,
            S3ClientSettings.REGION,
            S3Service.REPOSITORY_S3_CAS_TTL_SETTING,
            S3Service.REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING,
            S3Repository.ACCESS_KEY_SETTING,
            S3Repository.SECRET_KEY_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, S3ClientSettings> clientsSettings = S3ClientSettings.load(settings);
        getService().refreshAndClearCache(clientsSettings);
    }

    @Override
    public void close() throws IOException {
        getService().close();
    }
}
