/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.remote;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.RemoteStoreSettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Supplier;

public class RemoteStoreCustomMetadataResolver {

    private final RemoteStoreSettings remoteStoreSettings;
    private final Supplier<Version> minNodeVersionSupplier;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final Settings settings;

    public RemoteStoreCustomMetadataResolver(
        RemoteStoreSettings remoteStoreSettings,
        Supplier<Version> minNodeVersionSupplier,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Settings settings
    ) {
        this.remoteStoreSettings = remoteStoreSettings;
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.settings = settings;
    }

    public RemoteStorePathStrategy getPathStrategy() {
        PathType pathType;
        PathHashAlgorithm pathHashAlgorithm;
        // Min node version check ensures that we are enabling the new prefix type only when all the nodes understand it.
        pathType = Version.V_2_14_0.compareTo(minNodeVersionSupplier.get()) <= 0 ? remoteStoreSettings.getPathType() : PathType.FIXED;
        // If the path type is fixed, hash algorithm is not applicable.
        pathHashAlgorithm = pathType == PathType.FIXED ? null : remoteStoreSettings.getPathHashAlgorithm();
        return new RemoteStorePathStrategy(pathType, pathHashAlgorithm);
    }

    public boolean isTranslogMetadataEnabled() {
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(getRemoteStoreTranslogRepo(settings));
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        return Version.V_2_15_0.compareTo(minNodeVersionSupplier.get()) <= 0
            && remoteStoreSettings.isTranslogMetadataEnabled()
            && blobStoreRepository.blobStore().isBlobMetadataEnabled();
    }

}
