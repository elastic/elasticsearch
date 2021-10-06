/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.nio.file.Path;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code location}</dt><dd>Path to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size.
 *      Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class FsRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(FsRepository.class);

    public static final String TYPE = "fs";

    public static final Setting<String> LOCATION_SETTING = new Setting<>("location", "", Function.identity(), Property.NodeScope);
    public static final Setting<String> REPOSITORIES_LOCATION_SETTING = new Setting<>(
        "repositories.fs.location",
        LOCATION_SETTING,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> REPOSITORIES_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "repositories.fs.chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    private final Environment environment;

    private final ByteSizeValue chunkSize;

    /**
     * Constructs a shared file system repository.
     */
    public FsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        super(metadata, namedXContentRegistry, clusterService, bigArrays, recoverySettings, BlobPath.EMPTY);
        this.environment = environment;
        String location = REPOSITORIES_LOCATION_SETTING.get(metadata.settings());
        if (location.isEmpty()) {
            logger.warn(
                "the repository location is missing, it should point to a shared file system location"
                    + " that is available on all master and data nodes"
            );
            throw new RepositoryException(metadata.name(), "missing location");
        }
        Path locationFile = environment.resolveRepoFile(location);
        if (locationFile == null) {
            if (environment.repoFiles().length > 0) {
                logger.warn(
                    "The specified location [{}] doesn't start with any " + "repository paths specified by the path.repo setting: [{}] ",
                    location,
                    environment.repoFiles()
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo"
                );
            } else {
                logger.warn(
                    "The specified location [{}] should start with a repository path specified by"
                        + " the path.repo setting, but the path.repo setting was not set on this node",
                    location
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo because this setting is empty"
                );
            }
        }

        if (CHUNK_SIZE_SETTING.exists(metadata.settings())) {
            this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        } else {
            this.chunkSize = REPOSITORIES_CHUNK_SIZE_SETTING.get(environment.settings());
        }
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
        final Path locationFile = environment.resolveRepoFile(location);
        return new FsBlobStore(bufferSize, locationFile, isReadOnly());
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public boolean hasAtomicOverwrites() {
        // We overwrite a file by deleting the old file and then renaming the new file into place, which is not atomic.
        // Also on Windows the overwrite may fail if the file is opened for reading at the time.
        return false;
    }
}
