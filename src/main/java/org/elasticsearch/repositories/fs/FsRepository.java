/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.fs;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p/>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code location}</dt><dd>Path to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </ol>
 */
public class FsRepository extends BlobStoreRepository {

    public final static String TYPE = "fs";

    private final FsBlobStore blobStore;

    private ByteSizeValue chunkSize;

    private final BlobPath basePath;

    private boolean compress;

    /**
     * Constructs new shared file system repository
     *
     * @param name                 repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository index shard repository
     * @throws IOException
     */
    @Inject
    public FsRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);
        Path locationFile;
        String location = repositorySettings.settings().get("location", settings.get("repositories.fs.location"));
        if (location == null) {
            logger.warn("using local fs location for gateway, should be changed to be a shared location across nodes");
            throw new RepositoryException(name.name(), "missing location");
        } else {
            locationFile = PathUtils.get(location);
        }
        blobStore = new FsBlobStore(settings, locationFile);
        this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size", settings.getAsBytesSize("repositories.fs.chunk_size", null));
        this.compress = repositorySettings.settings().getAsBoolean("compress", settings.getAsBoolean("repositories.fs.compress", false));
        this.basePath = BlobPath.cleanPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isCompress() {
        return compress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }
}
