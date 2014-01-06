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

package org.elasticsearch.repositories.azure;

import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.elasticsearch.cloud.azure.AzureStorageService;
import org.elasticsearch.cloud.azure.blobstore.AzureBlobStore;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Azure file system implementation of the BlobStoreRepository
 * <p/>
 * Azure file system repository supports the following settings:
 * <dl>
 * <dt>{@code container}</dt><dd>Azure container name. Defaults to elasticsearch-snapshots</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to 64mb.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class AzureRepository extends BlobStoreRepository {

    public final static String TYPE = "azure";
    public final static String CONTAINER_DEFAULT = "elasticsearch-snapshots";

    private final AzureBlobStore blobStore;

    private final BlobPath basePath;

    private ByteSizeValue chunkSize;

    private boolean compress;

    /**
     * Constructs new shared file system repository
     *
     * @param name                 repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository index shard repository
     * @param azureStorageService  Azure Storage service
     * @throws java.io.IOException
     */
    @Inject
    public AzureRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository, AzureStorageService azureStorageService) throws IOException, URISyntaxException, StorageException {
        super(name.getName(), repositorySettings, indexShardRepository);

        String container = repositorySettings.settings().get(AzureStorageService.Fields.CONTAINER,
                componentSettings.get(AzureStorageService.Fields.CONTAINER, CONTAINER_DEFAULT));

        int concurrentStreams = repositorySettings.settings().getAsInt(AzureStorageService.Fields.CONCURRENT_STREAMS,
                componentSettings.getAsInt(AzureStorageService.Fields.CONCURRENT_STREAMS, 5));
        ExecutorService concurrentStreamPool = EsExecutors.newScaling(1, concurrentStreams, 5, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory(settings, "[azure_stream]"));

        this.blobStore = new AzureBlobStore(settings, azureStorageService, container, concurrentStreamPool);
        this.chunkSize = repositorySettings.settings().getAsBytesSize(AzureStorageService.Fields.CHUNK_SIZE,
                componentSettings.getAsBytesSize(AzureStorageService.Fields.CHUNK_SIZE, new ByteSizeValue(64, ByteSizeUnit.MB)));

        if (this.chunkSize.getMb() > 64) {
            logger.warn("azure repository does not support yet size > 64mb. Fall back to 64mb.");
            this.chunkSize = new ByteSizeValue(64, ByteSizeUnit.MB);
        }

        this.compress = repositorySettings.settings().getAsBoolean(AzureStorageService.Fields.COMPRESS,
                componentSettings.getAsBoolean(AzureStorageService.Fields.COMPRESS, false));
        String basePath = repositorySettings.settings().get(AzureStorageService.Fields.BASE_PATH, null);

        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            basePath = Strings.trimLeadingCharacter(basePath, '/');
            BlobPath path = new BlobPath();
            for(String elem : Strings.splitStringToArray(basePath, '/')) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        logger.debug("using container [{}], chunk_size [{}], concurrent_streams [{}], compress [{}], base_path [{}]",
                container, chunkSize, concurrentStreams, compress, basePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
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


}
