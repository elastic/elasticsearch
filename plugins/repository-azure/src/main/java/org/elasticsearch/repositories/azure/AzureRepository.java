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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.LocationMode;
import org.elasticsearch.cloud.azure.blobstore.AzureBlobStore;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotCreationException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

/**
 * Azure file system implementation of the BlobStoreRepository
 * <p>
 * Azure file system repository supports the following settings:
 * <dl>
 * <dt>{@code container}</dt><dd>Azure container name. Defaults to elasticsearch-snapshots</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to 64mb.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class AzureRepository extends BlobStoreRepository {

    public final static String TYPE = "azure";
    public final static String CONTAINER_DEFAULT = "elasticsearch-snapshots";

    static public final class Repository {
        public static final String ACCOUNT = "account";
        public static final String LOCATION_MODE = "location_mode";
        public static final String CONTAINER = "container";
        public static final String CHUNK_SIZE = "chunk_size";
        public static final String COMPRESS = "compress";
        public static final String BASE_PATH = "base_path";
    }

    private final AzureBlobStore blobStore;

    private final BlobPath basePath;

    private ByteSizeValue chunkSize;

    private boolean compress;
    private final boolean readonly;

    @Inject
    public AzureRepository(RepositoryName name, RepositorySettings repositorySettings,
                           IndexShardRepository indexShardRepository,
                           AzureBlobStore azureBlobStore) throws IOException, URISyntaxException, StorageException {
        super(name.getName(), repositorySettings, indexShardRepository);

        String container = repositorySettings.settings().get(Repository.CONTAINER,
                settings.get(Storage.CONTAINER, CONTAINER_DEFAULT));

        this.blobStore = azureBlobStore;
        this.chunkSize = repositorySettings.settings().getAsBytesSize(Repository.CHUNK_SIZE,
                settings.getAsBytesSize(Storage.CHUNK_SIZE, new ByteSizeValue(64, ByteSizeUnit.MB)));

        if (this.chunkSize.getMb() > 64) {
            logger.warn("azure repository does not support yet size > 64mb. Fall back to 64mb.");
            this.chunkSize = new ByteSizeValue(64, ByteSizeUnit.MB);
        }

        this.compress = repositorySettings.settings().getAsBoolean(Repository.COMPRESS,
                settings.getAsBoolean(Storage.COMPRESS, false));
        String modeStr = repositorySettings.settings().get(Repository.LOCATION_MODE, null);
        if (modeStr != null) {
            LocationMode locationMode = LocationMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
            if (locationMode == LocationMode.SECONDARY_ONLY) {
                readonly = true;
            } else {
                readonly = false;
            }
        } else {
            readonly = false;
        }

        String basePath = repositorySettings.settings().get(Repository.BASE_PATH, null);

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
        logger.debug("using container [{}], chunk_size [{}], compress [{}], base_path [{}]",
                container, chunkSize, compress, basePath);
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

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<String> indices, MetaData metaData) {
        try {
            if (!blobStore.doesContainerExist(blobStore.container())) {
                logger.debug("container [{}] does not exist. Creating...", blobStore.container());
                blobStore.createContainer(blobStore.container());
            }
            super.initializeSnapshot(snapshotId, indices, metaData);
        } catch (StorageException | URISyntaxException e) {
            logger.warn("can not initialize container [{}]: [{}]", blobStore.container(), e.getMessage());
            throw new SnapshotCreationException(snapshotId, e);
        }
    }

    @Override
    public String startVerification() {
        if (readonly == false) {
            try {
                if (!blobStore.doesContainerExist(blobStore.container())) {
                    logger.debug("container [{}] does not exist. Creating...", blobStore.container());
                    blobStore.createContainer(blobStore.container());
                }
            } catch (StorageException | URISyntaxException e) {
                logger.warn("can not initialize container [{}]: [{}]", blobStore.container(), e.getMessage());
                throw new RepositoryVerificationException(repositoryName, "can not initialize container " + blobStore.container(), e);
            }
        }
        return super.startVerification();
    }

    @Override
    public boolean readOnly() {
        return readonly;
    }
}
