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

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.blobstore.AzureBlobStore;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.SettingsException;
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
import java.util.function.Function;

import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.getEffectiveSetting;
import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.getValue;

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

    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(64, ByteSizeUnit.MB);

    public final static String TYPE = "azure";

    public static final class Repository {
        public static final Setting<String> ACCOUNT_SETTING = Setting.simpleString("account", Property.NodeScope);
        public static final Setting<String> CONTAINER_SETTING =
            new Setting<>("container", "elasticsearch-snapshots", Function.identity(), Property.NodeScope);
        public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", Property.NodeScope);
        public static final Setting<String> LOCATION_MODE_SETTING = Setting.simpleString("location_mode", Property.NodeScope);
        public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, Property.NodeScope);
        public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false, Property.NodeScope);
    }

    private final AzureBlobStore blobStore;
    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final boolean compress;
    private final boolean readonly;

    @Inject
    public AzureRepository(RepositoryName name, RepositorySettings repositorySettings,
                           IndexShardRepository indexShardRepository,
                           AzureBlobStore azureBlobStore) throws IOException, URISyntaxException, StorageException {
        super(name.getName(), repositorySettings, indexShardRepository);

        String container = getValue(repositorySettings, Repository.CONTAINER_SETTING, Storage.CONTAINER_SETTING);

        this.blobStore = azureBlobStore;
        ByteSizeValue configuredChunkSize = getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Storage.CHUNK_SIZE_SETTING);
        if (configuredChunkSize.getMb() > MAX_CHUNK_SIZE.getMb()) {
            Setting<ByteSizeValue> setting = getEffectiveSetting(repositorySettings, Repository.CHUNK_SIZE_SETTING, Storage.CHUNK_SIZE_SETTING);
            throw new SettingsException("["  + setting.getKey() + "] must not exceed [" + MAX_CHUNK_SIZE + "] but is set to [" + configuredChunkSize + "].");
        } else {
            this.chunkSize = configuredChunkSize;
        }

        this.compress = getValue(repositorySettings, Repository.COMPRESS_SETTING, Storage.COMPRESS_SETTING);
        String modeStr = getValue(repositorySettings, Repository.LOCATION_MODE_SETTING, Storage.LOCATION_MODE_SETTING);
        if (Strings.hasLength(modeStr)) {
            LocationMode locationMode = LocationMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
            readonly = locationMode == LocationMode.SECONDARY_ONLY;
        } else {
            readonly = false;
        }

        String basePath = getValue(repositorySettings, Repository.BASE_PATH_SETTING, Storage.BASE_PATH_SETTING);

        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            basePath = Strings.trimLeadingCharacter(basePath, '/');
            BlobPath path = new BlobPath();
            for(String elem : basePath.split("/")) {
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
            throw new SnapshotCreationException(repositoryName, snapshotId, e);
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
