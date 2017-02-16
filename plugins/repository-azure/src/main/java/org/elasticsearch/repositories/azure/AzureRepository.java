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
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.MAX_CHUNK_SIZE;
import static org.elasticsearch.cloud.azure.storage.AzureStorageService.MIN_CHUNK_SIZE;
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

    public static final String TYPE = "azure";

    public static final class Repository {
        public static final Setting<String> ACCOUNT_SETTING = Setting.simpleString("account", Property.NodeScope);
        public static final Setting<String> CONTAINER_SETTING =
            new Setting<>("container", "elasticsearch-snapshots", Function.identity(), Property.NodeScope);
        public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", Property.NodeScope);
        public static final Setting<String> LOCATION_MODE_SETTING = Setting.simpleString("location_mode", Property.NodeScope);
        public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE, Property.NodeScope);
        public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false, Property.NodeScope);
    }

    private final AzureBlobStore blobStore;
    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final boolean compress;
    private final boolean readonly;

    public AzureRepository(RepositoryMetaData metadata, Environment environment,
                           NamedXContentRegistry namedXContentRegistry, AzureStorageService storageService)
        throws IOException, URISyntaxException, StorageException {
        super(metadata, environment.settings(), namedXContentRegistry);

        blobStore = new AzureBlobStore(metadata, environment.settings(), storageService);
        String container = getValue(metadata.settings(), settings, Repository.CONTAINER_SETTING, Storage.CONTAINER_SETTING);
        this.chunkSize = getValue(metadata.settings(), settings, Repository.CHUNK_SIZE_SETTING, Storage.CHUNK_SIZE_SETTING);
        this.compress = getValue(metadata.settings(), settings, Repository.COMPRESS_SETTING, Storage.COMPRESS_SETTING);
        String modeStr = getValue(metadata.settings(), settings, Repository.LOCATION_MODE_SETTING, Storage.LOCATION_MODE_SETTING);
        Boolean forcedReadonly = metadata.settings().getAsBoolean("readonly", null);
        // If the user explicitly did not define a readonly value, we set it by ourselves depending on the location mode setting.
        // For secondary_only setting, the repository should be read only
        if (forcedReadonly == null) {
            if (Strings.hasLength(modeStr)) {
                LocationMode locationMode = LocationMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
                this.readonly = locationMode == LocationMode.SECONDARY_ONLY;
            } else {
                this.readonly = false;
            }
        } else {
            readonly = forcedReadonly;
        }

        String basePath = getValue(metadata.settings(), settings, Repository.BASE_PATH_SETTING, Storage.BASE_PATH_SETTING);

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
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetadata) {
        if (blobStore.doesContainerExist(blobStore.container()) == false) {
            throw new IllegalArgumentException("The bucket [" + blobStore.container() + "] does not exist. Please create it before " +
                " creating an azure snapshot repository backed by it.");
        }
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }
}
