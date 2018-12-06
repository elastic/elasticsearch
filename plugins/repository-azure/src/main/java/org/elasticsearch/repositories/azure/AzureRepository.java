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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotId;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.repositories.azure.AzureStorageService.MAX_CHUNK_SIZE;
import static org.elasticsearch.repositories.azure.AzureStorageService.MIN_CHUNK_SIZE;

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
    private static final Logger logger = LogManager.getLogger(AzureRepository.class);

    public static final String TYPE = "azure";

    public static final class Repository {
        @Deprecated // Replaced by client
        public static final Setting<String> ACCOUNT_SETTING = new Setting<>("account", "default", Function.identity(),
            Property.NodeScope, Property.Deprecated);
        public static final Setting<String> CLIENT_NAME = new Setting<>("client", ACCOUNT_SETTING, Function.identity());
        public static final Setting<String> CONTAINER_SETTING =
            new Setting<>("container", "elasticsearch-snapshots", Function.identity(), Property.NodeScope);
        public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", Property.NodeScope);
        public static final Setting<LocationMode> LOCATION_MODE_SETTING = new Setting<>("location_mode",
                s -> LocationMode.PRIMARY_ONLY.toString(), s -> LocationMode.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope);
        public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE, Property.NodeScope);
        public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false, Property.NodeScope);
        public static final Setting<Boolean> READONLY_SETTING = Setting.boolSetting("readonly", false, Property.NodeScope);
    }

    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final boolean compress;
    private final Environment environment;
    private final AzureStorageService storageService;
    private final boolean readonly;

    public AzureRepository(RepositoryMetaData metadata, Environment environment, NamedXContentRegistry namedXContentRegistry,
            AzureStorageService storageService) {
        super(metadata, environment.settings(), namedXContentRegistry);
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.compress = Repository.COMPRESS_SETTING.get(metadata.settings());
        this.environment = environment;
        this.storageService = storageService;

        final String basePath = Strings.trimLeadingCharacter(Repository.BASE_PATH_SETTING.get(metadata.settings()), '/');
        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            BlobPath path = new BlobPath();
            for(final String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        // If the user explicitly did not define a readonly value, we set it by ourselves depending on the location mode setting.
        // For secondary_only setting, the repository should be read only
        final LocationMode locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        if (Repository.READONLY_SETTING.exists(metadata.settings())) {
            this.readonly = Repository.READONLY_SETTING.get(metadata.settings());
        } else {
            this.readonly = locationMode == LocationMode.SECONDARY_ONLY;
        }
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AzureBlobStore createBlobStore() throws URISyntaxException, StorageException {
        final AzureBlobStore blobStore = new AzureBlobStore(metadata, storageService);

        logger.debug((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
            "using container [{}], chunk_size [{}], compress [{}], base_path [{}]",
            blobStore, chunkSize, compress, basePath));
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
        try {
            final AzureBlobStore blobStore = (AzureBlobStore) blobStore();
            if (blobStore.containerExist() == false) {
                throw new IllegalArgumentException("The bucket [" + blobStore + "] does not exist. Please create it before "
                        + " creating an azure snapshot repository backed by it.");
            }
        } catch (URISyntaxException | StorageException e) {
            throw new SnapshotCreationException(metadata.name(), snapshotId, e);
        }
        super.initializeSnapshot(snapshotId, indices, clusterMetadata);
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }
}
