/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;

import java.util.Locale;
import java.util.Map;
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
public class AzureRepository extends MeteredBlobStoreRepository {
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
        public static final Setting<Boolean> READONLY_SETTING = Setting.boolSetting(READONLY_SETTING_KEY, false, Property.NodeScope);
        // see ModelHelper.BLOB_DEFAULT_MAX_SINGLE_UPLOAD_SIZE
        private static final ByteSizeValue DEFAULT_MAX_SINGLE_UPLOAD_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);
        public static final Setting<ByteSizeValue> MAX_SINGLE_PART_UPLOAD_SIZE_SETTING =
            Setting.byteSizeSetting("max_single_part_upload_size", DEFAULT_MAX_SINGLE_UPLOAD_SIZE, Property.NodeScope);
    }

    private final ByteSizeValue chunkSize;
    private final AzureStorageService storageService;
    private final boolean readonly;

    public AzureRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final AzureStorageService storageService,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings) {
        super(metadata,
            namedXContentRegistry,
            clusterService,
            bigArrays,
            recoverySettings,
            buildBasePath(metadata),
            buildLocation(metadata));
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.storageService = storageService;

        // If the user explicitly did not define a readonly value, we set it by ourselves depending on the location mode setting.
        // For secondary_only setting, the repository should be read only
        final LocationMode locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        if (Repository.READONLY_SETTING.exists(metadata.settings())) {
            this.readonly = Repository.READONLY_SETTING.get(metadata.settings());
        } else {
            this.readonly = locationMode.isSecondary();
        }
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = Strings.trimLeadingCharacter(Repository.BASE_PATH_SETTING.get(metadata.settings()), '/');
        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            BlobPath path = BlobPath.EMPTY;
            for(final String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            return path;
        } else {
            return BlobPath.EMPTY;
        }
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", Repository.BASE_PATH_SETTING.get(metadata.settings()),
            "container", Repository.CONTAINER_SETTING.get(metadata.settings()));
    }

    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    protected AzureBlobStore createBlobStore() {
        final AzureBlobStore blobStore = new AzureBlobStore(metadata, storageService, bigArrays);

        logger.debug(() -> new ParameterizedMessage(
            "using container [{}], chunk_size [{}], compress [{}], base_path [{}]",
            blobStore, chunkSize, isCompress(), basePath()));
        return blobStore;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }
}
