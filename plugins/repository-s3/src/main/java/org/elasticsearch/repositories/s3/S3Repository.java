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

package org.elasticsearch.repositories.s3;

import com.amazonaws.Protocol;
import org.elasticsearch.cloud.aws.AwsS3Service;
import org.elasticsearch.cloud.aws.blobstore.S3BlobStore;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>S3 bucket</dd>
 * <dt>{@code region}</dt><dd>S3 region. Defaults to us-east</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class S3Repository extends BlobStoreRepository {

    public final static String TYPE = "s3";

    public interface Repositories {
        Setting<String> KEY_SETTING = new Setting<>("repositories.s3.access_key", AwsS3Service.CLOUD_S3.KEY_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> SECRET_SETTING = new Setting<>("repositories.s3.secret_key", AwsS3Service.CLOUD_S3.SECRET_SETTING, Function.identity(), false, Setting.Scope.CLUSTER);
        Setting<String> BUCKET_SETTING = Setting.simpleString("repositories.s3.bucket", false, Setting.Scope.CLUSTER);
        Setting<String> REGION_SETTING = new Setting<>("repositories.s3.region", AwsS3Service.CLOUD_S3.REGION_SETTING, s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);
        Setting<String> ENDPOINT_SETTING = new Setting<>("repositories.s3.endpoint", AwsS3Service.CLOUD_S3.ENDPOINT_SETTING, s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("repositories.s3.protocol", AwsS3Service.CLOUD_S3.PROTOCOL_SETTING, s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), false, Setting.Scope.CLUSTER);
        Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("repositories.s3.server_side_encryption", false, false, Setting.Scope.CLUSTER);
        Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting("repositories.s3.buffer_size", S3BlobStore.MIN_BUFFER_SIZE, false, Setting.Scope.CLUSTER);
        Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting("repositories.s3.max_retries", 3, false, Setting.Scope.CLUSTER);
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("repositories.s3.chunk_size", new ByteSizeValue(100, ByteSizeUnit.MB), false, Setting.Scope.CLUSTER);
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("repositories.s3.compress", false, false, Setting.Scope.CLUSTER);
        Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("repositories.s3.storage_class", false, Setting.Scope.CLUSTER);
        Setting<String> CANNED_ACL_SETTING = Setting.simpleString("repositories.s3.canned_acl", false, Setting.Scope.CLUSTER);
        Setting<String> BASE_PATH_SETTING = Setting.simpleString("repositories.s3.base_path", false, Setting.Scope.CLUSTER);
    }

    public interface Repository {
        Setting<String> KEY_SETTING = Setting.simpleString("access_key", false, Setting.Scope.CLUSTER);
        Setting<String> SECRET_SETTING = Setting.simpleString("secret_key", false, Setting.Scope.CLUSTER);
        Setting<String> BUCKET_SETTING = Setting.simpleString("bucket", false, Setting.Scope.CLUSTER);
        Setting<String> ENDPOINT_SETTING = Setting.simpleString("endpoint", false, Setting.Scope.CLUSTER);
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), false, Setting.Scope.CLUSTER);
        Setting<String> REGION_SETTING = new Setting<>("region", "", s -> s.toLowerCase(Locale.ROOT), false, Setting.Scope.CLUSTER);
        Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("server_side_encryption", false, false, Setting.Scope.CLUSTER);
        Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting("buffer_size", S3BlobStore.MIN_BUFFER_SIZE, false, Setting.Scope.CLUSTER);
        Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting("max_retries", 3, false, Setting.Scope.CLUSTER);
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size", "-1", false, Setting.Scope.CLUSTER);
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false, false, Setting.Scope.CLUSTER);
        Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class", false, Setting.Scope.CLUSTER);
        Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl", false, Setting.Scope.CLUSTER);
        Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", false, Setting.Scope.CLUSTER);
    }

    private final S3BlobStore blobStore;

    private final BlobPath basePath;

    private ByteSizeValue chunkSize;

    private boolean compress;

    /**
     * Constructs new shared file system repository
     *
     * @param name                 repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository index shard repository
     * @param s3Service            S3 service
     */
    @Inject
    public S3Repository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository, AwsS3Service s3Service) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);

        String bucket = getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING);
        if (bucket == null) {
            throw new RepositoryException(name.name(), "No bucket defined for s3 gateway");
        }

        String endpoint = getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING);
        Protocol protocol = getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING);
        String region = getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING);
        // If no region is defined either in region, repositories.s3.region, cloud.aws.s3.region or cloud.aws.region
        // we fallback to Default bucket - null
        if (Strings.isEmpty(region)) {
            region = null;
        }

        boolean serverSideEncryption = getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING);
        ByteSizeValue bufferSize = getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING);
        Integer maxRetries = getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING);
        this.chunkSize = getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING);
        this.compress = getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING);

        // Parse and validate the user's S3 Storage Class setting
        String storageClass = getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING);
        String cannedACL = getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING);

        logger.debug("using bucket [{}], region [{}], endpoint [{}], protocol [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], max_retries [{}], cannedACL [{}], storageClass [{}]",
                bucket, region, endpoint, protocol, chunkSize, serverSideEncryption, bufferSize, maxRetries, cannedACL, storageClass);

        String key = getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING);
        String secret = getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING);

        blobStore = new S3BlobStore(settings, s3Service.client(endpoint, protocol, region, key, secret, maxRetries),
                bucket, region, serverSideEncryption, bufferSize, maxRetries, cannedACL, storageClass);

        String basePath = getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING);
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for(String elem : Strings.splitStringToArray(basePath, '/')) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }
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

    public static <T> T getValue(RepositorySettings repositorySettings,
                                 Setting<T> repositorySetting,
                                 Setting<T> repositoriesSetting) {
        if (repositorySetting.exists(repositorySettings.settings())) {
            return repositorySetting.get(repositorySettings.settings());
        } else {
            return repositoriesSetting.get(repositorySettings.globalSettings());
        }
    }
}
