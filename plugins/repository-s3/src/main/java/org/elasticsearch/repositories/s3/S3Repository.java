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

import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>S3 bucket</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
class S3Repository extends BlobStoreRepository {

    public static final String TYPE = "s3";

    /**
     * Global S3 repositories settings. Starting with: repositories.s3
     * NOTE: These are legacy settings. Use the named client config settings above.
     */
    public interface Repositories {
        /**
         * repositories.s3.bucket: The name of the bucket to be used for snapshots.
         */
        Setting<String> BUCKET_SETTING = Setting.simpleString("repositories.s3.bucket", Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.server_side_encryption: When set to true files are encrypted on server side using AES256 algorithm.
         * Defaults to false.
         */
        Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING =
            Setting.boolSetting("repositories.s3.server_side_encryption", false, Property.NodeScope, Property.Deprecated);

        /**
         * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
         * the available memory for smaller heaps.
         */
        ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
                Math.max(
                        ByteSizeUnit.MB.toBytes(5), // minimum value
                        Math.min(
                                ByteSizeUnit.MB.toBytes(100),
                                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
                ByteSizeUnit.BYTES);

        /**
         * repositories.s3.buffer_size: Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
         * the S3 repository will use the AWS Multipart Upload API to split the chunk into several parts, each of buffer_size length, and
         * to upload each part in its own request. Note that setting a buffer size lower than 5mb is not allowed since it will prevents the
         * use of the Multipart API and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
         */
        Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
            Setting.byteSizeSetting("repositories.s3.buffer_size", DEFAULT_BUFFER_SIZE,
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB), Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.max_retries: Number of retries in case of S3 errors. Defaults to 3.
         */
        Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting("repositories.s3.max_retries", 3, Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.use_throttle_retries: Set to `true` if you want to throttle retries. Defaults to AWS SDK default value (`false`).
         */
        Setting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.boolSetting("repositories.s3.use_throttle_retries",
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES, Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.chunk_size: Big files can be broken down into chunks during snapshotting if needed. Defaults to 1g.
         */
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("repositories.s3.chunk_size", new ByteSizeValue(1, ByteSizeUnit.GB),
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB), Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.compress: When set to true metadata files are stored in compressed format. This setting doesn’t affect index
         * files that are already compressed by default. Defaults to false.
         */
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("repositories.s3.compress", false, Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.storage_class: Sets the S3 storage class type for the backup files. Values may be standard, reduced_redundancy,
         * standard_ia. Defaults to standard.
         */
        Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("repositories.s3.storage_class", Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.canned_acl: The S3 repository supports all S3 canned ACLs : private, public-read, public-read-write,
         * authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control. Defaults to private.
         */
        Setting<String> CANNED_ACL_SETTING = Setting.simpleString("repositories.s3.canned_acl", Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.base_path: Specifies the path within bucket to repository data. Defaults to root directory.
         */
        Setting<String> BASE_PATH_SETTING = Setting.simpleString("repositories.s3.base_path", Property.NodeScope, Property.Deprecated);
        /**
         * repositories.s3.path_style_access: When set to true configures the client to use path-style access for all requests.
         Amazon S3 supports virtual-hosted-style and path-style access in all Regions. The path-style syntax, however,
         requires that you use the region-specific endpoint when attempting to access a bucket.
         The default behaviour is to detect which access style to use based on the configured endpoint (an IP will result
         in path-style access) and the bucket being accessed (some buckets are not valid DNS names). Setting this flag
         will result in path-style access being used for all requests.
         */
        Setting<Boolean> PATH_STYLE_ACCESS_SETTING = Setting.boolSetting("repositories.s3.path_style_access", false,
            Property.NodeScope, Property.Deprecated);
    }

    /**
     * Per S3 repository specific settings. Same settings as Repositories settings but without the repositories.s3 prefix.
     * If undefined, they use the repositories.s3.xxx equivalent setting.
     */
    public interface Repository {

        Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

        /**
         * server_side_encryption
         * @see  Repositories#SERVER_SIDE_ENCRYPTION_SETTING
         */
        Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("server_side_encryption", false);

        /**
         * buffer_size
         * @see  Repositories#BUFFER_SIZE_SETTING
         */
        Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
            Setting.byteSizeSetting("buffer_size", Repositories.DEFAULT_BUFFER_SIZE,
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB));
        /**
         * max_retries
         * @see  Repositories#MAX_RETRIES_SETTING
         */
        Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting("max_retries", 3, Property.Deprecated);
        /**
         * use_throttle_retries
         * @see  Repositories#USE_THROTTLE_RETRIES_SETTING
         */
        Setting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.boolSetting("use_throttle_retries",
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES, Property.Deprecated);
        /**
         * chunk_size
         * @see  Repositories#CHUNK_SIZE_SETTING
         */
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("chunk_size", new ByteSizeValue(1, ByteSizeUnit.GB),
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB));
        /**
         * compress
         * @see  Repositories#COMPRESS_SETTING
         */
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);
        /**
         * storage_class
         * @see  Repositories#STORAGE_CLASS_SETTING
         */
        Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");
        /**
         * canned_acl
         * @see  Repositories#CANNED_ACL_SETTING
         */
        Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");
        /**
         * base_path
         * @see  Repositories#BASE_PATH_SETTING
         */
        Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");
        /**
         * path_style_access
         * @see  Repositories#PATH_STYLE_ACCESS_SETTING
         */
        Setting<Boolean> PATH_STYLE_ACCESS_SETTING = Setting.boolSetting("path_style_access", false, Property.Deprecated);
    }

    private final S3BlobStore blobStore;

    private final BlobPath basePath;

    private ByteSizeValue chunkSize;

    private boolean compress;

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(RepositoryMetaData metadata, Settings settings,
                        NamedXContentRegistry namedXContentRegistry, AwsS3Service s3Service) throws IOException {
        super(metadata, settings, namedXContentRegistry);

        String bucket = getValue(metadata.settings(), settings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING);
        if (bucket == null) {
            throw new RepositoryException(metadata.name(), "No bucket defined for s3 gateway");
        }

        boolean serverSideEncryption = getValue(metadata.settings(), settings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING);
        ByteSizeValue bufferSize = getValue(metadata.settings(), settings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING);
        this.chunkSize = getValue(metadata.settings(), settings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING);
        this.compress = getValue(metadata.settings(), settings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING);

        // We make sure that chunkSize is bigger or equal than/to bufferSize
        if (this.chunkSize.getBytes() < bufferSize.getBytes()) {
            throw new RepositoryException(metadata.name(), Repository.CHUNK_SIZE_SETTING.getKey() + " (" + this.chunkSize +
                ") can't be lower than " + Repository.BUFFER_SIZE_SETTING.getKey() + " (" + bufferSize + ").");
        }

        // Parse and validate the user's S3 Storage Class setting
        String storageClass = getValue(metadata.settings(), settings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING);
        String cannedACL = getValue(metadata.settings(), settings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING);

        logger.debug("using bucket [{}], chunk_size [{}], server_side_encryption [{}], " +
            "buffer_size [{}], cannedACL [{}], storageClass [{}]",
            bucket, chunkSize, serverSideEncryption, bufferSize, cannedACL, storageClass);

        AmazonS3 client = s3Service.client(metadata.settings());
        blobStore = new S3BlobStore(settings, client, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass);

        String basePath = getValue(metadata.settings(), settings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING);
        if (Strings.hasLength(basePath)) {
            if (basePath.startsWith("/")) {
                basePath = basePath.substring(1);
                deprecationLogger.deprecated("S3 repository base_path trimming the leading `/`, and " +
                                                 "leading `/` will not be supported for the S3 repository in future releases");
            }
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
    }

    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getValue(Settings repositorySettings,
                                 Settings globalSettings,
                                 Setting<T> repositorySetting,
                                 Setting<T> repositoriesSetting) {
        if (repositorySetting.exists(repositorySettings)) {
            return repositorySetting.get(repositorySettings);
        } else {
            return repositoriesSetting.get(globalSettings);
        }
    }
}
