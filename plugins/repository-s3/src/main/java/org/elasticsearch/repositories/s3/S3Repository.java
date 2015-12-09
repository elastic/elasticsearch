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

import org.elasticsearch.cloud.aws.AwsS3Service;
import org.elasticsearch.cloud.aws.AwsS3Service.CLOUD_AWS;
import org.elasticsearch.cloud.aws.AwsS3Service.REPOSITORY_S3;
import org.elasticsearch.cloud.aws.blobstore.S3BlobStore;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.Locale;

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

        String bucket = repositorySettings.settings().get("bucket", settings.get(REPOSITORY_S3.BUCKET));
        if (bucket == null) {
            throw new RepositoryException(name.name(), "No bucket defined for s3 gateway");
        }

        String endpoint = repositorySettings.settings().get("endpoint", settings.get(REPOSITORY_S3.ENDPOINT));
        String protocol = repositorySettings.settings().get("protocol", settings.get(REPOSITORY_S3.PROTOCOL));

        String region = repositorySettings.settings().get("region", settings.get(REPOSITORY_S3.REGION));
        if (region == null) {
            // Bucket setting is not set - use global region setting
            String regionSetting = settings.get(CLOUD_AWS.REGION);
            if (regionSetting != null) {
                regionSetting = regionSetting.toLowerCase(Locale.ENGLISH);
                if ("us-east".equals(regionSetting) || "us-east-1".equals(regionSetting)) {
                    // Default bucket - setting region to null
                    region = null;
                } else if ("us-west".equals(regionSetting) || "us-west-1".equals(regionSetting)) {
                    region = "us-west-1";
                } else if ("us-west-2".equals(regionSetting)) {
                    region = "us-west-2";
                } else if ("ap-southeast".equals(regionSetting) || "ap-southeast-1".equals(regionSetting)) {
                    region = "ap-southeast-1";
                } else if ("ap-southeast-2".equals(regionSetting)) {
                    region = "ap-southeast-2";
                } else if ("ap-northeast".equals(regionSetting) || "ap-northeast-1".equals(regionSetting)) {
                    region = "ap-northeast-1";
                } else if ("eu-west".equals(regionSetting) || "eu-west-1".equals(regionSetting)) {
                    region = "eu-west-1";
                } else if ("eu-central".equals(regionSetting) || "eu-central-1".equals(regionSetting)) {
                    region = "eu-central-1";
                } else if ("sa-east".equals(regionSetting) || "sa-east-1".equals(regionSetting)) {
                    region = "sa-east-1";
                } else if ("cn-north".equals(regionSetting) || "cn-north-1".equals(regionSetting)) {
                    region = "cn-north-1";
                }
            }
        }

        boolean serverSideEncryption = repositorySettings.settings().getAsBoolean("server_side_encryption", settings.getAsBoolean(REPOSITORY_S3.SERVER_SIDE_ENCRYPTION, false));
        ByteSizeValue bufferSize = repositorySettings.settings().getAsBytesSize("buffer_size", settings.getAsBytesSize(REPOSITORY_S3.BUFFER_SIZE, null));
        Integer maxRetries = repositorySettings.settings().getAsInt("max_retries", settings.getAsInt(REPOSITORY_S3.MAX_RETRIES, 3));
        this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size", settings.getAsBytesSize(REPOSITORY_S3.CHUNK_SIZE, new ByteSizeValue(100, ByteSizeUnit.MB)));
        this.compress = repositorySettings.settings().getAsBoolean("compress", settings.getAsBoolean(REPOSITORY_S3.COMPRESS, false));

        // Parse and validate the user's S3 Storage Class setting
        String storageClass = repositorySettings.settings().get("storage_class", settings.get(REPOSITORY_S3.STORAGE_CLASS, null));
        String cannedACL = repositorySettings.settings().get("canned_acl", settings.get(REPOSITORY_S3.CANNED_ACL, null));

        logger.debug("using bucket [{}], region [{}], endpoint [{}], protocol [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], max_retries [{}], cannedACL [{}], storageClass [{}]",
                bucket, region, endpoint, protocol, chunkSize, serverSideEncryption, bufferSize, maxRetries, cannedACL, storageClass);

        blobStore = new S3BlobStore(settings, s3Service.client(endpoint, protocol, region, repositorySettings.settings().get("access_key"), repositorySettings.settings().get("secret_key"), maxRetries),
                bucket, region, serverSideEncryption, bufferSize, maxRetries, cannedACL, storageClass);

        String basePath = repositorySettings.settings().get("base_path", settings.get(REPOSITORY_S3.BASE_PATH));
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

}
