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

package org.elasticsearch.cloud.aws;

import com.amazonaws.Protocol;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.repositories.s3.S3Repository.Repositories;
import static org.elasticsearch.repositories.s3.S3Repository.Repository;
import static org.elasticsearch.repositories.s3.S3Repository.getValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;

public class RepositoryS3SettingsTests extends ESTestCase {

    private static final Settings AWS = Settings.builder()
        .put(AwsS3Service.KEY_SETTING.getKey(), "global-key")
        .put(AwsS3Service.SECRET_SETTING.getKey(), "global-secret")
        .put(AwsS3Service.PROTOCOL_SETTING.getKey(), "https")
        .put(AwsS3Service.PROXY_HOST_SETTING.getKey(), "global-proxy-host")
        .put(AwsS3Service.PROXY_PORT_SETTING.getKey(), 10000)
        .put(AwsS3Service.PROXY_USERNAME_SETTING.getKey(), "global-proxy-username")
        .put(AwsS3Service.PROXY_PASSWORD_SETTING.getKey(), "global-proxy-password")
        .put(AwsS3Service.SIGNER_SETTING.getKey(), "global-signer")
        .put(AwsS3Service.REGION_SETTING.getKey(), "global-region")
        .build();

    private static final Settings S3 = Settings.builder()
        .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3-key")
        .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3-secret")
        .put(AwsS3Service.CLOUD_S3.PROTOCOL_SETTING.getKey(), "http")
        .put(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.getKey(), "s3-proxy-host")
        .put(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.getKey(), 20000)
        .put(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.getKey(), "s3-proxy-username")
        .put(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.getKey(), "s3-proxy-password")
        .put(AwsS3Service.CLOUD_S3.SIGNER_SETTING.getKey(), "s3-signer")
        .put(AwsS3Service.CLOUD_S3.REGION_SETTING.getKey(), "s3-region")
        .put(AwsS3Service.CLOUD_S3.ENDPOINT_SETTING.getKey(), "s3-endpoint")
        .build();

    private static final Settings REPOSITORIES = Settings.builder()
        .put(Repositories.KEY_SETTING.getKey(), "repositories-key")
        .put(Repositories.SECRET_SETTING.getKey(), "repositories-secret")
        .put(Repositories.BUCKET_SETTING.getKey(), "repositories-bucket")
        .put(Repositories.PROTOCOL_SETTING.getKey(), "https")
        .put(Repositories.REGION_SETTING.getKey(), "repositories-region")
        .put(Repositories.ENDPOINT_SETTING.getKey(), "repositories-endpoint")
        .put(Repositories.SERVER_SIDE_ENCRYPTION_SETTING.getKey(), true)
        .put(Repositories.BUFFER_SIZE_SETTING.getKey(), "6mb")
        .put(Repositories.MAX_RETRIES_SETTING.getKey(), 4)
        .put(Repositories.CHUNK_SIZE_SETTING.getKey(), "110mb")
        .put(Repositories.COMPRESS_SETTING.getKey(), true)
        .put(Repositories.STORAGE_CLASS_SETTING.getKey(), "repositories-class")
        .put(Repositories.CANNED_ACL_SETTING.getKey(), "repositories-acl")
        .put(Repositories.BASE_PATH_SETTING.getKey(), "repositories-basepath")
        .build();

    private static final Settings REPOSITORY = Settings.builder()
        .put(Repository.KEY_SETTING.getKey(), "repository-key")
        .put(Repository.SECRET_SETTING.getKey(), "repository-secret")
        .put(Repository.BUCKET_SETTING.getKey(), "repository-bucket")
        .put(Repository.PROTOCOL_SETTING.getKey(), "https")
        .put(Repository.REGION_SETTING.getKey(), "repository-region")
        .put(Repository.ENDPOINT_SETTING.getKey(), "repository-endpoint")
        .put(Repository.SERVER_SIDE_ENCRYPTION_SETTING.getKey(), false)
        .put(Repository.BUFFER_SIZE_SETTING.getKey(), "7mb")
        .put(Repository.MAX_RETRIES_SETTING.getKey(), 5)
        .put(Repository.CHUNK_SIZE_SETTING.getKey(), "120mb")
        .put(Repository.COMPRESS_SETTING.getKey(), false)
        .put(Repository.STORAGE_CLASS_SETTING.getKey(), "repository-class")
        .put(Repository.CANNED_ACL_SETTING.getKey(), "repository-acl")
        .put(Repository.BASE_PATH_SETTING.getKey(), "repository-basepath")
        .build();

    /**
     * We test when only cloud.aws settings are set
     */
    public void testRepositorySettingsGlobalOnly() {
        Settings nodeSettings = buildSettings(AWS);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, Settings.EMPTY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("global-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("global-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("global-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), isEmptyString());
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("global-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(10000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("global-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("global-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("global-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(false));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(5L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(3));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(100L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(false));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), isEmptyString());
    }

    /**
     * We test when cloud.aws settings are overloaded by cloud.aws.s3 settings
     */
    public void testRepositorySettingsGlobalOverloadedByS3() {
        Settings nodeSettings = buildSettings(AWS, S3);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, Settings.EMPTY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("s3-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("s3-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTP));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("s3-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("s3-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("s3-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(20000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("s3-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("s3-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("s3-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(false));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(5L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(3));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(100L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(false));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), isEmptyString());
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), isEmptyString());
    }

    /**
     * We test when cloud.aws settings are overloaded by repositories.s3 settings
     */
    public void testRepositorySettingsGlobalOverloadedByRepositories() {
        Settings nodeSettings = buildSettings(AWS, REPOSITORIES);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, Settings.EMPTY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("repositories-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("repositories-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), is("repositories-bucket"));
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("repositories-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("repositories-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("global-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(10000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("global-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("global-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("global-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(true));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(6L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(4));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(110L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(true));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING),
            is("repositories-class"));
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), is("repositories-acl"));
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), is("repositories-basepath"));
    }

    /**
     * We test when cloud.aws.s3 settings are overloaded by repositories.s3 settings
     */
    public void testRepositorySettingsS3OverloadedByRepositories() {
        Settings nodeSettings = buildSettings(AWS, S3, REPOSITORIES);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, Settings.EMPTY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("repositories-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("repositories-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), is("repositories-bucket"));
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("repositories-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("repositories-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("s3-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(20000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("s3-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("s3-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("s3-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(true));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(6L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(4));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(110L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(true));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING),
            is("repositories-class"));
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), is("repositories-acl"));
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), is("repositories-basepath"));
    }

    /**
     * We test when cloud.aws settings are overloaded by single repository settings
     */
    public void testRepositorySettingsGlobalOverloadedByRepository() {
        Settings nodeSettings = buildSettings(AWS);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, REPOSITORY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("repository-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("repository-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), is("repository-bucket"));
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("repository-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("repository-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("global-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(10000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("global-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("global-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("global-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(false));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(7L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(5));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(120L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(false));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING),
            is("repository-class"));
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), is("repository-acl"));
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), is("repository-basepath"));
    }

    /**
     * We test when cloud.aws.s3 settings are overloaded by single repository settings
     */
    public void testRepositorySettingsS3OverloadedByRepository() {
        Settings nodeSettings = buildSettings(AWS, S3);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, REPOSITORY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("repository-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("repository-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), is("repository-bucket"));
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("repository-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("repository-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("s3-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(20000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("s3-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("s3-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("s3-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(false));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(7L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(5));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(120L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(false));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING),
            is("repository-class"));
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), is("repository-acl"));
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), is("repository-basepath"));
    }

    /**
     * We test when repositories settings are overloaded by single repository settings
     */
    public void testRepositorySettingsRepositoriesOverloadedByRepository() {
        Settings nodeSettings = buildSettings(AWS, S3, REPOSITORIES);
        RepositorySettings repositorySettings =  new RepositorySettings(nodeSettings, REPOSITORY);
        assertThat(getValue(repositorySettings, Repository.KEY_SETTING, Repositories.KEY_SETTING), is("repository-key"));
        assertThat(getValue(repositorySettings, Repository.SECRET_SETTING, Repositories.SECRET_SETTING), is("repository-secret"));
        assertThat(getValue(repositorySettings, Repository.BUCKET_SETTING, Repositories.BUCKET_SETTING), is("repository-bucket"));
        assertThat(getValue(repositorySettings, Repository.PROTOCOL_SETTING, Repositories.PROTOCOL_SETTING), is(Protocol.HTTPS));
        assertThat(getValue(repositorySettings, Repository.REGION_SETTING, Repositories.REGION_SETTING), is("repository-region"));
        assertThat(getValue(repositorySettings, Repository.ENDPOINT_SETTING, Repositories.ENDPOINT_SETTING), is("repository-endpoint"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.get(nodeSettings), is("s3-proxy-host"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.get(nodeSettings), is(20000));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.get(nodeSettings), is("s3-proxy-username"));
        assertThat(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.get(nodeSettings), is("s3-proxy-password"));
        assertThat(AwsS3Service.CLOUD_S3.SIGNER_SETTING.get(nodeSettings), is("s3-signer"));
        assertThat(getValue(repositorySettings, Repository.SERVER_SIDE_ENCRYPTION_SETTING, Repositories.SERVER_SIDE_ENCRYPTION_SETTING),
            is(false));
        assertThat(getValue(repositorySettings, Repository.BUFFER_SIZE_SETTING, Repositories.BUFFER_SIZE_SETTING).getMb(), is(7L));
        assertThat(getValue(repositorySettings, Repository.MAX_RETRIES_SETTING, Repositories.MAX_RETRIES_SETTING), is(5));
        assertThat(getValue(repositorySettings, Repository.CHUNK_SIZE_SETTING, Repositories.CHUNK_SIZE_SETTING).getMb(), is(120L));
        assertThat(getValue(repositorySettings, Repository.COMPRESS_SETTING, Repositories.COMPRESS_SETTING), is(false));
        assertThat(getValue(repositorySettings, Repository.STORAGE_CLASS_SETTING, Repositories.STORAGE_CLASS_SETTING),
            is("repository-class"));
        assertThat(getValue(repositorySettings, Repository.CANNED_ACL_SETTING, Repositories.CANNED_ACL_SETTING), is("repository-acl"));
        assertThat(getValue(repositorySettings, Repository.BASE_PATH_SETTING, Repositories.BASE_PATH_SETTING), is("repository-basepath"));
    }

    private Settings buildSettings(Settings... global) {
        Settings.Builder builder = Settings.builder();
        for (Settings settings : global) {
            builder.put(settings);
        }
        return builder.build();
    }
}
