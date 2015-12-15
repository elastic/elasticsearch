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

import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.common.component.LifecycleComponent;

/**
 *
 */
public interface AwsS3Service extends LifecycleComponent<AwsS3Service> {

    final class CLOUD_AWS {
        public static final String KEY = "cloud.aws.access_key";
        public static final String SECRET = "cloud.aws.secret_key";
        public static final String PROTOCOL = "cloud.aws.protocol";
        public static final String PROXY_HOST = "cloud.aws.proxy.host";
        public static final String PROXY_PORT = "cloud.aws.proxy.port";
        public static final String PROXY_USERNAME = "cloud.aws.proxy.username";
        public static final String PROXY_PASSWORD = "cloud.aws.proxy.password";
        public static final String SIGNER = "cloud.aws.signer";
        public static final String REGION = "cloud.aws.region";
        @Deprecated
        public static final String DEPRECATED_PROXY_HOST = "cloud.aws.proxy_host";
        @Deprecated
        public static final String DEPRECATED_PROXY_PORT = "cloud.aws.proxy_port";
    }

    final class CLOUD_S3 {
        public static final String KEY = "cloud.aws.s3.access_key";
        public static final String SECRET = "cloud.aws.s3.secret_key";
        public static final String PROTOCOL = "cloud.aws.s3.protocol";
        public static final String PROXY_HOST = "cloud.aws.s3.proxy.host";
        public static final String PROXY_PORT = "cloud.aws.s3.proxy.port";
        public static final String PROXY_USERNAME = "cloud.aws.s3.proxy.username";
        public static final String PROXY_PASSWORD = "cloud.aws.s3.proxy.password";
        public static final String SIGNER = "cloud.aws.s3.signer";
        public static final String ENDPOINT = "cloud.aws.s3.endpoint";
        @Deprecated
        public static final String DEPRECATED_PROXY_HOST = "cloud.aws.s3.proxy_host";
        @Deprecated
        public static final String DEPRECATED_PROXY_PORT = "cloud.aws.s3.proxy_port";
    }

    final class REPOSITORY_S3 {
        public static final String BUCKET = "repositories.s3.bucket";
        public static final String ENDPOINT = "repositories.s3.endpoint";
        public static final String PROTOCOL = "repositories.s3.protocol";
        public static final String REGION = "repositories.s3.region";
        public static final String SERVER_SIDE_ENCRYPTION = "repositories.s3.server_side_encryption";
        public static final String BUFFER_SIZE = "repositories.s3.buffer_size";
        public static final String MAX_RETRIES = "repositories.s3.max_retries";
        public static final String CHUNK_SIZE = "repositories.s3.chunk_size";
        public static final String COMPRESS = "repositories.s3.compress";
        public static final String STORAGE_CLASS = "repositories.s3.storage_class";
        public static final String CANNED_ACL = "repositories.s3.canned_acl";
        public static final String BASE_PATH = "repositories.s3.base_path";
    }



    AmazonS3 client();

    AmazonS3 client(String endpoint, String protocol, String region, String account, String key);

    AmazonS3 client(String endpoint, String protocol, String region, String account, String key, Integer maxRetries);
}
