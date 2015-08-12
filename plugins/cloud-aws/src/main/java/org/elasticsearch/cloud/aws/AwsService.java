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

public interface AwsService {
    /**
     * Should be either moved to Core if this settings makes sense
     * Or removed. See https://github.com/elastic/elasticsearch/issues/12809
     */
    @Deprecated
    final class CLOUD {
        public static final String KEY = "cloud.key";
        public static final String ACCOUNT = "cloud.account";
    }

    final class CLOUD_AWS {
        public static final String KEY = "cloud.aws.access_key";
        public static final String SECRET = "cloud.aws.secret_key";
        public static final String PROTOCOL = "cloud.aws.protocol";
        public static final String PROXY_HOST = "cloud.aws.proxy_host";
        public static final String PROXY_PORT = "cloud.aws.proxy_port";
        public static final String SIGNER = "cloud.aws.signer";
        public static final String REGION = "cloud.aws.region";

    }
}
