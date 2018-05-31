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

import java.io.Closeable;
import java.util.Map;

interface AwsS3Service extends Closeable {

    /**
     * Creates then caches an {@code AmazonS3} client using the current client
     * settings. Returns an {@code AmazonS3Reference} wrapper which has to be
     * released as soon as it is not needed anymore.
     */
    AmazonS3Reference client(String clientName);

    /**
     * Updates settings for building clients and clears the client cache. Future
     * client requests will use the new settings to lazily build new clients.
     *
     * @param clientsSettings the new refreshed settings
     * @return the old stale settings
     */
    Map<String, S3ClientSettings> refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings);

}
