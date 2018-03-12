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

import java.util.Map;

interface AwsS3Service {

    /**
     * Creates then caches an {@code AmazonS3} client using the current client
     * settings.
     */
    AmazonS3Reference client(String clientName);

    /**
     * Updates settings for building clients. Future client requests will use the
     * new settings. Implementations SHOULD drop the client cache to prevent reusing
     * clients with old settings from cache.
     *
     * @param clientsSettings
     *            the new settings
     * @return the old settings
     */
    Map<String, S3ClientSettings> updateClientsSettings(Map<String, S3ClientSettings> clientsSettings);

    /**
     * Releases cached clients. Subsequent client requests will recreate client
     * instances. Does not touch the client settings.
     */
    void releaseCachedClients();
}
