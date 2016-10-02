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
import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.repository.s3.S3RepositoryPlugin;

import java.util.IdentityHashMap;

public class TestAwsS3Service extends InternalAwsS3Service {
    public static class TestPlugin extends S3RepositoryPlugin {
        @Override
        protected AwsS3Service createStorageService(Settings settings) {
            return new TestAwsS3Service(settings);
        }
    }

    IdentityHashMap<AmazonS3, TestAmazonS3> clients = new IdentityHashMap<AmazonS3, TestAmazonS3>();

    public TestAwsS3Service(Settings settings) {
        super(settings);
    }


    @Override
    public synchronized AmazonS3 client(Settings repositorySettings, String endpoint, Protocol protocol, String region, Integer maxRetries,
                                        boolean useThrottleRetries, Boolean pathStyleAccess) {
        return cachedWrapper(super.client(repositorySettings, endpoint, protocol, region, maxRetries, useThrottleRetries, pathStyleAccess));
    }

    private AmazonS3 cachedWrapper(AmazonS3 client) {
        TestAmazonS3 wrapper = clients.get(client);
        if (wrapper == null) {
            wrapper = new TestAmazonS3(client, settings);
            clients.put(client, wrapper);
        }
        return wrapper;
    }

    @Override
    protected synchronized void doClose() throws ElasticsearchException {
        super.doClose();
        clients.clear();
    }


}
