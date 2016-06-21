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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.IdentityHashMap;

public class TestAwsS3Service extends InternalAwsS3Service {
    public static class TestPlugin extends Plugin {
        public void onModule(S3Module s3Module) {
            S3Module.s3ServiceImpl = TestAwsS3Service.class;
        }
    }

    IdentityHashMap<AmazonS3, TestAmazonS3> clients = new IdentityHashMap<AmazonS3, TestAmazonS3>();

    @Inject
    public TestAwsS3Service(Settings settings) {
        super(settings);
    }


    @Override
    public synchronized AmazonS3 client(String endpoint, Protocol protocol, String region, String account, String key, Integer maxRetries,
                                        boolean useThrottleRetries) {
        return cachedWrapper(super.client(endpoint, protocol, region, account, key, maxRetries, useThrottleRetries));
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
