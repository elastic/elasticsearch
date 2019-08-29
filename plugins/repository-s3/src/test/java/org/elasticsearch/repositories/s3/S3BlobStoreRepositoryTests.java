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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class S3BlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static final ConcurrentMap<String, byte[]> blobs = new ConcurrentHashMap<>();
    private static String bucket;
    private static ByteSizeValue bufferSize;
    private static boolean serverSideEncryption;
    private static String cannedACL;
    private static String storageClass;

    @BeforeClass
    public static void setUpRepositorySettings() {
        bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        bufferSize = new ByteSizeValue(randomIntBetween(5, 50), ByteSizeUnit.MB);
        serverSideEncryption = randomBoolean();
        if (randomBoolean()) {
            cannedACL = randomFrom(CannedAccessControlList.values()).toString();
        }
        if (randomBoolean()) {
            storageClass = randomValueOtherThan(StorageClass.Glacier, () -> randomFrom(StorageClass.values())).toString();
        }
    }

    @After
    public void wipeRepository() {
        blobs.clear();
    }

    @Override
    protected String repositoryType() {
        return S3Repository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(S3Repository.BUCKET_SETTING.getKey(), bucket)
            .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), bufferSize)
            .put(S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getKey(), serverSideEncryption)
            .put(S3Repository.CANNED_ACL_SETTING.getKey(), cannedACL)
            .put(S3Repository.STORAGE_CLASS_SETTING.getKey(), storageClass)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestS3RepositoryPlugin.class);
    }

    public static class TestS3RepositoryPlugin extends S3RepositoryPlugin {

        public TestS3RepositoryPlugin(final Settings settings) {
            super(settings);
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(final Environment env, final NamedXContentRegistry registry,
                                                               final ThreadPool threadPool) {
            return Collections.singletonMap(S3Repository.TYPE,
                    metadata -> new S3Repository(metadata, registry, new S3Service() {
                        @Override
                        AmazonS3 buildClient(S3ClientSettings clientSettings) {
                            return new MockAmazonS3(blobs, bucket, serverSideEncryption, cannedACL, storageClass);
                        }
                    }, threadPool));
        }
    }
}
