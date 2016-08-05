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

import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.cloud.aws.AwsS3Service;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.repositories.s3.S3Repository.Repositories;
import static org.elasticsearch.repositories.s3.S3Repository.Repository;
import static org.elasticsearch.repositories.s3.S3Repository.getValue;
import static org.hamcrest.Matchers.containsString;

public class S3RepositoryTests extends ESTestCase {

    private static class DummyS3Client extends AbstractAmazonS3 {
        @Override
        public boolean doesBucketExist(String bucketName) {
            return true;
        }
    }

    private static class DummyS3Service extends AbstractLifecycleComponent implements AwsS3Service {
        public DummyS3Service() {
            super(Settings.EMPTY);
        }
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public AmazonS3 client(Settings repositorySettings, String endpoint, Protocol protocol, String region, Integer maxRetries,
                               boolean useThrottleRetries, Boolean pathStyleAccess) {
            return new DummyS3Client();
        }
    }

    public void testSettingsResolution() throws Exception {
        Settings localSettings = Settings.builder().put(Repository.KEY_SETTING.getKey(), "key1").build();
        Settings globalSettings = Settings.builder().put(Repositories.KEY_SETTING.getKey(), "key2").build();

        assertEquals("key1", getValue(localSettings, globalSettings, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals("key1", getValue(localSettings, Settings.EMPTY, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals("key2", getValue(Settings.EMPTY, globalSettings, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals("", getValue(Settings.EMPTY, Settings.EMPTY, Repository.KEY_SETTING, Repositories.KEY_SETTING));
    }

    public void testInvalidChunkBufferSizeSettings() throws IOException {
        // chunk < buffer should fail
        assertInvalidBuffer(10, 5, RepositoryException.class, "chunk_size (5mb) can't be lower than buffer_size (10mb).");
        // chunk > buffer should pass
        assertValidBuffer(5, 10);
        // chunk = buffer should pass
        assertValidBuffer(5, 5);
        // buffer < 5mb should fail
        assertInvalidBuffer(4, 10, IllegalArgumentException.class,
            "Failed to parse value [4mb] for setting [buffer_size] must be >= 5mb");
        // chunk > 5tb should fail
        assertInvalidBuffer(5, 6000000, IllegalArgumentException.class,
            "Failed to parse value [5.7tb] for setting [chunk_size] must be <= 5tb");
    }

    private void assertValidBuffer(long bufferMB, long chunkMB) throws IOException {
        RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
            .put(Repository.BUFFER_SIZE_SETTING.getKey(), new ByteSizeValue(bufferMB, ByteSizeUnit.MB))
            .put(Repository.CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkMB, ByteSizeUnit.MB)).build());
        new S3Repository(metadata, Settings.EMPTY, new DummyS3Service());
    }

    private void assertInvalidBuffer(int bufferMB, int chunkMB, Class<? extends Exception> clazz, String msg) throws IOException {
        RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
            .put(Repository.BUFFER_SIZE_SETTING.getKey(), new ByteSizeValue(bufferMB, ByteSizeUnit.MB))
            .put(Repository.CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkMB, ByteSizeUnit.MB)).build());

        Exception e = expectThrows(clazz, () -> new S3Repository(metadata, Settings.EMPTY, new DummyS3Service()));
        assertThat(e.getMessage(), containsString(msg));
    }
}
