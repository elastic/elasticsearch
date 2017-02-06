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

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import org.elasticsearch.cloud.aws.AwsS3Service;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

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
        DummyS3Service() {
            super(Settings.EMPTY);
        }
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public AmazonS3 client(Settings repositorySettings, Integer maxRetries,
                               boolean useThrottleRetries, Boolean pathStyleAccess) {
            return new DummyS3Client();
        }
    }

    public void testSettingsResolution() throws Exception {
        Settings localSettings = Settings.builder().put(Repository.KEY_SETTING.getKey(), "key1").build();
        Settings globalSettings = Settings.builder().put(Repositories.KEY_SETTING.getKey(), "key2").build();

        assertEquals(new SecureString("key1".toCharArray()),
                     getValue(localSettings, globalSettings, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals(new SecureString("key1".toCharArray()),
                     getValue(localSettings, Settings.EMPTY, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals(new SecureString("key2".toCharArray()),
                     getValue(Settings.EMPTY, globalSettings, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertEquals(new SecureString("".toCharArray()),
                     getValue(Settings.EMPTY, Settings.EMPTY, Repository.KEY_SETTING, Repositories.KEY_SETTING));
        assertWarnings("[" + Repository.KEY_SETTING.getKey() + "] setting was deprecated",
                       "[" + Repositories.KEY_SETTING.getKey() + "] setting was deprecated");
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
        new S3Repository(metadata, Settings.EMPTY, NamedXContentRegistry.EMPTY, new DummyS3Service());
    }

    private void assertInvalidBuffer(int bufferMB, int chunkMB, Class<? extends Exception> clazz, String msg) throws IOException {
        RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
            .put(Repository.BUFFER_SIZE_SETTING.getKey(), new ByteSizeValue(bufferMB, ByteSizeUnit.MB))
            .put(Repository.CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkMB, ByteSizeUnit.MB)).build());

        Exception e = expectThrows(clazz, () -> new S3Repository(metadata, Settings.EMPTY, NamedXContentRegistry.EMPTY,
            new DummyS3Service()));
        assertThat(e.getMessage(), containsString(msg));
    }

    public void testBasePathSetting() throws IOException {
        RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
            .put(Repository.BASE_PATH_SETTING.getKey(), "/foo/bar").build());
        S3Repository s3repo = new S3Repository(metadata, Settings.EMPTY, NamedXContentRegistry.EMPTY, new DummyS3Service());
        assertEquals("foo/bar/", s3repo.basePath().buildAsString()); // make sure leading `/` is removed and trailing is added
        assertWarnings("S3 repository base_path" +
                " trimming the leading `/`, and leading `/` will not be supported for the S3 repository in future releases");
        metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.EMPTY);
        Settings settings = Settings.builder().put(Repositories.BASE_PATH_SETTING.getKey(), "/foo/bar").build();
        s3repo = new S3Repository(metadata, settings, NamedXContentRegistry.EMPTY, new DummyS3Service());
        assertEquals("foo/bar/", s3repo.basePath().buildAsString()); // make sure leading `/` is removed and trailing is added
        assertWarnings("S3 repository base_path" +
                " trimming the leading `/`, and leading `/` will not be supported for the S3 repository in future releases");
    }

    public void testDefaultBufferSize() {
        ByteSizeValue defaultBufferSize = S3Repository.Repository.BUFFER_SIZE_SETTING.get(Settings.EMPTY);
        assertThat(defaultBufferSize, Matchers.lessThanOrEqualTo(new ByteSizeValue(100, ByteSizeUnit.MB)));
        assertThat(defaultBufferSize, Matchers.greaterThanOrEqualTo(new ByteSizeValue(5, ByteSizeUnit.MB)));

        ByteSizeValue defaultNodeBufferSize = S3Repository.Repositories.BUFFER_SIZE_SETTING.get(Settings.EMPTY);
        assertEquals(defaultBufferSize, defaultNodeBufferSize);
    }
}
