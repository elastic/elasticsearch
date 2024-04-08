/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AbstractAmazonS3;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class S3RepositoryTests extends ESTestCase {

    private static class DummyS3Client extends AbstractAmazonS3 {

        @Override
        public void shutdown() {
            // TODO check is closed
        }
    }

    private static class DummyS3Service extends S3Service {

        DummyS3Service(Environment environment, ResourceWatcherService resourceWatcherService) {
            super(environment, Settings.EMPTY, resourceWatcherService);
        }

        @Override
        public AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
            return new AmazonS3Reference(new DummyS3Client());
        }

        @Override
        public void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {}

        @Override
        public void close() {}
    }

    public void testInvalidChunkBufferSizeSettings() {
        // chunk < buffer should fail
        final Settings s1 = bufferAndChunkSettings(10, 5);
        final Exception e1 = expectThrows(RepositoryException.class, () -> createS3Repo(getRepositoryMetadata(s1)));
        assertThat(e1.getMessage(), containsString("chunk_size (5mb) can't be lower than buffer_size (10mb)"));
        // chunk > buffer should pass
        final Settings s2 = bufferAndChunkSettings(5, 10);
        createS3Repo(getRepositoryMetadata(s2)).close();
        // chunk = buffer should pass
        final Settings s3 = bufferAndChunkSettings(5, 5);
        createS3Repo(getRepositoryMetadata(s3)).close();
        // buffer < 5mb should fail
        final Settings s4 = bufferAndChunkSettings(4, 10);
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s4)).close()
        );
        assertThat(e2.getMessage(), containsString("failed to parse value [4mb] for setting [buffer_size], must be >= [5mb]"));
        final Settings s5 = bufferAndChunkSettings(5, 6000000);
        final IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s5)).close()
        );
        assertThat(e3.getMessage(), containsString("failed to parse value [6000000mb] for setting [chunk_size], must be <= [5tb]"));
    }

    private Settings bufferAndChunkSettings(long buffer, long chunk) {
        return Settings.builder()
            .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), new ByteSizeValue(buffer, ByteSizeUnit.MB).getStringRep())
            .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunk, ByteSizeUnit.MB).getStringRep())
            .build();
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        return new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(settings).build());
    }

    public void testBasePathSetting() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder().put(S3Repository.BASE_PATH_SETTING.getKey(), "foo/bar").build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertEquals("foo/bar/", s3repo.basePath().buildAsString());
        }
    }

    public void testDefaultBufferSize() {
        final RepositoryMetadata metadata = new RepositoryMetadata("dummy-repo", "mock", Settings.EMPTY);
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertThat(s3repo.getBlobStore(), is(nullValue()));
            s3repo.start();
            final long defaultBufferSize = ((S3BlobStore) s3repo.blobStore()).bufferSizeInBytes();
            assertThat(s3repo.getBlobStore(), not(nullValue()));
            assertThat(defaultBufferSize, Matchers.lessThanOrEqualTo(100L * 1024 * 1024));
            assertThat(defaultBufferSize, Matchers.greaterThanOrEqualTo(5L * 1024 * 1024));
        }
    }

    private S3Repository createS3Repo(RepositoryMetadata metadata) {
        return new S3Repository(
            metadata,
            NamedXContentRegistry.EMPTY,
            new DummyS3Service(Mockito.mock(Environment.class), Mockito.mock(ResourceWatcherService.class)),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            S3RepositoriesMetrics.NOOP
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually on test/main threads
            }
        };
    }
}
