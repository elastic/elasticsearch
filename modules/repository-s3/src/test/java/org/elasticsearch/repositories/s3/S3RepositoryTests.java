/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;

import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class S3RepositoryTests extends ESTestCase {

    private static class DummyS3Client implements S3Client {

        @Override
        public void close() {
            // TODO check is closed
        }

        @Override
        public String serviceName() {
            return "DummyS3Client";
        }
    }

    private static class DummyS3Service extends S3Service {

        DummyS3Service(
            Environment environment,
            ClusterService clusterService,
            ProjectResolver projectResolver,
            ResourceWatcherService resourceWatcherService
        ) {
            super(environment, clusterService, projectResolver, resourceWatcherService, () -> null);
        }

        @Override
        public AmazonS3Reference client(@Nullable ProjectId projectId, RepositoryMetadata repositoryMetadata) {
            return new AmazonS3Reference(new DummyS3Client(), mock(SdkHttpClient.class));
        }

        @Override
        public void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {}

        @Override
        public void doClose() {
            // nothing to clean up
        }
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
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), ByteSizeValue.of(buffer, ByteSizeUnit.MB).getStringRep())
            .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.of(chunk, ByteSizeUnit.MB).getStringRep())
            .build();
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        return new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(settings).build());
    }

    public void testBasePathSetting() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.BASE_PATH_SETTING.getKey(), "foo/bar")
                .build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertEquals("foo/bar/", s3repo.basePath().buildAsString());
        }
    }

    public void testDefaultBufferSize() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket").build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertThat(s3repo.getBlobStore(), is(nullValue()));
            s3repo.start();
            final long defaultBufferSize = ((S3BlobStore) s3repo.blobStore()).bufferSizeInBytes();
            assertThat(s3repo.getBlobStore(), not(nullValue()));
            assertThat(defaultBufferSize, Matchers.lessThanOrEqualTo(100L * 1024 * 1024));
            assertThat(defaultBufferSize, Matchers.greaterThanOrEqualTo(5L * 1024 * 1024));
        }
    }

    public void testMissingBucketName() {
        final var metadata = new RepositoryMetadata("repo", "mock", Settings.EMPTY);
        assertThrows(IllegalArgumentException.class, () -> createS3Repo(metadata));
    }

    public void testEmptyBucketName() {
        final var settings = Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "").build();
        final var metadata = new RepositoryMetadata("repo", "mock", settings);
        assertThrows(IllegalArgumentException.class, () -> createS3Repo(metadata));
    }

    private S3Repository createS3Repo(RepositoryMetadata metadata) {
        final ProjectId projectId = randomProjectIdOrDefault();
        final S3Repository s3Repository = new S3Repository(
            projectId,
            metadata,
            NamedXContentRegistry.EMPTY,
            new DummyS3Service(
                mock(Environment.class),
                ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                mock(ResourceWatcherService.class)
            ),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            S3RepositoriesMetrics.NOOP
        );
        assertThat(s3Repository.getProjectId(), equalTo(projectId));
        return s3Repository;
    }

    public void testAnalysisFailureDetail() {
        try (
            S3Repository s3repo = createS3Repo(
                new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket").build())
            )
        ) {
            assertThat(
                s3repo.getAnalysisFailureExtraDetail(),
                allOf(
                    containsString("storage system underneath this repository behaved incorrectly"),
                    containsString("incorrectly claims to be S3-compatible"),
                    containsString("report this incompatibility to your storage supplier"),
                    containsString("unless you can demonstrate that the same issue exists when using a genuine AWS S3 repository"),
                    containsString(ReferenceDocs.SNAPSHOT_REPOSITORY_ANALYSIS.toString()),
                    containsString(ReferenceDocs.S3_COMPATIBLE_REPOSITORIES.toString())
                )
            );
        }
    }

    // ensures that chunkSize is limited to chunk_size setting, when buffer_size * parts_num is bigger
    public void testChunkSizeLimit() {
        var meta = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), "1GB")
                .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), "100MB")
                .put(S3Repository.MAX_MULTIPART_PARTS.getKey(), 10_000) // ~1TB
                .build()
        );
        try (var repo = createS3Repo(meta)) {
            assertEquals(ByteSizeValue.ofGb(1), repo.chunkSize());
        }
    }

    // ensures that chunkSize is limited to buffer_size * parts_num, when chunk_size setting is bigger
    public void testPartsNumLimit() {
        var meta = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), "5TB")
                .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), "100MB")
                .put(S3Repository.MAX_MULTIPART_PARTS.getKey(), 10_000)
                .build()
        );
        try (var repo = createS3Repo(meta)) {
            assertEquals(ByteSizeValue.ofMb(1_000_000), repo.chunkSize());
        }
    }
}
