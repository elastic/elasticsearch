/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class TableLocationTests extends ESTestCase {

    public void testS3UsesTheBucketAndTheBasePathOfTheRepository() {
        final RepositoryMetadata repository = new RepositoryMetadata("lake", "s3", Settings.builder().put("bucket", "my-bucket").build());
        // the base path of the repository is already part of its blob path, so it appears once
        final BlobPath basePath = BlobPath.EMPTY.add("warehouse").add("logs");

        final TableLocation location = TableLocation.of(repository, basePath, List.of("my-index", "uuid-1"), null);

        assertThat(location.rootUri(), equalTo("s3://my-bucket/warehouse/logs/my-index/uuid-1"));
        assertThat(location.blobPath().parts(), equalTo(List.of("warehouse", "logs", "my-index", "uuid-1")));
    }

    public void testS3WithoutABasePath() {
        final RepositoryMetadata repository = new RepositoryMetadata("lake", "s3", Settings.builder().put("bucket", "my-bucket").build());

        final TableLocation location = TableLocation.of(repository, BlobPath.EMPTY, List.of("my-index"), null);

        assertThat(location.rootUri(), equalTo("s3://my-bucket/my-index"));
    }

    public void testS3NeedsABucket() {
        final RepositoryMetadata repository = new RepositoryMetadata("lake", "s3", Settings.EMPTY);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TableLocation.of(repository, BlobPath.EMPTY, List.of("my-index"), null)
        );
        assertThat(e.getMessage(), containsString("[bucket]"));
    }

    public void testFileSystemRepositoryUsesTheResolvedLocation() {
        final Path repoRoot = createTempDir();
        final RepositoryMetadata repository = new RepositoryMetadata("lake", "fs", Settings.builder().put("location", "lake-dir").build());

        final TableLocation location = TableLocation.of(
            repository,
            BlobPath.EMPTY,
            List.of("my-index", "uuid-1"),
            name -> repoRoot.resolve(name)
        );

        assertThat(location.rootUri(), startsWith("file:///"));
        assertThat(location.rootUri(), endsWith("/lake-dir/my-index/uuid-1"));
        assertThat(location.blobPath().parts(), equalTo(List.of("my-index", "uuid-1")));
    }

    public void testOtherRepositoryTypesAreRejectedWithAClearMessage() {
        final RepositoryMetadata repository = new RepositoryMetadata("lake", "gcs", Settings.EMPTY);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TableLocation.of(repository, BlobPath.EMPTY, List.of("my-index"), null)
        );
        assertThat(e.getMessage(), containsString("[gcs]"));
        assertThat(e.getMessage(), containsString("[s3] and [fs]"));
    }
}
