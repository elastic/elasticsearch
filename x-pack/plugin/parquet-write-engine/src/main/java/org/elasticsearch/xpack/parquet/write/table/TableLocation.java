/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.core.Nullable;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

/**
 * Where an Iceberg table lives: the URI that goes into its metadata, and the blob path in the repository that the same place maps to.
 *
 * <p>The metadata of an Iceberg table holds the full URI of every file, and a reader outside Elasticsearch (the ES|QL Iceberg data
 * source, Spark, Trino, pyiceberg) opens a file by that URI. So the URI must be the real location of the object, and it is derived from
 * the repository and never configured separately: for an S3 repository it is {@code s3://<bucket>/<base path>/<table path>}, and for a
 * file system repository it is the {@code file://} URI of the resolved location. The base path comes from the blob path of the
 * repository, which already includes the {@code base_path} setting, so there is one source of truth.
 *
 * @param rootUri  the URI of the table root, without a trailing slash
 * @param blobPath the blob path of the table root, to give to {@link RepositoryFileIO}, below the base path of the repository
 */
public record TableLocation(String rootUri, BlobPath blobPath) {

    /**
     * @param repository         the metadata of the repository (its type and settings)
     * @param repositoryBasePath the base path of the repository, as {@code BlobStoreRepository#basePath()} gives it
     * @param tablePath          the path of the table below the base path, for example the name and the uuid of the index
     * @param fsLocationResolver resolves the {@code location} setting of a file system repository to a path (it can be relative to
     *                           {@code path.repo}); null when no such repository is expected
     * @throws IllegalArgumentException for a repository type that has no URI scheme for tables yet
     */
    public static TableLocation of(
        RepositoryMetadata repository,
        BlobPath repositoryBasePath,
        List<String> tablePath,
        @Nullable Function<String, Path> fsLocationResolver
    ) {
        BlobPath blobPath = repositoryBasePath;
        for (String segment : tablePath) {
            blobPath = blobPath.add(segment);
        }
        final String relative = String.join("/", blobPath.parts());
        switch (repository.type()) {
            case "s3" -> {
                final String bucket = repository.settings().get("bucket");
                if (bucket == null || bucket.isEmpty()) {
                    throw new IllegalArgumentException("repository [" + repository.name() + "] has no [bucket] setting");
                }
                return new TableLocation("s3://" + bucket + (relative.isEmpty() ? "" : "/" + relative), blobPath);
            }
            case "fs" -> {
                final String location = repository.settings().get("location");
                if (location == null || location.isEmpty()) {
                    throw new IllegalArgumentException("repository [" + repository.name() + "] has no [location] setting");
                }
                if (fsLocationResolver == null) {
                    throw new IllegalArgumentException(
                        "the location of the file system repository [" + repository.name() + "] cannot be resolved"
                    );
                }
                final String root = fsLocationResolver.apply(location).toUri().toString().replaceAll("/+$", "");
                return new TableLocation(root + (relative.isEmpty() ? "" : "/" + relative), blobPath);
            }
            default -> throw new IllegalArgumentException(
                "repository ["
                    + repository.name()
                    + "] has the type ["
                    + repository.type()
                    + "], and tables are supported only on [s3] and [fs]"
            );
        }
    }
}
