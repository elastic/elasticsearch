/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.xpack.esql.datasources.DataSourceStorageResolver;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Opens the files of a mount, wherever they are: a file below the base path of a snapshot repository, or a URI that an ES|QL data
 * source reads. It is the one place that knows about both, so that the mount API and the read-only engine only hold a
 * {@link FileLocation}.
 *
 * <p>A data source is read through the {@link DataSourceStorageResolver} of the ES|QL plugin, which resolves the data source, checks the
 * URI against its type, decrypts its secrets and borrows a pooled client. The resolver is a component of the ES|QL plugin, so the
 * transport actions of this plugin, which Guice builds when the node starts, hand it over with {@link #bind}.
 */
public final class ParquetFiles {

    private final Supplier<RepositoriesService> repositories;
    private final ClusterService clusterService;
    private final AtomicReference<DataSourceStorageResolver> dataSources = new AtomicReference<>();

    public ParquetFiles(Supplier<RepositoriesService> repositories, ClusterService clusterService) {
        this.repositories = repositories;
        this.clusterService = clusterService;
    }

    /** Gives this the ES|QL resolver that reads data sources. Calling it again changes nothing. */
    public void bind(DataSourceStorageResolver resolver) {
        dataSources.compareAndSet(null, resolver);
    }

    /**
     * Opens the file and finds out its length with a request. An object of a data source holds a borrow and must be closed with
     * {@link #close}.
     *
     * @throws IllegalArgumentException if the repository or the data source does not exist, or cannot read the file
     */
    public StorageObject open(FileLocation location, ProjectId projectId) {
        return open(location, projectId, OptionalLong.empty());
    }

    /** Opens a file whose length is known, for example from the segment that references it, without a request. */
    public StorageObject open(FileLocation location, ProjectId projectId, long knownLength) {
        return open(location, projectId, OptionalLong.of(knownLength));
    }

    private StorageObject open(FileLocation location, ProjectId projectId, OptionalLong knownLength) {
        return switch (location) {
            case RepositoryLocation repository -> knownLength.isPresent()
                ? RepositoryStorageObject.open(repositories.get(), projectId, repository, knownLength.getAsLong())
                : RepositoryStorageObject.open(repositories.get(), projectId, repository);
            case DataSourceLocation dataSource -> openDataSource(dataSource, projectId, knownLength);
        };
    }

    private StorageObject openDataSource(DataSourceLocation location, ProjectId projectId, OptionalLong knownLength) {
        DataSourceStorageResolver resolver = dataSources.get();
        if (resolver == null) {
            throw new IllegalStateException("ES|QL data sources are not available on this node");
        }
        // every node has the data sources in its cluster state, so resolving one needs no request to the master
        ProjectMetadata project = clusterService.state().metadata().getProject(projectId);
        return resolver.open(project, location.dataSource(), location.uri(), knownLength);
    }

    /** Closes what {@link #open} returned if it holds a borrow, and does nothing for a file of a repository. */
    public static void close(StorageObject object) {
        if (object instanceof Closeable closeable) {
            IOUtils.closeWhileHandlingException(closeable);
        }
    }

    /** Like {@link #close} but fails if the borrow cannot be released. */
    public static void closeAndRethrow(StorageObject object) throws IOException {
        if (object instanceof Closeable closeable) {
            closeable.close();
        }
    }
}
