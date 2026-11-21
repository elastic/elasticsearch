/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.elasticsearch.xpack.parquet.write.table.TableReader;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Opens readers of Iceberg tables in the repositories of the node: the table of an index of the write engine, which its search replicas
 * follow, and a table that an index mounts. A component of the node, so that the mount actions get it by injection.
 */
public final class TableReaders {

    private final Supplier<RepositoriesService> repositories;
    private final Environment environment;
    private final ThreadPool threadPool;

    public TableReaders(Supplier<RepositoriesService> repositories, Environment environment, ThreadPool threadPool) {
        this.repositories = repositories;
        this.environment = environment;
        this.threadPool = threadPool;
    }

    /**
     * A reader of the table at {@code tablePath} below the base path of the repository. It never creates or writes the table.
     *
     * @throws IOException if the repository does not exist or does not store blobs
     */
    public TableReader open(ProjectId projectId, String repositoryName, List<String> tablePath) throws IOException {
        final BlobStoreRepository repository = repository(projectId, repositoryName);
        final TableLocation location = TableLocation.of(
            repository.getMetadata(),
            repository.basePath(),
            tablePath,
            environment::resolveRepoDir
        );
        return TableReader.open(blobStoreOf(threadPool, repository), location);
    }

    BlobStoreRepository repository(ProjectId projectId, String repositoryName) throws IOException {
        final RepositoriesService service = repositories.get();
        if (service == null) {
            throw new IOException("the node has not finished starting");
        }
        final Repository repository = service.repository(projectId, repositoryName);
        if (repository instanceof BlobStoreRepository == false) {
            throw new IOException("repository [" + repositoryName + "] does not store blobs");
        }
        return (BlobStoreRepository) repository;
    }

    /**
     * A repository gives its blob store only to threads of the snapshot pools and of {@code generic}, and a flush or a read of a table
     * may run on a pool of its own (see {@link ParquetThreadPools}). Such a thread asks a {@code generic} thread for the blob store, and
     * keeps it: the table and the files use it directly from then on.
     */
    static BlobStore blobStoreOf(ThreadPool threadPool, BlobStoreRepository repository) {
        final String executor = EsExecutors.executorName(Thread.currentThread());
        if (ThreadPool.Names.GENERIC.equals(executor) || ThreadPool.Names.SNAPSHOT.equals(executor)) {
            return repository.blobStore();
        }
        final PlainActionFuture<BlobStore> future = new PlainActionFuture<>();
        threadPool.generic().execute(() -> {
            try {
                future.onResponse(repository.blobStore());
            } catch (Throwable t) {
                // an Error too: a caller that waits must not wait for ever
                future.onFailure(t instanceof Exception e ? e : new IllegalStateException("failed to get the blob store", t));
            }
        });
        return future.actionGet();
    }
}
