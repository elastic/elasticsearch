/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Transport action that attempts to write a blob to a repository. Used by {@link RepositoryAnalyzeAction} to verify overwrite protection:
 * multiple nodes attempt to write the same blob name concurrently; with overwrite protection at most one write should succeed.
 */
public class BlobOverwriteAction extends HandledTransportAction<BlobOverwriteAction.Request, BlobOverwriteAction.Response> {

    private static final Logger logger = LogManager.getLogger(BlobOverwriteAction.class);

    static final String NAME = "cluster:admin/repository/analyze/blob_overwrite";

    private final RepositoriesService repositoriesService;

    BlobOverwriteAction(TransportService transportService, ActionFilters actionFilters, RepositoriesService repositoriesService) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT));
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ActionListener.completeWith(listener.map(Response::new), () -> {
            final Repository repository = repositoriesService.repository(request.repositoryName);
            if (repository instanceof BlobStoreRepository == false) {
                throw new IllegalArgumentException("repository [" + request.repositoryName + "] is not a blob-store repository");
            }
            if (repository.isReadOnly()) {
                throw new IllegalArgumentException("repository [" + request.repositoryName + "] is read-only");
            }
            final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            final BlobPath path = blobStoreRepository.basePath().add(request.blobPath);
            final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);
            assert task instanceof CancellableTask;

            logger.trace("handling [{}]", request);

            try {
                writeBlob(blobStoreRepository, blobContainer, request, task);
                return true;
            } catch (Exception e) {
                // There is no specific exception thrown on overwrite, so we must catch them all.
                logger.trace(() -> "write failed for [" + request + "]", e);
                return false;
            }
        });
    }

    private static void writeBlob(BlobStoreRepository repository, BlobContainer blobContainer, Request request, Task task)
        throws IOException {
        final RandomBlobContent content = new RandomBlobContent(
            request.repositoryName,
            request.seed,
            ((CancellableTask) task)::isCancelled,
            () -> {}
        );
        final boolean atomic = request.blobSize <= BlobAnalyzeAction.MAX_ATOMIC_WRITE_SIZE;
        if (atomic) {
            final RandomBlobContentBytesReference bytesReference = new RandomBlobContentBytesReference(
                content,
                Math.toIntExact(request.blobSize)
            );
            blobContainer.writeBlobAtomic(OperationPurpose.REPOSITORY_ANALYSIS, request.blobName, bytesReference, true);
        } else {
            blobContainer.writeBlob(
                OperationPurpose.REPOSITORY_ANALYSIS,
                request.blobName,
                repository.maybeRateLimitSnapshots(new RandomBlobContentStream(content, request.blobSize), (ignored) -> {}),
                request.blobSize,
                true
            );
        }
    }

    public static class Request extends LegacyActionRequest {

        private final String repositoryName;
        private final String blobPath;
        private final String blobName;
        private final long blobSize;
        private final long seed;

        Request(String repositoryName, String blobPath, String blobName, long blobSize, long seed) {
            this.repositoryName = repositoryName;
            this.blobPath = blobPath;
            this.blobName = blobName;
            this.blobSize = blobSize;
            this.seed = seed;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            blobPath = in.readString();
            blobName = in.readString();
            blobSize = in.readVLong();
            seed = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeString(blobPath);
            out.writeString(blobName);
            out.writeVLong(blobSize);
            out.writeLong(seed);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        public String getDescription() {
            return "blob overwrite [" + repositoryName + ":" + blobPath + "/" + blobName + ", size=" + blobSize + ", seed=" + seed + "]";
        }

        String repository() {
            return repositoryName;
        }

        String blobPath() {
            return blobPath;
        }

        String blobName() {
            return blobName;
        }

        @Override
        public String toString() {
            return "BlobOverwriteAction.Request{" + getDescription() + "}";
        }
    }

    public static class Response extends ActionResponse {

        private final boolean writeSuccess;

        Response(boolean writeSuccess) {
            this.writeSuccess = writeSuccess;
        }

        Response(StreamInput in) throws IOException {
            writeSuccess = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(writeSuccess);
        }

        boolean writeSuccess() {
            return writeSuccess;
        }
    }
}
