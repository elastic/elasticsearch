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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Action which instructs a node to read a range of a blob from a {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository}
 * (possibly the entire blob) and compute its checksum. It is acceptable if the blob is not found but we do not accept the blob being
 * otherwise unreadable.
 */
class GetBlobChecksumAction extends HandledTransportAction<GetBlobChecksumAction.Request, GetBlobChecksumAction.Response> {

    private static final Logger logger = LogManager.getLogger(GetBlobChecksumAction.class);

    static final String NAME = "cluster:admin/repository/analyze/blob/read";

    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(8);

    private final RepositoriesService repositoriesService;

    GetBlobChecksumAction(TransportService transportService, ActionFilters actionFilters, RepositoriesService repositoriesService) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT));
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        assert task instanceof CancellableTask;
        CancellableTask cancellableTask = (CancellableTask) task;

        final Repository repository = repositoriesService.repository(request.getRepositoryName());
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob store repository");
        }

        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobContainer blobContainer = blobStoreRepository.blobStore()
            .blobContainer(blobStoreRepository.basePath().add(request.getBlobPath()));

        logger.trace("handling [{}]", request);

        final InputStream rawInputStream;
        try {
            if (request.isWholeBlob()) {
                rawInputStream = blobContainer.readBlob(OperationPurpose.REPOSITORY_ANALYSIS, request.getBlobName());
            } else {
                rawInputStream = blobContainer.readBlob(
                    OperationPurpose.REPOSITORY_ANALYSIS,
                    request.getBlobName(),
                    request.getRangeStart(),
                    request.getRangeLength()
                );
            }
        } catch (FileNotFoundException | NoSuchFileException e) {
            logger.trace("blob not found for [{}]", request);
            listener.onResponse(Response.BLOB_NOT_FOUND);
            return;
        } catch (IOException e) {
            logger.warn("failed to read blob for [{}]", request);
            listener.onFailure(e);
            return;
        }

        logger.trace("reading blob for [{}]", request);

        final AtomicLong throttleNanos = new AtomicLong();
        final InputStream throttledInputStream = blobStoreRepository.maybeRateLimitRestores(rawInputStream, throttleNanos::addAndGet);
        final CRC32 crc32 = new CRC32();
        final byte[] buffer = new byte[BUFFER_SIZE];
        long bytesRead = 0L;
        final long startTimeNanos = System.nanoTime();
        long firstByteNanos = startTimeNanos;

        boolean success = false;
        try {
            while (true) {
                final int readSize;
                try {
                    readSize = throttledInputStream.read(buffer, 0, buffer.length);
                } catch (IOException e) {
                    logger.warn("exception while read blob for [{}]", request);
                    listener.onFailure(e);
                    return;
                }

                if (readSize == -1) {
                    break;
                }

                if (readSize > 0) {
                    if (bytesRead == 0L) {
                        firstByteNanos = System.nanoTime();
                    }

                    crc32.update(buffer, 0, readSize);
                    bytesRead += readSize;
                }

                if (cancellableTask.isCancelled()) {
                    throw new RepositoryVerificationException(
                        request.repositoryName,
                        "cancelled [" + request.getDescription() + "] after reading [" + bytesRead + "] bytes"
                    );
                }
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(throttledInputStream);
            }
        }
        try {
            throttledInputStream.close();
        } catch (IOException e) {
            throw new RepositoryVerificationException(
                request.repositoryName,
                "failed to close input stream when handling [" + request.getDescription() + "]",
                e
            );
        }

        final long endTimeNanos = System.nanoTime();

        if (request.isWholeBlob() == false && bytesRead != request.getRangeLength()) {
            throw new RepositoryVerificationException(
                request.repositoryName,
                "unexpectedly read [" + bytesRead + "] bytes when handling [" + request.getDescription() + "]"
            );
        }

        final Response response = new Response(
            bytesRead,
            crc32.getValue(),
            firstByteNanos - startTimeNanos,
            endTimeNanos - startTimeNanos,
            throttleNanos.get()
        );
        logger.trace("responding to [{}] with [{}]", request, response);
        listener.onResponse(response);
    }

    static class Request extends ActionRequest {

        private final String repositoryName;
        private final String blobPath;
        private final String blobName;

        // Requesting the range [rangeStart, rangeEnd), but we treat the range [0, 0) as a special case indicating to read the whole blob.
        private final long rangeStart;
        private final long rangeEnd;

        Request(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            blobPath = in.readString();
            blobName = in.readString();
            rangeStart = in.readVLong();
            rangeEnd = in.readVLong();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        Request(String repositoryName, String blobPath, String blobName, long rangeStart, long rangeEnd) {
            assert rangeStart == 0L && rangeEnd == 0L || 0L <= rangeStart && rangeStart < rangeEnd : rangeStart + "-" + rangeEnd;
            this.repositoryName = repositoryName;
            this.blobPath = blobPath;
            this.blobName = blobName;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
        }

        String getRepositoryName() {
            return repositoryName;
        }

        String getBlobPath() {
            return blobPath;
        }

        String getBlobName() {
            return blobName;
        }

        long getRangeStart() {
            assert isWholeBlob() == false;
            return rangeStart;
        }

        long getRangeEnd() {
            assert isWholeBlob() == false;
            return rangeEnd;
        }

        long getRangeLength() {
            assert isWholeBlob() == false;
            return rangeEnd - rangeStart;
        }

        /**
         * @return whether we should read the whole blob or a range.
         */
        boolean isWholeBlob() {
            return rangeEnd == 0L;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeString(blobPath);
            out.writeString(blobName);
            out.writeVLong(rangeStart);
            out.writeVLong(rangeEnd);
        }

        @Override
        public String getDescription() {
            return "retrieve ["
                + (isWholeBlob() ? "whole blob" : (getRangeStart() + "-" + getRangeEnd()))
                + "] from ["
                + getRepositoryName()
                + ":"
                + getBlobPath()
                + "/"
                + getBlobName()
                + "]";
        }

        @Override
        public String toString() {
            return "GetRepositoryBlobChecksumRequest{" + getDescription() + "}";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false; // no children
                }
            };
        }
    }

    static class Response extends ActionResponse {

        static Response BLOB_NOT_FOUND = new Response(0L, 0L, 0L, 0L, 0L);

        private final long bytesRead; // 0 if not found
        private final long checksum; // 0 if not found
        private final long firstByteNanos; // 0 if not found
        private final long elapsedNanos; // 0 if not found
        private final long throttleNanos; // 0 if not found

        Response(long bytesRead, long checksum, long firstByteNanos, long elapsedNanos, long throttleNanos) {
            this.bytesRead = bytesRead;
            this.checksum = checksum;
            this.firstByteNanos = firstByteNanos;
            this.elapsedNanos = elapsedNanos;
            this.throttleNanos = throttleNanos;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            this.bytesRead = in.readVLong();
            this.checksum = in.readLong();
            this.firstByteNanos = in.readVLong();
            this.elapsedNanos = in.readVLong();
            this.throttleNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(bytesRead);
            out.writeLong(checksum);
            out.writeVLong(firstByteNanos);
            out.writeVLong(elapsedNanos);
            out.writeVLong(throttleNanos);
        }

        @Override
        public String toString() {
            return "GetRepositoryBlobChecksumResponse{"
                + "bytesRead="
                + bytesRead
                + ", checksum="
                + checksum
                + ", firstByteNanos="
                + firstByteNanos
                + ", elapsedNanos="
                + elapsedNanos
                + ", throttleNanos="
                + throttleNanos
                + '}';
        }

        long getBytesRead() {
            return bytesRead;
        }

        long getChecksum() {
            return checksum;
        }

        long getFirstByteNanos() {
            return firstByteNanos;
        }

        long getElapsedNanos() {
            return elapsedNanos;
        }

        long getThrottleNanos() {
            return throttleNanos;
        }

        boolean isNotFound() {
            return bytesRead == 0L && checksum == 0L && firstByteNanos == 0L && elapsedNanos == 0L && throttleNanos == 0L;
        }

    }
}
