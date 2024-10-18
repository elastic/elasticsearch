/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.repositories.blobstore.testkit.analyze.ContendedRegisterAnalyzeAction.bytesFromLong;
import static org.elasticsearch.repositories.blobstore.testkit.analyze.ContendedRegisterAnalyzeAction.longFromBytes;

class UncontendedRegisterAnalyzeAction extends HandledTransportAction<UncontendedRegisterAnalyzeAction.Request, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(UncontendedRegisterAnalyzeAction.class);

    static final String NAME = "cluster:admin/repository/analyze/register/uncontended";

    private final RepositoriesService repositoriesService;

    UncontendedRegisterAnalyzeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT));
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        logger.trace("handling [{}]", request);
        updateRegister(
            request,
            bytesFromLong(request.getExpectedValue() + 1),
            repositoriesService.repository(request.getRepositoryName()),
            ActionListener.assertOnce(listener.map(ignored -> ActionResponse.Empty.INSTANCE))
        );
    }

    static void verifyFinalValue(Request request, Repository repository, ActionListener<Void> listener) {
        // ensure that the repo accepts an empty register
        logger.trace("handling final value [{}]", request);
        updateRegister(request, BytesArray.EMPTY, repository, ActionListener.assertOnce(listener));
    }

    private static void updateRegister(Request request, BytesReference newValue, Repository repository, ActionListener<Void> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob-store repository");
        }
        if (repository.isReadOnly()) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is read-only");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobPath path = blobStoreRepository.basePath().add(request.getContainerPath());
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        blobContainer.compareAndExchangeRegister(
            OperationPurpose.REPOSITORY_ANALYSIS,
            request.getRegisterName(),
            bytesFromLong(request.getExpectedValue()),
            newValue,
            new ActionListener<>() {
                @Override
                public void onResponse(OptionalBytesReference optionalBytesReference) {
                    ActionListener.completeWith(listener, () -> {
                        if (optionalBytesReference.isPresent() == false) {
                            throw new RepositoryVerificationException(
                                repository.getMetadata().name(),
                                Strings.format(
                                    "uncontended register operation failed: expected [%d] but did not observe any value",
                                    request.getExpectedValue()
                                )
                            );
                        }

                        final var witness = longFromBytes(optionalBytesReference.bytesReference());
                        if (witness != request.getExpectedValue()) {
                            throw new RepositoryVerificationException(
                                repository.getMetadata().name(),
                                Strings.format(
                                    "uncontended register operation failed: expected [%d] but observed [%d]",
                                    request.getExpectedValue(),
                                    witness
                                )
                            );
                        }

                        return null;
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof UnsupportedOperationException) {
                        // Registers are not supported on all repository types, and that's ok.
                        listener.onResponse(null);
                    } else {
                        listener.onFailure(e);
                    }
                }
            }
        );
    }

    static class Request extends ActionRequest {
        private final String repositoryName;
        private final String containerPath;
        private final String registerName;
        private final long expectedValue;

        Request(String repositoryName, String containerPath, String registerName, long expectedValue) {
            this.repositoryName = repositoryName;
            this.containerPath = containerPath;
            this.registerName = registerName;
            this.expectedValue = expectedValue;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            assert in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0);
            repositoryName = in.readString();
            containerPath = in.readString();
            registerName = in.readString();
            expectedValue = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0);
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeString(containerPath);
            out.writeString(registerName);
            out.writeVLong(expectedValue);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        String getRepositoryName() {
            return repositoryName;
        }

        String getContainerPath() {
            return containerPath;
        }

        String getRegisterName() {
            return registerName;
        }

        long getExpectedValue() {
            return expectedValue;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String toString() {
            return getDescription();
        }

        @Override
        public String getDescription() {
            return Strings.format(
                """
                    UncontendedRegisterAnalyzeAction.Request{\
                    repositoryName='%s', containerPath='%s', registerName='%s', expectedValue='%d'}""",
                repositoryName,
                containerPath,
                registerName,
                expectedValue
            );
        }
    }
}
