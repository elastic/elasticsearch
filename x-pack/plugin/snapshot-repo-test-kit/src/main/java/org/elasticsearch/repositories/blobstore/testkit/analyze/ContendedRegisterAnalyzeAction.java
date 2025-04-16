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
import org.elasticsearch.action.ActionRunnable;
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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * An action which atomically increments a register using {@link BlobContainer#compareAndExchangeRegister}. There will be multiple parties
 * accessing the register concurrently in order to test behaviour under contention.
 */
class ContendedRegisterAnalyzeAction extends HandledTransportAction<ContendedRegisterAnalyzeAction.Request, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(ContendedRegisterAnalyzeAction.class);

    static final String NAME = "cluster:admin/repository/analyze/register";

    private final RepositoriesService repositoriesService;
    private final Executor executor;

    ContendedRegisterAnalyzeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT));
        this.repositoriesService = repositoriesService;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> outerListenerOld) {
        final var outerListener = ActionListener.assertOnce(outerListenerOld);
        final Repository repository = repositoriesService.repository(request.getRepositoryName());
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob-store repository");
        }
        if (repository.isReadOnly()) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is read-only");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobPath path = blobStoreRepository.basePath().add(request.getContainerPath());
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        logger.trace("handling [{}]", request);

        assert task instanceof CancellableTask;

        final String registerName = request.getRegisterName();
        final ActionListener<OptionalBytesReference> initialValueListener = new ActionListener<>() {
            @Override
            public void onResponse(OptionalBytesReference maybeInitialBytes) {
                final long initialValue = maybeInitialBytes.isPresent() ? longFromBytes(maybeInitialBytes.bytesReference()) : 0L;

                ActionListener.run(outerListener.<Void>map(ignored -> ActionResponse.Empty.INSTANCE), l -> {
                    if (initialValue < 0 || initialValue >= request.getRequestCount()) {
                        throw new IllegalStateException("register holds unexpected value [" + initialValue + "]");
                    }

                    class Execution extends ActionRunnable<Void> {
                        private long currentValue;

                        private final ActionListener<OptionalBytesReference> witnessListener;

                        Execution(long currentValue) {
                            super(l);
                            this.currentValue = currentValue;
                            this.witnessListener = listener.delegateFailure(this::handleWitness);
                        }

                        @Override
                        protected void doRun() {
                            if (((CancellableTask) task).notifyIfCancelled(listener) == false) {
                                blobContainer.compareAndExchangeRegister(
                                    OperationPurpose.REPOSITORY_ANALYSIS,
                                    registerName,
                                    bytesFromLong(currentValue),
                                    bytesFromLong(currentValue + 1L),
                                    witnessListener
                                );
                            }
                        }

                        private void handleWitness(ActionListener<Void> delegate, OptionalBytesReference witnessOrEmpty) {
                            if (witnessOrEmpty.isPresent() == false) {
                                // Concurrent activity prevented us from updating the value, or even reading the concurrently-updated
                                // result, so we must just try again.
                                executor.execute(Execution.this);
                                return;
                            }

                            final long witness = longFromBytes(witnessOrEmpty.bytesReference());
                            if (witness == currentValue) {
                                delegate.onResponse(null);
                            } else if (witness < currentValue || witness >= request.getRequestCount()) {
                                delegate.onFailure(new IllegalStateException("register holds unexpected value [" + witness + "]"));
                            } else {
                                currentValue = witness;
                                executor.execute(Execution.this);
                            }
                        }

                    }

                    new Execution(initialValue).run();

                });
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof UnsupportedOperationException) {
                    // Registers are not supported on all repository types, and that's ok. If it's not supported here then the final
                    // check will also be unsupported, so it doesn't matter that we didn't do anything before this successful response.
                    outerListener.onResponse(ActionResponse.Empty.INSTANCE);
                } else {
                    outerListener.onFailure(e);
                }
            }
        };

        if (request.getInitialRead() > request.getRequestCount()) {
            blobContainer.getRegister(OperationPurpose.REPOSITORY_ANALYSIS, registerName, initialValueListener.delegateFailure((l, r) -> {
                if (r.isPresent()) {
                    l.onResponse(r);
                } else {
                    l.onFailure(new IllegalStateException("register read failed due to contention"));
                }
            }));
        } else {
            blobContainer.compareAndExchangeRegister(
                OperationPurpose.REPOSITORY_ANALYSIS,
                registerName,
                bytesFromLong(request.getInitialRead()),
                bytesFromLong(
                    request.getInitialRead() == request.getRequestCount() ? request.getRequestCount() + 1 : request.getInitialRead()
                ),
                initialValueListener
            );
        }
    }

    static class Request extends ActionRequest {
        private final String repositoryName;
        private final String containerPath;
        private final String registerName;
        private final int requestCount;
        private final int initialRead;

        Request(String repositoryName, String containerPath, String registerName, int requestCount, int initialRead) {
            this.repositoryName = repositoryName;
            this.containerPath = containerPath;
            this.registerName = registerName;
            this.requestCount = requestCount;
            this.initialRead = initialRead;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            assert in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0);
            repositoryName = in.readString();
            containerPath = in.readString();
            registerName = in.readString();
            requestCount = in.readVInt();
            initialRead = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0);
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeString(containerPath);
            out.writeString(registerName);
            out.writeVInt(requestCount);
            out.writeVInt(initialRead);
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

        int getRequestCount() {
            return requestCount;
        }

        int getInitialRead() {
            return initialRead;
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
                    ContendedRegisterAnalyzeAction.Request{\
                    repositoryName='%s', containerPath='%s', registerName='%s', requestCount='%d', initialRead='%d'}""",
                repositoryName,
                containerPath,
                registerName,
                requestCount,
                initialRead
            );
        }
    }

    static long longFromBytes(BytesReference bytesReference) {
        if (bytesReference.length() == 0) {
            return 0L;
        } else if (bytesReference.length() == Long.BYTES) {
            try (var baos = new ByteArrayOutputStream(Long.BYTES)) {
                bytesReference.writeTo(baos);
                final var bytes = baos.toByteArray();
                assert bytes.length == Long.BYTES;
                return ByteUtils.readLongBE(bytes, 0);
            } catch (IOException e) {
                assert false : "no IO takes place";
                throw new IllegalStateException("unexpected conversion error", e);
            }
        } else {
            throw new IllegalArgumentException("cannot read long from BytesReference of length " + bytesReference.length());
        }
    }

    static BytesReference bytesFromLong(long value) {
        if (value == 0L) {
            return BytesArray.EMPTY;
        } else {
            final var bytes = new byte[Long.BYTES];
            ByteUtils.writeLongBE(value, bytes, 0);
            return new BytesArray(bytes);
        }
    }
}
