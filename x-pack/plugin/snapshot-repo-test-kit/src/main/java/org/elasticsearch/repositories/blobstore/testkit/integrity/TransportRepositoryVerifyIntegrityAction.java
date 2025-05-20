/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

/**
 * Transport action that actually runs the {@link RepositoryIntegrityVerifier} and sends response chunks back to the coordinating node.
 */
class TransportRepositoryVerifyIntegrityAction extends HandledTransportAction<
    TransportRepositoryVerifyIntegrityAction.Request,
    RepositoryVerifyIntegrityResponse> {

    // NB runs on the master because that's the expected place to read metadata blobs from the repository, but not an actual
    // TransportMasterNodeAction since we don't want to retry on a master failover

    static final String ACTION_NAME = TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE.name() + "[m]";
    private final RepositoriesService repositoriesService;
    private final TransportService transportService;
    private final Executor executor;

    TransportRepositoryVerifyIntegrityAction(
        TransportService transportService,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        Executor executor
    ) {
        super(ACTION_NAME, transportService, actionFilters, Request::new, executor);
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.executor = executor;
    }

    static class Request extends LegacyActionRequest {
        private final DiscoveryNode coordinatingNode;
        private final long coordinatingTaskId;
        private final RepositoryVerifyIntegrityParams requestParams;

        Request(DiscoveryNode coordinatingNode, long coordinatingTaskId, RepositoryVerifyIntegrityParams requestParams) {
            this.coordinatingNode = coordinatingNode;
            this.coordinatingTaskId = coordinatingTaskId;
            this.requestParams = Objects.requireNonNull(requestParams);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            coordinatingNode = new DiscoveryNode(in);
            coordinatingTaskId = in.readVLong();
            requestParams = new RepositoryVerifyIntegrityParams(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            coordinatingNode.writeTo(out);
            out.writeVLong(coordinatingTaskId);
            requestParams.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new RepositoryVerifyIntegrityTask(id, type, action, getDescription(), parentTaskId, headers);
        }
    }

    @Override
    protected void doExecute(Task rawTask, Request request, ActionListener<RepositoryVerifyIntegrityResponse> listener) {
        final var responseWriter = new RepositoryVerifyIntegrityResponseChunk.Writer() {

            // no need to obtain a fresh connection each time - this connection shouldn't close, so if it does we can fail the verification
            final Transport.Connection responseConnection = transportService.getConnection(request.coordinatingNode);

            @Override
            public void writeResponseChunk(RepositoryVerifyIntegrityResponseChunk responseChunk, ActionListener<Void> listener) {
                transportService.sendChildRequest(
                    responseConnection,
                    TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
                    new TransportRepositoryVerifyIntegrityResponseChunkAction.Request(request.coordinatingTaskId, responseChunk),
                    rawTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<TransportResponse>(
                        listener.map(ignored -> null),
                        in -> ActionResponse.Empty.INSTANCE,
                        executor
                    )
                );
            }
        };

        final LongSupplier currentTimeMillisSupplier = transportService.getThreadPool()::absoluteTimeInMillis;
        final var repository = (BlobStoreRepository) repositoriesService.repository(request.requestParams.repository());
        final var task = (RepositoryVerifyIntegrityTask) rawTask;

        SubscribableListener

            .<RepositoryData>newForked(l -> repository.getRepositoryData(executor, l))
            .andThenApply(repositoryData -> {
                final var cancellableThreads = new CancellableThreads();
                task.addListener(() -> cancellableThreads.cancel("task cancelled"));
                final var verifier = new RepositoryIntegrityVerifier(
                    currentTimeMillisSupplier,
                    repository,
                    responseWriter,
                    request.requestParams.withResolvedDefaults(repository.threadPool().info(ThreadPool.Names.SNAPSHOT_META)),
                    repositoryData,
                    cancellableThreads
                );
                task.setStatusSupplier(verifier::getStatus);
                return verifier;
            })
            .<RepositoryIntegrityVerifier>andThen(
                (l, repositoryIntegrityVerifier) -> new RepositoryVerifyIntegrityResponseChunk.Builder(
                    responseWriter,
                    RepositoryVerifyIntegrityResponseChunk.Type.START_RESPONSE,
                    currentTimeMillisSupplier.getAsLong()
                ).write(l.map(ignored -> repositoryIntegrityVerifier))
            )
            .<RepositoryVerifyIntegrityResponse>andThen((l, repositoryIntegrityVerifier) -> repositoryIntegrityVerifier.start(l))
            .addListener(listener);
    }
}
