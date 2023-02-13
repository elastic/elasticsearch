/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.integrity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.isNullOrBlank;
import static org.elasticsearch.core.Strings.format;

public class VerifyRepositoryIntegrityAction extends ActionType<VerifyRepositoryIntegrityAction.Response> {

    public static final VerifyRepositoryIntegrityAction INSTANCE = new VerifyRepositoryIntegrityAction();
    public static final String NAME = "cluster:admin/repository/verify_integrity";

    private VerifyRepositoryIntegrityAction() {
        super(NAME, VerifyRepositoryIntegrityAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String repository;
        private final String resultsIndex;
        private final int metaThreadPoolConcurrency;
        private final int blobThreadPoolConcurrency;
        private final int snapshotVerificationConcurrency;
        private final int indexVerificationConcurrency;
        private final int indexSnapshotVerificationConcurrency;
        private final boolean verifyBlobContents;
        private final ByteSizeValue maxBytesPerSec;
        private final boolean waitForCompletion;

        public Request(
            String repository,
            String resultsIndex,
            int metaThreadPoolConcurrency,
            int blobThreadPoolConcurrency,
            int snapshotVerificationConcurrency,
            int indexVerificationConcurrency,
            int indexSnapshotVerificationConcurrency,
            boolean verifyBlobContents,
            ByteSizeValue maxBytesPerSec,
            boolean waitForCompletion
        ) {
            this.repository = Objects.requireNonNull(repository, "repository");
            this.resultsIndex = Objects.requireNonNull(resultsIndex, "resultsIndex");
            this.metaThreadPoolConcurrency = requireNonNegative("metaThreadPoolConcurrency", metaThreadPoolConcurrency);
            this.blobThreadPoolConcurrency = requireNonNegative("blobThreadPoolConcurrency", blobThreadPoolConcurrency);
            this.snapshotVerificationConcurrency = requireNonNegative("snapshotVerificationConcurrency", snapshotVerificationConcurrency);
            this.indexVerificationConcurrency = requireNonNegative("indexVerificationConcurrency", indexVerificationConcurrency);
            this.indexSnapshotVerificationConcurrency = requireNonNegative(
                "indexSnapshotVerificationConcurrency",
                indexSnapshotVerificationConcurrency
            );
            this.verifyBlobContents = verifyBlobContents;
            if (maxBytesPerSec.getBytes() < 1) {
                throw new IllegalArgumentException("invalid rate limit");
            }
            this.maxBytesPerSec = maxBytesPerSec;
            this.waitForCompletion = waitForCompletion;
        }

        private static int requireNonNegative(String name, int value) {
            if (value < 0) {
                throw new IllegalArgumentException("argument [" + name + "] must be at least [0]");
            }
            return value;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.repository = in.readString();
            this.resultsIndex = in.readString();
            this.metaThreadPoolConcurrency = in.readVInt();
            this.blobThreadPoolConcurrency = in.readVInt();
            this.snapshotVerificationConcurrency = in.readVInt();
            this.indexVerificationConcurrency = in.readVInt();
            this.indexSnapshotVerificationConcurrency = in.readVInt();
            this.verifyBlobContents = in.readBoolean();
            this.maxBytesPerSec = ByteSizeValue.readFrom(in);
            this.waitForCompletion = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(resultsIndex);
            out.writeVInt(metaThreadPoolConcurrency);
            out.writeVInt(blobThreadPoolConcurrency);
            out.writeVInt(snapshotVerificationConcurrency);
            out.writeVInt(indexVerificationConcurrency);
            out.writeVInt(indexSnapshotVerificationConcurrency);
            out.writeBoolean(verifyBlobContents);
            maxBytesPerSec.writeTo(out);
            out.writeBoolean(waitForCompletion);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new VerifyRepositoryIntegrityAction.Task(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "verify repository integrity {"
                + "repository='"
                + repository
                + '\''
                + ", resultsIndex='"
                + resultsIndex
                + '\''
                + ", metaThreadPoolConcurrency="
                + metaThreadPoolConcurrency
                + ", blobThreadPoolConcurrency="
                + blobThreadPoolConcurrency
                + ", snapshotVerificationConcurrency="
                + snapshotVerificationConcurrency
                + ", indexVerificationConcurrency="
                + indexVerificationConcurrency
                + ", indexSnapshotVerificationConcurrency="
                + indexSnapshotVerificationConcurrency
                + ", verifyBlobContents="
                + verifyBlobContents
                + ", maxBytesPerSec="
                + maxBytesPerSec
                + ", waitForCompletion="
                + waitForCompletion
                + '}';
        }

        public String getRepository() {
            return repository;
        }

        public String getResultsIndex() {
            return resultsIndex;
        }

        public int getMetaThreadPoolConcurrency() {
            return metaThreadPoolConcurrency;
        }

        public int getBlobThreadPoolConcurrency() {
            return blobThreadPoolConcurrency;
        }

        public int getSnapshotVerificationConcurrency() {
            return snapshotVerificationConcurrency;
        }

        public int getIndexVerificationConcurrency() {
            return indexVerificationConcurrency;
        }

        public int getIndexSnapshotVerificationConcurrency() {
            return indexSnapshotVerificationConcurrency;
        }

        public boolean getVerifyBlobContents() {
            return verifyBlobContents;
        }

        public ByteSizeValue getMaxBytesPerSec() {
            return maxBytesPerSec;
        }

        public boolean getWaitForCompletion() {
            return waitForCompletion;
        }

        public Request withResolvedDefaults(long currentTimeMillis, ThreadPool.Info metadataThreadPoolInfo) {
            if (isNullOrBlank(resultsIndex) == false
                && metaThreadPoolConcurrency > 0
                && blobThreadPoolConcurrency > 0
                && snapshotVerificationConcurrency > 0
                && indexVerificationConcurrency > 0
                && indexSnapshotVerificationConcurrency > 0) {
                return this;
            }

            final var maxThreads = Math.max(1, metadataThreadPoolInfo.getMax());
            final var halfMaxThreads = Math.max(1, maxThreads / 2);
            final var request = new Request(
                repository,
                isNullOrBlank(resultsIndex) ? ("repository-metadata-verification-" + repository + "-" + currentTimeMillis) : resultsIndex,
                metaThreadPoolConcurrency > 0 ? metaThreadPoolConcurrency : halfMaxThreads,
                blobThreadPoolConcurrency > 0 ? blobThreadPoolConcurrency : 1,
                snapshotVerificationConcurrency > 0 ? snapshotVerificationConcurrency : halfMaxThreads,
                indexVerificationConcurrency > 0 ? indexVerificationConcurrency : maxThreads,
                indexSnapshotVerificationConcurrency > 0 ? indexSnapshotVerificationConcurrency : 1,
                verifyBlobContents,
                maxBytesPerSec,
                waitForCompletion
            );
            request.masterNodeTimeout(masterNodeTimeout());
            return request;
        }

        @Override
        public String toString() {
            return getDescription();
        }
    }

    public static class Task extends CancellableTask {

        private volatile Supplier<VerifyRepositoryIntegrityAction.Status> statusSupplier;

        public Task(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
        }

        public void setStatusSupplier(Supplier<VerifyRepositoryIntegrityAction.Status> statusSupplier) {
            this.statusSupplier = statusSupplier;
        }

        @Override
        public Status getStatus() {
            return Optional.ofNullable(statusSupplier).map(Supplier::get).orElse(null);
        }
    }

    public record Status(
        String repositoryName,
        long repositoryGeneration,
        String repositoryUUID,
        long snapshotCount,
        long snapshotsVerified,
        long indexCount,
        long indicesVerified,
        long indexSnapshotCount,
        long indexSnapshotsVerified,
        long blobsVerified,
        long blobBytesVerified,
        long throttledNanos,
        long anomalyCount,
        String resultsIndex
    ) implements org.elasticsearch.tasks.Task.Status {

        public static String NAME = "verify_repository_status";

        public Status(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readVLong(),
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeVLong(repositoryGeneration);
            out.writeString(repositoryUUID);
            out.writeVLong(snapshotCount);
            out.writeVLong(snapshotsVerified);
            out.writeVLong(indexCount);
            out.writeVLong(indicesVerified);
            out.writeVLong(indexSnapshotCount);
            out.writeVLong(indexSnapshotsVerified);
            out.writeVLong(blobsVerified);
            out.writeVLong(blobBytesVerified);
            out.writeVLong(throttledNanos);
            out.writeVLong(anomalyCount);
            out.writeString(resultsIndex);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("repository");
            builder.field("name", repositoryName);
            builder.field("uuid", repositoryUUID);
            builder.field("generation", repositoryGeneration);
            builder.endObject();
            builder.startObject("snapshots");
            builder.field("verified", snapshotsVerified);
            builder.field("total", snapshotCount);
            builder.endObject();
            builder.startObject("indices");
            builder.field("verified", indicesVerified);
            builder.field("total", indexCount);
            builder.endObject();
            builder.startObject("index_snapshots");
            builder.field("verified", indexSnapshotsVerified);
            builder.field("total", indexSnapshotCount);
            builder.endObject();
            builder.startObject("blobs");
            builder.field("verified", blobsVerified);
            builder.humanReadableField("verified_size_in_bytes", "verified_size", ByteSizeValue.ofBytes(blobBytesVerified));
            if (throttledNanos > 0) {
                builder.humanReadableField("throttled_time_in_millis", "throttled_time", TimeValue.timeValueNanos(throttledNanos));
            }
            builder.endObject();
            builder.field("anomalies", anomalyCount);
            builder.field("results_index", resultsIndex);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TaskId taskId;
        private final String repositoryName;
        private final long repositoryGeneration;
        private final String repositoryUUID;
        private final long snapshotCount;
        private final long indexCount;
        private final long indexSnapshotCount;
        private final String resultsIndex;

        public Response(
            TaskId taskId,
            String repositoryName,
            long repositoryGeneration,
            String repositoryUUID,
            long snapshotCount,
            long indexCount,
            long indexSnapshotCount,
            String resultsIndex
        ) {
            this.taskId = taskId;
            this.repositoryName = repositoryName;
            this.repositoryGeneration = repositoryGeneration;
            this.repositoryUUID = repositoryUUID;
            this.snapshotCount = snapshotCount;
            this.indexCount = indexCount;
            this.indexSnapshotCount = indexSnapshotCount;
            this.resultsIndex = resultsIndex;
        }

        public Response(StreamInput in) throws IOException {
            this(
                TaskId.readFromStream(in),
                in.readString(),
                in.readVLong(),
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            taskId.writeTo(out);
            out.writeString(repositoryName);
            out.writeVLong(repositoryGeneration);
            out.writeString(repositoryUUID);
            out.writeVLong(snapshotCount);
            out.writeVLong(indexCount);
            out.writeVLong(indexSnapshotCount);
            out.writeString(resultsIndex);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("task", taskId.toString());
            builder.startObject("repository");
            builder.field("name", repositoryName);
            builder.field("uuid", repositoryUUID);
            builder.field("generation", repositoryGeneration);
            builder.endObject();
            builder.startObject("snapshots");
            builder.field("total", snapshotCount);
            builder.endObject();
            builder.startObject("indices");
            builder.field("total", indexCount);
            builder.endObject();
            builder.startObject("index_snapshots");
            builder.field("total", indexSnapshotCount);
            builder.endObject();
            builder.field("results_index", resultsIndex);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this, false, false);
        }

        public TaskId getTaskId() {
            return taskId;
        }

        public String getResultsIndex() {
            return resultsIndex;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final RepositoriesService repositoriesService;
        private final NodeClient client;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            NodeClient client
        ) {
            super(
                NAME,
                true,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                Response::new,
                ThreadPool.Names.SNAPSHOT_META
            );
            this.repositoriesService = repositoriesService;
            this.client = client;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected void masterOperation(
            org.elasticsearch.tasks.Task task,
            Request request,
            ClusterState state,
            ActionListener<Response> listener
        ) {
            final BlobStoreRepository repository;
            if (repositoriesService.repository(request.getRepository())instanceof BlobStoreRepository blobStoreRepository) {
                repository = blobStoreRepository;
            } else {
                throw new UnsupportedOperationException(
                    format("repository [%s] does not support metadata verification", request.getRepository())
                );
            }

            // TODO add docs about blob deletions while this is running
            final var foregroundTask = (Task) task;
            final var backgroundTask = (Task) taskManager.register("background", task.getAction(), request);
            final var cancellableThreads = new CancellableThreads();

            foregroundTask.addListener(() -> cancellableThreads.cancel("foreground task cancelled"));
            backgroundTask.addListener(() -> cancellableThreads.cancel("task cancelled"));

            final ClusterStateListener noLongerMasterListener = event -> {
                if (event.localNodeMaster() == false) {
                    cancellableThreads.cancel("no longer master");
                }
            };
            clusterService.addListener(noLongerMasterListener);

            final Runnable cleanup = () -> Releasables.closeExpectNoException(
                () -> cancellableThreads.cancel("end of task"),
                () -> clusterService.removeListener(noLongerMasterListener),
                () -> taskManager.unregister(backgroundTask)
            );

            final ActionListener<Void> backgroundTaskListener;
            final ActionListener<Response> foregroundTaskListener;
            if (request.getWaitForCompletion()) {
                final var responseListener = new ListenableActionFuture<Response>();
                final var finalListener = ActionListener.notifyOnce(listener);
                foregroundTaskListener = finalListener.delegateFailure((l, r) -> responseListener.onResponse(r));
                backgroundTaskListener = ActionListener.runAfter(
                    finalListener.delegateFailure((l, v) -> responseListener.addListener(l)),
                    cleanup
                );
            } else {
                foregroundTaskListener = listener;
                backgroundTaskListener = ActionListener.wrap(cleanup);
            }

            MetadataVerifier.run(
                repository,
                client,
                request.withResolvedDefaults(
                    clusterService.threadPool().absoluteTimeInMillis(),
                    clusterService.threadPool().info(ThreadPool.Names.SNAPSHOT_META)
                ),
                cancellableThreads,
                backgroundTask,
                foregroundTaskListener.delegateResponse(
                    (l, e) -> Releasables.closeExpectNoException(() -> backgroundTaskListener.onFailure(e), () -> l.onFailure(e))
                ),
                backgroundTaskListener
            );
        }
    }
}
