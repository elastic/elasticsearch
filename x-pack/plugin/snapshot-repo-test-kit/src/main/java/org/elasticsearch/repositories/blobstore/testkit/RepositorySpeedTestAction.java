/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Action which distributes a bunch of {@link BlobSpeedTestAction}s over the nodes in the cluster, with limited concurrency, and collects
 * the results. Tries to fail fast by cancelling everything if any child task fails, or the timeout is reached, to avoid consuming
 * unnecessary resources. On completion, does a best-effort wait until the blob list contains all the expected blobs, then deletes them all.
 */
public class RepositorySpeedTestAction extends ActionType<RepositorySpeedTestAction.Response> {

    private static final Logger logger = LogManager.getLogger(RepositorySpeedTestAction.class);

    public static final RepositorySpeedTestAction INSTANCE = new RepositorySpeedTestAction();
    public static final String NAME = "cluster:admin/repository/speed_test";

    private RepositorySpeedTestAction() {
        super(NAME, Response::new);
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {
        // Needs to run on a non-voting-only master-eligible node or a data node so that it has access to the repository so it can clean up
        // at the end. However today there's no good way to reroute an action onto a suitable node apart from routing it tothe elected
        // master, so we run it there. This should be no big deal, coordinating the test isn't particularly heavy.

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            RepositoriesService repositoriesService,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                RepositorySpeedTestAction.Request::new,
                indexNameExpressionResolver,
                RepositorySpeedTestAction.Response::new,
                ThreadPool.Names.SAME
            );
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
            final Repository repository = repositoriesService.repository(request.getRepositoryName());
            if (repository instanceof BlobStoreRepository == false) {
                throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob-store repository");
            }
            if (repository.isReadOnly()) {
                throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is read-only");
            }

            request.reseed(threadPool.relativeTimeInMillis());

            assert task instanceof CancellableTask;
            new AsyncAction(
                transportService,
                (BlobStoreRepository) repository,
                (CancellableTask) task,
                request,
                state.nodes(),
                threadPool::relativeTimeInMillis,
                listener
            ).run();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }
    }

    public static class AsyncAction {

        private final TransportService transportService;
        private final BlobStoreRepository repository;
        private final CancellableTask task;
        private final Request request;
        private final DiscoveryNodes discoveryNodes;
        private final LongSupplier currentTimeMillisSupplier;
        private final ActionListener<Response> listener;
        private final long timeoutTimeMillis;

        // choose the blob path nondeterministically to avoid clashes, assuming that the actual path doesn't matter for reproduction
        private final String blobPath = "temp-speed-test-" + UUIDs.randomBase64UUID();

        private final Queue<VerifyBlobTask> queue = ConcurrentCollections.newQueue();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final Semaphore innerFailures = new Semaphore(5); // limit the number of suppressed failures
        private final int workerCount;
        private final GroupedActionListener<Void> workersListener;
        private final Set<String> expectedBlobs = ConcurrentCollections.newConcurrentSet();
        private final List<BlobSpeedTestAction.Response> responses;

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private OptionalLong listingStartTimeNanos = OptionalLong.empty();
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private OptionalLong deleteStartTimeNanos = OptionalLong.empty();

        public AsyncAction(
            TransportService transportService,
            BlobStoreRepository repository,
            CancellableTask task,
            Request request,
            DiscoveryNodes discoveryNodes,
            LongSupplier currentTimeMillisSupplier,
            ActionListener<Response> listener
        ) {
            this.transportService = transportService;
            this.repository = repository;
            this.task = task;
            this.request = request;
            this.discoveryNodes = discoveryNodes;
            this.currentTimeMillisSupplier = currentTimeMillisSupplier;
            this.timeoutTimeMillis = currentTimeMillisSupplier.getAsLong() + request.getTimeout().millis();
            this.listener = listener;

            this.workerCount = request.getConcurrency();
            this.workersListener = new GroupedActionListener<>(
                new ThreadedActionListener<>(logger, transportService.getThreadPool(), ThreadPool.Names.SNAPSHOT, new ActionListener<>() {
                    @Override
                    public void onResponse(Collection<Void> voids) {
                        onWorkerCompletion();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert false : e; // workers should not fail
                        onWorkerCompletion();
                    }
                }, false),
                workerCount
            );

            responses = new ArrayList<>(request.blobCount);
        }

        private void fail(Exception e) {
            if (failure.compareAndSet(null, e)) {
                transportService.getTaskManager().cancelTaskAndDescendants(task, "task failed", false, ActionListener.wrap(() -> {}));
            } else {
                if (innerFailures.tryAcquire()) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof TaskCancelledException || cause instanceof ReceiveTimeoutTransportException) {
                        innerFailures.release();
                    } else {
                        failure.get().addSuppressed(e);
                    }
                }
            }
        }

        /**
         * Check that we haven't already failed or been cancelled or timed out; if newly cancelled or timed out then record this as the root
         * cause of failure.
         */
        private boolean isRunning() {
            if (failure.get() != null) {
                return false;
            }

            if (task.isCancelled()) {
                failure.compareAndSet(null, new RepositoryVerificationException(request.repositoryName, "verification cancelled"));
                // if this CAS failed then we're failing for some other reason, nbd; also if the task is cancelled then its descendants are
                // also cancelled, so no further action is needed either way.
                return false;
            }

            if (timeoutTimeMillis < currentTimeMillisSupplier.getAsLong()) {
                if (failure.compareAndSet(
                    null,
                    new RepositoryVerificationException(request.repositoryName, "speed test timed out after [" + request.getTimeout() + "]")
                )) {
                    transportService.getTaskManager().cancelTaskAndDescendants(task, "timed out", false, ActionListener.wrap(() -> {}));
                }
                // if this CAS failed then we're already failing for some other reason, nbd
                return false;
            }

            return true;
        }

        public void run() {
            assert queue.isEmpty() && failure.get() == null : "must only run action once";

            logger.info("running speed test of repository [{}] using path [{}]", request.getRepositoryName(), blobPath);

            final Random random = new Random(request.getSeed());

            final ObjectContainer<DiscoveryNode> nodesContainer = discoveryNodes.getMasterAndDataNodes().values();
            final List<DiscoveryNode> nodes = new ArrayList<>(nodesContainer.size());
            for (ObjectCursor<DiscoveryNode> cursor : nodesContainer) {
                nodes.add(cursor.value);
            }

            final long maxBlobSize = request.getMaxBlobSize().getBytes();
            final List<Long> blobSizes = new ArrayList<>();
            for (long s = 1; s < maxBlobSize; s <<= 1) {
                blobSizes.add(s);
            }
            blobSizes.add(maxBlobSize);

            for (int i = 0; i < request.getBlobCount(); i++) {
                final long targetLength = blobSizes.get(random.nextInt(blobSizes.size()));
                final boolean smallBlob = targetLength <= Integer.MAX_VALUE; // we only use the non-atomic API for larger blobs
                final VerifyBlobTask verifyBlobTask = new VerifyBlobTask(
                    nodes.get(random.nextInt(nodes.size())),
                    new BlobSpeedTestAction.Request(
                        request.getRepositoryName(),
                        blobPath,
                        "speed-test-" + i + "-" + UUIDs.randomBase64UUID(random),
                        targetLength,
                        random.nextLong(),
                        nodes,
                        smallBlob && (random.nextInt(50) == 0), // TODO magic 50
                        repository.supportURLRepo() && smallBlob && random.nextInt(50) == 0
                    )
                ); // TODO magic 50
                queue.add(verifyBlobTask);
            }

            for (int i = 0; i < workerCount; i++) {
                processNextTask();
            }
        }

        private void processNextTask() {
            final VerifyBlobTask thisTask = queue.poll();
            if (isRunning() == false || thisTask == null) {
                workersListener.onResponse(null);
            } else {
                logger.trace("processing [{}]", thisTask);
                // NB although all this is on the SAME thread, the per-blob verification runs on a SNAPSHOT thread so we don't have to worry
                // about local requests resulting in a stack overflow here
                final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(
                    TimeValue.timeValueMillis(timeoutTimeMillis - currentTimeMillisSupplier.getAsLong())
                );
                transportService.sendChildRequest(
                    thisTask.node,
                    BlobSpeedTestAction.NAME,
                    thisTask.request,
                    task,
                    transportRequestOptions,
                    new TransportResponseHandler<BlobSpeedTestAction.Response>() {
                        @Override
                        public void handleResponse(BlobSpeedTestAction.Response response) {
                            logger.trace("finished [{}]", thisTask);
                            expectedBlobs.add(thisTask.request.getBlobName()); // each task cleans up its own mess on failure
                            synchronized (responses) {
                                responses.add(response);
                            }
                            processNextTask();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug(new ParameterizedMessage("failed [{}]", thisTask), exp);
                            fail(exp);
                            workersListener.onResponse(null);
                        }

                        @Override
                        public BlobSpeedTestAction.Response read(StreamInput in) throws IOException {
                            return new BlobSpeedTestAction.Response(in);
                        }
                    }
                );
            }

        }

        private BlobContainer getBlobContainer() {
            return repository.blobStore().blobContainer(new BlobPath().add(blobPath));
        }

        private void onWorkerCompletion() {
            tryListAndCleanup(0);
        }

        private void tryListAndCleanup(final int retryCount) {
            if (timeoutTimeMillis < currentTimeMillisSupplier.getAsLong() || task.isCancelled()) {
                logger.warn(
                    "speed test of repository [{}] failed in cleanup phase after [{}] attempts, attempting best-effort cleanup "
                        + "but you may need to manually remove [{}]",
                    request.getRepositoryName(),
                    retryCount,
                    blobPath
                );
                isRunning(); // set failure if not already set
                deleteContainerAndSendResponse();
            } else {
                boolean retry = true;
                try {
                    if (listingStartTimeNanos.isEmpty()) {
                        assert retryCount == 0 : retryCount;
                        logger.trace(
                            "all tasks completed, checking expected blobs exist in [{}:{}] before cleanup",
                            request.repositoryName,
                            blobPath
                        );
                        listingStartTimeNanos = OptionalLong.of(System.nanoTime());
                    } else {
                        logger.trace(
                            "retrying check that expected blobs exist in [{}:{}] before cleanup, retry count = {}",
                            request.repositoryName,
                            blobPath,
                            retryCount
                        );
                    }
                    final BlobContainer blobContainer = getBlobContainer();
                    final Map<String, BlobMetadata> blobsMap = blobContainer.listBlobs();

                    final Set<String> missingBlobs = new HashSet<>(expectedBlobs);
                    missingBlobs.removeAll(blobsMap.keySet());
                    if (missingBlobs.isEmpty()) {
                        logger.trace(
                            "all expected blobs found after {} failed attempts, cleaning up [{}:{}]",
                            retryCount,
                            request.getRepositoryName(),
                            blobPath
                        );
                        retry = false;
                        deleteContainerAndSendResponse();
                    } else {
                        logger.debug(
                            "expected blobs [{}] missing in [{}:{}], trying again; retry count = {}",
                            request.repositoryName,
                            blobPath,
                            missingBlobs,
                            retryCount
                        );
                    }
                } catch (Exception e) {
                    assert retry;
                    logger.debug(
                        new ParameterizedMessage(
                            "failure during cleanup of [{}:{}], will retry; retry count = {}",
                            request.getRepositoryName(),
                            blobPath,
                            retryCount
                        ),
                        e
                    );
                    fail(e);
                } finally {
                    if (retry) {
                        // Either there were missing blobs, or else listing the blobs failed.
                        transportService.getThreadPool()
                            .scheduleUnlessShuttingDown(
                                TimeValue.timeValueSeconds(10),
                                ThreadPool.Names.SNAPSHOT,
                                () -> tryListAndCleanup(retryCount + 1)
                            );
                    }
                }
            }
        }

        private void deleteContainerAndSendResponse() {
            try {
                assert deleteStartTimeNanos.isEmpty() : deleteStartTimeNanos;
                deleteStartTimeNanos = OptionalLong.of(System.nanoTime());
                getBlobContainer().delete();
            } catch (Exception e) {
                fail(e);
            } finally {
                sendResponse();
            }
        }

        private void sendResponse() {
            final Exception exception = failure.get();
            if (exception == null) {
                assert listingStartTimeNanos.isPresent();
                assert deleteStartTimeNanos.isPresent();
                final long completionTimeNanos = System.nanoTime();

                logger.trace("[{}] completed successfully", request.getDescription());

                listener.onResponse(
                    new Response(
                        request.getRepositoryName(),
                        request.blobCount,
                        request.concurrency,
                        request.maxBlobSize,
                        request.seed,
                        blobPath,
                        responses,
                        deleteStartTimeNanos.getAsLong() - listingStartTimeNanos.getAsLong(),
                        completionTimeNanos - deleteStartTimeNanos.getAsLong()
                    )
                );
            } else {
                logger.debug(new ParameterizedMessage("speed test of repository [{}] failed", request.repositoryName), exception);
                listener.onFailure(
                    new RepositoryVerificationException(
                        request.getRepositoryName(),
                        "speed test failed, you may need to manually remove [" + blobPath + "]",
                        exception
                    )
                );
            }
        }

        private static class VerifyBlobTask {
            final DiscoveryNode node;
            final BlobSpeedTestAction.Request request;

            VerifyBlobTask(DiscoveryNode node, BlobSpeedTestAction.Request request) {
                this.node = node;
                this.request = request;
            }

            @Override
            public String toString() {
                return "VerifyBlobTask{" + "node=" + node + ", request=" + request + '}';
            }
        }
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String repositoryName;

        private int blobCount = 100;
        private int concurrency = 10;
        private long seed = 0L;
        private TimeValue timeout = TimeValue.timeValueSeconds(30);

        private ByteSizeValue maxBlobSize = ByteSizeValue.ofMb(10);

        public Request(String repositoryName) {
            this.repositoryName = repositoryName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            seed = in.readLong();
            blobCount = in.readVInt();
            concurrency = in.readVInt();
            timeout = in.readTimeValue();
            maxBlobSize = new ByteSizeValue(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeLong(seed);
            out.writeVInt(blobCount);
            out.writeVInt(concurrency);
            out.writeTimeValue(timeout);
            maxBlobSize.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }

        public void blobCount(int blobCount) {
            if (blobCount <= 0) {
                throw new IllegalArgumentException("blobCount must be >0, but was [" + blobCount + "]");
            }
            this.blobCount = blobCount;
        }

        public void concurrency(int concurrency) {
            if (concurrency <= 0) {
                throw new IllegalArgumentException("concurrency must be >0, but was [" + concurrency + "]");
            }
            this.concurrency = concurrency;
        }

        public void seed(long seed) {
            this.seed = seed;
        }

        public void timeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        public void maxBlobSize(ByteSizeValue maxBlobSize) {
            if (maxBlobSize.getBytes() <= 0) {
                throw new IllegalArgumentException("maxBlobSize must be >0, but was [" + maxBlobSize + "]");
            }
            this.maxBlobSize = maxBlobSize;
        }

        public int getBlobCount() {
            return blobCount;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public String getRepositoryName() {
            return repositoryName;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public long getSeed() {
            return seed;
        }

        public ByteSizeValue getMaxBlobSize() {
            return maxBlobSize;
        }

        @Override
        public String toString() {
            return "Request{" + getDescription() + '}';
        }

        @Override
        public String getDescription() {
            return "speed test [repository="
                + repositoryName
                + ", blobCount="
                + blobCount
                + ", concurrency="
                + concurrency
                + ", seed="
                + seed
                + ", timeout="
                + timeout
                + ", maxBlobSize="
                + maxBlobSize
                + "]";
        }

        public void reseed(long newSeed) {
            if (seed == 0L) {
                seed = newSeed;
            }
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private final String repositoryName;
        private final int blobCount;
        private final int concurrency;
        private final ByteSizeValue maxBlobSize;
        private final long seed;
        private final String blobPath;
        private final List<BlobSpeedTestAction.Response> blobResponses;
        private final long listingTimeNanos;
        private final long deleteTimeNanos;

        public Response(
            String repositoryName,
            int blobCount,
            int concurrency,
            ByteSizeValue maxBlobSize,
            long seed,
            String blobPath,
            List<BlobSpeedTestAction.Response> blobResponses,
            long listingTimeNanos,
            long deleteTimeNanos
        ) {
            this.repositoryName = repositoryName;
            this.blobCount = blobCount;
            this.concurrency = concurrency;
            this.maxBlobSize = maxBlobSize;
            this.seed = seed;
            this.blobPath = blobPath;
            this.blobResponses = blobResponses;
            this.listingTimeNanos = listingTimeNanos;
            this.deleteTimeNanos = deleteTimeNanos;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            blobCount = in.readVInt();
            concurrency = in.readVInt();
            maxBlobSize = new ByteSizeValue(in);
            seed = in.readLong();
            blobPath = in.readString();
            blobResponses = in.readList(BlobSpeedTestAction.Response::new);
            listingTimeNanos = in.readVLong();
            deleteTimeNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeVInt(blobCount);
            out.writeVInt(concurrency);
            maxBlobSize.writeTo(out);
            out.writeLong(seed);
            out.writeString(blobPath);
            out.writeList(blobResponses);
            out.writeVLong(listingTimeNanos);
            out.writeVLong(deleteTimeNanos);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("repository", repositoryName);
            builder.field("blob_count", blobCount);
            builder.field("concurrency", concurrency);
            builder.field("max_blob_size", maxBlobSize);
            builder.field("seed", seed);
            builder.field("blob_path", blobPath);

            builder.startArray("blobs");
            for (BlobSpeedTestAction.Response blobResponse : blobResponses) {
                blobResponse.toXContent(builder, params);
            }
            builder.endArray();

            builder.field("listing_nanos", listingTimeNanos);
            builder.field("delete_nanos", deleteTimeNanos);

            builder.endObject();
            return builder;
        }
    }

}
