/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.blobstore.testkit.BlobAnalyzeAction.MAX_ATOMIC_WRITE_SIZE;
import static org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit.humanReadableNanos;

/**
 * Action which distributes a bunch of {@link BlobAnalyzeAction}s over the nodes in the cluster, with limited concurrency, and collects
 * the results. Tries to fail fast by cancelling everything if any child task fails, or the timeout is reached, to avoid consuming
 * unnecessary resources. On completion, does a best-effort wait until the blob list contains all the expected blobs, then deletes them all.
 */
public class RepositoryAnalyzeAction extends ActionType<RepositoryAnalyzeAction.Response> {

    private static final Logger logger = LogManager.getLogger(RepositoryAnalyzeAction.class);

    public static final RepositoryAnalyzeAction INSTANCE = new RepositoryAnalyzeAction();
    public static final String NAME = "cluster:admin/repository/analyze";

    private RepositoryAnalyzeAction() {
        super(NAME, Response::new);
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final TransportService transportService;
        private final ClusterService clusterService;
        private final RepositoriesService repositoriesService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            RepositoriesService repositoriesService
        ) {
            super(NAME, transportService, actionFilters, RepositoryAnalyzeAction.Request::new, ThreadPool.Names.SAME);
            this.transportService = transportService;
            this.clusterService = clusterService;
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final ClusterState state = clusterService.state();

            final ThreadPool threadPool = transportService.getThreadPool();
            request.reseed(threadPool.relativeTimeInMillis());

            final DiscoveryNode localNode = transportService.getLocalNode();
            if (isSnapshotNode(localNode)) {
                final Repository repository = repositoriesService.repository(request.getRepositoryName());
                if (repository instanceof BlobStoreRepository == false) {
                    throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob-store repository");
                }
                if (repository.isReadOnly()) {
                    throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is read-only");
                }

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
                return;
            }

            if (request.getReroutedFrom() != null) {
                assert false : request.getReroutedFrom();
                throw new IllegalArgumentException(
                    "analysis of repository ["
                        + request.getRepositoryName()
                        + "] rerouted from ["
                        + request.getReroutedFrom()
                        + "] to non-snapshot node"
                );
            }

            request.reroutedFrom(localNode);
            final List<DiscoveryNode> snapshotNodes = getSnapshotNodes(state.nodes());
            if (snapshotNodes.isEmpty()) {
                listener.onFailure(
                    new IllegalArgumentException("no snapshot nodes found for analysis of repository [" + request.getRepositoryName() + "]")
                );
            } else {
                if (snapshotNodes.size() > 1) {
                    snapshotNodes.remove(state.nodes().getMasterNode());
                }
                final DiscoveryNode targetNode = snapshotNodes.get(new Random(request.getSeed()).nextInt(snapshotNodes.size()));
                RepositoryAnalyzeAction.logger.trace("rerouting analysis [{}] to [{}]", request.getDescription(), targetNode);
                transportService.sendChildRequest(
                    targetNode,
                    NAME,
                    request,
                    task,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(listener, Response::new)
                );
            }
        }
    }

    private static boolean isSnapshotNode(DiscoveryNode discoveryNode) {
        return (discoveryNode.canContainData() || discoveryNode.isMasterNode())
            && RepositoriesService.isDedicatedVotingOnlyNode(discoveryNode.getRoles()) == false;
    }

    private static List<DiscoveryNode> getSnapshotNodes(DiscoveryNodes discoveryNodes) {
        final ObjectContainer<DiscoveryNode> nodesContainer = discoveryNodes.getMasterAndDataNodes().values();
        final List<DiscoveryNode> nodes = new ArrayList<>(nodesContainer.size());
        for (ObjectCursor<DiscoveryNode> cursor : nodesContainer) {
            if (isSnapshotNode(cursor.value)) {
                nodes.add(cursor.value);
            }
        }
        return nodes;
    }

    /**
     * Compute a collection of blob sizes which respects the blob count and the limits on the size of each blob and the total size to write.
     * Tries its best to achieve an even spread of different-sized blobs to try and exercise the various size-based code paths well. See
     * the corresponding unit tests for examples of its output.
     */
    // Exposed for tests
    static List<Long> getBlobSizes(Request request) {

        int blobCount = request.getBlobCount();
        long maxTotalBytes = request.getMaxTotalDataSize().getBytes();
        long maxBlobSize = request.getMaxBlobSize().getBytes();

        if (maxTotalBytes - maxBlobSize < blobCount - 1) {
            throw new IllegalArgumentException(
                "cannot satisfy max total bytes ["
                    + maxTotalBytes
                    + "B/"
                    + request.getMaxTotalDataSize()
                    + "]: must write at least one byte per blob and at least one max-sized blob which is ["
                    + (blobCount + maxBlobSize - 1)
                    + "B] in total"
            );
        }

        final List<Long> blobSizes = new ArrayList<>();
        for (long s = 1; 0 < s && s < maxBlobSize; s <<= 1) {
            blobSizes.add(s);
        }
        blobSizes.add(maxBlobSize);
        // TODO also use sizes that aren't a power of two

        // Try and form an even spread of blob sizes by accounting for as many evenly spreads repeats as possible up-front.
        final long evenSpreadSize = blobSizes.stream().mapToLong(l -> l).sum();
        int evenSpreadCount = 0;
        while (blobSizes.size() <= blobCount && blobCount - blobSizes.size() <= maxTotalBytes - evenSpreadSize) {
            evenSpreadCount += 1;
            maxTotalBytes -= evenSpreadSize;
            blobCount -= blobSizes.size();
        }

        if (evenSpreadCount == 0) {
            // Not enough bytes for even a single even spread, account for the one max-sized blob anyway.
            blobCount -= 1;
            maxTotalBytes -= maxBlobSize;
        }

        final List<Long> perBlobSizes = new BlobCountCalculator(blobCount, maxTotalBytes, blobSizes).calculate();

        if (evenSpreadCount == 0) {
            perBlobSizes.add(maxBlobSize);
        } else {
            for (final long blobSize : blobSizes) {
                for (int i = 0; i < evenSpreadCount; i++) {
                    perBlobSizes.add(blobSize);
                }
            }
        }

        assert perBlobSizes.size() == request.getBlobCount();
        assert perBlobSizes.stream().mapToLong(l -> l).sum() <= request.getMaxTotalDataSize().getBytes();
        assert perBlobSizes.stream().allMatch(l -> 1 <= l && l <= request.getMaxBlobSize().getBytes());
        assert perBlobSizes.stream().anyMatch(l -> l == request.getMaxBlobSize().getBytes());
        return perBlobSizes;
    }

    /**
     * Calculates a reasonable set of blob sizes, with the correct number of blobs and a total size that respects the max in the request.
     */
    private static class BlobCountCalculator {

        private final int blobCount;
        private final long maxTotalBytes;
        private final List<Long> blobSizes;
        private final int sizeCount;

        private final int[] blobsBySize;
        private long totalBytes;
        private int totalBlobs;

        BlobCountCalculator(int blobCount, long maxTotalBytes, List<Long> blobSizes) {
            this.blobCount = blobCount;
            this.maxTotalBytes = maxTotalBytes;
            assert blobCount <= maxTotalBytes;

            this.blobSizes = blobSizes;
            sizeCount = blobSizes.size();
            assert sizeCount > 0;

            blobsBySize = new int[sizeCount];
            assert invariant();
        }

        List<Long> calculate() {
            // add blobs roughly evenly while keeping the total size under maxTotalBytes
            addBlobsRoughlyEvenly(sizeCount - 1);

            // repeatedly remove a blob and replace it with some number of smaller blobs, until there are enough blobs
            while (totalBlobs < blobCount) {
                assert totalBytes <= maxTotalBytes;

                final int minSplitCount = Arrays.stream(blobsBySize).skip(1).allMatch(i -> i <= 1) ? 1 : 2;
                final int splitIndex = IntStream.range(1, sizeCount).filter(i -> blobsBySize[i] >= minSplitCount).reduce(-1, (i, j) -> j);
                assert splitIndex > 0 : "split at " + splitIndex;
                assert blobsBySize[splitIndex] >= minSplitCount;

                final long splitSize = blobSizes.get(splitIndex);
                blobsBySize[splitIndex] -= 1;
                totalBytes -= splitSize;
                totalBlobs -= 1;

                addBlobsRoughlyEvenly(splitIndex - 1);
                assert invariant();
            }

            return getPerBlobSizes();
        }

        private List<Long> getPerBlobSizes() {
            assert invariant();

            final List<Long> perBlobSizes = new ArrayList<>(blobCount);
            for (int sizeIndex = 0; sizeIndex < sizeCount; sizeIndex++) {
                final long size = blobSizes.get(sizeIndex);
                for (int i = 0; i < blobsBySize[sizeIndex]; i++) {
                    perBlobSizes.add(size);
                }
            }

            return perBlobSizes;
        }

        private void addBlobsRoughlyEvenly(int startingIndex) {
            while (totalBlobs < blobCount && totalBytes < maxTotalBytes) {
                boolean progress = false;
                for (int sizeIndex = startingIndex; 0 <= sizeIndex && totalBlobs < blobCount && totalBytes < maxTotalBytes; sizeIndex--) {
                    final long size = blobSizes.get(sizeIndex);
                    if (totalBytes + size <= maxTotalBytes) {
                        progress = true;
                        blobsBySize[sizeIndex] += 1;
                        totalBlobs += 1;
                        totalBytes += size;
                    }
                }
                assert progress;
                assert invariant();
            }
        }

        private boolean invariant() {
            assert IntStream.of(blobsBySize).sum() == totalBlobs : this;
            assert IntStream.range(0, sizeCount).mapToLong(i -> blobSizes.get(i) * blobsBySize[i]).sum() == totalBytes : this;
            assert totalBlobs <= blobCount : this;
            assert totalBytes <= maxTotalBytes : this;
            return true;
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
        private final String blobPath = "temp-analysis-" + UUIDs.randomBase64UUID();

        private final Queue<VerifyBlobTask> queue = ConcurrentCollections.newQueue();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final Semaphore innerFailures = new Semaphore(5); // limit the number of suppressed failures
        private final int workerCount;
        private final CountDown workerCountdown;
        private final Set<String> expectedBlobs = ConcurrentCollections.newConcurrentSet();
        private final List<BlobAnalyzeAction.Response> responses;
        private final RepositoryPerformanceSummary.Builder summary = new RepositoryPerformanceSummary.Builder();

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
            this.workerCountdown = new CountDown(workerCount);

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
                    new RepositoryVerificationException(request.repositoryName, "analysis timed out after [" + request.getTimeout() + "]")
                )) {
                    transportService.getTaskManager().cancelTaskAndDescendants(task, "timed out", false, ActionListener.wrap(() -> {}));
                }
                // if this CAS failed then we're already failing for some other reason, nbd
                return false;
            }

            return true;
        }

        public void run() {
            assert queue.isEmpty() : "must only run action once";
            assert failure.get() == null : "must only run action once";
            assert workerCountdown.isCountedDown() == false : "must only run action once";

            logger.info("running analysis of repository [{}] using path [{}]", request.getRepositoryName(), blobPath);

            final Random random = new Random(request.getSeed());

            final List<DiscoveryNode> nodes = getSnapshotNodes(discoveryNodes);
            final List<Long> blobSizes = getBlobSizes(request);
            Collections.shuffle(blobSizes, random);

            for (int i = 0; i < request.getBlobCount(); i++) {
                final long targetLength = blobSizes.get(i);
                final boolean smallBlob = targetLength <= MAX_ATOMIC_WRITE_SIZE; // avoid the atomic API for larger blobs
                final boolean abortWrite = smallBlob && request.isAbortWritePermitted() && rarely(random);
                final VerifyBlobTask verifyBlobTask = new VerifyBlobTask(
                    nodes.get(random.nextInt(nodes.size())),
                    new BlobAnalyzeAction.Request(
                        request.getRepositoryName(),
                        blobPath,
                        "test-blob-" + i + "-" + UUIDs.randomBase64UUID(random),
                        targetLength,
                        random.nextLong(),
                        nodes,
                        request.getReadNodeCount(),
                        request.getEarlyReadNodeCount(),
                        smallBlob && rarely(random),
                        repository.supportURLRepo()
                            && repository.hasAtomicOverwrites()
                            && smallBlob
                            && rarely(random)
                            && abortWrite == false,
                        abortWrite
                    )
                );
                queue.add(verifyBlobTask);
            }

            for (int i = 0; i < workerCount; i++) {
                processNextTask();
            }
        }

        private boolean rarely(Random random) {
            return random.nextDouble() < request.getRareActionProbability();
        }

        private void processNextTask() {
            final VerifyBlobTask thisTask = queue.poll();
            if (isRunning() == false || thisTask == null) {
                onWorkerCompletion();
            } else {
                logger.trace("processing [{}]", thisTask);
                // NB although all this is on the SAME thread, the per-blob verification runs on a SNAPSHOT thread so we don't have to worry
                // about local requests resulting in a stack overflow here
                final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(
                    TimeValue.timeValueMillis(timeoutTimeMillis - currentTimeMillisSupplier.getAsLong())
                );
                transportService.sendChildRequest(
                    thisTask.node,
                    BlobAnalyzeAction.NAME,
                    thisTask.request,
                    task,
                    transportRequestOptions,
                    new TransportResponseHandler<BlobAnalyzeAction.Response>() {
                        @Override
                        public void handleResponse(BlobAnalyzeAction.Response response) {
                            logger.trace("finished [{}]", thisTask);
                            if (thisTask.request.getAbortWrite() == false) {
                                expectedBlobs.add(thisTask.request.getBlobName()); // each task cleans up its own mess on failure
                            }
                            if (request.detailed) {
                                synchronized (responses) {
                                    responses.add(response);
                                }
                            }
                            summary.add(response);
                            processNextTask();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug(new ParameterizedMessage("failed [{}]", thisTask), exp);
                            fail(exp);
                            onWorkerCompletion();
                        }

                        @Override
                        public BlobAnalyzeAction.Response read(StreamInput in) throws IOException {
                            return new BlobAnalyzeAction.Response(in);
                        }
                    }
                );
            }

        }

        private BlobContainer getBlobContainer() {
            return repository.blobStore().blobContainer(repository.basePath().add(blobPath));
        }

        private void onWorkerCompletion() {
            if (workerCountdown.countDown()) {
                transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
                    final long listingStartTimeNanos = System.nanoTime();
                    ensureConsistentListing();
                    final long deleteStartTimeNanos = System.nanoTime();
                    deleteContainer();
                    sendResponse(listingStartTimeNanos, deleteStartTimeNanos);
                }));
            }
        }

        private void ensureConsistentListing() {
            if (timeoutTimeMillis < currentTimeMillisSupplier.getAsLong() || task.isCancelled()) {
                logger.warn(
                    "analysis of repository [{}] failed before cleanup phase, attempting best-effort cleanup "
                        + "but you may need to manually remove [{}]",
                    request.getRepositoryName(),
                    blobPath
                );
                isRunning(); // set failure if not already set
            } else {
                logger.trace(
                    "all tasks completed, checking expected blobs exist in [{}:{}] before cleanup",
                    request.repositoryName,
                    blobPath
                );
                try {
                    final BlobContainer blobContainer = getBlobContainer();
                    final Set<String> missingBlobs = new HashSet<>(expectedBlobs);
                    final Map<String, BlobMetadata> blobsMap = blobContainer.listBlobs();
                    missingBlobs.removeAll(blobsMap.keySet());

                    if (missingBlobs.isEmpty()) {
                        logger.trace("all expected blobs found, cleaning up [{}:{}]", request.getRepositoryName(), blobPath);
                    } else {
                        final RepositoryVerificationException repositoryVerificationException = new RepositoryVerificationException(
                            request.repositoryName,
                            "expected blobs " + missingBlobs + " missing in [" + request.repositoryName + ":" + blobPath + "]"
                        );
                        logger.debug("failing due to missing blobs", repositoryVerificationException);
                        fail(repositoryVerificationException);
                    }
                } catch (Exception e) {
                    logger.debug(new ParameterizedMessage("failure during cleanup of [{}:{}]", request.getRepositoryName(), blobPath), e);
                    fail(e);
                }
            }
        }

        private void deleteContainer() {
            try {
                final BlobContainer blobContainer = getBlobContainer();
                blobContainer.delete();
                if (failure.get() != null) {
                    return;
                }
                final Map<String, BlobMetadata> blobsMap = blobContainer.listBlobs();
                if (blobsMap.isEmpty() == false) {
                    final RepositoryVerificationException repositoryVerificationException = new RepositoryVerificationException(
                        request.repositoryName,
                        "failed to clean up blobs " + blobsMap.keySet()
                    );
                    logger.debug("failing due to leftover blobs", repositoryVerificationException);
                    fail(repositoryVerificationException);
                }
            } catch (Exception e) {
                fail(e);
            }
        }

        private void sendResponse(final long listingStartTimeNanos, final long deleteStartTimeNanos) {
            final Exception exception = failure.get();
            if (exception == null) {
                final long completionTimeNanos = System.nanoTime();

                logger.trace("[{}] completed successfully", request.getDescription());

                listener.onResponse(
                    new Response(
                        transportService.getLocalNode().getId(),
                        transportService.getLocalNode().getName(),
                        request.getRepositoryName(),
                        request.blobCount,
                        request.concurrency,
                        request.readNodeCount,
                        request.earlyReadNodeCount,
                        request.maxBlobSize,
                        request.maxTotalDataSize,
                        request.seed,
                        request.rareActionProbability,
                        blobPath,
                        summary.build(),
                        responses,
                        deleteStartTimeNanos - listingStartTimeNanos,
                        completionTimeNanos - deleteStartTimeNanos
                    )
                );
            } else {
                logger.debug(new ParameterizedMessage("analysis of repository [{}] failed", request.repositoryName), exception);
                listener.onFailure(
                    new RepositoryVerificationException(
                        request.getRepositoryName(),
                        "analysis failed, you may need to manually remove [" + blobPath + "]",
                        exception
                    )
                );
            }
        }

        private static class VerifyBlobTask {
            final DiscoveryNode node;
            final BlobAnalyzeAction.Request request;

            VerifyBlobTask(DiscoveryNode node, BlobAnalyzeAction.Request request) {
                this.node = node;
                this.request = request;
            }

            @Override
            public String toString() {
                return "VerifyBlobTask{" + "node=" + node + ", request=" + request + '}';
            }
        }
    }

    public static class Request extends ActionRequest {

        private final String repositoryName;

        private int blobCount = 100;
        private int concurrency = 10;
        private int readNodeCount = 10;
        private int earlyReadNodeCount = 2;
        private long seed = 0L;
        private double rareActionProbability = 0.02;
        private TimeValue timeout = TimeValue.timeValueSeconds(30);
        private ByteSizeValue maxBlobSize = ByteSizeValue.ofMb(10);
        private ByteSizeValue maxTotalDataSize = ByteSizeValue.ofGb(1);
        private boolean detailed = false;
        private DiscoveryNode reroutedFrom = null;
        private boolean abortWritePermitted = true;

        public Request(String repositoryName) {
            this.repositoryName = repositoryName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            seed = in.readLong();
            rareActionProbability = in.readDouble();
            blobCount = in.readVInt();
            concurrency = in.readVInt();
            readNodeCount = in.readVInt();
            earlyReadNodeCount = in.readVInt();
            timeout = in.readTimeValue();
            maxBlobSize = new ByteSizeValue(in);
            maxTotalDataSize = new ByteSizeValue(in);
            detailed = in.readBoolean();
            reroutedFrom = in.readOptionalWriteable(DiscoveryNode::new);
            if (in.getVersion().onOrAfter(Version.V_7_14_0)) {
                abortWritePermitted = in.readBoolean();
            } else {
                abortWritePermitted = false;
            }
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
            out.writeDouble(rareActionProbability);
            out.writeVInt(blobCount);
            out.writeVInt(concurrency);
            out.writeVInt(readNodeCount);
            out.writeVInt(earlyReadNodeCount);
            out.writeTimeValue(timeout);
            maxBlobSize.writeTo(out);
            maxTotalDataSize.writeTo(out);
            out.writeBoolean(detailed);
            out.writeOptionalWriteable(reroutedFrom);
            if (out.getVersion().onOrAfter(Version.V_7_14_0)) {
                out.writeBoolean(abortWritePermitted);
            } else if (abortWritePermitted) {
                throw new IllegalStateException("cannot send abortWritePermitted request to node of version [" + out.getVersion() + "]");
            }
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        public void blobCount(int blobCount) {
            if (blobCount <= 0) {
                throw new IllegalArgumentException("blobCount must be >0, but was [" + blobCount + "]");
            }
            if (blobCount > 100000) {
                // Coordination work is O(blobCount) but is supposed to be lightweight, so limit the blob count.
                throw new IllegalArgumentException("blobCount must be <= 100000, but was [" + blobCount + "]");
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

        public void maxTotalDataSize(ByteSizeValue maxTotalDataSize) {
            if (maxTotalDataSize.getBytes() <= 0) {
                throw new IllegalArgumentException("maxTotalDataSize must be >0, but was [" + maxTotalDataSize + "]");
            }
            this.maxTotalDataSize = maxTotalDataSize;
        }

        public void detailed(boolean detailed) {
            this.detailed = detailed;
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

        public ByteSizeValue getMaxTotalDataSize() {
            return maxTotalDataSize;
        }

        public boolean getDetailed() {
            return detailed;
        }

        public DiscoveryNode getReroutedFrom() {
            return reroutedFrom;
        }

        public void reroutedFrom(DiscoveryNode discoveryNode) {
            reroutedFrom = discoveryNode;
        }

        public void readNodeCount(int readNodeCount) {
            if (readNodeCount <= 0) {
                throw new IllegalArgumentException("readNodeCount must be >0, but was [" + readNodeCount + "]");
            }
            this.readNodeCount = readNodeCount;
        }

        public int getReadNodeCount() {
            return readNodeCount;
        }

        public void earlyReadNodeCount(int earlyReadNodeCount) {
            if (earlyReadNodeCount < 0) {
                throw new IllegalArgumentException("earlyReadNodeCount must be >=0, but was [" + earlyReadNodeCount + "]");
            }
            this.earlyReadNodeCount = earlyReadNodeCount;
        }

        public int getEarlyReadNodeCount() {
            return earlyReadNodeCount;
        }

        public void rareActionProbability(double rareActionProbability) {
            if (rareActionProbability < 0. || rareActionProbability > 1.) {
                throw new IllegalArgumentException(
                    "rareActionProbability must be between 0 and 1, but was [" + rareActionProbability + "]"
                );
            }
            this.rareActionProbability = rareActionProbability;
        }

        public double getRareActionProbability() {
            return rareActionProbability;
        }

        public void abortWritePermitted(boolean abortWritePermitted) {
            this.abortWritePermitted = abortWritePermitted;
        }

        public boolean isAbortWritePermitted() {
            return abortWritePermitted;
        }

        @Override
        public String toString() {
            return "Request{" + getDescription() + '}';
        }

        @Override
        public String getDescription() {
            return "analysis [repository="
                + repositoryName
                + ", blobCount="
                + blobCount
                + ", concurrency="
                + concurrency
                + ", readNodeCount="
                + readNodeCount
                + ", earlyReadNodeCount="
                + earlyReadNodeCount
                + ", seed="
                + seed
                + ", rareActionProbability="
                + rareActionProbability
                + ", timeout="
                + timeout
                + ", maxBlobSize="
                + maxBlobSize
                + ", maxTotalDataSize="
                + maxTotalDataSize
                + ", detailed="
                + detailed
                + ", abortWritePermitted="
                + abortWritePermitted
                + "]";
        }

        public void reseed(long newSeed) {
            if (seed == 0L) {
                seed = newSeed;
            }
        }

    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

        private final String coordinatingNodeId;
        private final String coordinatingNodeName;
        private final String repositoryName;
        private final int blobCount;
        private final int concurrency;
        private final int readNodeCount;
        private final int earlyReadNodeCount;
        private final ByteSizeValue maxBlobSize;
        private final ByteSizeValue maxTotalDataSize;
        private final long seed;
        private final double rareActionProbability;
        private final String blobPath;
        private final RepositoryPerformanceSummary summary;
        private final List<BlobAnalyzeAction.Response> blobResponses;
        private final long listingTimeNanos;
        private final long deleteTimeNanos;

        public Response(
            String coordinatingNodeId,
            String coordinatingNodeName,
            String repositoryName,
            int blobCount,
            int concurrency,
            int readNodeCount,
            int earlyReadNodeCount,
            ByteSizeValue maxBlobSize,
            ByteSizeValue maxTotalDataSize,
            long seed,
            double rareActionProbability,
            String blobPath,
            RepositoryPerformanceSummary summary,
            List<BlobAnalyzeAction.Response> blobResponses,
            long listingTimeNanos,
            long deleteTimeNanos
        ) {
            this.coordinatingNodeId = coordinatingNodeId;
            this.coordinatingNodeName = coordinatingNodeName;
            this.repositoryName = repositoryName;
            this.blobCount = blobCount;
            this.concurrency = concurrency;
            this.readNodeCount = readNodeCount;
            this.earlyReadNodeCount = earlyReadNodeCount;
            this.maxBlobSize = maxBlobSize;
            this.maxTotalDataSize = maxTotalDataSize;
            this.seed = seed;
            this.rareActionProbability = rareActionProbability;
            this.blobPath = blobPath;
            this.summary = summary;
            this.blobResponses = blobResponses;
            this.listingTimeNanos = listingTimeNanos;
            this.deleteTimeNanos = deleteTimeNanos;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            coordinatingNodeId = in.readString();
            coordinatingNodeName = in.readString();
            repositoryName = in.readString();
            blobCount = in.readVInt();
            concurrency = in.readVInt();
            readNodeCount = in.readVInt();
            earlyReadNodeCount = in.readVInt();
            maxBlobSize = new ByteSizeValue(in);
            maxTotalDataSize = new ByteSizeValue(in);
            seed = in.readLong();
            rareActionProbability = in.readDouble();
            blobPath = in.readString();
            summary = new RepositoryPerformanceSummary(in);
            blobResponses = in.readList(BlobAnalyzeAction.Response::new);
            listingTimeNanos = in.readVLong();
            deleteTimeNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(coordinatingNodeId);
            out.writeString(coordinatingNodeName);
            out.writeString(repositoryName);
            out.writeVInt(blobCount);
            out.writeVInt(concurrency);
            out.writeVInt(readNodeCount);
            out.writeVInt(earlyReadNodeCount);
            maxBlobSize.writeTo(out);
            maxTotalDataSize.writeTo(out);
            out.writeLong(seed);
            out.writeDouble(rareActionProbability);
            out.writeString(blobPath);
            summary.writeTo(out);
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

            builder.startObject("coordinating_node");
            builder.field("id", coordinatingNodeId);
            builder.field("name", coordinatingNodeName);
            builder.endObject();

            builder.field("repository", repositoryName);
            builder.field("blob_count", blobCount);
            builder.field("concurrency", concurrency);
            builder.field("read_node_count", readNodeCount);
            builder.field("early_read_node_count", earlyReadNodeCount);
            builder.humanReadableField("max_blob_size_bytes", "max_blob_size", maxBlobSize);
            builder.humanReadableField("max_total_data_size_bytes", "max_total_data_size", maxTotalDataSize);
            builder.field("seed", seed);
            builder.field("rare_action_probability", rareActionProbability);
            builder.field("blob_path", blobPath);

            builder.startArray("issues_detected");
            // nothing to report here, if we detected an issue then we would have thrown an exception, but we include this to emphasise
            // that we are only detecting issues, not guaranteeing their absence
            builder.endArray();

            builder.field("summary", summary);

            if (blobResponses.size() > 0) {
                builder.startArray("details");
                for (BlobAnalyzeAction.Response blobResponse : blobResponses) {
                    blobResponse.toXContent(builder, params);
                }
                builder.endArray();
            }

            humanReadableNanos(builder, "listing_elapsed_nanos", "listing_elapsed", listingTimeNanos);
            humanReadableNanos(builder, "delete_elapsed_nanos", "delete_elapsed", deleteTimeNanos);

            builder.endObject();
            return builder;
        }
    }

}
