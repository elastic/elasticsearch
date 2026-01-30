/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit.humanReadableNanos;

/**
 * Action which instructs a node to write a blob to the blob store and verify that it can be read correctly by other nodes. The other nodes
 * may read the whole blob or just a range; we verify the data that is read by checksum using {@link GetBlobChecksumAction}.
 *
 * The other nodes may optionally be instructed to attempt their read just before the write completes (which may indicate that the blob is
 * not found but must not yield partial data), and may optionally overwrite the blob while the reads are ongoing (which may yield either
 * version of the blob, but again must not yield partial data). Usually, however, we write once and only read after the write completes, and
 * in this case we insist that the read succeeds.
 *
 * The writer may also attempt to copy the blob, either just before the write completes (which may fail with not found)
 * or after (which should not fail). The writer may overwrite the source while the copy is in progress. If a copy is attempted,
 * readers will read the copy instead of the original. As above, if the copy succeeds, then readers should see a complete copy.
 * If the source is overwritten while the copy is in progress, readers may see either the original blob or the new one but no
 * mixture or partial result.
 *
 * <pre>
 *
 * +---------+                           +-------+                               +---------+
 * | Writer  |                           | Repo  |                               | Readers |
 * +---------+                           +-------+                               +---------+
 *      | --------------\                    |                                        |
 *      |-| Write phase |                    |                                        |
 *      | |-------------|                    |                                        |
 *      |                                    |                                        |
 *      | Write blob with random content     |                                        |
 *      |-----------------------------------→|                                        |
 *      |                                    |                                        |
 *      | Copy blob during write (rarely)    |                                        |
 *      |-----------------------------------→|                                        |
 *      |                                    |                                        |
 *      |                      Copy complete |                                        |
 *      |←-----------------------------------|                                        |
 *      |                                    |                                        |
 *      | Read range during write (rarely)   |                                        |
 *      |----------------------------------------------------------------------------→|
 *      |                                    |                                        |
 *      |                                    |                             Read range |
 *      |                                    |←---------------------------------------|
 *      |                                    |                                        |
 *      |                                    | Contents of range, or "not found"      |
 *      |                                    |---------------------------------------→|
 *      |                                    |                                        |
 *      |                               Acknowledge read, including checksum if found |
 *      |←----------------------------------------------------------------------------|
 *      |                                    |                                        |
 *      |                     Write complete |                                        |
 *      |←-----------------------------------|                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      | -------------\                     |                                        |
 *      |-| Read phase |                     |                                        |
 *      | |------------|                     |                                        |
 *      |                                    |                                        |
 *      | Copy blob (rarely)                 |                                        |
 *      |-----------------------------------→|                                        |
 *      |                                    |                                        |
 *      | TODO: Overwrite source (rarely)    |                                        |
 *      |-----------------------------------→|                                        |
 *      |                                    |                                        |
 *      |                 Overwrite complete |                                        |
 *      |←-----------------------------------|                                        |
 *      |                                    |                                        |
 *      |                      Copy complete |                                        |
 *      |←-----------------------------------|                                        |
 *      |                                    |                                        |
 *      | Read range [a,b)                   |                                        |
 *      |----------------------------------------------------------------------------→|
 *      |                                    |                                        |
 *      |                                    |                             Read range |
 *      |                                    |←---------------------------------------|
 *      |                                    |                                        |
 *      | Overwrite blob (rarely)            |                                        |
 *      |-----------------------------------→|                                        |
 *      |                                    |                                        |
 *      |                                    | Contents of range                      |
 *      |                                    |---------------------------------------→|
 *      |                                    |                                        |
 *      |                 Overwrite complete |                                        |
 *      |←-----------------------------------|                                        |
 *      |                                    |                                        |
 *      |                                    |               Ack read (with checksum) |
 *      |←----------------------------------------------------------------------------|
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      |                                    |                                        |
 *      | ---------------\                   |                                        |
 *      |-| Verify phase |                   |                                        |
 *      | |--------------|                   |                                        |
 *      |                                    |                                        |
 *      | Confirm checksums                  |                                        |
 *      |------------------                  |                                        |
 *      |                 |                  |                                        |
 *      |←-----------------                  |                                        |
 *      |                                    |                                        |
 *
 * </pre>
 *
 *
 * On success, details of how long everything took are returned. On failure, cancels the remote read tasks to try and avoid consuming
 * unnecessary resources.
 */
public class BlobAnalyzeAction extends HandledTransportAction<BlobAnalyzeAction.Request, BlobAnalyzeAction.Response> {

    /**
     * Turns off copy-during-write as a test: minio implements this with a long wait
     * and a client timeout. See: #135565
     */
    public static final Setting<Boolean> ENABLE_COPY_DURING_WRITE_CONTENTION = Setting.boolSetting(
        "repositories.blobstore.testkit.analyze.copy_during_write_contention",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    private final boolean enableEarlyCopy;

    private static final Logger logger = LogManager.getLogger(BlobAnalyzeAction.class);

    static final String NAME = "cluster:admin/repository/analyze/blob";

    private final RepositoriesService repositoriesService;
    private final TransportService transportService;

    BlobAnalyzeAction(
        TransportService transportService,
        Settings settings,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT));
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.enableEarlyCopy = ENABLE_COPY_DURING_WRITE_CONTENTION.get(settings);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final Repository repository = repositoriesService.repository(request.getRepositoryName());
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is not a blob-store repository");
        }
        if (repository.isReadOnly()) {
            throw new IllegalArgumentException("repository [" + request.getRepositoryName() + "] is read-only");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobPath path = blobStoreRepository.basePath().add(request.blobPath);
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        logger.trace("handling [{}]", request);

        assert task instanceof CancellableTask;
        new BlobAnalysis(transportService, (CancellableTask) task, request, blobStoreRepository, blobContainer, listener, enableEarlyCopy)
            .run();
    }

    /**
     * The atomic write API is based around a {@link BytesReference} which uses {@code int} for lengths and offsets and so on, so we can
     * only use it to write a blob with size at most {@link Integer#MAX_VALUE}:
     */
    static final long MAX_ATOMIC_WRITE_SIZE = Integer.MAX_VALUE;

    /**
     * Analysis on a single blob, performing the write(s) and orchestrating the read(s).
     */
    private static class BlobAnalysis {
        private final TransportService transportService;
        private final CancellableTask task;
        private final Request request;
        private final BlobStoreRepository repository;
        private final BlobContainer blobContainer;
        private final Random random;
        private final boolean checksumWholeBlob;
        private final long checksumStart;
        private final long checksumEnd;
        // If a copy is requested, do exactly one so that the number of blobs created is controlled by RepositoryAnalyzeAction.
        // Doing the copy in step 1 exercises copy before read completes. Step 2 exercises copy after read completes or the happy path.
        private final boolean doEarlyCopy;
        private final List<DiscoveryNode> earlyReadNodes;
        private final List<DiscoveryNode> readNodes;
        private final List<DiscoveryNode> readCopyNodes;
        private final List<NodeResponse> nodeResponses;
        private final RefCountingListener readResponseListeners;
        private WriteDetails write1Details;
        private WriteDetails write2Details;
        private final CancellableThreads cancellableThreads = new CancellableThreads();

        BlobAnalysis(
            TransportService transportService,
            CancellableTask task,
            Request request,
            BlobStoreRepository repository,
            BlobContainer blobContainer,
            ActionListener<Response> listener,
            boolean enableEarlyCopy
        ) {
            this.transportService = transportService;
            this.task = task;
            this.request = request;
            this.repository = repository;
            this.blobContainer = blobContainer;
            this.random = new Random(this.request.seed);

            checksumWholeBlob = random.nextBoolean();
            if (checksumWholeBlob) {
                checksumStart = 0L;
                checksumEnd = request.targetLength;
            } else {
                checksumStart = randomLongBetween(0L, request.targetLength);
                checksumEnd = randomLongBetween(checksumStart + 1, request.targetLength + 1);
            }
            doEarlyCopy = enableEarlyCopy && random.nextBoolean();

            final ArrayList<DiscoveryNode> nodes = new ArrayList<>(request.nodes); // copy for shuffling purposes
            if (request.readEarly) {
                Collections.shuffle(nodes, random);
                earlyReadNodes = nodes.stream().limit(request.earlyReadNodeCount).collect(Collectors.toList());
            } else {
                earlyReadNodes = List.of();
            }
            Collections.shuffle(nodes, random);
            readNodes = nodes.stream().limit(request.readNodeCount).collect(Collectors.toList());
            if (request.getCopyBlobName() != null) {
                Collections.shuffle(nodes, random);
                readCopyNodes = nodes.stream().limit(request.readNodeCount).collect(Collectors.toList());
            } else {
                readCopyNodes = List.of();
            }

            nodeResponses = Collections.synchronizedList(new ArrayList<>(earlyReadNodes.size() + readNodes.size() + readCopyNodes.size()));
            readResponseListeners = new RefCountingListener(
                new ThreadedActionListener<>(
                    transportService.getThreadPool().executor(ThreadPool.Names.SNAPSHOT),
                    listener.delegateResponse((l, e) -> l.onFailure(cleanUpAndWrapException(e))).map(ignored -> makeResponse())
                )
            );
            task.addListener(() -> cancellableThreads.cancel(task.getReasonCancelled()));
        }

        /**
         * Execute the blob analysis: write the blob and then fire off the read requests, possibly with some extra reads or writes according
         * to the options in the request.
         */
        void run() {
            try {
                write1Details = writeRandomBlob(
                    request.readEarly || request.getAbortWrite() || (request.targetLength <= MAX_ATOMIC_WRITE_SIZE && random.nextBoolean()),
                    true,
                    this::onLastReadForInitialWrite
                );

                if (request.writeAndOverwrite) {
                    assert request.targetLength <= MAX_ATOMIC_WRITE_SIZE : "oversized atomic write";
                    write2Details = writeRandomBlob(true, false, this::doReadAfterWrite);
                } else {
                    doReadAfterWrite();
                }
            } catch (Exception e) {
                readResponseListeners.acquire().onFailure(e);
                transportService.getTaskManager().cancelTaskAndDescendants(task, "task failed", false, ActionListener.noop());
            } finally {
                readResponseListeners.close();
            }
        }

        /**
         * Write a randomly-generated blob.
         *
         * @param atomic       If {@code true} then always use {@link BlobContainer#writeBlobAtomic} to write the blob, otherwise sometimes
         *                     use {@link BlobContainer#writeBlob}.
         * @param failIfExists Passed through to {@link BlobContainer#writeBlob} or {@link BlobContainer#writeBlobAtomic}.
         * @param onLastRead   Callback executed just before emitting the last few bytes of the blob, allowing the caller to trigger some
         *                     reads just before the write operation completes and/or to fail the write operation part-way through.
         * @return the details of the write operation, reported back to the coordinating node.
         */
        private WriteDetails writeRandomBlob(boolean atomic, boolean failIfExists, Runnable onLastRead) {
            assert atomic == false || request.targetLength <= MAX_ATOMIC_WRITE_SIZE : "oversized atomic write";
            final RandomBlobContent content = new RandomBlobContent(
                request.getRepositoryName(),
                random.nextLong(),
                task::isCancelled,
                onLastRead
            );
            final AtomicLong throttledNanos = new AtomicLong();

            if (logger.isTraceEnabled()) {
                logger.trace("writing blob [atomic={}, failIfExists={}] for [{}]", atomic, failIfExists, request.getDescription());
            }
            final long startNanos = System.nanoTime();

            // TODO push some of this writing logic down into the blob container implementation.
            // E.g. for S3 blob containers we would like to choose somewhat more randomly between single-part and multi-part uploads,
            // rather than relying on the usual distinction based on the size of the blob.

            if (atomic || (request.targetLength <= MAX_ATOMIC_WRITE_SIZE && random.nextBoolean())) {
                final RandomBlobContentBytesReference bytesReference = new RandomBlobContentBytesReference(
                    content,
                    Math.toIntExact(request.getTargetLength())
                ) {
                    @Override
                    public StreamInput streamInput() throws IOException {
                        return new InputStreamStreamInput(
                            repository.maybeRateLimitSnapshots(super.streamInput(), throttledNanos::addAndGet)
                        );
                    }
                };
                if (atomic) {
                    cancellableThreads.execute(() -> {
                        try {
                            blobContainer.writeBlobAtomic(
                                OperationPurpose.REPOSITORY_ANALYSIS,
                                request.blobName,
                                bytesReference,
                                failIfExists
                            );
                        } catch (Exception e) {
                            if (ExceptionsHelper.unwrap(e, BlobWriteAbortedException.class) != null) {
                                assert request.getAbortWrite() : "write unexpectedly aborted";
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                } else {
                    cancellableThreads.execute(() -> {
                        try {
                            blobContainer.writeBlob(OperationPurpose.REPOSITORY_ANALYSIS, request.blobName, bytesReference, failIfExists);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            } else {
                cancellableThreads.execute(() -> {
                    try {
                        blobContainer.writeBlob(
                            OperationPurpose.REPOSITORY_ANALYSIS,
                            request.blobName,
                            repository.maybeRateLimitSnapshots(
                                new RandomBlobContentStream(content, request.getTargetLength()),
                                throttledNanos::addAndGet
                            ),
                            request.targetLength,
                            failIfExists
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            final long elapsedNanos = System.nanoTime() - startNanos;
            final long checksum = content.getChecksum(checksumStart, checksumEnd);
            if (logger.isTraceEnabled()) {
                logger.trace("finished writing blob for [{}], got checksum [{}]", request.getDescription(), checksum);
            }
            return new WriteDetails(request.targetLength, elapsedNanos, throttledNanos.get(), checksum);
        }

        /**
         * Called just before completing the first write operation, after transmitting almost the whole blob's contents. Possibly does one
         * or more of the following:
         * <ul>
         *     <li>Try and copy the blob, which should fail.</li>
         *     <li>Fire off some early read operations, which may succeed or fail but mustn't see partial results.</li>
         *     <li>Abort the write operation.</li>
         * </ul>
         */
        private void onLastReadForInitialWrite() {
            var copySuccess = false;
            if (request.copyBlobName != null && doEarlyCopy) {
                try {
                    blobContainer.copyBlob(
                        OperationPurpose.REPOSITORY_ANALYSIS,
                        blobContainer,
                        request.blobName,
                        request.copyBlobName,
                        request.targetLength
                    );
                    copySuccess = true;
                } catch (UnsupportedOperationException uoe) {
                    // not all repositories support copy
                } catch (NoSuchFileException | FileNotFoundException ignored) {
                    // assume this is due to copy starting before the source was finished
                    logger.trace("copy FNF before write completed: {}", request.blobName);
                } catch (IOException e) {
                    if (request.getAbortWrite() == false) {
                        throw new RepositoryVerificationException(
                            request.getRepositoryName(),
                            "failed to copy blob before write: [" + request.blobName + "]",
                            e
                        );
                    }
                }
            }
            if (earlyReadNodes.isEmpty() == false) {
                if (logger.isTraceEnabled()) {
                    logger.trace("sending read request to [{}] for [{}] before write complete", earlyReadNodes, request.getDescription());
                }
                readOnNodes(earlyReadNodes, request.getBlobName(), true);
                if (copySuccess) {
                    readOnNodes(earlyReadNodes, request.getCopyBlobName(), true);
                }
            }
            if (request.getAbortWrite()) {
                throw new BlobWriteAbortedException();
            }
        }

        /**
         * Called after completing the first write operation. If this action involves a second overwrite operation then this is called just
         * before completing that second write, after transmitting almost the whole blob's new contents. Otherwise, called once the first
         * write operation has succeeded.
         * <p>
         * Fires off read requests to other nodes, which should all succeed. May also copy the blob and then fire off read requests for the
         * copy.
         */
        private void doReadAfterWrite() {
            if (logger.isTraceEnabled()) {
                logger.trace("sending read request to [{}] for [{}] after write complete", readNodes, request.getDescription());
            }
            var copySuccess = false;
            if (request.copyBlobName != null && doEarlyCopy == false && request.getAbortWrite() == false) {
                try {
                    blobContainer.copyBlob(
                        OperationPurpose.REPOSITORY_ANALYSIS,
                        blobContainer,
                        request.blobName,
                        request.copyBlobName,
                        request.targetLength
                    );
                    copySuccess = true;
                } catch (UnsupportedOperationException uoe) {
                    // not all repositories support copy
                } catch (IOException e) {
                    readResponseListeners.acquire()
                        .onFailure(
                            new RepositoryVerificationException(
                                request.getRepositoryName(),
                                "failed to copy blob after write: [" + request.blobName + "]",
                                e
                            )
                        );
                    return;
                }
            }

            readOnNodes(readNodes, request.blobName, false);
            if (copySuccess) {
                readOnNodes(readCopyNodes, request.copyBlobName, false);
            }
        }

        /**
         * Fire off a collection of read operations, collecting the responses with {@link #addNodeResponse} and retaining references to
         * {@link #readResponseListeners} until the operations complete.
         */
        private void readOnNodes(List<DiscoveryNode> nodes, String blobName, boolean beforeWriteComplete) {
            for (DiscoveryNode node : nodes) {
                if (task.isCancelled() == false) {
                    // no need for extra synchronization after checking if we were cancelled here -- we haven't notified the outer listener
                    // yet so any bans on the children are still in place
                    final GetBlobChecksumAction.Request blobChecksumRequest = getBlobChecksumRequest(blobName);
                    transportService.sendChildRequest(
                        node,
                        GetBlobChecksumAction.NAME,
                        blobChecksumRequest,
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            readResponseListeners.<GetBlobChecksumAction.Response>acquire(
                                response -> addNodeResponse(node, beforeWriteComplete, response)
                            )
                                .delegateResponse(
                                    (l, e) -> l.onFailure(
                                        new RepositoryVerificationException(
                                            request.getRepositoryName(),
                                            "["
                                                + blobChecksumRequest
                                                + "] ("
                                                + (beforeWriteComplete ? "before" : "after")
                                                + " write complete) failed on node ["
                                                + node
                                                + "]",
                                            e
                                        )
                                    )
                                ),
                            GetBlobChecksumAction.Response::new,
                            TransportResponseHandler.TRANSPORT_WORKER
                        )
                    );
                }
            }
        }

        private GetBlobChecksumAction.Request getBlobChecksumRequest(String blobName) {
            return new GetBlobChecksumAction.Request(
                request.getRepositoryName(),
                request.getBlobPath(),
                blobName,
                checksumStart,
                checksumWholeBlob ? 0L : checksumEnd
            );
        }

        private void addNodeResponse(DiscoveryNode node, boolean beforeWriteComplete, GetBlobChecksumAction.Response response) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "received read response [{}] from [{}] for [{}] [beforeWriteComplete={}]",
                    response,
                    node,
                    request.getDescription(),
                    beforeWriteComplete
                );
            }
            nodeResponses.add(new NodeResponse(node, beforeWriteComplete, response));
        }

        /**
         * Called on failure: attempts to clean up the created blob(s) and then adds a {@link RepositoryVerificationException} wrapper to
         * the exception.
         */
        private Exception cleanUpAndWrapException(Exception exception) {
            if (logger.isTraceEnabled()) {
                logger.trace(() -> "analysis failed [" + request.getDescription() + "] cleaning up", exception);
            }
            try {
                blobContainer.deleteBlobsIgnoringIfNotExists(
                    OperationPurpose.REPOSITORY_ANALYSIS,
                    request.getCopyBlobName() == null
                        ? Iterators.single(request.getBlobName())
                        : List.of(request.getBlobName(), request.getCopyBlobName()).iterator()
                );
            } catch (IOException ioException) {
                exception.addSuppressed(ioException);
                logger.warn(
                    () -> format(
                        "failure during post-failure cleanup while analysing repository [%s], you may need to manually remove [%s/%s]%s",
                        request.getRepositoryName(),
                        request.getBlobPath(),
                        request.getBlobName(),
                        request.getCopyBlobName() == null ? "" : format(" and/or [%s/%s]", request.getBlobPath(), request.getCopyBlobName())
                    ),
                    exception
                );
            }
            return new RepositoryVerificationException(
                request.getRepositoryName(),
                "failure processing [" + request.getDescription() + "]",
                exception
            );
        }

        /**
         * @return The final response to send back to the coordinator.
         */
        private Response makeResponse() {
            if (task.isCancelled()) {
                throw new RepositoryVerificationException(request.getRepositoryName(), "cancelled during checksum verification");
            }

            final long checksumLength = checksumEnd - checksumStart;
            final String expectedChecksumDescription;
            final LongPredicate checksumPredicate;
            if (write2Details == null) {
                checksumPredicate = l -> l == write1Details.checksum;
                expectedChecksumDescription = Long.toString(write1Details.checksum);
            } else {
                checksumPredicate = l -> l == write1Details.checksum || l == write2Details.checksum;
                expectedChecksumDescription = write1Details.checksum + " or " + write2Details.checksum;
            }

            boolean anyFound = false;
            RepositoryVerificationException failure = null;
            for (final NodeResponse nodeResponse : nodeResponses) {
                final GetBlobChecksumAction.Response response = nodeResponse.response;
                final RepositoryVerificationException nodeFailure;
                if (response.isNotFound()) {
                    if (request.readEarly || request.getAbortWrite()) {
                        nodeFailure = null; // "not found" is legitimate iff we tried to read it before the write completed
                    } else {
                        nodeFailure = new RepositoryVerificationException(
                            request.getRepositoryName(),
                            "node [" + nodeResponse.node + "] reported blob not found after it was written"
                        );
                    }
                } else {
                    anyFound = true;
                    final long actualChecksum = response.getChecksum();
                    if (response.getBytesRead() == checksumLength && checksumPredicate.test(actualChecksum)) {
                        nodeFailure = null; // checksum ok
                    } else {
                        nodeFailure = new RepositoryVerificationException(
                            request.getRepositoryName(),
                            "node ["
                                + nodeResponse.node
                                + "] failed during analysis: expected to read ["
                                + checksumStart
                                + "-"
                                + checksumEnd
                                + "], ["
                                + checksumLength
                                + "] bytes, with checksum ["
                                + expectedChecksumDescription
                                + "] but read ["
                                + response
                                + "]"
                        );
                    }
                }

                if (nodeFailure != null) {
                    if (failure == null) {
                        failure = nodeFailure;
                    } else {
                        failure.addSuppressed(nodeFailure);
                    }
                }
            }

            if (request.getAbortWrite() && anyFound) {
                final RepositoryVerificationException atomicityFailure = new RepositoryVerificationException(
                    request.getRepositoryName(),
                    "upload of blob was aborted, but blob was erroneously found by at least one node"
                );
                if (failure == null) {
                    failure = atomicityFailure;
                } else {
                    failure.addSuppressed(atomicityFailure);
                }
            }

            if (failure != null) {
                throw failure;
            }

            final long overwriteElapsedNanos = write2Details == null ? 0L : write2Details.elapsedNanos;
            final long overwriteThrottledNanos = write2Details == null ? 0L : write2Details.throttledNanos;
            return new Response(
                transportService.getLocalNode().getId(),
                transportService.getLocalNode().getName(),
                request.blobName,
                request.targetLength,
                request.readEarly,
                request.writeAndOverwrite,
                checksumStart,
                checksumEnd,
                write1Details.elapsedNanos,
                overwriteElapsedNanos,
                write1Details.throttledNanos + overwriteThrottledNanos,
                nodeResponses.stream()
                    .map(
                        nr -> new ReadDetail(
                            nr.node.getId(),
                            nr.node.getName(),
                            nr.beforeWriteComplete,
                            nr.response.isNotFound(),
                            nr.response.getFirstByteNanos(),
                            nr.response.getElapsedNanos(),
                            nr.response.getThrottleNanos()
                        )
                    )
                    .collect(Collectors.toList())
            );
        }

        /**
         * @return random non-negative long in [min, max)
         */
        private long randomLongBetween(long min, long max) {
            assert 0 <= min && min <= max;
            final long range = max - min;
            return range == 0 ? min : min + (random.nextLong() & Long.MAX_VALUE) % range;
        }
    }

    private static class NodeResponse {
        final DiscoveryNode node;
        final boolean beforeWriteComplete;
        final GetBlobChecksumAction.Response response;

        NodeResponse(DiscoveryNode node, boolean beforeWriteComplete, GetBlobChecksumAction.Response response) {
            this.node = node;
            this.beforeWriteComplete = beforeWriteComplete;
            this.response = response;
        }
    }

    private static class WriteDetails {
        final long bytesWritten;
        final long elapsedNanos;
        final long throttledNanos;
        final long checksum;

        private WriteDetails(long bytesWritten, long elapsedNanos, long throttledNanos, long checksum) {
            this.bytesWritten = bytesWritten;
            this.elapsedNanos = elapsedNanos;
            this.throttledNanos = throttledNanos;
            this.checksum = checksum;
        }
    }

    static class Request extends LegacyActionRequest {

        private static final TransportVersion REPO_ANALYSIS_COPY_BLOB = TransportVersion.fromName("repo_analysis_copy_blob");

        private final String repositoryName;
        private final String blobPath;
        private final String blobName;

        private final long targetLength;
        private final long seed;
        private final List<DiscoveryNode> nodes;
        private final int readNodeCount;
        private final int earlyReadNodeCount;
        private final boolean readEarly;
        private final boolean writeAndOverwrite;
        private final boolean abortWrite;
        @Nullable
        private final String copyBlobName;

        Request(
            String repositoryName,
            String blobPath,
            String blobName,
            long targetLength,
            long seed,
            List<DiscoveryNode> nodes,
            int readNodeCount,
            int earlyReadNodeCount,
            boolean readEarly,
            boolean writeAndOverwrite,
            boolean abortWrite,
            @Nullable String copyBlobName
        ) {
            assert 0 < targetLength;
            assert targetLength <= MAX_ATOMIC_WRITE_SIZE || (readEarly == false && writeAndOverwrite == false) : "oversized atomic write";
            assert writeAndOverwrite == false || abortWrite == false : "cannot set writeAndOverwrite and abortWrite";
            this.repositoryName = repositoryName;
            this.blobPath = blobPath;
            this.blobName = blobName;
            this.targetLength = targetLength;
            this.seed = seed;
            this.nodes = nodes;
            this.readNodeCount = readNodeCount;
            this.earlyReadNodeCount = earlyReadNodeCount;
            this.readEarly = readEarly;
            this.writeAndOverwrite = writeAndOverwrite;
            this.abortWrite = abortWrite;
            this.copyBlobName = copyBlobName;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            repositoryName = in.readString();
            blobPath = in.readString();
            blobName = in.readString();
            targetLength = in.readVLong();
            seed = in.readLong();
            nodes = in.readCollectionAsList(DiscoveryNode::new);
            readNodeCount = in.readVInt();
            earlyReadNodeCount = in.readVInt();
            readEarly = in.readBoolean();
            writeAndOverwrite = in.readBoolean();
            abortWrite = in.readBoolean();
            if (in.getTransportVersion().supports(REPO_ANALYSIS_COPY_BLOB)) {
                copyBlobName = in.readOptionalString();
            } else {
                copyBlobName = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repositoryName);
            out.writeString(blobPath);
            out.writeString(blobName);
            out.writeVLong(targetLength);
            out.writeLong(seed);
            out.writeCollection(nodes);
            out.writeVInt(readNodeCount);
            out.writeVInt(earlyReadNodeCount);
            out.writeBoolean(readEarly);
            out.writeBoolean(writeAndOverwrite);
            out.writeBoolean(abortWrite);
            if (out.getTransportVersion().supports(REPO_ANALYSIS_COPY_BLOB)) {
                out.writeOptionalString(copyBlobName);
            } else if (copyBlobName != null) {
                assert false : out.getTransportVersion();
                throw new IllegalStateException(
                    "cannot serialize " + this + "] using transport version [" + out.getTransportVersion() + "]"
                );
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "blob analysis ["
                + repositoryName
                + ":"
                + blobPath
                + "/"
                + blobName
                + ", length="
                + targetLength
                + ", seed="
                + seed
                + ", readEarly="
                + readEarly
                + ", writeAndOverwrite="
                + writeAndOverwrite
                + ", abortWrite="
                + abortWrite
                + ", copyBlobName="
                + copyBlobName
                + "]";
        }

        @Override
        public String toString() {
            return "BlobAnalyzeAction.Request{" + getDescription() + "}";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
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

        String getCopyBlobName() {
            return copyBlobName;
        }

        long getTargetLength() {
            return targetLength;
        }

        boolean getAbortWrite() {
            return abortWrite;
        }

    }

    static class Response extends ActionResponse implements ToXContentObject {

        private final String nodeId;
        private final String nodeName;
        private final String blobName;
        private final long blobLength;
        private final boolean readEarly;
        private final boolean overwrite;
        private final long checksumStart;
        private final long checksumEnd;

        private final long writeElapsedNanos;
        private final long overwriteElapsedNanos;
        private final long writeThrottledNanos;
        private final List<ReadDetail> readDetails;

        Response(
            String nodeId,
            String nodeName,
            String blobName,
            long blobLength,
            boolean readEarly,
            boolean overwrite,
            long checksumStart,
            long checksumEnd,
            long writeElapsedNanos,
            long overwriteElapsedNanos,
            long writeThrottledNanos,
            List<ReadDetail> readDetails
        ) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.blobName = blobName;
            this.blobLength = blobLength;
            this.readEarly = readEarly;
            this.overwrite = overwrite;
            this.checksumStart = checksumStart;
            this.checksumEnd = checksumEnd;
            this.writeElapsedNanos = writeElapsedNanos;
            this.overwriteElapsedNanos = overwriteElapsedNanos;
            this.writeThrottledNanos = writeThrottledNanos;
            this.readDetails = readDetails;
        }

        Response(StreamInput in) throws IOException {
            nodeId = in.readString();
            nodeName = in.readString();
            blobName = in.readString();
            blobLength = in.readVLong();
            readEarly = in.readBoolean();
            overwrite = in.readBoolean();
            checksumStart = in.readVLong();
            checksumEnd = in.readVLong();
            writeElapsedNanos = in.readVLong();
            overwriteElapsedNanos = in.readVLong();
            writeThrottledNanos = in.readVLong();
            readDetails = in.readCollectionAsList(ReadDetail::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(nodeName);
            out.writeString(blobName);
            out.writeVLong(blobLength);
            out.writeBoolean(readEarly);
            out.writeBoolean(overwrite);
            out.writeVLong(checksumStart);
            out.writeVLong(checksumEnd);
            out.writeVLong(writeElapsedNanos);
            out.writeVLong(overwriteElapsedNanos);
            out.writeVLong(writeThrottledNanos);
            out.writeCollection(readDetails);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject("blob");
            builder.field("name", blobName);
            builder.humanReadableField("size_bytes", "size", ByteSizeValue.ofBytes(blobLength));
            builder.field("read_start", checksumStart);
            builder.field("read_end", checksumEnd);
            builder.field("read_early", readEarly);
            builder.field("overwritten", overwrite);
            builder.endObject();

            builder.startObject("writer_node");
            builder.field("id", nodeId);
            builder.field("name", nodeName);
            builder.endObject();

            humanReadableNanos(builder, "write_elapsed_nanos", "write_elapsed", writeElapsedNanos);
            if (overwrite) {
                humanReadableNanos(builder, "overwrite_elapsed_nanos", "overwrite_elapsed", overwriteElapsedNanos);
            }
            humanReadableNanos(builder, "write_throttled_nanos", "write_throttled", writeThrottledNanos);

            builder.startArray("reads");
            for (ReadDetail readDetail : readDetails) {
                readDetail.toXContent(builder, params);
            }
            builder.endArray();

            builder.endObject();
            return builder;
        }

        long getWriteBytes() {
            return blobLength + (overwrite ? blobLength : 0L);
        }

        long getWriteThrottledNanos() {
            return writeThrottledNanos;
        }

        long getWriteElapsedNanos() {
            return writeElapsedNanos + overwriteElapsedNanos;
        }

        List<ReadDetail> getReadDetails() {
            return readDetails;
        }

        long getChecksumBytes() {
            return checksumEnd - checksumStart;
        }
    }

    static class ReadDetail implements Writeable, ToXContentFragment {

        private final String nodeId;
        private final String nodeName;
        private final boolean beforeWriteComplete;
        private final boolean isNotFound;
        private final long firstByteNanos;
        private final long throttleNanos;
        private final long elapsedNanos;

        ReadDetail(
            String nodeId,
            String nodeName,
            boolean beforeWriteComplete,
            boolean isNotFound,
            long firstByteNanos,
            long elapsedNanos,
            long throttleNanos
        ) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.beforeWriteComplete = beforeWriteComplete;
            this.isNotFound = isNotFound;
            this.firstByteNanos = firstByteNanos;
            this.throttleNanos = throttleNanos;
            this.elapsedNanos = elapsedNanos;
        }

        ReadDetail(StreamInput in) throws IOException {
            nodeId = in.readString();
            nodeName = in.readString();
            beforeWriteComplete = in.readBoolean();
            isNotFound = in.readBoolean();
            firstByteNanos = in.readVLong();
            throttleNanos = in.readVLong();
            elapsedNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(nodeName);
            out.writeBoolean(beforeWriteComplete);
            out.writeBoolean(isNotFound);
            out.writeVLong(firstByteNanos);
            out.writeVLong(throttleNanos);
            out.writeVLong(elapsedNanos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject("node");
            builder.field("id", nodeId);
            builder.field("name", nodeName);
            builder.endObject();

            if (beforeWriteComplete) {
                builder.field("before_write_complete", true);
            }

            if (isNotFound) {
                builder.field("found", false);
            } else {
                builder.field("found", true);
                humanReadableNanos(builder, "first_byte_time_nanos", "first_byte_time", firstByteNanos);
                humanReadableNanos(builder, "elapsed_nanos", "elapsed", elapsedNanos);
                humanReadableNanos(builder, "throttled_nanos", "throttled", throttleNanos);
            }

            builder.endObject();
            return builder;
        }

        long getFirstByteNanos() {
            return firstByteNanos;
        }

        long getThrottledNanos() {
            return throttleNanos;
        }

        long getElapsedNanos() {
            return elapsedNanos;
        }
    }

}
