/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ResponseHeadersCollector;
import org.elasticsearch.compute.operator.fetch.FetchGather;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * The coordinator's operator for a {@link FetchExec}. It groups the pages of the cut into batches, sends one fetch request
 * per batch to each node that holds some of the batch's documents, and appends the fetched columns to the rows, in the
 * order of the cut.
 * <p>
 * A {@code TopN} cut is usually one page, so one batch. A {@code LIMIT} cut passes on the pages of the exchange as they
 * arrive, and those can be small, down to the matches of one segment each. A page leaves at once when no batch fetches.
 * While batches fetch, the pages that arrive wait, and leave together once they fill a batch or a batch ends. So a stream
 * of small pages costs a few requests instead of one per page, and full pages still fetch up to
 * {@link Batching#maxBatchesInFlight} at the same time.
 * <p>
 * {@link AsyncOperator} keeps the pages in order and limits how many the operator holds. The responses arrive on other
 * threads, which only store them. The driver thread then puts the fetched columns next to the rows, with the block factory
 * of the driver.
 * <p>
 * A fetch request frees no reader context, because a later batch can read the same shard. Once the input ends, the cut is
 * complete, and the reader contexts that none of its rows read can go before the fetch loads the documents. The others go
 * once the operator closes, when nothing fetches anymore.
 * <p>
 * A fetch can fail for some shards: a shard whose context is gone, a request a node rejected, a node that left. When the
 * query allows partial results, the page goes on without the rows of those shards, in the order of the cut, and the query
 * reports the shards as failed ({@link FetchFailures}). Otherwise, and for failures such as a cancellation or a response
 * that doesn't match its request, the fetch fails the query.
 */
public final class FetchOperator extends AsyncOperator<FetchOperator.Fetched> {
    private static final Logger logger = LogManager.getLogger(FetchOperator.class);

    /**
     * The batches that fetch at the same time.
     */
    public static final int MAX_BATCHES_IN_FLIGHT = 4;

    /**
     * The pages of the cut the operator holds at once: fetching, waiting for a batch, or fetched and not out yet. Well above
     * {@link #MAX_BATCHES_IN_FLIGHT}, so the pages that arrive while those batches fetch can wait and leave together. These
     * are rows of the cut, which never holds more rows than its limit.
     */
    public static final int MAX_PAGES_HELD = 64;

    /**
     * How the operator groups the pages of the cut into batches.
     *
     * @param maxPagesHeld       the pages it holds at once: fetching, waiting for a batch, or fetched and not out yet
     * @param maxBatchesInFlight the batches that fetch at the same time
     * @param maxBatchRows       the rows a batch takes from the pages that wait, and the rows that must wait before a batch
     *                           leaves while others fetch. A batch takes at least one page, so a larger page goes alone.
     */
    public record Batching(int maxPagesHeld, int maxBatchesInFlight, int maxBatchRows) {
        public Batching {
            if (maxBatchesInFlight < 1) {
                throw new IllegalArgumentException("maxBatchesInFlight must be positive but was [" + maxBatchesInFlight + "]");
            }
            if (maxPagesHeld < maxBatchesInFlight) {
                throw new IllegalArgumentException(
                    "maxPagesHeld [" + maxPagesHeld + "] must be at least maxBatchesInFlight [" + maxBatchesInFlight + "]"
                );
            }
            if (maxBatchRows < 1) {
                throw new IllegalArgumentException("maxBatchRows must be positive but was [" + maxBatchRows + "]");
            }
        }

        /**
         * Batches whose fetched columns fill about one page of {@link Operator#TARGET_PAGE_SIZE}, the size the source
         * operators aim for, so the responses of a batch stay about as large as a page.
         */
        static Batching forFetched(List<Attribute> fetched) {
            int rowSize = 0;
            for (Attribute attribute : fetched) {
                rowSize += attribute.dataType().estimatedSize();
            }
            int maxBatchRows = Math.max(Operator.MIN_TARGET_PAGE_SIZE, Operator.TARGET_PAGE_SIZE / Math.max(1, rowSize));
            return new Batching(MAX_PAGES_HELD, MAX_BATCHES_IN_FLIGHT, maxBatchRows);
        }
    }

    /**
     * Sends the fetch requests of one query, each as a child of the task of the query, with the index expressions the
     * query was authorized with.
     */
    public interface Client {
        /**
         * The thread context the responses arrive in, so the operator can merge the response headers of every node.
         */
        ThreadContext threadContext();

        /**
         * The node the origin of a document names, or {@code null} once it left the cluster.
         */
        @Nullable
        DiscoveryNode node(String clusterAlias, String nodeId);

        /**
         * Fetches the documents of {@code shards} from {@code node}. The listener runs on another thread, and must take the
         * pages of the response before it returns.
         */
        void fetch(
            DiscoveryNode node,
            String clusterAlias,
            List<FetchRequest.ShardDocs> shards,
            PhysicalPlan fetchPlan,
            ActionListener<FetchResponse> listener
        );

        /**
         * The cut is complete, and its rows read only the reader contexts in {@code read}. The others can go.
         */
        void cutEnded(Set<ShardSearchContextId> read);

        /**
         * Nothing fetches anymore, so the reader contexts of the rows can go too.
         */
        void fetchEnded();
    }

    /**
     * @param docChannel   the input channel that holds the documents of the rows
     * @param fetchedTypes the element type of each fetched column
     * @param fetchPlan    the plan each node runs to load the fetched columns
     * @param batching     how the pages of the cut group into batches
     * @param failures     decides whether the query goes on without the rows of a failed fetch, and records the shards it
     *                     went on without
     */
    public record Factory(
        int docChannel,
        List<ElementType> fetchedTypes,
        PhysicalPlan fetchPlan,
        Batching batching,
        Client client,
        FetchFailures failures
    ) implements OperatorFactory {
        /**
         * The operator of {@code exec}. The nodes that own the documents load the fetched columns with a plan built here,
         * without an extraction preference, so each column gets the element type of its data type.
         */
        static Factory forExec(FetchExec exec, int docChannel, Client client, FetchFailures failures) {
            List<Attribute> fetched = exec.fetchedAttributes();
            Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
            PhysicalPlan load = new FieldExtractExec(
                Source.EMPTY,
                new FetchSourceExec(Source.EMPTY, doc, null),
                fetched,
                MappedFieldType.FieldExtractPreference.NONE
            );
            PhysicalPlan fetchPlan = EstimatesRowSize.estimateRowSize(0, new ProjectExec(Source.EMPTY, load, fetched));
            List<ElementType> fetchedTypes = fetched.stream()
                .map(a -> PlannerUtils.toElementType(a.dataType(), MappedFieldType.FieldExtractPreference.NONE))
                .toList();
            return new Factory(docChannel, fetchedTypes, fetchPlan, Batching.forFetched(fetched), client, failures);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(driverContext, docChannel, fetchedTypes, fetchPlan, batching, client, failures);
        }

        @Override
        public String describe() {
            return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
        }
    }

    /**
     * The result of one page of the cut. The pages of a batch come out one after the other, so the first one carries the
     * whole batch and the others only take the pages it gathered for them.
     */
    sealed interface Fetched permits FetchedBatch, LaterPageOfBatch {}

    /**
     * The result of the first page of a batch: the pages of the batch and the fetched columns of their documents, the pages
     * of every node one node after the other in the order of the batch.
     *
     * @param responseRows for each row of the pages of the batch, in order, the row of {@code fetched} that holds its
     *                     values, or {@code -1} to drop the row because the fetch of its shard failed
     */
    record FetchedBatch(List<Page> cuts, FetchBatchPlanner.Batch batch, List<Page> fetched, int[] responseRows) implements Fetched {}

    /**
     * The result of a later page of a batch. The first page of the batch owns its rows and gathers them.
     */
    enum LaterPageOfBatch implements Fetched {
        INSTANCE
    }

    /**
     * A page of the cut that waits for a batch.
     */
    private record QueuedPage(Page cut, ActionListener<Fetched> listener) {}

    private final int docChannel;
    private final List<ElementType> fetchedTypes;
    private final PhysicalPlan fetchPlan;
    private final Batching batching;
    private final Client client;
    private final FetchFailures fetchFailures;
    /**
     * The response headers of every node. The completion of a page only sees the ones of the node that answered last.
     */
    private final ResponseHeadersCollector nodeHeaders;
    /**
     * The request to each node that answered, added by the thread that receives its response.
     */
    private final Queue<NodeRequest> requests = ConcurrentCollections.newQueue();
    /**
     * The pages that wait for a batch, in the order of the cut. Guards {@link #queuedRows} and {@link #batchesInFlight} too.
     */
    private final Deque<QueuedPage> queued = new ArrayDeque<>();
    private int queuedRows;
    private int batchesInFlight;
    // a batch can go out from the thread that ended the batch before it
    private final LongAdder documents = new LongAdder();
    private final LongAdder planNanos = new LongAdder();

    // touched by the driver thread only
    private long rowsReceived;
    private int pagesEmitted;
    private long gatherNanos;
    /**
     * The pages the first page of a batch gathered for the later pages of the batch, which come out next.
     */
    private final Deque<Page> gatheredForLaterPages = new ArrayDeque<>();
    /**
     * The reader contexts that the rows of the cut read so far.
     */
    private final Set<ShardSearchContextId> contextsRead = new HashSet<>();
    private boolean cutEnded;

    FetchOperator(
        DriverContext driverContext,
        int docChannel,
        List<ElementType> fetchedTypes,
        PhysicalPlan fetchPlan,
        Batching batching,
        Client client,
        FetchFailures fetchFailures
    ) {
        super(driverContext, client.threadContext(), batching.maxPagesHeld());
        this.docChannel = docChannel;
        this.fetchedTypes = fetchedTypes;
        this.fetchPlan = fetchPlan;
        this.batching = batching;
        this.client = client;
        this.fetchFailures = fetchFailures;
        this.nodeHeaders = new ResponseHeadersCollector(client.threadContext());
    }

    @Override
    protected void performAsync(Page cut, ActionListener<Fetched> listener) {
        rowsReceived += cut.getPositionCount();
        recordContextsRead(cut);
        synchronized (queued) {
            queued.addLast(new QueuedPage(cut, listener));
            queuedRows += cut.getPositionCount();
        }
        sendQueuedPages();
    }

    /**
     * Records the reader contexts that the rows of {@code cut} read. The origins of a page can name contexts that none of its
     * rows read, so the rows decide. A page whose documents didn't leave their node is the planner's to reject.
     */
    private void recordContextsRead(Page cut) {
        if (cut.getBlock(docChannel) instanceof DocBlock docBlock && docBlock.asVector().origins() != null) {
            DocVector documents = docBlock.asVector();
            DocOrigins origins = documents.origins();
            IntVector ordinals = documents.shards();
            boolean[] recorded = new boolean[origins.size()];
            for (int p = 0; p < cut.getPositionCount(); p++) {
                int ordinal = ordinals.getInt(p);
                if (recorded[ordinal] == false) {
                    recorded[ordinal] = true;
                    contextsRead.add(origins.get(ordinal).contextId());
                }
            }
        }
    }

    /**
     * Once the input ends, the cut is complete, so the reader contexts that none of its rows read can go.
     */
    @Override
    public void finish() {
        if (cutEnded == false) {
            cutEnded = true;
            client.cutEnded(contextsRead);
        }
        super.finish();
    }

    /**
     * Sends the pages that wait as batches. A batch takes the next pages in the order of the cut while their rows fit
     * {@link Batching#maxBatchRows}. It leaves at once when no batch fetches. While batches fetch, it leaves only once the
     * pages that wait fill it, up to {@link Batching#maxBatchesInFlight} batches at once. Otherwise the pages wait for a
     * batch to end. Runs on the driver thread when a page arrives, and on the thread that ended a batch.
     */
    private void sendQueuedPages() {
        while (true) {
            List<QueuedPage> pages;
            synchronized (queued) {
                if (queued.isEmpty() || batchesInFlight >= batching.maxBatchesInFlight()) {
                    return;
                }
                if (batchesInFlight > 0 && queuedRows < batching.maxBatchRows()) {
                    return;
                }
                pages = takeBatch();
                batchesInFlight++;
            }
            sendBatch(pages);
        }
    }

    /**
     * Takes the next pages that wait while their rows fit {@link Batching#maxBatchRows}, and at least one.
     */
    private List<QueuedPage> takeBatch() {
        assert Thread.holdsLock(queued);
        List<QueuedPage> pages = new ArrayList<>();
        int rows = 0;
        do {
            QueuedPage page = queued.removeFirst();
            pages.add(page);
            rows += page.cut().getPositionCount();
        } while (queued.isEmpty() == false && rows + queued.peekFirst().cut().getPositionCount() <= batching.maxBatchRows());
        queuedRows -= rows;
        return pages;
    }

    private void sendBatch(List<QueuedPage> pages) {
        List<Page> cuts = pages.stream().map(QueuedPage::cut).toList();
        ActionListener<FetchedBatch> listener = endingTheBatch(pages);
        FetchBatchPlanner.Batch batch;
        long planStart = System.nanoTime();
        try {
            batch = FetchBatchPlanner.plan(cuts, docChannel);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        planNanos.add(System.nanoTime() - planStart);
        int batchDocuments = batch.nodes().stream().mapToInt(FetchBatchPlanner.NodeBatch::docCount).sum();
        documents.add(batchDocuments);
        if (batch.nodes().isEmpty()) {
            listener.onResponse(new FetchedBatch(cuts, batch, List.of(), batch.responseRows()));
            return;
        }
        logger.debug(
            "fetching [{}] documents for the [{}] rows of [{}] pages from [{}] nodes",
            batchDocuments,
            batch.responseRows().length,
            cuts.size(),
            batch.nodes().size()
        );
        new BatchFetch(cuts, batch, listener).send();
    }

    /**
     * Ends a batch and completes the listener of every page of it: the first page gets the batch, the others get their turn
     * to take the pages it gathers for them. The batch stops counting as in flight before its pages complete, so a page that
     * arrives once the driver has their output leaves at once. Then the pages that waited leave.
     * <p>
     * A failure fails every page of the batch, then the pages that wait, and only then ends the batch, so no batch goes out
     * for a query that fails.
     */
    private ActionListener<FetchedBatch> endingTheBatch(List<QueuedPage> pages) {
        return new ActionListener<>() {
            @Override
            public void onResponse(FetchedBatch fetched) {
                batchEnded();
                pages.getFirst().listener().onResponse(fetched);
                for (QueuedPage page : pages.subList(1, pages.size())) {
                    page.listener().onResponse(LaterPageOfBatch.INSTANCE);
                }
                sendQueuedPages();
            }

            @Override
            public void onFailure(Exception e) {
                for (QueuedPage page : pages) {
                    page.listener().onFailure(e);
                }
                failQueuedPages(e);
                batchEnded();
            }
        };
    }

    private void batchEnded() {
        synchronized (queued) {
            batchesInFlight--;
        }
    }

    /**
     * Fails the pages that wait for a batch, so no batch goes out for them.
     */
    private void failQueuedPages(Exception e) {
        List<QueuedPage> dropped;
        synchronized (queued) {
            dropped = new ArrayList<>(queued);
            queued.clear();
            queuedRows = 0;
        }
        for (QueuedPage page : dropped) {
            page.listener().onFailure(e);
        }
    }

    /**
     * The requests of one batch. Completes the listener of the batch once every node answered or failed: with the pages of
     * every node whose fetch didn't fail and the rows to drop, or with the failure that fails the query.
     */
    private final class BatchFetch {
        private final List<Page> cuts;
        private final FetchBatchPlanner.Batch batch;
        private final ActionListener<FetchedBatch> listener;
        /**
         * The failures that fail the query.
         */
        private final FailureCollector failures = new FailureCollector();
        /**
         * The fetched pages of each node, set by the thread that receives its response. Empty for a node whose fetch failed.
         */
        private final AtomicReferenceArray<List<Page>> fetchedByNode;
        /**
         * The position of the first shard of each node among the shards of the batch.
         */
        private final int[] firstShardOfNode;
        /**
         * For each shard of the batch, {@code 1} once its fetch failed and the batch goes on without its rows.
         */
        private final AtomicIntegerArray failedShards;

        BatchFetch(List<Page> cuts, FetchBatchPlanner.Batch batch, ActionListener<FetchedBatch> listener) {
            this.cuts = cuts;
            this.batch = batch;
            this.listener = listener;
            this.fetchedByNode = new AtomicReferenceArray<>(batch.nodes().size());
            this.firstShardOfNode = new int[batch.nodes().size()];
            int shards = 0;
            for (int n = 0; n < batch.nodes().size(); n++) {
                firstShardOfNode[n] = shards;
                shards += batch.nodes().get(n).shards().size();
            }
            this.failedShards = new AtomicIntegerArray(shards);
        }

        void send() {
            try (RefCountingRunnable refs = new RefCountingRunnable(this::onResponded)) {
                for (int n = 0; n < batch.nodes().size(); n++) {
                    if (failures.hasFailure()) {
                        // the failure fails the query, so the other nodes have nothing to load
                        break;
                    }
                    FetchBatchPlanner.NodeBatch node = batch.nodes().get(n);
                    DiscoveryNode target = client.node(node.clusterAlias(), node.nodeId());
                    if (target == null) {
                        nodeFailed(n, new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster"));
                        continue;
                    }
                    // a second completion would release the reference of the node twice and complete the batch too early
                    ActionListener<FetchResponse> nodeListener = ActionListener.notifyOnce(
                        new NodeListener(n, node, target.getName(), refs.acquire())
                    );
                    try {
                        client.fetch(target, node.clusterAlias(), node.shards(), fetchPlan, nodeListener);
                    } catch (Exception e) {
                        nodeListener.onFailure(e);
                    }
                }
            }
        }

        /**
         * Runs on the thread of the last node to answer, or on the thread that sent the batch when no request went out.
         */
        private void onResponded() {
            Exception failure = failures.getFailure();
            List<Page> fetched = new ArrayList<>();
            for (int n = 0; n < fetchedByNode.length(); n++) {
                List<Page> pages = fetchedByNode.getAndSet(n, null);
                if (pages != null) {
                    fetched.addAll(pages);
                } else if (failure == null) {
                    failure = new IllegalStateException("no response from node [" + batch.nodes().get(n).nodeId() + "]");
                }
            }
            if (failure != null) {
                // the operator releases the pages of a failed batch
                FetchResponse.releasePages(fetched);
                listener.onFailure(failure);
                return;
            }
            listener.onResponse(new FetchedBatch(cuts, batch, fetched, responseRows()));
        }

        /**
         * The response row of each row of the batch, {@code -1} for the rows of the shards whose fetch failed.
         */
        private int[] responseRows() {
            boolean[] failed = new boolean[failedShards.length()];
            boolean anyFailed = false;
            for (int s = 0; s < failed.length; s++) {
                failed[s] = failedShards.get(s) != 0;
                anyFailed |= failed[s];
            }
            return anyFailed ? batch.responseRowsWithout(failed) : batch.responseRows();
        }

        /**
         * The fetch from node {@code n} failed as a whole. The batch goes on without the rows of its shards, or the failure
         * fails the query.
         */
        private void nodeFailed(int n, Exception e) {
            Exception failure = FetchFailures.unwrap(e);
            if (fetchFailures.tolerates(failure) == false) {
                failures.unwrapAndCollect(e);
                return;
            }
            FetchBatchPlanner.NodeBatch node = batch.nodes().get(n);
            logger.debug(
                () -> Strings.format(
                    "fetching from node [%s] failed, going on without the rows of its [%s] shards",
                    node.nodeId(),
                    node.shards().size()
                ),
                failure
            );
            for (int s = 0; s < node.shards().size(); s++) {
                shardFailed(n, s, failure);
            }
            fetchedByNode.set(n, List.of());
        }

        /**
         * The batch goes on without the rows of shard {@code s} of node {@code n}, and the query reports the shard as failed.
         */
        private void shardFailed(int n, int s, Exception failure) {
            FetchBatchPlanner.NodeBatch node = batch.nodes().get(n);
            fetchFailures.shardFailed(node.clusterAlias(), node.nodeId(), node.shards().get(s).shardId(), failure);
            failedShards.set(firstShardOfNode[n] + s, 1);
        }

        /**
         * Stores the pages of one node, or collects its failure, before it releases the reference of the node. So once the
         * last node answered, the batch finds every node's pages or a failure. Runs on the thread that receives the
         * response, and never throws: the transport would complete the listener a second time.
         */
        private final class NodeListener implements ActionListener<FetchResponse> {
            private final int index;
            private final FetchBatchPlanner.NodeBatch node;
            private final String nodeName;
            private final Releasable ref;
            private final long sentNanos = System.nanoTime();

            NodeListener(int index, FetchBatchPlanner.NodeBatch node, String nodeName, Releasable ref) {
                this.index = index;
                this.node = node;
                this.nodeName = nodeName;
                this.ref = ref;
            }

            @Override
            public void onResponse(FetchResponse response) {
                List<Page> unclaimed = List.of();
                try {
                    nodeHeaders.collect();
                    requests.add(
                        new NodeRequest(
                            nodeName,
                            node.shards().size(),
                            node.docCount(),
                            System.nanoTime() - sentNanos,
                            response.tookNanos(),
                            response.setupNanos()
                        )
                    );
                    List<Page> pages = response.takePages();
                    unclaimed = pages;
                    if (keepShardsThatAnswered(response, pages) == false) {
                        return;
                    }
                    for (Page page : pages) {
                        // the driver thread reads and releases them
                        page.allowPassingToDifferentDriver();
                    }
                    fetchedByNode.set(index, pages);
                    unclaimed = List.of();
                } catch (Exception e) {
                    failures.unwrapAndCollect(e);
                } finally {
                    try {
                        FetchResponse.releasePages(unclaimed);
                    } finally {
                        ref.close();
                    }
                }
            }

            /**
             * Checks that the response answers the request shard by shard, and lets the batch go on without the shards whose
             * fetch failed. Returns {@code false} when a failure fails the query. A response that doesn't match its request
             * always fails it, because its rows can't be put next to the right documents.
             */
            private boolean keepShardsThatAnswered(FetchResponse response, List<Page> pages) {
                List<FetchResponse.ShardResult> results = response.shardResults();
                if (results.size() != node.shards().size()) {
                    return mismatch("answered for [" + results.size() + "] shards out of [" + node.shards().size() + "]");
                }
                int expectedRows = 0;
                boolean anyFailed = false;
                for (int s = 0; s < results.size(); s++) {
                    FetchResponse.ShardResult result = results.get(s);
                    FetchRequest.ShardDocs shard = node.shards().get(s);
                    if (result.shardId().equals(shard.shardId()) == false) {
                        return mismatch("answered for " + result.shardId() + " instead of " + shard.shardId());
                    }
                    if (result.failure() == null) {
                        expectedRows += shard.docCount();
                    } else if (fetchFailures.tolerates(result.failure())) {
                        anyFailed = true;
                    } else {
                        failures.unwrapAndCollect(result.failure());
                        return false;
                    }
                }
                if (response.rows() != expectedRows) {
                    return mismatch("returned [" + response.rows() + "] rows for [" + expectedRows + "] documents");
                }
                int pageRows = 0;
                for (Page page : pages) {
                    pageRows += page.getPositionCount();
                }
                if (pageRows != expectedRows) {
                    return mismatch("sent pages of [" + pageRows + "] rows for [" + expectedRows + "] documents");
                }
                for (int s = 0; s < results.size(); s++) {
                    FetchResponse.ShardResult result = results.get(s);
                    int docs = node.shards().get(s).docCount();
                    if (result.failure() == null && result.rows() != docs) {
                        return mismatch("returned [" + result.rows() + "] rows for the [" + docs + "] documents of " + result.shardId());
                    }
                }
                if (anyFailed) {
                    for (int s = 0; s < results.size(); s++) {
                        Exception failure = results.get(s).failure();
                        if (failure != null) {
                            logger.debug(
                                () -> Strings.format(
                                    "fetching from node [%s] failed for a shard, going on without its rows",
                                    node.nodeId()
                                ),
                                failure
                            );
                            shardFailed(index, s, failure);
                        }
                    }
                }
                return true;
            }

            private boolean mismatch(String problem) {
                failures.unwrapAndCollect(new IllegalStateException("node [" + node.nodeId() + "] " + problem));
                return false;
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    nodeFailed(index, e);
                } finally {
                    ref.close();
                }
            }
        }
    }

    /**
     * The next page of the cut with its fetched columns. Runs on the driver thread, with the block factory of the driver.
     */
    @Override
    public Page getOutput() {
        Fetched result = fetchFromBuffer();
        if (result == null) {
            return null;
        }
        pagesEmitted++;
        return switch (result) {
            case FetchedBatch batch -> gather(batch);
            case LaterPageOfBatch later -> gatheredForLaterPages.removeFirst();
        };
    }

    /**
     * Appends the fetched columns to the rows of every page of the batch. Returns the first page, and keeps the others for
     * the later pages of the batch.
     */
    private Page gather(FetchedBatch batch) {
        long gatherStart = System.nanoTime();
        // releases the pages of the batch and the fetched pages, also when it fails
        List<Page> gathered = FetchGather.gather(
            driverContext().blockFactory(),
            batch.cuts(),
            batch.fetched(),
            fetchedTypes,
            batch.responseRows(),
            batch.batch().deduplicated()
        );
        gatherNanos += System.nanoTime() - gatherStart;
        assert gathered.size() == batch.cuts().size() : "one page out for each page of the batch, but got " + gathered.size();
        gatheredForLaterPages.addAll(gathered.subList(1, gathered.size()));
        return gathered.getFirst();
    }

    @Override
    protected void releaseFetchedOnAnyThread(Fetched result) {
        switch (result) {
            // the fetched pages already allow passing to another driver
            case FetchedBatch batch -> Releasables.closeExpectNoException(
                () -> batch.cuts().forEach(AsyncOperator::releasePageOnAnyThread),
                () -> FetchResponse.releasePages(batch.fetched())
            );
            case LaterPageOfBatch later -> {
                // the first page of the batch owns the rows
            }
        }
    }

    @Override
    protected void doClose() {
        failQueuedPages(new TaskCancelledException("the fetch operator closed before the page went out"));
        Page gathered;
        while ((gathered = gatheredForLaterPages.pollFirst()) != null) {
            gathered.releaseBlocks();
        }
        // a batch still in flight finds its contexts gone, and its result is dropped anyway
        client.fetchEnded();
        nodeHeaders.finish();
    }

    @Override
    protected Operator.Status status(long receivedPages, long completedPages, long processNanos) {
        return new Status(
            receivedPages,
            completedPages,
            processNanos,
            rowsReceived,
            documents.sum(),
            pagesEmitted,
            planNanos.sum(),
            gatherNanos,
            List.copyOf(requests)
        );
    }

    @Override
    public String toString() {
        return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
    }

    /**
     * What the fetch did and where its time went. The process time runs from the arrival of a page to the last response of
     * its batch, summed over the pages. The query phase already counted the documents, so the totals of a profile don't
     * count them again.
     */
    public static final class Status extends AsyncOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "fetch",
            Status::new
        );

        private final long rowsReceived;
        private final long documents;
        private final int pagesEmitted;
        private final long planNanos;
        private final long gatherNanos;
        private final List<NodeRequest> requests;

        /**
         * @param rowsReceived rows of the cut
         * @param documents    documents the requests asked for, fewer than the rows when rows share documents
         * @param pagesEmitted pages of rows with their fetched columns
         * @param planNanos    grouping the documents of the cut into requests
         * @param gatherNanos  appending the fetched columns to the rows
         * @param requests     the request to each node that answered, one per node and batch of the cut
         */
        public Status(
            long receivedPages,
            long completedPages,
            long processNanos,
            long rowsReceived,
            long documents,
            int pagesEmitted,
            long planNanos,
            long gatherNanos,
            List<NodeRequest> requests
        ) {
            super(receivedPages, completedPages, processNanos);
            this.rowsReceived = rowsReceived;
            this.documents = documents;
            this.pagesEmitted = pagesEmitted;
            this.planNanos = planNanos;
            this.gatherNanos = gatherNanos;
            this.requests = List.copyOf(requests);
        }

        Status(StreamInput in) throws IOException {
            super(in);
            this.rowsReceived = in.readVLong();
            this.documents = in.readVLong();
            this.pagesEmitted = in.readVInt();
            this.planNanos = in.readVLong();
            this.gatherNanos = in.readVLong();
            this.requests = in.readCollectionAsImmutableList(NodeRequest::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(rowsReceived);
            out.writeVLong(documents);
            out.writeVInt(pagesEmitted);
            out.writeVLong(planNanos);
            out.writeVLong(gatherNanos);
            out.writeCollection(requests);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long documents() {
            return documents;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public long planNanos() {
            return planNanos;
        }

        public long gatherNanos() {
            return gatherNanos;
        }

        public List<NodeRequest> requests() {
            return requests;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            builder.field("rows_received", rowsReceived);
            builder.field("documents", documents);
            builder.field("pages_emitted", pagesEmitted);
            nanos(builder, "plan", planNanos);
            nanos(builder, "gather", gatherNanos);
            builder.startArray("requests");
            for (NodeRequest request : requests) {
                request.toXContent(builder, params);
            }
            builder.endArray();
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass() || super.equals(o) == false) {
                return false;
            }
            Status status = (Status) o;
            return rowsReceived == status.rowsReceived
                && documents == status.documents
                && pagesEmitted == status.pagesEmitted
                && planNanos == status.planNanos
                && gatherNanos == status.gatherNanos
                && requests.equals(status.requests);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rowsReceived, documents, pagesEmitted, planNanos, gatherNanos, requests);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // only nodes that run the fetch phase plan a fetch
            return FetchRequest.ESQL_FETCH;
        }
    }

    /**
     * The request to one node, as the coordinator saw it and as the node reported it.
     *
     * @param requestNanos from sending the request to receiving its response, on the coordinator. Without
     *                     {@code tookNanos}, it is the transport, the authorization and the time the request queued
     * @param tookNanos    from receiving the request to building the response, on the node
     * @param setupNanos   the part of {@code tookNanos} before the drivers started: contexts, access checks, planning
     */
    public record NodeRequest(String node, int shards, int documents, long requestNanos, long tookNanos, long setupNanos)
        implements
            Writeable,
            ToXContentObject {

        NodeRequest(StreamInput in) throws IOException {
            this(in.readString(), in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(node);
            out.writeVInt(shards);
            out.writeVInt(documents);
            out.writeVLong(requestNanos);
            out.writeVLong(tookNanos);
            out.writeVLong(setupNanos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("node", node);
            builder.field("shards", shards);
            builder.field("documents", documents);
            nanos(builder, "request", requestNanos);
            nanos(builder, "took", tookNanos);
            nanos(builder, "setup", setupNanos);
            return builder.endObject();
        }
    }

    private static void nanos(XContentBuilder builder, String name, long nanos) throws IOException {
        builder.field(name + "_nanos", nanos);
        if (builder.humanReadable()) {
            builder.field(name + "_time", TimeValue.timeValueNanos(nanos));
        }
    }
}
