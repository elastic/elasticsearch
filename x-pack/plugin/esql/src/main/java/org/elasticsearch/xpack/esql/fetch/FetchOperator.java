/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.fetch.FetchGather;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The coordinator's operator for a {@link FetchExec}. It keeps the rows of the cut, sends one fetch request to each
 * node that holds some of their documents, and appends the fetched columns to the rows, in the order of the cut.
 * <p>
 * A {@code TopN} emits nothing before it saw all of its input, so the cut arrives as one burst, and the operator sends
 * its requests once its input finished. The driver waits for the responses. They arrive on other threads, which only
 * store them. The driver thread then puts the fetched columns next to the rows, with the block factory of the driver.
 * <p>
 * For now every failure of a fetch fails the query: a shard whose context is gone, a node that left, a request a data
 * node rejected.
 */
public final class FetchOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(FetchOperator.class);

    /**
     * Sends the fetch requests of one query, each as a child of the task of the query, with the index expressions the
     * query was authorized with.
     */
    public interface Client {
        /**
         * The node a document reference names, or {@code null} once it left the cluster.
         */
        @Nullable
        DiscoveryNode node(String clusterAlias, String nodeId);

        /**
         * Fetches the documents of {@code shards} from {@code node}. The listener runs on another thread, and must take the
         * pages of the response before it returns.
         *
         * @param releaseAfter the contexts the node frees once it answered
         */
        void fetch(
            DiscoveryNode node,
            String clusterAlias,
            List<FetchRequest.ShardDocs> shards,
            PhysicalPlan fetchPlan,
            List<ShardSearchContextId> releaseAfter,
            ActionListener<FetchResponse> listener
        );
    }

    /**
     * @param docRefChannel the input channel that holds the document references
     * @param fetchedTypes  the element type of each fetched column
     * @param fetchPlan     the plan each node runs to load the fetched columns
     * @param finalStage    whether no later fetch of the query reads the same contexts, so a node frees them once it
     *                      answered
     */
    public record Factory(int docRefChannel, List<ElementType> fetchedTypes, PhysicalPlan fetchPlan, boolean finalStage, Client client)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(driverContext, docRefChannel, fetchedTypes, fetchPlan, finalStage, client);
        }

        @Override
        public String describe() {
            return "FetchOperator[docRefChannel=" + docRefChannel + ", fetchedTypes=" + fetchedTypes + "]";
        }
    }

    private final DriverContext driverContext;
    private final int docRefChannel;
    private final List<ElementType> fetchedTypes;
    private final PhysicalPlan fetchPlan;
    private final boolean finalStage;
    private final Client client;

    // touched by the driver thread only
    private final List<Page> input = new ArrayList<>();
    private final Deque<Page> output = new ArrayDeque<>();
    private boolean finished;
    @Nullable
    private FetchBatchPlanner.Batch batch;
    private boolean gathered;

    // shared with the threads that receive the responses
    private final FailureCollector failures = new FailureCollector();
    /**
     * Completes once every node answered or failed.
     */
    private final SubscribableListener<Void> responded = new SubscribableListener<>();
    /**
     * The fetched pages of each node of {@link #batch}, set by the thread that receives its response.
     */
    @Nullable
    private AtomicReferenceArray<List<Page>> fetchedByNode;
    private final Object lock = new Object();
    /**
     * Set once the operator is closed. A response that arrives later releases its own pages. Guarded by {@link #lock}.
     */
    private boolean closed;

    FetchOperator(
        DriverContext driverContext,
        int docRefChannel,
        List<ElementType> fetchedTypes,
        PhysicalPlan fetchPlan,
        boolean finalStage,
        Client client
    ) {
        this.driverContext = driverContext;
        this.docRefChannel = docRefChannel;
        this.fetchedTypes = fetchedTypes;
        this.fetchPlan = fetchPlan;
        this.finalStage = finalStage;
        this.client = client;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        input.add(page);
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        sendRequests();
    }

    /**
     * Sends one request to each node that holds documents of the cut.
     */
    private void sendRequests() {
        batch = FetchBatchPlanner.plan(input, docRefChannel);
        if (batch.nodes().isEmpty()) {
            responded.onResponse(null);
            return;
        }
        if (logger.isDebugEnabled()) {
            int documents = batch.nodes().stream().mapToInt(FetchBatchPlanner.NodeBatch::docCount).sum();
            logger.debug(
                "fetching [{}] documents for [{}] rows from [{}] nodes",
                documents,
                batch.responseRows().length,
                batch.nodes().size()
            );
        }
        fetchedByNode = new AtomicReferenceArray<>(batch.nodes().size());
        // the driver waits for the responses before it completes, so a late response finds the operator closed
        driverContext.addAsyncAction();
        try (RefCountingRunnable refs = new RefCountingRunnable(this::onResponded)) {
            for (int n = 0; n < batch.nodes().size(); n++) {
                if (failures.hasFailure()) {
                    // the failure fails the query, so the other nodes have nothing to load
                    break;
                }
                FetchBatchPlanner.NodeBatch node = batch.nodes().get(n);
                DiscoveryNode target = client.node(node.clusterAlias(), node.nodeId());
                if (target == null) {
                    failures.unwrapAndCollect(new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster"));
                    break;
                }
                // a later fetch of the query would read the same contexts, so only the last one frees them
                List<ShardSearchContextId> releaseAfter = finalStage ? node.contextIds() : List.of();
                // a second completion would release the reference of the node twice and wake the driver too early
                ActionListener<FetchResponse> listener = ActionListener.notifyOnce(new NodeListener(n, node, refs.acquire()));
                try {
                    client.fetch(target, node.clusterAlias(), node.shards(), fetchPlan, releaseAfter, listener);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }
    }

    private void onResponded() {
        driverContext.removeAsyncAction();
        responded.onResponse(null);
    }

    /**
     * Stores the pages of one node, or collects its failure, before it releases the reference of the node. So once the
     * last node answered, the driver finds every page or every failure. Runs on the thread that receives the response, and
     * never throws: the transport would complete the listener a second time.
     */
    private final class NodeListener implements ActionListener<FetchResponse> {
        private final int index;
        private final FetchBatchPlanner.NodeBatch node;
        private final Releasable ref;

        NodeListener(int index, FetchBatchPlanner.NodeBatch node, Releasable ref) {
            this.index = index;
            this.node = node;
            this.ref = ref;
        }

        @Override
        public void onResponse(FetchResponse response) {
            List<Page> unclaimed = List.of();
            try {
                List<Page> pages = response.takePages();
                unclaimed = pages;
                Exception failure = failureOf(response);
                if (failure != null) {
                    failures.unwrapAndCollect(failure);
                    return;
                }
                for (Page page : pages) {
                    // the driver thread reads and releases them
                    page.allowPassingToDifferentDriver();
                }
                synchronized (lock) {
                    if (closed == false) {
                        fetchedByNode.set(index, pages);
                        unclaimed = List.of();
                    }
                }
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
         * The failure of the first shard that failed, or a response that doesn't hold a row for each document.
         */
        @Nullable
        private Exception failureOf(FetchResponse response) {
            for (FetchResponse.ShardResult result : response.shardResults()) {
                if (result.failure() != null) {
                    return result.failure();
                }
            }
            if (response.rows() != node.docCount()) {
                return new IllegalStateException(
                    "node [" + node.nodeId() + "] returned [" + response.rows() + "] rows for [" + node.docCount() + "] documents"
                );
            }
            return null;
        }

        @Override
        public void onFailure(Exception e) {
            try {
                failures.unwrapAndCollect(e);
            } finally {
                ref.close();
            }
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finished == false || responded.isDone() || failures.hasFailure()) {
            return NOT_BLOCKED;
        }
        return new IsBlockedResult(responded, "fetch");
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output.isEmpty() == false || (finished && gathered == false && responded.isDone());
    }

    @Override
    public Page getOutput() {
        checkFailure();
        if (gathered == false && finished && responded.isDone()) {
            gather();
        }
        return output.pollFirst();
    }

    /**
     * Appends the fetched columns to the rows of the cut. Runs on the driver thread, with the block factory of the driver.
     */
    private void gather() {
        // every node stored its pages or collected its failure before the responses completed
        checkFailure();
        List<Page> fetched = new ArrayList<>();
        if (fetchedByNode != null) {
            for (int n = 0; n < fetchedByNode.length(); n++) {
                List<Page> pages = fetchedByNode.get(n);
                if (pages == null) {
                    // the operator still owns the pages, so close releases them
                    throw new IllegalStateException("no response from node [" + batch.nodes().get(n).nodeId() + "]");
                }
                fetched.addAll(pages);
            }
            for (int n = 0; n < fetchedByNode.length(); n++) {
                fetchedByNode.set(n, null);
            }
        }
        gathered = true;
        List<Page> cut = new ArrayList<>(input);
        input.clear();
        output.addAll(
            FetchGather.gather(driverContext.blockFactory(), cut, fetched, fetchedTypes, batch.responseRows(), batch.deduplicated())
        );
    }

    @Override
    public boolean isFinished() {
        checkFailure();
        return finished && gathered && output.isEmpty();
    }

    private void checkFailure() {
        Exception e = failures.getFailure();
        if (e != null) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
        }
        List<Page> unreleased = new ArrayList<>(input);
        input.clear();
        unreleased.addAll(output);
        output.clear();
        if (fetchedByNode != null) {
            for (int n = 0; n < fetchedByNode.length(); n++) {
                List<Page> pages = fetchedByNode.getAndSet(n, null);
                if (pages != null) {
                    unreleased.addAll(pages);
                }
            }
        }
        FetchResponse.releasePages(unreleased);
    }

    @Override
    public String toString() {
        return "FetchOperator[docRefChannel=" + docRefChannel + ", fetchedTypes=" + fetchedTypes + "]";
    }
}
