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
import org.elasticsearch.compute.data.ElementType;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The coordinator's operator for a {@link FetchExec}. For each page of the cut it sends one fetch request to each node
 * that holds some of the page's documents, and appends the fetched columns to the rows, in the order of the cut.
 * <p>
 * {@link AsyncOperator} keeps the pages in order and limits how many fetch at the same time. The responses arrive on other
 * threads, which only store them. The driver thread then puts the fetched columns next to the rows, with the block factory
 * of the driver. A fetch frees no reader context: the query frees them all once it ends.
 * <p>
 * For now every failure of a fetch fails the query: a shard whose context is gone, a node that left, a request a data
 * node rejected.
 */
public final class FetchOperator extends AsyncOperator<FetchOperator.Fetched> {
    private static final Logger logger = LogManager.getLogger(FetchOperator.class);

    /**
     * The pages of the cut that fetch at the same time. A cut is usually one page, and a few at large limits.
     */
    public static final int MAX_OUTSTANDING_PAGES = 4;

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
    }

    /**
     * @param docChannel          the input channel that holds the documents of the rows
     * @param fetchedTypes        the element type of each fetched column
     * @param fetchPlan           the plan each node runs to load the fetched columns
     * @param maxOutstandingPages the pages of the cut that fetch at the same time
     */
    public record Factory(int docChannel, List<ElementType> fetchedTypes, PhysicalPlan fetchPlan, int maxOutstandingPages, Client client)
        implements
            OperatorFactory {
        /**
         * The operator of {@code exec}. The nodes that own the documents load the fetched columns with a plan built here,
         * without an extraction preference, so each column gets the element type of its data type.
         */
        static Factory forExec(FetchExec exec, int docChannel, Client client) {
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
            return new Factory(docChannel, fetchedTypes, fetchPlan, MAX_OUTSTANDING_PAGES, client);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(driverContext, docChannel, fetchedTypes, fetchPlan, maxOutstandingPages, client);
        }

        @Override
        public String describe() {
            return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
        }
    }

    /**
     * One page of the cut and the fetched columns of its documents: the pages of every node, one node after the other in
     * the order of the batch.
     */
    record Fetched(Page cut, FetchBatchPlanner.Batch batch, List<Page> fetched) {}

    private final int docChannel;
    private final List<ElementType> fetchedTypes;
    private final PhysicalPlan fetchPlan;
    private final Client client;
    /**
     * The response headers of every node. The completion of a page only sees the ones of the node that answered last.
     */
    private final ResponseHeadersCollector nodeHeaders;
    /**
     * The request to each node that answered, added by the thread that receives its response.
     */
    private final Queue<NodeRequest> requests = ConcurrentCollections.newQueue();

    // touched by the driver thread only
    private long rowsReceived;
    private long documents;
    private int pagesEmitted;
    private long planNanos;
    private long gatherNanos;

    FetchOperator(
        DriverContext driverContext,
        int docChannel,
        List<ElementType> fetchedTypes,
        PhysicalPlan fetchPlan,
        int maxOutstandingPages,
        Client client
    ) {
        super(driverContext, client.threadContext(), maxOutstandingPages);
        this.docChannel = docChannel;
        this.fetchedTypes = fetchedTypes;
        this.fetchPlan = fetchPlan;
        this.client = client;
        this.nodeHeaders = new ResponseHeadersCollector(client.threadContext());
    }

    @Override
    protected void performAsync(Page cut, ActionListener<Fetched> listener) {
        rowsReceived += cut.getPositionCount();
        FetchBatchPlanner.Batch batch;
        long planStart = System.nanoTime();
        try {
            batch = FetchBatchPlanner.plan(List.of(cut), docChannel);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        planNanos += System.nanoTime() - planStart;
        int pageDocuments = batch.nodes().stream().mapToInt(FetchBatchPlanner.NodeBatch::docCount).sum();
        documents += pageDocuments;
        if (batch.nodes().isEmpty()) {
            listener.onResponse(new Fetched(cut, batch, List.of()));
            return;
        }
        logger.debug("fetching [{}] documents for [{}] rows from [{}] nodes", pageDocuments, cut.getPositionCount(), batch.nodes().size());
        new PageFetch(cut, batch, listener).send();
    }

    /**
     * The requests of one page of the cut. Completes the listener of the page once every node answered or failed: with the
     * pages of every node, or with the failure.
     */
    private final class PageFetch {
        private final Page cut;
        private final FetchBatchPlanner.Batch batch;
        private final ActionListener<Fetched> listener;
        private final FailureCollector failures = new FailureCollector();
        /**
         * The fetched pages of each node, set by the thread that receives its response.
         */
        private final AtomicReferenceArray<List<Page>> fetchedByNode;

        PageFetch(Page cut, FetchBatchPlanner.Batch batch, ActionListener<Fetched> listener) {
            this.cut = cut;
            this.batch = batch;
            this.listener = listener;
            this.fetchedByNode = new AtomicReferenceArray<>(batch.nodes().size());
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
                        failures.unwrapAndCollect(new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster"));
                        break;
                    }
                    // a second completion would release the reference of the node twice and complete the page too early
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
         * Runs on the thread of the last node to answer, or on the driver thread when no request went out.
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
                // the operator releases the cut of a failed page
                FetchResponse.releasePages(fetched);
                listener.onFailure(failure);
                return;
            }
            listener.onResponse(new Fetched(cut, batch, fetched));
        }

        /**
         * Stores the pages of one node, or collects its failure, before it releases the reference of the node. So once the
         * last node answered, the page finds every node's pages or a failure. Runs on the thread that receives the
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
                    Exception failure = failureOf(response);
                    if (failure != null) {
                        failures.unwrapAndCollect(failure);
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
    }

    /**
     * Appends the fetched columns to the rows of the next page of the cut. Runs on the driver thread, with the block
     * factory of the driver.
     */
    @Override
    public Page getOutput() {
        Fetched result = fetchFromBuffer();
        if (result == null) {
            return null;
        }
        long gatherStart = System.nanoTime();
        // releases the cut and the fetched pages, also when it fails
        List<Page> gathered = FetchGather.gather(
            driverContext().blockFactory(),
            List.of(result.cut()),
            result.fetched(),
            fetchedTypes,
            result.batch().responseRows(),
            result.batch().deduplicated()
        );
        gatherNanos += System.nanoTime() - gatherStart;
        assert gathered.size() == 1 : "one page out for each page of the cut, but got " + gathered.size();
        pagesEmitted++;
        return gathered.getFirst();
    }

    @Override
    protected void releaseFetchedOnAnyThread(Fetched result) {
        // the fetched pages already allow passing to another driver
        Releasables.closeExpectNoException(() -> releasePageOnAnyThread(result.cut()), () -> FetchResponse.releasePages(result.fetched()));
    }

    @Override
    protected void doClose() {
        nodeHeaders.finish();
    }

    @Override
    protected Operator.Status status(long receivedPages, long completedPages, long processNanos) {
        return new Status(
            receivedPages,
            completedPages,
            processNanos,
            rowsReceived,
            documents,
            pagesEmitted,
            planNanos,
            gatherNanos,
            List.copyOf(requests)
        );
    }

    @Override
    public String toString() {
        return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
    }

    /**
     * What the fetch did and where its time went. The process time runs from planning the requests of a page to the last
     * response of the page, summed over the pages. The query phase already counted the documents, so the totals of a
     * profile don't count them again.
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
         * @param requests     the request to each node that answered, one per node and page of the cut
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
