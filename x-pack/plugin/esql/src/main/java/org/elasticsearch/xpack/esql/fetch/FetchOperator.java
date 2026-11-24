/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.compute.operator.fetch.FetchGather;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * The coordinator's operator for a {@link FetchExec}. It holds the rows of the cut until its input ends, then sends one
 * fetch request to each node that holds some of their documents. Once every node answered, it appends the fetched columns
 * to the rows, in the order of the cut, and emits the pages of the cut one by one.
 * <p>
 * The cut of a {@code TopN} or a {@code LIMIT} holds no more rows than its limit, and the operator holds all of them.
 * <p>
 * Each reader context appears in one request only, so every request asks its node to free the contexts it reads once it
 * answered, like the fetch phase of {@code _search}. Before the requests go out, the contexts that none of the rows read
 * can go. The contexts left, of a request that failed or never went out, go once the operator closes.
 * <p>
 * A side-car {@link AsyncOperator} sends the requests. It waits for the responses, merges their response headers into the
 * thread context of the driver, and fails on the first request that failed. The responses arrive on other threads, which
 * only store them. The driver thread then puts the fetched columns next to the rows, with the block factory of the driver.
 * <p>
 * A fetch that fails, because a context is gone, a node rejected the request or a node left, fails the query, also when
 * the query allows partial results.
 */
public final class FetchOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(FetchOperator.class);

    /**
     * @param fetchedTypes the element type of each fetched column
     * @param attributes   the attributes each node loads, the fetched columns
     */
    public record Factory(int docChannel, List<ElementType> fetchedTypes, List<Attribute> attributes, QueryFetchClient client)
        implements
            OperatorFactory {
        /**
         * The operator of {@code exec}. The nodes that own the documents load the fetched columns without an extraction
         * preference, so each column gets the element type of its data type.
         */
        static Factory forExec(FetchExec exec, int docChannel, QueryFetchClient client) {
            List<Attribute> fetched = exec.fetchedAttributes();
            List<ElementType> fetchedTypes = fetched.stream()
                .map(a -> PlannerUtils.toElementType(a.dataType(), MappedFieldType.FieldExtractPreference.NONE))
                .toList();
            return new Factory(docChannel, fetchedTypes, fetched, client);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(driverContext, docChannel, fetchedTypes, attributes, client);
        }

        @Override
        public String describe() {
            return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
        }
    }

    /**
     * The response of one node: its rows in request order, and the warnings of its fetch driver, like a value that failed
     * to load. The warnings come in the completion info of the response, not in its headers.
     */
    private record NodeResponse(List<Page> pages, Set<String> warnings) {}

    private final DriverContext driverContext;
    private final int docChannel;
    private final List<ElementType> fetchedTypes;
    private final List<Attribute> attributes;
    private final QueryFetchClient client;
    private final NodeRequests nodeRequests;
    /**
     * What the request of each node that answered did, added by the thread that receives its response.
     */
    private final Queue<NodeFetchStats> nodeStats = ConcurrentCollections.newQueue();

    // touched by the driver thread only
    private final List<Page> cut = new ArrayList<>();
    /**
     * The reader contexts that the rows of the cut read.
     */
    private final Set<ShardSearchContextId> contextsRead = new HashSet<>();
    private boolean inputEnded;
    /**
     * The requests of the cut, planned once the input ended.
     */
    private FetchBatchPlanner.Batch batch;
    private final List<Page> fetched = new ArrayList<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    /**
     * The pages of the cut with their fetched columns, once every node answered.
     */
    private Deque<Page> output;
    private int pagesReceived;
    private long rowsReceived;
    private long documents;
    private int pagesEmitted;
    private long planNanos;
    private long gatherNanos;

    FetchOperator(
        DriverContext driverContext,
        int docChannel,
        List<ElementType> fetchedTypes,
        List<Attribute> attributes,
        QueryFetchClient client
    ) {
        this.driverContext = driverContext;
        this.docChannel = docChannel;
        this.fetchedTypes = fetchedTypes;
        this.attributes = attributes;
        this.client = client;
        this.nodeRequests = new NodeRequests(driverContext, client.threadContext());
    }

    @Override
    public boolean needsInput() {
        return inputEnded == false;
    }

    @Override
    public void addInput(Page page) {
        pagesReceived++;
        rowsReceived += page.getPositionCount();
        cut.add(page);
        addContextsReadBy(page);
    }

    /**
     * Adds the reader contexts that the rows of {@code page} read. The origins of a page can name contexts that none of
     * its rows read, so the rows decide. A page whose documents didn't leave their node is the planner's to reject.
     */
    private void addContextsReadBy(Page page) {
        if (page.getBlock(docChannel) instanceof DocBlock docBlock && docBlock.asVector().origins() != null) {
            DocVector documents = docBlock.asVector();
            DocOrigins origins = documents.origins();
            IntVector ordinals = documents.shards();
            boolean[] recorded = new boolean[origins.size()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                int ordinal = ordinals.getInt(p);
                if (recorded[ordinal] == false) {
                    recorded[ordinal] = true;
                    contextsRead.add(origins.get(ordinal).contextId());
                }
            }
        }
    }

    /**
     * Once the input ends, the cut is complete. The reader contexts that none of its rows read go, and each node that holds
     * some of their documents gets its request.
     */
    @Override
    public void finish() {
        if (inputEnded) {
            return;
        }
        inputEnded = true;
        client.inputEnded(contextsRead);
        long planStart = System.nanoTime();
        batch = FetchBatchPlanner.plan(cut, docChannel);
        planNanos = System.nanoTime() - planStart;
        for (FetchBatchPlanner.NodeBatch node : batch.nodes()) {
            documents += node.docCount();
        }
        logger.debug(
            "fetching [{}] documents for the [{}] rows of [{}] pages from [{}] nodes",
            documents,
            rowsReceived,
            pagesReceived,
            batch.nodes().size()
        );
        for (int n = 0; n < batch.nodes().size(); n++) {
            nodeRequests.addInput(new Page(driverContext.blockFactory().newConstantIntBlockWith(n, 1)));
        }
        nodeRequests.finish();
    }

    @Override
    public IsBlockedResult isBlocked() {
        // the operator takes pages until its input ends, and emits them once it gathered
        if (inputEnded == false || output != null) {
            return NOT_BLOCKED;
        }
        return nodeRequests.isBlocked();
    }

    /**
     * Nothing until every node answered. Then the pages of the cut with their fetched columns, one at a time. Runs on the
     * driver thread, with the block factory of the driver.
     */
    @Override
    public Page getOutput() {
        if (inputEnded == false) {
            return null;
        }
        if (output == null) {
            NodeResponse response;
            while ((response = nodeRequests.fetchFromBuffer()) != null) {
                fetched.addAll(response.pages());
                warnings.addAll(response.warnings());
            }
            if (nodeRequests.isFinished() == false) {
                return null;
            }
            output = new ArrayDeque<>(gather());
        }
        Page page = output.pollFirst();
        if (page != null) {
            pagesEmitted++;
        }
        return page;
    }

    /**
     * Appends the fetched columns to the rows of every page of the cut.
     */
    private List<Page> gather() {
        // Only the driver thread adds them. A response can arrive after a failed driver finished its context, which takes
        // no more warnings, but the rows it explains never go out then.
        for (String warning : warnings) {
            driverContext.addWarning(warning);
        }
        List<Page> inputPages = List.copyOf(cut);
        List<Page> fetchedPages = List.copyOf(fetched);
        // the gather releases them, also when it fails
        cut.clear();
        fetched.clear();
        long gatherStart = System.nanoTime();
        List<Page> gathered = FetchGather.gather(
            driverContext.blockFactory(),
            inputPages,
            fetchedPages,
            fetchedTypes,
            batch.responseRows(),
            batch.deduplicated()
        );
        gatherNanos = System.nanoTime() - gatherStart;
        assert gathered.size() == inputPages.size() : "one page out for each page of the cut, but got " + gathered.size();
        return gathered;
    }

    @Override
    public boolean isFinished() {
        return output != null && output.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (output != null) {
            return output.isEmpty() == false;
        }
        return inputEnded && nodeRequests.canProduceMoreDataWithoutExtraInput();
    }

    /**
     * Releases what the operator holds. A request still in flight finds its contexts gone, and its response is dropped
     * anyway. An operator that closes before its input ended never ended its cut, and this lets every context go.
     */
    @Override
    public void close() {
        try {
            Releasables.closeExpectNoException(
                Releasables.wrap(cut),
                Releasables.wrap(fetched),
                output == null ? () -> {} : Releasables.wrap(output),
                // on the driver thread, which gets the response headers of every node
                nodeRequests
            );
        } finally {
            client.fetchEnded();
        }
    }

    @Override
    public Status status() {
        return new Status(pagesReceived, rowsReceived, documents, pagesEmitted, planNanos, gatherNanos, List.copyOf(nodeStats));
    }

    @Override
    public String toString() {
        return "FetchOperator[docChannel=" + docChannel + ", fetchedTypes=" + fetchedTypes + "]";
    }

    /**
     * Sends the request of each node of the cut and keeps its response until the operator reads it, in the order of the
     * nodes. Each input page holds one value: the position of its node in the requests of the cut.
     */
    private final class NodeRequests extends AsyncOperator<NodeResponse> {
        NodeRequests(DriverContext driverContext, ThreadContext threadContext) {
            // every node gets its request at once
            super(driverContext, threadContext, Integer.MAX_VALUE);
        }

        @Override
        protected void performAsync(Page node, ActionListener<NodeResponse> listener) {
            FetchBatchPlanner.NodeBatch request = batch.nodes().get(node.<IntBlock>getBlock(0).getInt(0));
            DiscoveryNode target;
            try {
                target = client.node(new FetchNode(request.clusterAlias(), request.nodeId()));
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            long sentNanos = System.nanoTime();
            client.fetch(target, request.shards(), attributes, listener.delegateFailureAndWrap((l, response) -> {
                nodeStats.add(
                    new NodeFetchStats(
                        target.getName(),
                        request.shards().size(),
                        request.docCount(),
                        System.nanoTime() - sentNanos,
                        response.tookNanos(),
                        response.setupNanos()
                    )
                );
                List<Page> pages = response.takePages();
                for (Page page : pages) {
                    // the driver thread reads and releases them
                    page.allowPassingToDifferentDriver();
                }
                // the base class releases the page that names the node only when the request fails
                releasePageOnAnyThread(node);
                l.onResponse(new NodeResponse(pages, response.completionInfo().warnings()));
            }));
        }

        @Override
        protected void releaseFetchedOnAnyThread(NodeResponse response) {
            // the pages already allow passing to another driver
            Releasables.closeExpectNoException(response.pages());
        }

        @Override
        public Page getOutput() {
            throw new UnsupportedOperationException("the fetch operator reads the responses");
        }

        @Override
        protected void doClose() {}
    }

    /**
     * What the fetch did and where its time went. The query phase already counted the documents, so the totals of a profile
     * don't count them again.
     */
    public static final class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "fetch",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(Status.class, (builder, status, params) -> {
            builder.startObject();
            builder.field("pages_received", status.pagesReceived);
            builder.field("rows_received", status.rowsReceived);
            builder.field("documents", status.documents);
            builder.field("pages_emitted", status.pagesEmitted);
            nanos(builder, "plan", status.planNanos);
            nanos(builder, "gather", status.gatherNanos);
            builder.startArray("nodes");
            for (NodeFetchStats node : status.nodes) {
                NodeFetchStats.PROFILE_SCHEMA.write(builder, node, params);
            }
            builder.endArray();
            return builder.endObject();
        })
            .required("pages_received", ProfileSchema.integer())
            .required("rows_received", ProfileSchema.integer())
            .required("documents", ProfileSchema.integer())
            .required("pages_emitted", ProfileSchema.integer())
            .required("plan_nanos", ProfileSchema.integer())
            .optional("plan_time", ProfileSchema.string())
            .required("gather_nanos", ProfileSchema.integer())
            .optional("gather_time", ProfileSchema.string())
            .required("nodes", ProfileSchema.array(NodeFetchStats.PROFILE_SCHEMA.schema()))
            .build();

        private final int pagesReceived;
        private final long rowsReceived;
        private final long documents;
        private final int pagesEmitted;
        private final long planNanos;
        private final long gatherNanos;
        private final List<NodeFetchStats> nodes;

        /**
         * @param pagesReceived pages of the cut
         * @param rowsReceived  rows of the cut
         * @param documents     documents the requests asked for, fewer than the rows when rows share documents
         * @param pagesEmitted  pages of rows with their fetched columns
         * @param planNanos     grouping the documents of the cut into requests
         * @param gatherNanos   appending the fetched columns to the rows
         * @param nodes         the fetch from each node that answered
         */
        public Status(
            int pagesReceived,
            long rowsReceived,
            long documents,
            int pagesEmitted,
            long planNanos,
            long gatherNanos,
            List<NodeFetchStats> nodes
        ) {
            this.pagesReceived = pagesReceived;
            this.rowsReceived = rowsReceived;
            this.documents = documents;
            this.pagesEmitted = pagesEmitted;
            this.planNanos = planNanos;
            this.gatherNanos = gatherNanos;
            this.nodes = List.copyOf(nodes);
        }

        Status(StreamInput in) throws IOException {
            this.pagesReceived = in.readVInt();
            this.rowsReceived = in.readVLong();
            this.documents = in.readVLong();
            this.pagesEmitted = in.readVInt();
            this.planNanos = in.readVLong();
            this.gatherNanos = in.readVLong();
            this.nodes = in.readCollectionAsImmutableList(NodeFetchStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVLong(rowsReceived);
            out.writeVLong(documents);
            out.writeVInt(pagesEmitted);
            out.writeVLong(planNanos);
            out.writeVLong(gatherNanos);
            out.writeCollection(nodes);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesReceived() {
            return pagesReceived;
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

        public List<NodeFetchStats> nodes() {
            return nodes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Status status = (Status) o;
            return pagesReceived == status.pagesReceived
                && rowsReceived == status.rowsReceived
                && documents == status.documents
                && pagesEmitted == status.pagesEmitted
                && planNanos == status.planNanos
                && gatherNanos == status.gatherNanos
                && nodes.equals(status.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, rowsReceived, documents, pagesEmitted, planNanos, gatherNanos, nodes);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // only nodes that run the fetch phase plan a fetch
            return FetchRequest.ESQL_FETCH;
        }
    }

    /**
     * The fetch from one node: what it asked for, and its time on the coordinator and on the node.
     *
     * @param requestNanos from sending the request to receiving its response, on the coordinator. Without
     *                     {@code tookNanos}, it is the transport, the authorization and the time the request queued
     * @param tookNanos    from receiving the request to building the response, on the node
     * @param setupNanos   the part of {@code tookNanos} before the drivers started: contexts, access checks, planning
     */
    public record NodeFetchStats(String node, int shards, int documents, long requestNanos, long tookNanos, long setupNanos)
        implements
            Writeable,
            ToXContentObject {

        static final ProfileSchema<NodeFetchStats> PROFILE_SCHEMA = ProfileSchema.of(NodeFetchStats.class, (builder, stats, params) -> {
            builder.startObject();
            builder.field("node", stats.node);
            builder.field("shards", stats.shards);
            builder.field("documents", stats.documents);
            nanos(builder, "request", stats.requestNanos);
            nanos(builder, "took", stats.tookNanos);
            nanos(builder, "setup", stats.setupNanos);
            return builder.endObject();
        })
            .required("node", ProfileSchema.string())
            .required("shards", ProfileSchema.integer())
            .required("documents", ProfileSchema.integer())
            .required("request_nanos", ProfileSchema.integer())
            .optional("request_time", ProfileSchema.string())
            .required("took_nanos", ProfileSchema.integer())
            .optional("took_time", ProfileSchema.string())
            .required("setup_nanos", ProfileSchema.integer())
            .optional("setup_time", ProfileSchema.string())
            .build();

        NodeFetchStats(StreamInput in) throws IOException {
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
            return PROFILE_SCHEMA.write(builder, this, params);
        }
    }

    private static void nanos(XContentBuilder builder, String name, long nanos) throws IOException {
        builder.field(name + "_nanos", nanos);
        if (builder.humanReadable()) {
            builder.field(name + "_time", TimeValue.timeValueNanos(nanos));
        }
    }
}
