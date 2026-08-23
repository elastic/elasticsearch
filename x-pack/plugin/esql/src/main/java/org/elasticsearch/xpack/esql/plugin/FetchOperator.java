/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-side operator that fetches deferred field values from owning data nodes after the query has
 * narrowed the candidate row set.
 * <p>
 * Each input page carries a column of serialized {@link FetchHandle}s plus any coordinator columns that
 * should be retained. For every input page the operator:
 * <ol>
 *     <li>decodes and groups handles by target session ({@code nodeId}, {@code retainedSessionId})</li>
 *     <li>opens a {@link FetchService.TargetExchange} per target session when needed</li>
 *     <li>sends batches of handles to the data node via the exchange</li>
 *     <li>collects response pages from the exchange and merges fetched columns back onto the input rows</li>
 *     <li>emits one output page once every group for that input page has completed</li>
 * </ol>
 * An optional {@code pushdownPlan} may be supplied so filtering happens on the data node. Mapped responses
 * include a trailing position-mapping column ({@link FetchSource#POSITION_ATTRIBUTE_NAME})
 * so rows pruned by pushdown can be omitted from the merged output; see {@link FetchPushdownOperatorBuilder} for the
 * supported pushdown shape.
 * <p>
 * Transport and data-node execution are handled by {@link FetchService}; this operator owns the coordinator
 * merge and exchange lifecycle only.
 */
public final class FetchOperator implements Operator {
    record GroupPages(List<Page> pages, boolean hasPositionMapping, int handleCount) {}

    public record Factory(
        int handleChannel,
        List<FetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxPendingInputs,
        FetchService.ClientFactory clientFactory
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(
                driverContext,
                handleChannel,
                requestFields,
                outputFields,
                pushdownPlan,
                configuration,
                maxPendingInputs,
                clientFactory.create()
            );
        }

        @Override
        public String describe() {
            return "FetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
        }
    }

    private record TargetSession(String nodeId, String retainedSessionId) {}

    private static final class Group {
        private final TargetSession target;
        private final List<FetchHandle> handles = new ArrayList<>();

        private Group(TargetSession target) {
            this.target = target;
        }
    }

    private static final class PendingInput {
        private final Page inputPage;
        private final int[] groupByPosition;
        private final int[] offsetByPosition;
        private final List<PendingGroup> groups;

        private PendingInput(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<PendingGroup> groups) {
            this.inputPage = inputPage;
            this.groupByPosition = groupByPosition;
            this.offsetByPosition = offsetByPosition;
            this.groups = groups;
        }

        /**
         * A pending input with no rows to fetch. It is immediately complete and flows through the regular merge
         * path so the emitted page carries the same schema (input columns plus fetched columns) as every other
         * output page; downstream operators address the fetched columns by channel even on empty pages.
         */
        static PendingInput empty(Page inputPage) {
            return new PendingInput(inputPage, new int[0], new int[0], List.of());
        }

        boolean isComplete() {
            return groups.stream().allMatch(PendingGroup::isComplete);
        }

        List<GroupPages> pagesByGroup() {
            List<GroupPages> pagesByGroup = new ArrayList<>(groups.size());
            for (PendingGroup group : groups) {
                pagesByGroup.add(new GroupPages(group.pages, group.hasPositionMapping, group.group.handles.size()));
            }
            return pagesByGroup;
        }
    }

    private static final class PendingGroup {
        private final Group group;
        private final FetchService.TargetExchange exchange;
        private final long batchId;
        private final List<Page> pages = new ArrayList<>();
        private boolean batchSent;
        private boolean batchCompleted;
        private boolean complete;
        private boolean hasPositionMapping;

        private PendingGroup(Group group, FetchService.TargetExchange exchange, long batchId) {
            this.group = group;
            this.exchange = exchange;
            this.batchId = batchId;
        }

        boolean isComplete() {
            return complete;
        }
    }

    private final DriverContext driverContext;
    private final int handleChannel;
    private final List<FetchService.FetchField> requestFields;
    private final List<Attribute> outputFields;
    private final PhysicalPlan pushdownPlan;
    private final Configuration configuration;
    private final int maxPendingInputs;
    private final FetchService.Client client;
    private final AtomicLong batchIds = new AtomicLong();
    private final Map<TargetSession, FetchService.TargetExchange> exchanges = new HashMap<>();
    private final Map<Long, PendingGroup> pendingByBatch = new HashMap<>();
    private final Deque<PendingInput> pendingInputs = new ArrayDeque<>();
    private boolean finishing;
    private Exception failure;
    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;
    private long batchesSent;
    private int exchangesOpened;

    // Note: no ThreadContext parameter on purpose. This operator only interacts with its exchanges synchronously
    // on the driver thread; response-header propagation for the async transport work is owned by
    // BidirectionalBatchExchangeClient and replayed via TargetExchangeChannel#close.
    FetchOperator(
        DriverContext driverContext,
        int handleChannel,
        List<FetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxPendingInputs,
        FetchService.Client client
    ) {
        if (requestFields.isEmpty()) {
            throw new IllegalArgumentException("fetch requires at least one request field");
        }
        if (outputFields.isEmpty()) {
            throw new IllegalArgumentException("fetch requires at least one output field");
        }
        if (requestFields.size() != outputFields.size()) {
            throw new IllegalArgumentException(
                "fetch request fields [" + requestFields.size() + "] must match output fields [" + outputFields.size() + "]"
            );
        }
        validatePushdownPlan(pushdownPlan);
        this.driverContext = driverContext;
        this.handleChannel = handleChannel;
        this.requestFields = List.copyOf(requestFields);
        this.outputFields = List.copyOf(outputFields);
        this.pushdownPlan = pushdownPlan;
        this.configuration = configuration;
        this.maxPendingInputs = maxPendingInputs;
        this.client = client;
    }

    @Override
    public boolean needsInput() {
        // TODO: Bound pending groups rather than input pages. One page can fan out into one batch per target session,
        // so this limit does not cap the number of outstanding fetch batches.
        return finishing == false && failure == null && pendingInputs.size() < maxPendingInputs;
    }

    @Override
    public void addInput(Page inputPage) {
        pagesReceived++;
        rowsReceived += inputPage.getPositionCount();
        if (inputPage.getPositionCount() == 0) {
            pendingInputs.addLast(PendingInput.empty(inputPage));
            return;
        }

        boolean success = false;
        PendingInput pendingInput = null;
        try {
            GroupedHandles groupedHandles = decodeHandles(inputPage);
            assert groupedHandles.groups().isEmpty() == false : "non-empty pages always produce at least one group";
            List<PendingGroup> pendingGroups = new ArrayList<>(groupedHandles.groups().size());
            pendingInput = new PendingInput(inputPage, groupedHandles.groupByPosition(), groupedHandles.offsetByPosition(), pendingGroups);
            pendingInputs.addLast(pendingInput);
            for (Group group : groupedHandles.groups()) {
                FetchService.TargetExchange exchange = exchanges.get(group.target);
                if (exchange == null) {
                    exchange = client.openTargetExchange(
                        group.target.nodeId(),
                        group.target.retainedSessionId(),
                        requestFields,
                        pushdownPlan,
                        configuration
                    );
                    exchanges.put(group.target, exchange);
                    exchangesOpened++;
                }
                long batchId = batchIds.incrementAndGet();
                PendingGroup pendingGroup = new PendingGroup(group, exchange, batchId);
                pendingGroups.add(pendingGroup);
                pendingByBatch.put(batchId, pendingGroup);
                exchange.sendBatch(batchId, group.handles);
                pendingGroup.batchSent = true;
                batchesSent++;
            }
            success = true;
        } catch (Exception e) {
            setFailure(e);
        } finally {
            if (success == false) {
                if (pendingInput != null) {
                    pendingInputs.remove(pendingInput);
                    releasePendingInput(pendingInput);
                } else {
                    inputPage.releaseBlocks();
                }
            }
        }
    }

    @Override
    public void finish() {
        finishing = true;
        for (FetchService.TargetExchange exchange : exchanges.values()) {
            exchange.finish();
        }
    }

    @Override
    public boolean isFinished() {
        checkExchangeFailures();
        // If there's a failure, return false so getOutput() is called to throw the exception.
        if (failure != null) {
            return false;
        }
        if (finishing == false || pendingInputs.isEmpty() == false) {
            return false;
        }
        for (FetchService.TargetExchange exchange : exchanges.values()) {
            if (exchange.isFinished() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return pendingInputs.isEmpty() == false || failure != null;
    }

    @Override
    public Page getOutput() {
        throwIfFailed();
        drainFetchedPages();
        throwIfFailed();
        PendingInput pendingInput = pendingInputs.peekFirst();
        if (pendingInput == null) {
            return null;
        }
        if (pendingInput.isComplete() == false) {
            return null;
        }
        pendingInputs.removeFirst();
        /*
         * This is the deliberately conservative streaming boundary: responses are collected incrementally, but the
         * coordinator emits only when every group for the input page is complete. A future evolution can relax this
         * to prefix output once the position-mapping column and last-page markers prove which rows survived.
         */
        return emit(
            mergeFetchedPage(
                pendingInput.inputPage,
                pendingInput.groupByPosition,
                pendingInput.offsetByPosition,
                pendingInput.pagesByGroup()
            )
        );
    }

    private Page emit(Page page) {
        pagesEmitted++;
        rowsEmitted += page.getPositionCount();
        return page;
    }

    @Override
    public IsBlockedResult isBlocked() {
        checkExchangeFailures();
        if (failure != null) {
            return NOT_BLOCKED;
        }
        drainFetchedPages();
        if (failure != null) {
            return NOT_BLOCKED;
        }
        PendingInput pendingInput = pendingInputs.peekFirst();
        if (pendingInput == null) {
            if (needsInput()) {
                return NOT_BLOCKED;
            }
            for (FetchService.TargetExchange exchange : exchanges.values()) {
                if (exchange.isFinished() == false) {
                    return exchange.waitForCompletion();
                }
            }
            return NOT_BLOCKED;
        }
        if (pendingInput.isComplete()) {
            return NOT_BLOCKED;
        }
        for (PendingGroup group : pendingInput.groups) {
            if (group.isComplete() == false) {
                return group.exchange.isBlocked();
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public void close() {
        for (PendingInput pendingInput : pendingInputs) {
            releasePendingInput(pendingInput);
        }
        pendingInputs.clear();
        pendingByBatch.clear();
        for (FetchService.TargetExchange exchange : exchanges.values()) {
            Releasables.closeExpectNoException(exchange);
        }
        client.close();
    }

    private void drainFetchedPages() {
        boolean foundPage;
        do {
            foundPage = false;
            for (FetchService.TargetExchange exchange : exchanges.values()) {
                Page page;
                while ((page = exchange.pollPage()) != null) {
                    foundPage = true;
                    receiveFetchedPage(page);
                    if (failure != null) {
                        return;
                    }
                }
                if (checkExchangeFailures()) {
                    return;
                }
            }
        } while (foundPage);
    }

    private boolean checkExchangeFailures() {
        if (failure != null) {
            return true;
        }
        for (FetchService.TargetExchange exchange : exchanges.values()) {
            Exception exchangeFailure = exchange.getFailure();
            if (exchangeFailure != null) {
                setFailure(exchangeFailure);
                return true;
            }
        }
        return false;
    }

    private void receiveFetchedPage(Page page) {
        boolean keepPage = false;
        try {
            BatchMetadata metadata = page.batchMetadata();
            if (metadata == null) {
                throw new IllegalStateException("fetch response page missing batch metadata");
            }
            PendingGroup group = pendingByBatch.get(metadata.batchId());
            if (group == null) {
                throw new IllegalStateException("received unexpected fetch batch [" + metadata.batchId() + "]");
            }
            if (page.getPositionCount() > 0) {
                page.allowPassingToDifferentDriver();
                group.pages.add(page);
                keepPage = true;
            }
            if (metadata.isLastPageInBatch()) {
                pendingByBatch.remove(metadata.batchId());
                try {
                    group.hasPositionMapping = validateFetchedPages(group.group, group.pages);
                    group.complete = true;
                } finally {
                    markBatchCompleted(group);
                }
            }
        } catch (Exception e) {
            setFailure(e);
        } finally {
            if (keepPage == false) {
                page.releaseBlocks();
            }
        }
    }

    private void setFailure(Exception e) {
        if (failure == null) {
            failure = e;
        }
    }

    private void throwIfFailed() {
        if (failure == null) {
            return;
        }
        Exception e = failure;
        if (e instanceof RuntimeException re) {
            throw re;
        }
        throw new IllegalStateException("fetch operator failed", e);
    }

    private void releasePendingInput(PendingInput pendingInput) {
        pendingInput.inputPage.releaseBlocks();
        for (PendingGroup pendingGroup : pendingInput.groups) {
            markBatchCompleted(pendingGroup);
            pendingByBatch.remove(pendingGroup.batchId);
            releasePages(pendingGroup.pages);
        }
    }

    private static void markBatchCompleted(PendingGroup pendingGroup) {
        if (pendingGroup.batchSent && pendingGroup.batchCompleted == false) {
            pendingGroup.batchCompleted = true;
            pendingGroup.exchange.markBatchCompleted(pendingGroup.batchId);
        }
    }

    @Override
    public String toString() {
        return "FetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
    }

    @Override
    public Operator.Status status() {
        return new Status(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, batchesSent, exchangesOpened);
    }

    /**
     * Profile status for the coordinator-side fetch phase.
     * <p>
     * Because pushdown filtering happens on the data node, {@code rowsReceived - rowsEmitted} is the number of rows
     * the pushdown pruned before they crossed the wire, and {@code batchesSent}/{@code exchangesOpened} show how the
     * fetch fanned out across target sessions.
     */
    public record Status(int pagesReceived, int pagesEmitted, long rowsReceived, long rowsEmitted, long batchesSent, int exchangesOpened)
        implements
            Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "fetch",
            Status::new
        );
        private static final TransportVersion ESQL_FETCH_OPERATOR_STATUS = TransportVersion.fromName("esql_remote_fetch_operator_status");

        Status(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(batchesSent);
            out.writeVInt(exchangesOpened);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return ESQL_FETCH_OPERATOR_STATUS;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("batches_sent", batchesSent);
            builder.field("exchanges_opened", exchangesOpened);
            return builder.endObject();
        }
    }

    private GroupedHandles decodeHandles(Page inputPage) {
        BytesRefBlock handlesBlock = inputPage.getBlock(handleChannel);
        Map<TargetSession, Integer> groupLookup = new LinkedHashMap<>();
        List<Group> groups = new ArrayList<>();
        int[] groupByPosition = new int[inputPage.getPositionCount()];
        int[] offsetByPosition = new int[inputPage.getPositionCount()];
        BytesRef scratch = new BytesRef();

        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            if (handlesBlock.isNull(position)) {
                throw new IllegalStateException("fetch handle column cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("fetch handle column must contain exactly one handle per row");
            }
            FetchHandle handle = FetchHandle.fromBytesRef(handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch));
            TargetSession target = new TargetSession(handle.nodeId(), handle.retainedSessionId());
            Integer groupIndex = groupLookup.get(target);
            if (groupIndex == null) {
                groupIndex = groups.size();
                groupLookup.put(target, groupIndex);
                groups.add(new Group(target));
            }
            Group group = groups.get(groupIndex);
            groupByPosition[position] = groupIndex;
            offsetByPosition[position] = group.handles.size();
            group.handles.add(handle);
        }
        return new GroupedHandles(groups, groupByPosition, offsetByPosition);
    }

    /**
     * Validates pages returned by a single fetch group and determines the response schema.
     * <p>
     * Plain fetches ({@code pushdownPlan == null}) must return exactly {@code outputFields.size()} columns and one
     * row per handle. Mapped fetches ({@code pushdownPlan != null}) must return exactly one extra trailing position
     * mapping column; mapped rows may be fewer than the number of handles when pushdown filters rows out.
     *
     * @return {@code true} if the pages carry an extra position-mapping column, {@code false} otherwise
     * @throws IllegalStateException on column count mismatch, inconsistent schemas, or unexpected row counts
     */
    private boolean validateFetchedPages(Group group, List<Page> pages) {
        boolean expectedPositionMapping = pushdownPlan != null;
        // Note: an empty page list is fine for mapped fetches (the pushdown filter may drop every row) but is
        // caught below for plain fetches, which must return exactly one row per handle.
        int positions = 0;
        boolean[] seenPositions = expectedPositionMapping ? new boolean[group.handles.size()] : null;
        for (Page page : pages) {
            boolean pageHasPosition = page.getBlockCount() == outputFields.size() + 1;
            if (expectedPositionMapping == false && pageHasPosition) {
                throw new IllegalStateException("fetch returned mapped response pages for a plain fetch");
            }
            if (expectedPositionMapping && pageHasPosition == false) {
                throw new IllegalStateException("fetch returned plain response pages for a pushdown fetch");
            }
            if (page.getBlockCount() != outputFields.size() + (expectedPositionMapping ? 1 : 0)) {
                throw new IllegalStateException(
                    "fetch returned ["
                        + page.getBlockCount()
                        + "] columns but expected ["
                        + (outputFields.size() + (expectedPositionMapping ? 1 : 0))
                        + "]"
                );
            }
            if (expectedPositionMapping) {
                Block positionBlock = page.getBlock(page.getBlockCount() - 1);
                if (positionBlock instanceof IntBlock == false) {
                    throw new IllegalStateException(
                        "fetch position-mapping column must be an IntBlock but was [" + positionBlock.getClass().getSimpleName() + "]"
                    );
                }
                validatePositionMapping(group, page, (IntBlock) positionBlock, seenPositions);
            }
            positions += page.getPositionCount();
        }
        if (expectedPositionMapping == false && positions != group.handles.size()) {
            throw new IllegalStateException("fetch returned [" + positions + "] rows but expected [" + group.handles.size() + "]");
        }
        return expectedPositionMapping;
    }

    private static void validatePositionMapping(Group group, Page page, IntBlock positionBlock, boolean[] seenPositions) {
        for (int row = 0; row < page.getPositionCount(); row++) {
            if (positionBlock.isNull(row)) {
                throw new IllegalStateException("fetch position-mapping column cannot contain nulls");
            }
            if (positionBlock.getValueCount(row) != 1) {
                throw new IllegalStateException("fetch position-mapping column must contain exactly one position per row");
            }
            int position = positionBlock.getInt(positionBlock.getFirstValueIndex(row));
            if (position < 0 || position >= group.handles.size()) {
                throw new IllegalStateException(
                    "fetch position-mapping value [" + position + "] out of range [0, " + group.handles.size() + ")"
                );
            }
            if (seenPositions[position]) {
                throw new IllegalStateException("fetch returned duplicate position [" + position + "]");
            }
            seenPositions[position] = true;
        }
    }

    static void validatePushdownPlan(PhysicalPlan plan) {
        FetchPushdownOperatorBuilder.validateSupportedPlan(plan);
    }

    private Page mergeFetchedPage(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<GroupPages> pagesByGroup) {
        if (pagesByGroup.stream().anyMatch(g -> g != null && g.hasPositionMapping())) {
            return mergeFetchedPageWithFiltering(inputPage, groupByPosition, offsetByPosition, pagesByGroup);
        }
        FetchedRowRef[] fetchedRows = resolveFetchedRows(groupByPosition, offsetByPosition, buildGroupMappings(pagesByGroup));
        for (FetchedRowRef rowRef : fetchedRows) {
            if (rowRef == null) {
                throw new IllegalStateException("fetch response did not contain the expected row");
            }
        }
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + outputFields.size()];
        Block.Builder[] builders = new Block.Builder[outputFields.size()];
        boolean success = false;
        try {
            for (int block = 0; block < inputPage.getBlockCount(); block++) {
                outputBlocks[block] = inputPage.getBlock(block);
                outputBlocks[block].incRef();
            }
            for (int field = 0; field < outputFields.size(); field++) {
                builders[field] = PlannerUtils.toElementType(outputFields.get(field).dataType())
                    .newBlockBuilder(inputPage.getPositionCount(), driverContext.blockFactory());
                for (FetchedRowRef rowRef : fetchedRows) {
                    Page fetchedPage = pagesByGroup.get(rowRef.group()).pages().get(rowRef.pageIndex());
                    builders[field].copyFrom(fetchedPage.getBlock(field), rowRef.position(), rowRef.position() + 1);
                }
                outputBlocks[inputPage.getBlockCount() + field] = builders[field].build();
            }
            Page output = new Page(inputPage.getPositionCount(), outputBlocks);
            success = true;
            return output;
        } finally {
            inputPage.releaseBlocks();
            releasePagesByGroup(pagesByGroup);
            Releasables.closeExpectNoException(builders);
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }
    }

    /**
     * Merges fetched pages when a server-side pushdown filter may have dropped rows. The fetched pages carry an
     * extra trailing column with original-position indices so we can match surviving rows back to the coordinator's
     * input page. Rows whose position is absent from the fetch response are omitted from the output.
     */
    private Page mergeFetchedPageWithFiltering(
        Page inputPage,
        int[] groupByPosition,
        int[] offsetByPosition,
        List<GroupPages> pagesByGroup
    ) {
        FetchedRowRef[] fetchedRows = resolveFetchedRows(groupByPosition, offsetByPosition, buildGroupMappings(pagesByGroup));

        // Keep only input positions whose corresponding rows survived the pushdown filter.
        int[] survivingPositions = new int[inputPage.getPositionCount()];
        List<FetchedRowRef> keptRows = new ArrayList<>(inputPage.getPositionCount());
        int survivors = 0;
        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            FetchedRowRef rowRef = fetchedRows[position];
            if (rowRef != null) {
                survivingPositions[survivors++] = position;
                keptRows.add(rowRef);
            }
        }

        // Input columns are narrowed with Block#filter, which preserves the original encoding (constant,
        // vector, ordinal). Fetched columns are a gather across pages, so they are rebuilt with builders.
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + outputFields.size()];
        Block.Builder[] builders = new Block.Builder[outputFields.size()];
        boolean success = false;
        try {
            for (int i = 0; i < inputPage.getBlockCount(); i++) {
                outputBlocks[i] = inputPage.getBlock(i).filter(false, survivingPositions, 0, survivors);
            }
            for (int field = 0; field < outputFields.size(); field++) {
                Block.Builder builder = PlannerUtils.toElementType(outputFields.get(field).dataType())
                    .newBlockBuilder(survivors, driverContext.blockFactory());
                builders[field] = builder;
                for (FetchedRowRef rowRef : keptRows) {
                    Page fetchedPage = pagesByGroup.get(rowRef.group()).pages().get(rowRef.pageIndex());
                    builder.copyFrom(fetchedPage.getBlock(field), rowRef.position(), rowRef.position() + 1);
                }
                outputBlocks[inputPage.getBlockCount() + field] = builder.build();
            }
            Page output = new Page(survivors, outputBlocks);
            success = true;
            return output;
        } finally {
            inputPage.releaseBlocks();
            releasePagesByGroup(pagesByGroup);
            Releasables.closeExpectNoException(builders);
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }
    }

    private static FetchedRowRef[][] buildGroupMappings(List<GroupPages> pagesByGroup) {
        FetchedRowRef[][] mappings = new FetchedRowRef[pagesByGroup.size()][];
        for (int group = 0; group < pagesByGroup.size(); group++) {
            GroupPages groupPages = pagesByGroup.get(group);
            FetchedRowRef[] mapping = new FetchedRowRef[groupPages.handleCount()];
            int runningOffset = 0;
            List<Page> pages = groupPages.pages();
            for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
                Page page = pages.get(pageIndex);
                IntBlock positionBlock = groupPages.hasPositionMapping() ? page.getBlock(page.getBlockCount() - 1) : null;
                for (int row = 0; row < page.getPositionCount(); row++) {
                    int position = positionBlock == null ? runningOffset++ : positionBlock.getInt(positionBlock.getFirstValueIndex(row));
                    if (mapping[position] != null) {
                        throw new IllegalStateException("fetch returned duplicate position [" + position + "]");
                    }
                    mapping[position] = new FetchedRowRef(group, pageIndex, row);
                }
            }
            mappings[group] = mapping;
        }
        return mappings;
    }

    private static FetchedRowRef[] resolveFetchedRows(int[] groupByPosition, int[] offsetByPosition, FetchedRowRef[][] groupMappings) {
        FetchedRowRef[] fetchedRows = new FetchedRowRef[groupByPosition.length];
        for (int position = 0; position < groupByPosition.length; position++) {
            fetchedRows[position] = groupMappings[groupByPosition[position]][offsetByPosition[position]];
        }
        return fetchedRows;
    }

    private static void releasePagesByGroup(List<GroupPages> pagesByGroup) {
        for (GroupPages group : pagesByGroup) {
            releasePages(group == null ? null : group.pages());
        }
    }

    private static void releasePages(List<Page> pages) {
        if (pages != null) {
            Releasables.closeExpectNoException(Releasables.wrap(pages));
        }
    }

    private record GroupedHandles(List<Group> groups, int[] groupByPosition, int[] offsetByPosition) {}

    private record FetchedRowRef(int group, int pageIndex, int position) {}
}
