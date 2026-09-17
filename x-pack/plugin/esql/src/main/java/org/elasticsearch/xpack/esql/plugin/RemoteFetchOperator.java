/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.compute.operator.exchange.BatchExchangeStatusResponse;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-side operator that fetches deferred field values from owning data nodes after the query has
 * narrowed the candidate row set.
 * <p>
 * Each input page carries a column of serialized {@link RemoteFetchHandle}s plus any coordinator columns that
 * should be retained. For every input page the operator:
 * <ol>
 *     <li>decodes and groups handles by target session ({@code nodeId}, {@code retainedSessionId})</li>
 *     <li>opens a {@link RemoteFetchService.TargetExchange} per target session when needed</li>
 *     <li>sends batches of handles to the data node via the exchange</li>
 *     <li>collects response pages from the exchange and merges fetched columns back onto the input rows</li>
 *     <li>emits one output page once every group for that input page has completed</li>
 * </ol>
 * An optional {@code pushdownPlan} may be supplied so filtering happens on the data node. Mapped responses
 * include a trailing position-mapping column ({@link org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource#POSITION_ATTRIBUTE_NAME})
 * so rows pruned by pushdown can be omitted from the merged output; see {@link RemoteFetchPushdownOperatorBuilder} for the
 * supported pushdown shape.
 * <p>
 * Transport and data-node execution are handled by {@link RemoteFetchService}; this operator owns the coordinator
 * merge and exchange lifecycle only.
 */
public final class RemoteFetchOperator implements Operator {
    record GroupPages(List<Page> pages, boolean hasPositionMapping, int handleCount) {}

    public record Factory(
        int handleChannel,
        List<RemoteFetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxOutstandingRequests,
        RemoteFetchService.ClientFactory clientFactory
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RemoteFetchOperator(
                driverContext,
                handleChannel,
                requestFields,
                outputFields,
                pushdownPlan,
                configuration,
                maxOutstandingRequests,
                clientFactory.create()
            );
        }

        @Override
        public String describe() {
            return "RemoteFetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
        }
    }

    private record TargetSession(String nodeId, String retainedSessionId) {}

    private static final class Group {
        private final TargetSession target;
        private final List<RemoteFetchHandle> handles = new ArrayList<>();

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
        private final RemoteFetchService.TargetExchange exchange;
        private final long batchId;
        private final List<Page> pages = new ArrayList<>();
        private boolean batchSent;
        private boolean batchCompleted;
        private boolean complete;
        private boolean hasPositionMapping;

        private PendingGroup(Group group, RemoteFetchService.TargetExchange exchange, long batchId) {
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
    private final List<RemoteFetchService.FetchField> requestFields;
    private final List<Attribute> outputFields;
    private final PhysicalPlan pushdownPlan;
    private final Configuration configuration;
    private final int maxOutstandingRequests;
    private final RemoteFetchService.Client client;
    private final AtomicLong batchIds = new AtomicLong();
    // Driver-thread only. Driver.status() snapshots operators on that thread after addInput()
    // returns; _tasks reads the cached snapshot and does not call status() live.
    private final Map<TargetSession, RemoteFetchService.TargetExchange> exchanges = new HashMap<>();
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
    private final boolean profile;
    private long firstInputNanos;
    private long firstResultNanos;
    private long lastResultNanos;
    private long processEndNanos;
    private long mergeNanos;
    private final AtomicLong exchangeWaitNanos = new AtomicLong();
    // Completion can run on the listener's thread, not the driver thread.
    private final Map<SubscribableListener<Void>, Long> pendingExchangeWaits = new ConcurrentHashMap<>();

    // Note: no ThreadContext parameter on purpose. This operator only interacts with its exchanges synchronously
    // on the driver thread; response-header propagation for the async transport work is owned by
    // BidirectionalBatchExchangeClient and replayed via TargetExchangeChannel#close.
    RemoteFetchOperator(
        DriverContext driverContext,
        int handleChannel,
        List<RemoteFetchService.FetchField> requestFields,
        List<Attribute> outputFields,
        PhysicalPlan pushdownPlan,
        Configuration configuration,
        int maxOutstandingRequests,
        RemoteFetchService.Client client
    ) {
        if (requestFields.isEmpty()) {
            throw new IllegalArgumentException("remote fetch requires at least one request field");
        }
        if (outputFields.isEmpty()) {
            throw new IllegalArgumentException("remote fetch requires at least one output field");
        }
        if (requestFields.size() != outputFields.size()) {
            throw new IllegalArgumentException(
                "remote fetch request fields [" + requestFields.size() + "] must match output fields [" + outputFields.size() + "]"
            );
        }
        validatePushdownPlan(pushdownPlan);
        this.driverContext = driverContext;
        this.handleChannel = handleChannel;
        this.requestFields = List.copyOf(requestFields);
        this.outputFields = List.copyOf(outputFields);
        this.pushdownPlan = pushdownPlan;
        this.configuration = configuration;
        this.maxOutstandingRequests = maxOutstandingRequests;
        this.client = client;
        this.profile = configuration.profile();
    }

    @Override
    public boolean needsInput() {
        return finishing == false && failure == null && pendingInputs.size() < maxOutstandingRequests;
    }

    @Override
    public void addInput(Page inputPage) {
        if (profile && firstInputNanos == 0L) {
            firstInputNanos = System.nanoTime();
        }
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
                RemoteFetchService.TargetExchange exchange = exchanges.get(group.target);
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
        for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
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
        for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
            if (exchange.isFinished() == false) {
                return false;
            }
        }
        if (profile && processEndNanos == 0L) {
            processEndNanos = System.nanoTime();
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
        long mergeStartNanos = profile ? System.nanoTime() : 0L;
        try {
            return emit(
                mergeFetchedPage(
                    pendingInput.inputPage,
                    pendingInput.groupByPosition,
                    pendingInput.offsetByPosition,
                    pendingInput.pagesByGroup()
                )
            );
        } finally {
            if (profile) {
                mergeNanos += System.nanoTime() - mergeStartNanos;
            }
        }
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
            for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
                if (exchange.isFinished() == false) {
                    return trackWait(exchange.waitForCompletion());
                }
            }
            return NOT_BLOCKED;
        }
        if (pendingInput.isComplete()) {
            return NOT_BLOCKED;
        }
        for (PendingGroup group : pendingInput.groups) {
            if (group.isComplete() == false) {
                return trackWait(group.exchange.isBlocked());
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public void close() {
        if (profile && firstInputNanos != 0L && processEndNanos == 0L) {
            processEndNanos = System.nanoTime();
        }
        for (PendingInput pendingInput : pendingInputs) {
            releasePendingInput(pendingInput);
        }
        pendingInputs.clear();
        pendingByBatch.clear();
        for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
            Releasables.closeExpectNoException(exchange);
        }
        client.close();
    }

    private void drainFetchedPages() {
        boolean foundPage;
        do {
            foundPage = false;
            for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
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
        for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
            Exception exchangeFailure = exchange.getFailure();
            if (exchangeFailure != null) {
                setFailure(exchangeFailure);
                return true;
            }
        }
        return false;
    }

    private void receiveFetchedPage(Page page) {
        if (profile) {
            long now = System.nanoTime();
            if (firstResultNanos == 0L) {
                firstResultNanos = now;
            }
            lastResultNanos = now;
        }
        boolean keepPage = false;
        try {
            BatchMetadata metadata = page.batchMetadata();
            if (metadata == null) {
                throw new IllegalStateException("remote fetch response page missing batch metadata");
            }
            PendingGroup group = pendingByBatch.get(metadata.batchId());
            if (group == null) {
                throw new IllegalStateException("received unexpected remote fetch batch [" + metadata.batchId() + "]");
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

    private IsBlockedResult trackWait(IsBlockedResult blocked) {
        if (profile == false || blocked.listener().isDone()) {
            return blocked;
        }
        SubscribableListener<Void> listener = blocked.listener();
        if (pendingExchangeWaits.putIfAbsent(listener, System.nanoTime()) == null) {
            listener.addListener(ActionListener.wrap(ignored -> completeExchangeWait(listener), e -> completeExchangeWait(listener)));
        }
        return blocked;
    }

    private void completeExchangeWait(SubscribableListener<Void> listener) {
        Long waitStartNanos = pendingExchangeWaits.remove(listener);
        if (waitStartNanos != null) {
            exchangeWaitNanos.addAndGet(System.nanoTime() - waitStartNanos);
        }
    }

    /*
     * Fetch failures currently fail the query even when allow_partial_results is true. Earlier shard failures still follow
     * the existing partial-results policy; this operator does not make selection over incomplete input globally complete.
     *
     * TODO: Support recoverable fetch failures when partial results are allowed: omit affected rows rather than substitute
     * nulls, preserve the order of complete rows from successful fetches, and mark the response partial with the affected
     * shard failures without double-counting failures already recorded during selection. Returning fewer than N rows is
     * acceptable; refilling vacancies requires extra candidates or another selection pass and is separate work. Keep
     * cancellation, query-wide errors, and the existing all-shards-failed behavior fatal. With partial results disabled,
     * failures that prevent selecting or fetching the global TopN must continue to fail the query.
     */
    private void throwIfFailed() {
        if (failure == null) {
            return;
        }
        Exception e = failure;
        if (e instanceof RuntimeException re) {
            throw re;
        }
        throw new IllegalStateException("remote fetch operator failed", e);
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
        return "RemoteFetchOperator[channel=" + handleChannel + ", requestFields=" + requestFields + "]";
    }

    @Override
    public Operator.Status status() {
        return new Status(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, batchesSent, exchangesOpened, buildProfile());
    }

    private Profile buildProfile() {
        if (profile == false) {
            return Profile.EMPTY;
        }
        long processNanos = firstInputNanos == 0L ? 0L : (processEndNanos == 0L ? System.nanoTime() : processEndNanos) - firstInputNanos;
        long timeToFirstResultNanos = firstResultNanos == 0L ? 0L : firstResultNanos - firstInputNanos;
        long responseNanos = lastResultNanos == 0L ? 0L : lastResultNanos - firstResultNanos;
        long totalSetupNanos = 0L;
        long maxSetupNanos = 0L;
        long bytesRead = 0L;
        long fetchNanos = 0L;
        long maxFetchNanos = 0L;
        long fetchCpuNanos = 0L;
        long valuesLoaded = 0L;
        long fieldLoadNanos = 0L;
        long sourceDocsLoaded = 0L;
        long sourceFieldReads = 0L;
        long sourceBytesLoaded = 0L;
        long requestPages = 0L;
        long requestRows = 0L;
        long requestSerializedBytes = 0L;
        long responsePages = 0L;
        long responseRows = 0L;
        long responseSerializedBytes = 0L;
        List<BidirectionalBatchExchangeClient.WorkerProfile> workers = new ArrayList<>();
        for (RemoteFetchService.TargetExchange exchange : exchanges.values()) {
            BidirectionalBatchExchangeClient.Profile exchangeProfile = exchange.profile();
            totalSetupNanos += exchangeProfile.totalSetupNanos();
            maxSetupNanos = Math.max(maxSetupNanos, exchangeProfile.maxSetupNanos());
            bytesRead += exchangeProfile.bytesRead();
            workers.addAll(exchangeProfile.workers());
            for (BidirectionalBatchExchangeClient.WorkerProfile worker : exchangeProfile.workers()) {
                requestPages += worker.request().pages();
                requestRows += worker.request().rows();
                requestSerializedBytes += worker.request().serializedBytes();
                BatchExchangeStatusResponse.Profile fetchProfile = worker.server();
                if (fetchProfile == null) {
                    continue;
                }
                fetchNanos += fetchProfile.driverTookNanos();
                maxFetchNanos = Math.max(maxFetchNanos, fetchProfile.driverTookNanos());
                fetchCpuNanos += fetchProfile.driverCpuNanos();
                valuesLoaded += fetchProfile.valuesLoaded();
                fieldLoadNanos += fetchProfile.fieldLoadNanos();
                sourceDocsLoaded += fetchProfile.sourceDocsLoaded();
                sourceFieldReads += fetchProfile.sourceFieldReads();
                sourceBytesLoaded += fetchProfile.sourceBytesLoaded();
                responsePages += fetchProfile.responsePages();
                responseRows += fetchProfile.responseRows();
                responseSerializedBytes += fetchProfile.responseSerializedBytes();
            }
        }
        workers.sort(
            Comparator.comparing(BidirectionalBatchExchangeClient.WorkerProfile::exchangeId)
                .thenComparingInt(BidirectionalBatchExchangeClient.WorkerProfile::workerId)
        );
        return new Profile(
            processNanos,
            timeToFirstResultNanos,
            responseNanos,
            exchangeWaitNanos.get(),
            mergeNanos,
            totalSetupNanos,
            maxSetupNanos,
            fetchNanos,
            maxFetchNanos,
            fetchCpuNanos,
            fieldLoadNanos,
            valuesLoaded,
            sourceDocsLoaded,
            sourceFieldReads,
            sourceBytesLoaded,
            bytesRead,
            requestPages,
            requestRows,
            requestSerializedBytes,
            responsePages,
            responseRows,
            responseSerializedBytes,
            workers
        );
    }

    /**
     * Profile status for the coordinator-side remote fetch phase.
     * <p>
     * Because pushdown filtering happens on the data node, {@code rowsReceived - rowsEmitted} is the number of rows
     * the pushdown pruned before they crossed the wire, and {@code batchesSent}/{@code exchangesOpened} show how the
     * fetch fanned out across target sessions.
     */
    public record Status(
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted,
        long batchesSent,
        int exchangesOpened,
        Profile profile
    ) implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "remote_fetch",
            Status::new
        );
        private static final TransportVersion ESQL_REMOTE_FETCH_OPERATOR_STATUS = TransportVersion.fromName(
            "esql_remote_fetch_operator_status"
        );

        Status(StreamInput in) throws IOException {
            this(
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVInt(),
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE) ? new Profile(in) : Profile.EMPTY
            );
        }

        public Status(int pagesReceived, int pagesEmitted, long rowsReceived, long rowsEmitted, long batchesSent, int exchangesOpened) {
            this(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, batchesSent, exchangesOpened, Profile.EMPTY);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(batchesSent);
            out.writeVInt(exchangesOpened);
            if (out.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE)) {
                profile.writeTo(out);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return ESQL_REMOTE_FETCH_OPERATOR_STATUS;
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
            if (profile.equals(Profile.EMPTY) == false) {
                profile.toXContent(builder);
            }
            return builder.endObject();
        }
    }

    /**
     * Timing and loading breakdown for remote fetch.
     * <p>
     * {@code processNanos} covers the operator's end-to-end critical path, {@code exchangeWaitNanos} sums the time
     * unresolved exchange listeners remained pending, and {@code responseNanos} is the span from the first to the last response page.
     * Setup and fetch totals sum work across exchanges, while their maxima identify the slowest individual exchange.
     * Request and response serialized bytes cover exchange-response payloads before transport framing or compression.
     * <p>
     * {@code valuesLoaded} aggregates {@link org.elasticsearch.compute.operator.OperatorStatus#valuesLoaded()} across every
     * operator in the server-side fetch driver. {@code fieldLoadNanos} and the {@code source*} fields come only from
     * {@link org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus} instances in that driver.
     */
    public record Profile(
        long processNanos,
        long timeToFirstResultNanos,
        long responseNanos,
        long exchangeWaitNanos,
        long mergeNanos,
        long totalSetupNanos,
        long maxSetupNanos,
        long fetchNanos,
        long maxFetchNanos,
        long fetchCpuNanos,
        long fieldLoadNanos,
        long valuesLoaded,
        long sourceDocsLoaded,
        long sourceFieldReads,
        long sourceBytesLoaded,
        long bytesRead,
        long requestPages,
        long requestRows,
        long requestSerializedBytes,
        long responsePages,
        long responseRows,
        long responseSerializedBytes,
        List<BidirectionalBatchExchangeClient.WorkerProfile> workers
    ) implements org.elasticsearch.common.io.stream.Writeable {
        static final Profile EMPTY = new Profile(
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            List.of()
        );

        public Profile {
            workers = List.copyOf(workers);
        }

        Profile(StreamInput in) throws IOException {
            this(
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
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE) ? in.readVLong() : 0L,
                in.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE)
                    ? in.readCollectionAsImmutableList(BidirectionalBatchExchangeClient.WorkerProfile::new)
                    : List.of()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(processNanos);
            out.writeVLong(timeToFirstResultNanos);
            out.writeVLong(responseNanos);
            out.writeVLong(exchangeWaitNanos);
            out.writeVLong(mergeNanos);
            out.writeVLong(totalSetupNanos);
            out.writeVLong(maxSetupNanos);
            out.writeVLong(fetchNanos);
            out.writeVLong(maxFetchNanos);
            out.writeVLong(fetchCpuNanos);
            out.writeVLong(fieldLoadNanos);
            out.writeVLong(valuesLoaded);
            out.writeVLong(sourceDocsLoaded);
            out.writeVLong(sourceFieldReads);
            out.writeVLong(sourceBytesLoaded);
            out.writeVLong(bytesRead);
            if (out.getTransportVersion().supports(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_GRANULAR_PROFILE)) {
                out.writeVLong(requestPages);
                out.writeVLong(requestRows);
                out.writeVLong(requestSerializedBytes);
                out.writeVLong(responsePages);
                out.writeVLong(responseRows);
                out.writeVLong(responseSerializedBytes);
                out.writeCollection(workers);
            }
        }

        void toXContent(XContentBuilder builder) throws IOException {
            builder.field("process_nanos", processNanos);
            builder.field("time_to_first_result_nanos", timeToFirstResultNanos);
            builder.field("response_nanos", responseNanos);
            builder.field("exchange_wait_nanos", exchangeWaitNanos);
            builder.field("merge_nanos", mergeNanos);
            builder.field("setup_nanos", totalSetupNanos);
            builder.field("max_setup_nanos", maxSetupNanos);
            builder.field("fetch_nanos", fetchNanos);
            builder.field("max_fetch_nanos", maxFetchNanos);
            builder.field("fetch_cpu_nanos", fetchCpuNanos);
            builder.field("field_load_nanos", fieldLoadNanos);
            builder.field("values_loaded", valuesLoaded);
            builder.field("source_docs_loaded", sourceDocsLoaded);
            builder.field("source_field_reads", sourceFieldReads);
            builder.field("source_bytes_loaded", sourceBytesLoaded);
            builder.field("bytes_read", bytesRead);
            builder.field("request_pages", requestPages);
            builder.field("request_rows", requestRows);
            builder.field("request_serialized_bytes", requestSerializedBytes);
            builder.field("response_pages", responsePages);
            builder.field("response_rows", responseRows);
            builder.field("response_serialized_bytes", responseSerializedBytes);
            builder.startArray("workers");
            for (BidirectionalBatchExchangeClient.WorkerProfile worker : workers) {
                workerToXContent(builder, worker);
            }
            builder.endArray();
        }

        private static void workerToXContent(XContentBuilder builder, BidirectionalBatchExchangeClient.WorkerProfile worker)
            throws IOException {
            builder.startObject();
            builder.field("exchange_id", worker.exchangeId());
            builder.field("node_id", worker.nodeId());
            builder.field("node_name", worker.nodeName());
            builder.field("worker_id", worker.workerId());
            builder.field("local", worker.local());
            builder.field("bytes_read", worker.bytesRead());
            builder.startObject("setup");
            builder.field("round_trip_nanos", worker.setupNanos());
            BatchExchangeStatusResponse.Profile server = worker.server();
            builder.endObject();
            exchangeToXContent(builder, "request", worker.request());
            if (server != null) {
                builder.startObject("response");
                builder.field("pages", server.responsePages());
                builder.field("rows", server.responseRows());
                builder.field("serialized_bytes", server.responseSerializedBytes());
                builder.endObject();
                builder.startObject("driver");
                builder.field("took_nanos", server.driverTookNanos());
                builder.field("cpu_nanos", server.driverCpuNanos());
                builder.field("field_load_nanos", server.fieldLoadNanos());
                builder.field("values_loaded", server.valuesLoaded());
                builder.field("source_docs_loaded", server.sourceDocsLoaded());
                builder.field("source_field_reads", server.sourceFieldReads());
                builder.field("source_bytes_loaded", server.sourceBytesLoaded());
                builder.endObject();
            }
            builder.endObject();
        }

        private static void exchangeToXContent(XContentBuilder builder, String name, ExchangeSinkHandler.Profile exchange)
            throws IOException {
            builder.startObject(name);
            builder.field("pages", exchange.pages());
            builder.field("rows", exchange.rows());
            builder.field("serialized_bytes", exchange.serializedBytes());
            builder.endObject();
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
                throw new IllegalStateException("remote fetch handle column cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("remote fetch handle column must contain exactly one handle per row");
            }
            RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(
                handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
            );
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
                throw new IllegalStateException("remote fetch returned mapped response pages for a plain fetch");
            }
            if (expectedPositionMapping && pageHasPosition == false) {
                throw new IllegalStateException("remote fetch returned plain response pages for a pushdown fetch");
            }
            if (page.getBlockCount() != outputFields.size() + (expectedPositionMapping ? 1 : 0)) {
                throw new IllegalStateException(
                    "remote fetch returned ["
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
                        "remote fetch position-mapping column must be an IntBlock but was ["
                            + positionBlock.getClass().getSimpleName()
                            + "]"
                    );
                }
                validatePositionMapping(group, page, (IntBlock) positionBlock, seenPositions);
            }
            positions += page.getPositionCount();
        }
        if (expectedPositionMapping == false && positions != group.handles.size()) {
            throw new IllegalStateException("remote fetch returned [" + positions + "] rows but expected [" + group.handles.size() + "]");
        }
        return expectedPositionMapping;
    }

    private static void validatePositionMapping(Group group, Page page, IntBlock positionBlock, boolean[] seenPositions) {
        for (int row = 0; row < page.getPositionCount(); row++) {
            if (positionBlock.isNull(row)) {
                throw new IllegalStateException("remote fetch position-mapping column cannot contain nulls");
            }
            if (positionBlock.getValueCount(row) != 1) {
                throw new IllegalStateException("remote fetch position-mapping column must contain exactly one position per row");
            }
            int position = positionBlock.getInt(positionBlock.getFirstValueIndex(row));
            if (position < 0 || position >= group.handles.size()) {
                throw new IllegalStateException(
                    "remote fetch position-mapping value [" + position + "] out of range [0, " + group.handles.size() + ")"
                );
            }
            if (seenPositions[position]) {
                throw new IllegalStateException("remote fetch returned duplicate position [" + position + "]");
            }
            seenPositions[position] = true;
        }
    }

    static void validatePushdownPlan(PhysicalPlan plan) {
        RemoteFetchPushdownOperatorBuilder.validateSupportedPlan(plan);
    }

    private Page mergeFetchedPage(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<GroupPages> pagesByGroup) {
        if (pagesByGroup.stream().anyMatch(g -> g != null && g.hasPositionMapping())) {
            return mergeFetchedPageWithFiltering(inputPage, groupByPosition, offsetByPosition, pagesByGroup);
        }
        FetchedRowRef[] fetchedRows = resolveFetchedRows(groupByPosition, offsetByPosition, buildGroupMappings(pagesByGroup));
        for (FetchedRowRef rowRef : fetchedRows) {
            if (rowRef == null) {
                throw new IllegalStateException("remote fetch response did not contain the expected row");
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
                        throw new IllegalStateException("remote fetch returned duplicate position [" + position + "]");
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
