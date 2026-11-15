/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverEarlyTerminationException;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * Coordinator source of completed sequences, with bounded search pages and asynchronous stage execution. Selection
 * retains keys and source handles, including already evaluated cells when a finite branch proof permits reuse.
 * Remaining payload is fetched under the same PIT for direct materialization or ordinary full stage replay,
 * preserving selected expansion rows, computed fields and original metadata.
 * <p>
 * Matching and stage CPU work run on the owning driver. Callbacks hand off one completion and wake the driver.
 * Snapshot renewal runs independently of output pulls, but its callbacks can wait for driver work on the shared monitor.
 * Closing waits for in-flight ownership through DriverContext and closes even a PIT returned after early termination.
 * Searches use the caller's credentials and a fixed PIT shard enumeration.
 * Retained output reuses values from its authorized selection read; each later payload read authorizes normally.
 * A fully retained result does not create a new search solely to reauthorize a value already read.
 */
public final class SequenceSourceOperator extends SourceOperator {
    private static final Logger logger = LogManager.getLogger(SequenceSourceOperator.class);
    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(2);
    private static final int MAX_PIT_CLOSE_ATTEMPTS = 3;
    private static final TimeValue PIT_CLOSE_RETRY_DELAY = TimeValue.timeValueMillis(100);
    private static final TimeValue PIT_CLOSE_ATTEMPT_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final int SEARCH_PAGE_SIZE = 256;
    private static final int PAYLOAD_BATCH_SIZE = 64;
    private static final String BREAKER_LABEL = "esql_sequence_reader";
    private static final Comparator<Candidate> CANDIDATE_ORDER = Comparator.comparing((Candidate c) -> c.ordinal)
        .thenComparing(c -> c.key.bytes(), SequenceKeyCodec::compare);

    /** Compiles factories for fresh stage drivers, with an ownership-transferring output consumer for each binding. */
    @FunctionalInterface
    public interface StagePlanner {
        Supplier<List<Driver>> plan(PhysicalPlan plan, Consumer<Page> output);
    }

    /** Runs prepared drivers as children of the query task, including cancellation and completion cleanup. */
    @FunctionalInterface
    public interface StageExecutor {
        void execute(List<Driver> drivers, ActionListener<Void> listener);
    }

    /** Supplies the native coordinator source while the local planner supplies ordinary stage driver preparation. */
    @FunctionalInterface
    public interface FactoryProvider {
        /** Borrows the existing registry for evaluators created separately by each source's driver context. */
        SourceOperatorFactory create(SequenceExec sequence, StagePlanner planner, @Nullable AnalysisRegistry analysisRegistry);
    }

    /** Selects bounded native compilation while keeping today's request factory and record equality unchanged. */
    public static FactoryProvider sourceFactoryProvider(BiFunction<SequenceExec, StagePlanner, Factory> dependencies) {
        Objects.requireNonNull(dependencies);
        return (plan, stagePlanner, analysisRegistry) -> {
            Sequence sequence = plan.sequence();
            return switch (sequence.pairing()) {
                case TODAY -> {
                    if (sequence.hasConditions()) {
                        throw new IllegalArgumentException("SEQUENCE pairing [today] cannot evaluate in-stage MEMBER conditions");
                    }
                    yield dependencies.apply(plan, stagePlanner);
                }
                case LATEST_VALID -> {
                    int maximum = SequenceLatestValidConfig.maximumCandidates(sequence.children().size());
                    int candidates = sequence.maxCandidates() == null
                        ? Math.min(SequenceLatestValidConfig.DEFAULT_MAX_CANDIDATES, maximum)
                        : sequence.maxCandidates();
                    validateConfiguredBound(sequence, candidates);
                    yield new LatestValidFactory(dependencies.apply(plan, stagePlanner), candidates, analysisRegistry);
                }
            };
        };
    }

    /** The registry belongs to the node; only the per-source compiler and its evaluator resources transfer to the source. */
    private record LatestValidFactory(Factory factory, int maxCandidates, @Nullable AnalysisRegistry analysisRegistry)
        implements
            SourceOperatorFactory {
        private LatestValidFactory {
            Objects.requireNonNull(factory);
        }

        @Override
        public SourceOperator get(DriverContext context) {
            return factory.getWithConditions(context, maxCandidates, analysisRegistry);
        }

        @Override
        public String describe() {
            return factory.describe();
        }
    }

    /** Request-scoped dependencies; each driver gets a separate matcher, snapshot and cursor set. */
    public record Factory(
        SequenceExec plan,
        Client client,
        Client cleanupClient,
        CancellableTask task,
        Configuration configuration,
        StagePlanner stagePlanner,
        StageExecutor stageExecutor,
        Supplier<ThreadContext.StoredContext> requestContext,
        @Nullable EsqlExecutionInfo executionInfo
    ) implements SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext context) {
            return new SequenceSourceOperator(this, context);
        }

        /** Admits prepared production state without adding components to the public request factory record. */
        SourceOperator get(DriverContext context, SequenceLatestValidConfig latestValid) {
            return new SequenceSourceOperator(
                this,
                context,
                DEFAULT_KEEP_ALIVE,
                ArrayUtil.MAX_ARRAY_LENGTH,
                Objects.requireNonNull(latestValid)
            );
        }

        /** Compiles conditions per source and transfers evaluator ownership after successful source construction. */
        SourceOperator getWithConditions(DriverContext context, int maxCandidates, @Nullable AnalysisRegistry analysisRegistry) {
            validateConfiguredBound(plan.sequence(), maxCandidates);
            var conditions = SequenceConditionCompiler.compile(
                plan.sequence(),
                maxCandidates,
                configuration.newFoldContext(),
                context,
                analysisRegistry
            );
            boolean success = false;
            try {
                var source = new SequenceSourceOperator(
                    this,
                    context,
                    DEFAULT_KEEP_ALIVE,
                    ArrayUtil.MAX_ARRAY_LENGTH,
                    conditions.config(),
                    conditions
                );
                success = true;
                return source;
            } finally {
                if (success == false) {
                    Releasables.closeWhileHandlingException(conditions);
                }
            }
        }

        @Override
        public String describe() {
            return "SequenceSource[stages=" + plan.sequence().children().size() + "]";
        }
    }

    private final Factory factory;
    private final DriverContext context;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final Sequence sequence;
    private final SequencePairing matcher;
    private final Releasable conditionResources;
    private final SequenceExecutionInfo executionInfo;
    private final TimeValue keepAlive;
    private final int retainedHandleCapacity;
    private final int latestValidBound;
    private final TimeValue renewalInterval;
    private final Warnings.Budget stageWarningsBudget = new Warnings.Budget();
    private final List<Cursor> cursors = new ArrayList<>();
    private final List<SequenceStageInput> payload = new ArrayList<>();
    private SubscribableListener<Void> wakeup = new SubscribableListener<>();
    private Completion completion;
    private boolean pending;
    private boolean finished;
    private boolean closed;
    private boolean closingPit;
    private int pitCloseAttempts;
    private boolean renewalPending;
    private Scheduler.ScheduledCancellable renewalTask;
    private long renewalGeneration;
    private boolean asyncRegistered;
    private boolean asyncReleased;
    private BytesReference pit;
    private OriginalIndices pitAuthorizationIndices;
    private RuntimeException failure;
    private MatchBatch batch;
    private boolean inputExhausted;
    private boolean initialTimeBoundReady;
    private SequenceOrdinal initialTimeBound;
    private long heldBytes;
    private int resolvedSources;
    private long candidateSearchRequests;
    private long payloadSearchRequests;
    private long candidateRowsFetched;
    private long payloadRowsFetched;
    private long completedMatches;
    private long rowsEmitted;
    private long stageDrivers;
    private long stageDriverCpuNanos;
    private long stageDriverIterations;
    private long candidateSearchTookMillis;
    private long payloadSearchTookMillis;
    private long maxPendingCandidates;

    private SequenceSourceOperator(Factory factory, DriverContext context) {
        this(factory, context, DEFAULT_KEEP_ALIVE);
    }

    /** A short injected lease allows real expiry tests without changing the command's production configuration. */
    SequenceSourceOperator(Factory factory, DriverContext context, TimeValue keepAlive) {
        this(factory, context, keepAlive, ArrayUtil.MAX_ARRAY_LENGTH);
    }

    /** A bounded injected capacity exercises lossless overflow fallback without allocating a multi-gigabyte cell. */
    SequenceSourceOperator(Factory factory, DriverContext context, TimeValue keepAlive, int retainedHandleCapacity) {
        this(factory, context, keepAlive, retainedHandleCapacity, null);
    }

    /** Internal entries use the same production admission checks as the selecting query factory. */
    SequenceSourceOperator(
        Factory factory,
        DriverContext context,
        TimeValue keepAlive,
        int retainedHandleCapacity,
        @Nullable SequenceLatestValidConfig latestValid
    ) {
        this(factory, context, keepAlive, retainedHandleCapacity, latestValid, null);
    }

    /** The source takes ownership of compiled evaluators together with its matcher and mandatory candidate values. */
    SequenceSourceOperator(
        Factory factory,
        DriverContext context,
        TimeValue keepAlive,
        int retainedHandleCapacity,
        @Nullable SequenceLatestValidConfig latestValid,
        @Nullable Releasable conditionResources
    ) {
        if (keepAlive.millis() < 3) {
            throw new IllegalArgumentException("SEQUENCE snapshot keep-alive must be at least three milliseconds");
        }
        sequence = factory.plan.sequence();
        if (latestValid != null) {
            latestValid.validateForProduction(sequence.children().size());
            validateConfiguredBound(sequence, latestValid.maxCandidates().orElseThrow());
        }
        latestValidBound = latestValid == null ? 0 : latestValid.maxCandidates().orElseThrow();
        this.factory = factory;
        this.conditionResources = conditionResources;
        this.context = context;
        this.keepAlive = keepAlive;
        if (retainedHandleCapacity < 1 || retainedHandleCapacity > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("invalid SEQUENCE retained handle capacity");
        }
        this.retainedHandleCapacity = retainedHandleCapacity;
        renewalInterval = TimeValue.timeValueMillis(keepAlive.millis() / 3);
        blockFactory = context.blockFactory().parent();
        breaker = LocalCircuitBreaker.forAsyncIo(context.breaker());
        if (latestValid == null && (sequence.pairing() == Sequence.Pairing.LATEST_VALID || sequence.hasConditions())) {
            throw new IllegalStateException("SEQUENCE latest_valid requires a prepared production configuration");
        }
        executionInfo = new SequenceExecutionInfo(factory.executionInfo());
        long[] maxGapNanos = null;
        if (sequence.hasLinkTimeLimits()) {
            maxGapNanos = new long[sequence.children().size()];
            for (int stage = 1; stage < maxGapNanos.length; stage++) {
                maxGapNanos[stage] = ((SequenceStage) sequence.children().get(stage)).maxGapNanos();
            }
        }
        matcher = latestValid == null
            ? SequencePairing.legacy(
                sequence.children().size(),
                new TimeValue(sequence.maxSpanNanos(), TimeUnit.NANOSECONDS),
                breaker,
                () -> factory.task != null && factory.task.isCancelled(),
                SequenceKeyCodec::compare,
                maxGapNanos
            )
            : new SequenceLatestValidMatcher(
                sequence.children().size(),
                new TimeValue(sequence.maxSpanNanos(), TimeUnit.NANOSECONDS),
                breaker,
                () -> factory.task != null && factory.task.isCancelled(),
                SequenceKeyCodec::compare,
                maxGapNanos,
                latestValid
            );
        boolean success = false;
        try {
            reserve(1024L + 256L * sequence.children().size() + 16L * sequence.output().size());
            for (int stage = 0; stage < sequence.children().size(); stage++) {
                var logical = (SequenceStage) sequence.children().get(stage);
                List<Attribute> payloads = new ArrayList<>();
                for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                    if (sequence.sourceStages().get(column) == stage) {
                        payloads.add(sequence.sourceAttributes().get(column));
                    }
                }
                List<SequenceLatestValidConfig.ValueDemand> demands = latestValid == null
                    ? List.of()
                    : latestValid.mandatoryDemands().get(stage);
                if (latestValid != null) {
                    // Includes the canonical demand projection and its temporary identity set/list overlap.
                    reserve(256L + 128L * demands.size());
                }
                var selection = latestValid == null
                    ? SequenceStageInput.selection(logical, payloads, this::reserve, stage + 1)
                    : SequenceStageInput.selection(logical, payloads, demands, this::reserve, stage + 1);
                payload.add(new SequenceStageInput(logical, payloads, stage + 1));
                for (int branch = 0; branch < selection.sourceCount(); branch++) {
                    reserve(1024);
                    cursors.add(new Cursor(stage, branch, selection, logical.keys(), demands));
                }
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeWhileHandlingException(this);
            }
        }
    }

    private static void validateConfiguredBound(Sequence sequence, int maxCandidates) {
        SequenceLatestValidConfig.validateCandidateBound(maxCandidates, sequence.children().size());
        if (sequence.maxCandidates() != null) {
            SequenceLatestValidConfig.validateCandidateBound(sequence.maxCandidates(), sequence.children().size());
            if (sequence.maxCandidates() != maxCandidates) {
                throw new IllegalArgumentException("SEQUENCE max_candidates configuration does not match its plan");
            }
        }
    }

    @Override
    public synchronized Page getOutput() {
        if (failure != null) {
            throw failure;
        }
        if (finished || closed) {
            return null;
        }
        context.checkForEarlyTermination();
        try {
            if (completion != null) {
                Completion ready = completion;
                completion = null;
                ready.next.run();
            }
            if (failure != null) {
                throw failure;
            }
            if (pending || completion != null) {
                return null;
            }
            if (pit == null) {
                prepareSnapshot();
                return null;
            }
            if (batch != null && batch.materializing) {
                return materialize();
            }
            if (inputExhausted) {
                finish();
                return null;
            }
            if (initialTimeBoundReady == false) {
                SequenceOrdinal earliest = null;
                for (Cursor cursor : cursors) {
                    if (cursor.stage != 0) {
                        continue;
                    }
                    context.checkForEarlyTermination();
                    if (cursor.offset == cursor.rows.size() && cursor.exhausted == false) {
                        load(cursor);
                        return null;
                    }
                    if (cursor.offset < cursor.rows.size()) {
                        SequenceOrdinal ordinal = cursor.rows.get(cursor.offset).ordinal;
                        if (earliest == null || ordinal.compareTo(earliest) < 0) {
                            earliest = ordinal;
                        }
                    }
                }
                // Later-stage events before every eligible start cannot participate. Derive the bound only after
                // evaluating every first-stage branch, including branches that require multiple filtered pages.
                // Excluded later-stage hits are not evaluated and therefore do not produce diagnostics.
                initialTimeBound = earliest;
                initialTimeBoundReady = true;
            }
            // Yield to the normal driver scheduler even when many unmatched candidates are buffered locally.
            for (int iteration = 0; iteration < 128; iteration++) {
                context.checkForEarlyTermination();
                if (noPossibleMatches()) {
                    return endInput();
                }
                Cursor next = null;
                for (Cursor cursor : cursors) {
                    if (cursor.offset == cursor.rows.size() && cursor.exhausted == false) {
                        if (batch != null) {
                            batch.prepare();
                            return materialize();
                        }
                        load(cursor);
                        return null;
                    }
                    if (cursor.offset < cursor.rows.size() && (next == null || before(cursor, next))) {
                        next = cursor;
                    }
                }
                if (next == null) {
                    return endInput();
                }
                Candidate row = next.rows.set(next.offset++, null);
                SequenceMatch selected;
                try (row) {
                    selected = matcher.match(next.stage, row.key.bytes(), row.ordinal, row.handle, row.takeValues());
                    maxPendingCandidates = Math.max(maxPendingCandidates, matcher.retainedCandidates());
                }
                if (next.offset == next.rows.size()) {
                    next.resetRows();
                    next.offset = 0;
                }
                if (selected != null) {
                    completedMatches++;
                    boolean accepted = false;
                    try {
                        if (batch == null) {
                            // LIMIT 1 fetches only its own payload. Later batches hold at most 64 matches and may
                            // flush earlier when candidate paging or the scheduler yield interrupts collection.
                            batch = new MatchBatch(rowsEmitted == 0 ? 1 : PAYLOAD_BATCH_SIZE);
                        }
                        batch.matches.add(selected);
                        accepted = true;
                    } finally {
                        if (accepted == false) {
                            selected.close();
                        }
                    }
                    if (batch.matches.size() == batch.capacity) {
                        batch.prepare();
                        return materialize();
                    }
                }
            }
            if (batch != null) {
                batch.prepare();
                return materialize();
            }
            return null;
        } catch (DriverEarlyTerminationException e) {
            // The driver treats downstream demand being satisfied as success and owns terminal cleanup.
            throw e;
        } catch (Exception e) {
            recordFailure(e);
            finish();
            throw failure;
        }
    }

    private boolean noPossibleMatches() {
        if (pending || completion != null) {
            return false;
        }
        for (Cursor cursor : cursors) {
            if (cursor.stage == 0 && (cursor.exhausted == false || cursor.offset < cursor.rows.size())) {
                return false;
            }
        }
        // Legacy viability includes advanced prefixes; latest-valid viability counts unused starts only.
        return matcher.possibleMatch() == false;
    }

    private Page endInput() throws IOException {
        inputExhausted = true;
        if (batch != null) {
            batch.prepare();
            return materialize();
        }
        finish();
        return null;
    }

    private SequenceOrdinal latestReadStart() {
        SequenceOrdinal latest = null;
        for (Cursor cursor : cursors) {
            if (cursor.stage != 0) {
                continue;
            }
            if (cursor.exhausted == false) {
                return null;
            }
            if (cursor.previous != null && (latest == null || cursor.previous.compareTo(latest) > 0)) {
                latest = cursor.previous;
            }
        }
        return latest;
    }

    private SequenceOrdinal latestViableStart() {
        // getOutput applies every stage completion before loading. Raw EOF alone cannot publish the final selection.
        if (pending || completion != null) {
            return null;
        }
        for (Cursor cursor : cursors) {
            if (cursor.stage == 0 && cursor.exhausted == false) {
                return null;
            }
        }
        SequenceOrdinal latest = matcher.latestUnusedStart();
        for (Cursor cursor : cursors) {
            if (cursor.stage == 0) {
                for (int row = cursor.offset; row < cursor.rows.size(); row++) {
                    context.checkForEarlyTermination();
                    SequenceOrdinal ordinal = cursor.rows.get(row).ordinal;
                    if (latest == null || ordinal.compareTo(latest) > 0) {
                        latest = ordinal;
                    }
                }
            }
        }
        return latest;
    }

    private static boolean before(Cursor left, Cursor right) {
        Candidate l = left.rows.get(left.offset), r = right.rows.get(right.offset);
        int ordinal = l.ordinal.compareTo(r.ordinal);
        if (ordinal != 0) {
            return ordinal < 0;
        }
        return left.stage != right.stage ? left.stage > right.stage : SequenceKeyCodec.compare(l.key.bytes(), r.key.bytes()) < 0;
    }

    private boolean needsPayload(int stage) {
        return sequence.sourceStages().contains(stage);
    }

    private void prepareSnapshot() {
        if (resolvedSources < cursors.size()) {
            Cursor cursor = cursors.get(resolvedSources);
            var relation = cursor.input.source(cursor.branch);
            if (relation.originalIndices().keySet().stream().anyMatch(alias -> alias.isEmpty() == false)) {
                throw new IllegalArgumentException("cross-cluster SEQUENCE sources are not supported");
            }
            String[] expressions = relation.originalIndices()
                .getOrDefault("", List.of(relation.indexPattern().split(",")))
                .toArray(String[]::new);
            var request = new SearchShardsRequest(expressions, IndexResolver.DEFAULT_OPTIONS, null, null, null, false, null);
            // Routing describes all shards belonging to this branch, independently of a can-match result.
            request.setIncludeSkippedShardsInIterators(true);
            begin();
            ActionListener<SearchShardsResponse> listener = ActionListener.notifyOnce(ActionListener.wrap(response -> {
                complete(() -> {
                    var scope = new SequenceSourceScope(response);
                    // The replaceable request now contains the caller-authorized aliases/indices. Capturing that
                    // resolved scope prevents an excluded alias from broadening PIT document permissions.
                    var authorizationExpressions = List.copyOf(Arrays.asList(request.indices()));
                    long bytes = scope.estimatedBytes();
                    for (String expression : authorizationExpressions) {
                        bytes = Math.addExact(bytes, 64L + 2L * expression.length());
                    }
                    reserve(bytes);
                    cursor.scope = scope;
                    cursor.authorizationExpressions = authorizationExpressions;
                    resolvedSources++;
                }, () -> {});
            }, this::failed));
            try (var ignored = factory.requestContext.get()) {
                factory.client.execute(EsqlSearchShardsAction.TYPE, request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }
        Set<String> indices = new LinkedHashSet<>();
        Set<String> concreteIndices = new LinkedHashSet<>();
        Set<String> scopedIndices = new LinkedHashSet<>();
        for (Cursor cursor : cursors) {
            var relation = cursor.input.source(cursor.branch);
            if (relation.originalIndices().keySet().stream().anyMatch(alias -> alias.isEmpty() == false)) {
                throw new IllegalArgumentException("cross-cluster SEQUENCE sources are not supported");
            }
            indices.addAll(SequenceSourceScope.positiveExpressions(cursor.authorizationExpressions));
            concreteIndices.addAll(relation.concreteQualifiedIndices());
            scopedIndices.addAll(cursor.scope.indexNames());
        }
        // Required-field admission can run before a branch's scope filter. Intersect the unions so crossed scopes
        // preserve those existing checks while narrowing the physical hint to final read indices.
        concreteIndices.retainAll(scopedIndices);
        pitAuthorizationIndices = new OriginalIndices(indices.toArray(String[]::new), IndexResolver.DEFAULT_OPTIONS);
        begin();
        ActionListener<OpenPointInTimeResponse> listener = ActionListener.notifyOnce(ActionListener.wrap(response -> {
            synchronized (this) {
                pit = response.getPointInTimeId();
                schedulePitRenewal();
            }
            complete(() -> {
                executionInfo.onOpen(response);
                if (response.getFailedShards() != 0) {
                    throw new IllegalStateException("SEQUENCE requires a complete point-in-time snapshot");
                }
            }, () -> {});
        }, this::failed));
        try (var ignored = factory.requestContext.get()) {
            // Expanding concrete names here would require direct index privileges from alias-only callers. Keep the
            // original positive expressions for authorization and restrict the contexts through PIT can-match instead.
            var request = new OpenPointInTimeRequest(factory.configuration.absoluteStartedTimeInMillis(), indices.toArray(String[]::new))
                .indicesOptions(IndexResolver.DEFAULT_OPTIONS)
                .keepAlive(keepAlive)
                .allowPartialSearchResults(false);
            request.indexFilter(concreteIndices.isEmpty() ? new MatchNoneQueryBuilder() : termsQuery("_index", concreteIndices));
            factory.client.execute(TransportOpenPointInTimeAction.TYPE, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private SearchSourceBuilder searchSource(SequenceStageInput input, int stage, int branch) {
        var relation = input.source(branch);
        var query = boolQuery().filter(termsQuery("_index", relation.concreteQualifiedIndices())).filter(existsQuery("@timestamp"));
        QueryBuilder stageQuery = input.searchQuery(branch);
        if (stageQuery != null) {
            query.filter(stageQuery);
        }
        QueryBuilder requestFilter = factory.plan.esFilter();
        if (requestFilter != null) {
            query.filter(requestFilter);
        }
        String numericType = input.timestampType(branch) == DataType.DATE_NANOS ? "date_nanos" : "date";
        return new SequenceHitReader(relation.output(), factory.configuration).fields(
            new SearchSourceBuilder().query(query)
                .size(SEARCH_PAGE_SIZE)
                .trackTotalHits(false)
                .sort(
                    new FieldSortBuilder("@timestamp").order(SortOrder.ASC)
                        .setNumericType(numericType)
                        .unmappedType(numericType)
                        .setFormat("strict_date_optional_time_nanos")
                )
                .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
        );
    }

    private void load(Cursor cursor) {
        if (cursor.stage > 0 && cursor.previous != null) {
            // getOutput applies stage completions before loading a drained cursor. Fully read first-stage
            // streams bound even buffered starts; filtered/foreign raw hits only make this bound looser.
            // End this reader alone: its fetched position cannot expire earlier candidates in other readers.
            SequenceOrdinal latest = latestReadStart();
            if (latest != null && cursor.previous.compareTo(latest) >= 0 && cursor.previous.exceedsSpan(latest, sequence.maxSpanNanos())) {
                cursor.exhausted = true;
                return;
            }
        }
        SearchSourceBuilder source = searchSource(cursor.input, cursor.stage, cursor.branch);
        DataType timestampType = cursor.input.timestampType(cursor.branch);
        if (cursor.stage > 0 && initialTimeBound != null) {
            // Keep equal timestamps: the shared PIT ordinal still decides strict event order.
            Instant earliest = Instant.ofEpochMilli(initialTimeBound.epochMillis()).plusNanos(initialTimeBound.nanoOfMilli());
            boolean nanos = timestampType == DataType.DATE_NANOS;
            // A nanos source cannot precede the epoch or follow its maximum representable instant. Avoid parsing
            // an ordinary date outside that domain as nanos, while keeping the source predicate and scope intact.
            // Ordinary dates beyond the supported ISO year interval keep an open derived lower bound.
            if ((nanos || withinDateFormatRange(initialTimeBound.epochMillis()))
                && (nanos == false || earliest.isBefore(Instant.EPOCH) == false)) {
                QueryBuilder lowerBound = nanos && earliest.isAfter(DateUtils.MAX_NANOSECOND_INSTANT)
                    ? new MatchNoneQueryBuilder()
                    : rangeQuery("@timestamp").gte(earliest.toString()).format("strict_date_optional_time_nanos");
                source.query(boolQuery().filter(source.query()).filter(lowerBound));
            }
        }
        if (cursor.stage > 0) {
            SequenceOrdinal latest = latestViableStart();
            Instant upper = latest == null ? null : latest.upperTimeBound(sequence.maxSpanNanos());
            boolean nanos = cursor.input.timestampType(cursor.branch) == DataType.DATE_NANOS;
            // Classify the reachable end, not the start: a pre-epoch start can still reach a nanos source.
            // Use the same ordinary-date ISO window as the derived lower bound; raw ordering remains exact outside it.
            if (upper != null
                && (nanos || withinDateFormatRange(upper.toEpochMilli()))
                && (nanos == false || upper.isAfter(DateUtils.MAX_NANOSECOND_INSTANT) == false)) {
                QueryBuilder upperBound = nanos && upper.isBefore(Instant.EPOCH)
                    ? new MatchNoneQueryBuilder()
                    : rangeQuery("@timestamp").lte(upper.toString()).format("strict_date_optional_time_nanos");
                source.query(boolQuery().filter(source.query()).filter(upperBound));
            }
        }
        if (cursor.after != null) {
            source.searchAfter(cursor.after);
        }
        search(cursor.stage, cursor.branch, source, false, response -> {
            SearchHit[] allHits = response.getHits().getHits();
            cursor.exhausted = allHits.length < SEARCH_PAGE_SIZE;
            if (allHits.length == 0) {
                return;
            }
            for (SearchHit hit : allHits) {
                SequenceOrdinal ordinal = ordinal(hit, timestampType);
                if (cursor.previous != null && cursor.previous.compareTo(ordinal) >= 0) {
                    throw new IllegalStateException("SEQUENCE source search did not advance strictly");
                }
                cursor.previous = ordinal;
            }
            // Numeric search_after uses the requested comparator units and avoids null/extreme date formatting.
            cursor.after = rawCandidateSortValues(allHits[allHits.length - 1]).clone();
            SearchHit[] hits = Arrays.stream(allHits).filter(cursor.scope::contains).toArray(SearchHit[]::new);
            if (hits.length == 0) {
                return;
            }
            List<InputRow> identities = new ArrayList<>(hits.length);
            long identityBytes = 0;
            try {
                for (SearchHit hit : hits) {
                    SequenceHitReader.ensureScalarTimestamp(hit);
                    SequenceOrdinal ordinal = ordinal(hit, timestampType);
                    long bytes = 256L + 2L * (hit.getIndex().length() + hit.getId().length());
                    breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
                    identityBytes += bytes;
                    identities.add(new InputRow(ordinal, hit.getIndex(), hit.getId()));
                }
                long admitted = identityBytes;
                Page raw = new SequenceHitReader(cursor.input.source(cursor.branch).output(), factory.configuration).read(
                    hits,
                    blockFactory
                );
                try (raw) {
                    runStage(cursor.input, cursor.branch, raw, pages -> {
                        try {
                            candidates(cursor, identities, pages);
                        } finally {
                            breaker.addWithoutBreaking(-admitted);
                        }
                    }, () -> breaker.addWithoutBreaking(-admitted));
                }
                identityBytes = 0; // The asynchronous stage owns identity admission now.
            } finally {
                breaker.addWithoutBreaking(-identityBytes);
            }
        });
    }

    private void candidates(Cursor cursor, List<InputRow> identities, List<Page> pages) {
        long countBytes = 32L + Integer.BYTES * (long) identities.size();
        reserve(countBytes);
        try {
            int[] expansions = new int[identities.size()];
            for (Page page : pages) {
                LongBlock marker = page.getBlock(page.getBlockCount() - 1);
                for (int row = 0; row < page.getPositionCount(); row++) {
                    context.checkForEarlyTermination();
                    int source = Math.toIntExact(marker.getLong(marker.getFirstValueIndex(row)));
                    int expansion = expansions[source];
                    expansions[source] = Math.incrementExact(expansion);
                    var key = cursor.codec().encode(page, row, cursor.stage, cursor.keys);
                    if (key == null) {
                        continue;
                    }
                    boolean success = false;
                    InputRow identity = identities.get(source);
                    long bytes = 0;
                    SequenceCandidateValues values = null;
                    try {
                        if (cursor.demands.isEmpty() == false) {
                            values = SequenceCandidateValues.copyRow(
                                page,
                                row,
                                cursor.demands,
                                cursor.mandatoryChannels(),
                                blockFactory,
                                breaker
                            );
                        }
                        EncodedHandle encoded = handle(cursor, expansion, identity, page, row);
                        bytes = encoded.bytes;
                        long admission = 512L + 8L * (identity.index.length() + identity.id.length());
                        reserve(admission);
                        bytes += admission;
                        cursor.add(new Candidate(key, identity.ordinal, encoded.value, bytes, values));
                        values = null;
                        success = true;
                    } finally {
                        if (success == false) {
                            try {
                                Releasables.close(values, key);
                            } finally {
                                release(bytes);
                            }
                        }
                    }
                }
            }
            cursor.rows.sort(CANDIDATE_ORDER);
        } finally {
            release(countBytes);
        }
    }

    private Page materialize() throws IOException {
        while (batch.stage < payload.size()) {
            if (needsPayload(batch.stage) == false) {
                batch.stage++;
                batch.branch = 0;
                continue;
            }
            var input = payload.get(batch.stage);
            if (batch.group != null) {
                batch.group.fetch();
                return null;
            }
            while (batch.branch < input.sourceCount()) {
                PayloadGroup group = new PayloadGroup(batch, batch.stage, batch.branch);
                if (group.events.isEmpty()) {
                    group.close();
                    batch.branch++;
                    continue;
                }
                batch.group = group;
                if (group.direct && group.retention.fetched().isEmpty()) {
                    group.materializeDirect();
                    continue;
                }
                group.fetch();
                return null;
            }
            batch.stage++;
            batch.branch = 0;
        }
        int count = batch.matches.size();
        Block[] blocks = new Block[sequence.output().size()];
        boolean transferred = false;
        try {
            for (int column = 0; column < blocks.length; column++) {
                try (
                    var builder = PlannerUtils.toElementType(sequence.output().get(column).dataType()).newBlockBuilder(count, blockFactory)
                ) {
                    for (int row = 0; row < count; row++) {
                        context.checkForEarlyTermination();
                        Block value = batch.cells[row][column];
                        if (value == null) {
                            throw new IllegalStateException("SEQUENCE selected payload is unavailable during payload replay");
                        }
                        builder.copyFrom(value, 0, 1);
                    }
                    blocks[column] = builder.build();
                }
            }
            Page page = new Page(count, blocks);
            batch.close();
            batch = null;
            rowsEmitted += count;
            transferred = true;
            return page;
        } finally {
            if (transferred == false) {
                Releasables.close(blocks);
            }
        }
    }

    private void runStage(SequenceStageInput input, int branch, Page raw, Consumer<List<Page>> consume, Runnable discard) {
        var binding = input.bind(branch, raw, blockFactory);
        var collected = new StagePages();
        List<Page> pages = collected.pages;
        List<Driver> drivers;
        try {
            raw.allowPassingToDifferentDriver();
            drivers = binding.createDrivers(factory.stagePlanner, collected::add);
            for (Driver driver : drivers) {
                driver.driverContext().setWarningsBudget(stageWarningsBudget);
            }
        } catch (Exception e) {
            Releasables.close(collected, binding);
            throw e;
        }
        stageDrivers += drivers.size();
        begin();
        ActionListener<Void> listener = ActionListener.notifyOnce(ActionListener.wrap(unused -> {
            recordStageCosts(drivers);
            Runnable cleanup = () -> { Releasables.close(collected, binding); };
            complete(() -> {
                try {
                    for (Driver driver : drivers) {
                        for (String warning : driver.driverContext().warnings()) {
                            context.addWarning(warning);
                        }
                    }
                    consume.accept(pages);
                } finally {
                    cleanup.run();
                }
            }, () -> {
                try {
                    discard.run();
                } finally {
                    cleanup.run();
                }
            });
        }, e -> {
            try {
                synchronized (this) {
                    recordStageCosts(drivers);
                    Releasables.close(discard::run, collected, binding);
                }
            } finally {
                failed(e);
            }
        }));
        try {
            factory.stageExecutor.execute(drivers, listener);
        } catch (Exception e) {
            try {
                Releasables.close(drivers);
            } finally {
                listener.onFailure(e);
            }
        }
    }

    private synchronized void recordStageCosts(List<Driver> drivers) {
        for (Driver driver : drivers) {
            stageDriverCpuNanos += driver.status().cpuNanos();
            stageDriverIterations += driver.status().iterations();
        }
    }

    private void search(int stage, int branch, SearchSourceBuilder source, boolean payloadSearch, Consumer<SearchResponse> consume) {
        search(stage, branch, source, payloadSearch, consume, () -> {});
    }

    private void search(
        int stage,
        int branch,
        SearchSourceBuilder source,
        boolean payloadSearch,
        Consumer<SearchResponse> consume,
        Runnable cleanup
    ) {
        search(stage, branch, source, payloadSearch, consume, cleanup, null);
    }

    private void search(
        int stage,
        int branch,
        SearchSourceBuilder source,
        boolean payloadSearch,
        Consumer<SearchResponse> consume,
        Runnable cleanup,
        @Nullable List<Attribute> directFields
    ) {
        // Validate the complete selected-replay demand even during the keys-only candidate phase. Otherwise an
        // output-only runtime field would escape the boundary whenever no match completed or an outer filter
        // removed every result. Fields pruned from the query are absent from this raw replay projection.
        Set<String> requiredFields = new LinkedHashSet<>();
        // Candidate ordering always requires event time. Replay retains the same declared repeatability policy,
        // while its physical identity fetch only loads fields actually referenced by the prepared stage.
        if (directFields == null) {
            requiredFields.add("@timestamp");
        }
        (directFields == null ? payload.get(stage).source(branch).output() : directFields).stream()
            .filter(FieldAttribute.class::isInstance)
            .map(FieldAttribute.class::cast)
            .filter(field -> field.dataType() != DataType.UNSUPPORTED)
            .map(field -> field.fieldName().string())
            .forEach(requiredFields::add);
        if (payloadSearch == false) {
            Cursor candidate = cursor(stage, branch);
            if (candidate.demands.isEmpty() == false) {
                // Mandatory-only values can introduce raw inputs absent from optional output replay. Use the
                // pruned selection schema, which already excludes undemanded KEEP columns, for this read only.
                candidate.input.source(branch)
                    .output()
                    .stream()
                    .filter(FieldAttribute.class::isInstance)
                    .map(FieldAttribute.class::cast)
                    .filter(field -> field.dataType() != DataType.UNSUPPORTED)
                    .map(field -> field.fieldName().string())
                    .forEach(requiredFields::add);
            }
        }
        List<String> repeatableFields = List.copyOf(requiredFields);
        source.query(
            new SequenceRepeatabilityQueryBuilder(
                source.query(),
                repeatableFields,
                List.copyOf(cursor(stage, branch).input.source(branch).concreteQualifiedIndices())
            )
        );
        if (payloadSearch) {
            payloadSearchRequests++;
        } else {
            candidateSearchRequests++;
        }
        source.pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(keepAlive));
        begin();
        ActionListener<SearchResponse> listener = ActionListener.notifyOnce(ActionListener.wrap(response -> {
            synchronized (this) {
                if (response.pointInTimeId() != null) {
                    pit = response.pointInTimeId();
                }
                schedulePitRenewal();
            }
            long admitted;
            try {
                long bytes = 256;
                for (SearchHit hit : response.getHits().getHits()) {
                    bytes = Math.addExact(bytes, SequenceHitReader.retainedBytes(hit));
                }
                admitted = bytes;
                breaker.addEstimateBytesAndMaybeBreak(admitted, BREAKER_LABEL);
                response.incRef();
            } catch (Exception e) {
                searchFailed(e, cleanup);
                return;
            }
            Runnable release = () -> {
                try {
                    response.decRef();
                } finally {
                    try {
                        breaker.addWithoutBreaking(-admitted);
                    } finally {
                        cleanup.run();
                    }
                }
            };
            complete(() -> {
                try {
                    executionInfo.onSearch(response);
                    if (payloadSearch) {
                        payloadRowsFetched += response.getHits().getHits().length;
                        payloadSearchTookMillis += response.getTook().millis();
                    } else {
                        candidateRowsFetched += response.getHits().getHits().length;
                        candidateSearchTookMillis += response.getTook().millis();
                    }
                    if (response.isTimedOut() || response.getFailedShards() != 0) {
                        throw new IllegalStateException("SEQUENCE requires a complete search response");
                    }
                    consume.accept(response);
                } finally {
                    release.run();
                }
            }, release);
        }, e -> searchFailed(e, cleanup)));
        try (var ignored = factory.requestContext.get()) {
            // Can-match removes shards and renumbers shardRequestIndex, changing _shard_doc's high bits. Keep all PIT
            // shards in the same order, with stage membership expressed in the query rather than request indices.
            var request = new SearchRequest(factory.configuration.absoluteStartedTimeInMillis()).source(source)
                .allowPartialSearchResults(false);
            request.setPointInTimeAuthorization(
                pitAuthorizationIndices,
                new OriginalIndices(cursor(stage, branch).authorizationExpressions.toArray(String[]::new), IndexResolver.DEFAULT_OPTIONS),
                new SequenceRepeatabilityQueryBuilder(
                    scope(stage, branch).query(),
                    repeatableFields,
                    List.copyOf(cursor(stage, branch).input.source(branch).concreteQualifiedIndices())
                )
            );
            request.setPreFilterShardSize(Integer.MAX_VALUE);
            factory.client.search(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void searchFailed(Exception e, Runnable cleanup) {
        try {
            synchronized (this) {
                cleanup.run();
            }
        } finally {
            failed(e);
        }
    }

    private void begin() {
        assert pending == false;
        // Reserve the entire asynchronous lifetime before the driver can finish. A late PIT-open response may need
        // to start cleanup after DriverContext.finish(), when registering another async action is forbidden.
        if (asyncRegistered == false) {
            context.addAsyncAction();
            asyncRegistered = true;
        }
        pending = true;
        wakeup = new SubscribableListener<>();
    }

    private void complete(Runnable next, Runnable discard) {
        SubscribableListener<Void> notify;
        synchronized (this) {
            pending = false;
            try {
                if (closed || finished) {
                    discard.run();
                } else {
                    completion = new Completion(next, discard);
                }
            } catch (Exception e) {
                recordFailure(e);
            } finally {
                if (closed || finished || failure != null) {
                    closePit();
                }
            }
            notify = wakeup;
        }
        notify.onResponse(null);
    }

    private void failed(Exception e) {
        SubscribableListener<Void> notify;
        synchronized (this) {
            pending = false;
            recordFailure(e);
            closePit();
            notify = wakeup;
        }
        notify.onResponse(null);
    }

    private void recordFailure(Exception e) {
        if (closed == false && finished == false) {
            if (failure == null) {
                executionInfo.onFailure(e);
                failure = asRuntime(e);
            } else if (failure != e) {
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * Stage drivers and downstream exchanges may remain active for longer than the snapshot's idle lease. Renew
     * independently of output pulls, including while a slow search leaves its already-completed shards idle.
     * Only one renewal may be in flight, and terminal cleanup waits for its latest PIT token before closing it.
     */
    private void schedulePitRenewal() {
        cancelPitRenewal();
        if (closed || finished || failure != null || pit == null || renewalPending) {
            return;
        }
        var threadPool = factory.client.threadPool();
        try (var ignored = factory.requestContext.get()) {
            long generation = renewalGeneration;
            renewalTask = threadPool.schedule(() -> renewPit(generation), renewalInterval, threadPool.generic());
        }
    }

    private void cancelPitRenewal() {
        renewalGeneration++;
        if (renewalTask != null) {
            renewalTask.cancel();
            renewalTask = null;
        }
    }

    private void renewPit(long generation) {
        var listener = ActionListener.notifyOnce(ActionListener.<SearchResponse>wrap(response -> {
            Exception error = response.isTimedOut() || response.getFailedShards() != 0
                ? new IllegalStateException("SEQUENCE requires a complete snapshot renewal response")
                : null;
            pitRenewed(response.pointInTimeId(), error);
        }, e -> pitRenewed(null, e)));
        try {
            SearchRequest request;
            synchronized (this) {
                // Cancellation cannot retract a command already queued on GENERIC. Invalidate that command before
                // it can replace the current scheduled handle or start another renewal.
                if (generation != renewalGeneration) {
                    return;
                }
                renewalTask = null;
                if (closed || finished || failure != null || pit == null || renewalPending) {
                    return;
                }
                renewalPending = true;
                // Match-all forces acquisition of the reader rather than a match-none can-match shortcut, which
                // may skip recreating a missing retryable reader. With size zero and total hits disabled, the
                // collector terminates before visiting documents and no document fields are fetched.
                var source = new SearchSourceBuilder().query(new MatchAllQueryBuilder())
                    .size(0)
                    .trackTotalHits(false)
                    .fetchSource(false)
                    .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(keepAlive));
                request = new SearchRequest(factory.configuration.absoluteStartedTimeInMillis()).source(source)
                    .allowPartialSearchResults(false);
                // Documentless renewal carries the snapshot-opening union; candidate/payload searches use narrower
                // stage authorization scopes. Retain every PIT shard position and the singleton node requests.
                request.setPointInTimeAuthorization(pitAuthorizationIndices, pitAuthorizationIndices, new MatchAllQueryBuilder());
                request.setPreFilterShardSize(Integer.MAX_VALUE);
            }
            try (var ignored = factory.requestContext.get()) {
                if (factory.task != null) {
                    factory.task.ensureNotCancelled();
                }
                factory.client.search(request, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void pitRenewed(@Nullable BytesReference token, @Nullable Exception error) {
        SubscribableListener<Void> notify = null;
        synchronized (this) {
            renewalPending = false;
            if (token != null) {
                pit = token;
            }
            if (error != null) {
                recordFailure(error);
            }
            try {
                schedulePitRenewal();
            } catch (Exception e) {
                recordFailure(e);
            }
            if (closed || finished || failure != null) {
                closePit();
            }
            if (failure != null) {
                notify = wakeup;
            }
        }
        // A renewal failure must wake a driver waiting for unrelated stage/search work without stealing that
        // work's completion slot or releasing its ownership early.
        if (notify != null) {
            notify.onResponse(null);
        }
    }

    /** Completion callbacks are detached so a timed-out request cannot retain the complete query owner. */
    private record PitCloseCompletion(ActionListener<ClosePointInTimeResponse> listener, Consumer<Exception> timeoutRejected) {}

    /** One close attempt owns its deadline and ignores late responses, failures and executor rejection. */
    private static final class PitCloseAttempt extends AbstractRunnable implements ActionListener<ClosePointInTimeResponse> {
        private final AtomicReference<PitCloseCompletion> completion;
        private volatile Scheduler.ScheduledCancellable timeout;

        private PitCloseAttempt(ActionListener<ClosePointInTimeResponse> listener, Consumer<Exception> timeoutRejected) {
            completion = new AtomicReference<>(new PitCloseCompletion(listener, timeoutRejected));
        }

        private void setTimeout(Scheduler.ScheduledCancellable value) {
            timeout = value;
            if (completion.get() == null) {
                // A deadline or rejection can complete the attempt before schedule returns its handle.
                timeout = null;
                value.cancel();
            }
        }

        private boolean isDone() {
            return completion.get() == null;
        }

        private PitCloseCompletion finish() {
            var result = completion.getAndSet(null);
            if (result != null) {
                var currentTimeout = timeout;
                timeout = null;
                if (currentTimeout != null) {
                    currentTimeout.cancel();
                }
            }
            return result;
        }

        @Override
        public void onResponse(ClosePointInTimeResponse response) {
            var result = finish();
            if (result != null) {
                result.listener().onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception failure) {
            var result = finish();
            if (result != null) {
                result.listener().onFailure(failure);
            }
        }

        @Override
        protected void doRun() {
            onFailure(new ElasticsearchTimeoutException("SEQUENCE PIT close attempt timed out"));
        }

        @Override
        public void onRejection(Exception rejection) {
            var result = finish();
            if (result != null) {
                result.timeoutRejected().accept(rejection);
            }
        }
    }

    /** Logs carry only fixed reason codes and bounded type names, never exception messages or the PIT token. */
    private record PitCloseFailure(String reason, String exceptionType, boolean retryable) {}

    private static PitCloseFailure pitCloseFailure(String reason, Throwable failure, boolean retryable) {
        String type = failure == null ? "none" : failure.getClass().getSimpleName();
        return new PitCloseFailure(reason, type.length() > 128 ? type.substring(0, 128) : type, retryable);
    }

    private static PitCloseFailure pitCloseFailure(Exception failure) {
        if (failure == null) {
            // The close response does not expose per-node causes; it also counts only newly freed contexts.
            return pitCloseFailure("incomplete_response", null, true);
        }
        // unwrapCause can log the original exception for deep wrapper chains. This typed scan never logs it.
        Throwable permanent = ExceptionsHelper.unwrap(
            failure,
            ElasticsearchSecurityException.class,
            ActionRequestValidationException.class,
            IllegalArgumentException.class,
            IllegalStateException.class,
            TaskCancelledException.class,
            NodeClosedException.class
        );
        if (permanent != null) {
            String reason;
            if (permanent instanceof ElasticsearchSecurityException) {
                reason = "authorization_failure";
            } else if (permanent instanceof TaskCancelledException) {
                reason = "cancelled";
            } else if (permanent instanceof NodeClosedException) {
                reason = "node_closed";
            } else if (permanent instanceof IllegalStateException) {
                reason = "invalid_state";
            } else {
                reason = "invalid_request";
            }
            return pitCloseFailure(reason, permanent, false);
        }
        Throwable rejected = ExceptionsHelper.unwrap(failure, EsRejectedExecutionException.class);
        if (rejected instanceof EsRejectedExecutionException rejection && rejection.isExecutorShutdown()) {
            return pitCloseFailure("executor_shutdown", rejected, false);
        }
        Throwable transientFailure = ExceptionsHelper.unwrap(
            failure,
            ConnectTransportException.class,
            ReceiveTimeoutTransportException.class,
            ElasticsearchTimeoutException.class,
            EsRejectedExecutionException.class
        );
        if (transientFailure != null) {
            String reason = transientFailure instanceof ConnectTransportException ? "connection_failure"
                : transientFailure instanceof EsRejectedExecutionException ? "executor_rejection"
                : "timeout";
            return pitCloseFailure(reason, transientFailure, true);
        }
        return pitCloseFailure("unclassified_failure", failure, false);
    }

    private void closePit() {
        cancelPitRenewal();
        if (pending || renewalPending || closingPit) {
            return;
        }
        if (pit == null) {
            releaseAsyncIfFinished();
            return;
        }
        BytesReference token = pit;
        closingPit = true;
        pitCloseAttempts++;
        var attempt = new PitCloseAttempt(
            ActionListener.wrap(response -> pitClosed(response.isSucceeded(), null), failure -> pitClosed(false, failure)),
            failure -> pitCloseUnavailable("deadline_unavailable", failure)
        );
        try {
            var threadPool = factory.cleanupClient.threadPool();
            attempt.setTimeout(threadPool.schedule(attempt, PIT_CLOSE_ATTEMPT_TIMEOUT, threadPool.generic()));
        } catch (Exception failure) {
            attempt.onRejection(failure);
            return;
        }
        if (attempt.isDone()) {
            return;
        }
        try (var ignored = factory.requestContext.get()) {
            // Cleanup must survive cancellation of the query's parent task, while retaining the same authorization.
            // A timed-out attempt can still complete remotely; its detached callback must not complete a later attempt.
            factory.cleanupClient.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(token), attempt);
        } catch (Exception e) {
            attempt.onFailure(e);
        }
    }

    private synchronized void pitClosed(boolean succeeded, Exception failure) {
        if (succeeded) {
            finishPitClose();
            return;
        }
        var description = pitCloseFailure(failure);
        logger.debug(
            "SEQUENCE PIT cleanup attempt [{}] failed; reason [{}], exception type [{}], retryable [{}]",
            pitCloseAttempts,
            description.reason(),
            description.exceptionType(),
            description.retryable()
        );
        if (description.retryable() == false || pitCloseAttempts >= MAX_PIT_CLOSE_ATTEMPTS) {
            abandonPitClose(description);
            return;
        }
        logger.debug("Retrying PIT cleanup after failed attempt [{}] of [{}]", pitCloseAttempts, MAX_PIT_CLOSE_ATTEMPTS);
        // Keep the PIT and async ownership while the retry is queued. An AbstractRunnable also receives rejection
        // after the timer dispatches to GENERIC; a plain lambda could otherwise lose its terminal callback.
        var retry = new AbstractRunnable() {
            @Override
            protected void doRun() {
                synchronized (SequenceSourceOperator.this) {
                    closingPit = false;
                    closePit();
                }
            }

            @Override
            public void onFailure(Exception e) {
                pitCloseUnavailable("retry_unavailable", e);
            }
        };
        try {
            var threadPool = factory.cleanupClient.threadPool();
            threadPool.schedule(retry, PIT_CLOSE_RETRY_DELAY, threadPool.generic());
        } catch (Exception e) {
            retry.onFailure(e);
        }
    }

    private synchronized void pitCloseUnavailable(String reason, Exception failure) {
        abandonPitClose(pitCloseFailure(reason, failure, false));
    }

    private void abandonPitClose(PitCloseFailure failure) {
        // A retried partial close can report false for contexts already freed by an earlier attempt. Preserve the
        // original query outcome and report only that full cleanup could not be confirmed, without logging the ID.
        logger.warn(
            "Could not confirm complete PIT cleanup after [{}] attempts for SEQUENCE query; reason [{}], exception type [{}]",
            pitCloseAttempts,
            failure.reason(),
            failure.exceptionType()
        );
        finishPitClose();
    }

    private void finishPitClose() {
        pit = null;
        closingPit = false;
        releaseAsyncIfFinished();
    }

    private void releaseAsyncIfFinished() {
        if (asyncRegistered
            && asyncReleased == false
            && (closed || finished || failure != null)
            && pending == false
            && renewalPending == false
            && closingPit == false
            && pit == null) {
            asyncReleased = true;
            context.removeAsyncAction();
        }
    }

    @Override
    public synchronized IsBlockedResult isBlocked() {
        return pending && completion == null && failure == null ? new IsBlockedResult(wakeup, "sequence search or stage") : NOT_BLOCKED;
    }

    @Override
    public synchronized SequenceSourceStatus status() {
        return new SequenceSourceStatus(
            candidateSearchRequests,
            payloadSearchRequests,
            candidateRowsFetched,
            payloadRowsFetched,
            completedMatches,
            rowsEmitted,
            stageDrivers,
            stageDriverCpuNanos,
            stageDriverIterations,
            candidateSearchTookMillis,
            payloadSearchTookMillis,
            maxPendingCandidates,
            matcher.latestValidCounters()
        );
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && failure == null;
    }

    @Override
    public synchronized void finish() {
        if (finished == false) {
            finished = true;
            try {
                try {
                    publishLimitSummaries();
                } finally {
                    clear();
                }
            } finally {
                closePit();
            }
        }
    }

    /** Terminal publication is separate from expression warning budgets and never runs in a PIT-close callback. */
    private void publishLimitSummaries() {
        if (latestValidBound == 0 || context.warningsMode() != DriverContext.WarningsMode.COLLECT || context.isFinished()) {
            return;
        }
        var counters = matcher.latestValidCounters();
        if (counters.candidatesDroppedAtBound() > 0) {
            context.addWarning(
                "SEQUENCE latest_valid dropped ["
                    + counters.candidatesDroppedAtBound()
                    + "] candidates at max_candidates ["
                    + latestValidBound
                    + "] across ["
                    + counters.keysThatHitBound()
                    + "] key lifetimes"
            );
        }
        if (counters.searchesAbandonedAtWorkLimit() > 0) {
            context.addWarning(
                "SEQUENCE latest_valid abandoned ["
                    + counters.searchesAbandonedAtWorkLimit()
                    + "] final-row searches at the work limit ["
                    + SequenceLatestValidConfig.SEARCH_STEP_LIMIT
                    + "]"
            );
        }
    }

    @Override
    public synchronized void close() {
        if (closed == false) {
            closed = true;
            finish();
        }
    }

    private void clear() {
        // Completions own response/stage resources. Early finish discards them without running more stage work.
        Completion ready = completion;
        completion = null;
        try {
            Releasables.close(
                ready == null ? null : ready.discard::run,
                batch,
                Releasables.wrap(cursors),
                matcher,
                conditionResources,
                () -> release(heldBytes)
            );
        } finally {
            batch = null;
            pitAuthorizationIndices = null;
        }
    }

    private static boolean withinDateFormatRange(long epochMillis) {
        return epochMillis >= DateUtils.MAX_MILLIS_BEFORE_MINUS_9999 && epochMillis <= DateUtils.MAX_MILLIS_BEFORE_9999;
    }

    /** The generated candidate sort has two long comparators; their raw values already use its requested numeric_type. */
    private static Object[] rawCandidateSortValues(SearchHit hit) {
        Object[] raw = hit.getRawSortValues();
        if (raw.length != 2 || (raw[0] instanceof Long) == false || (raw[1] instanceof Long) == false || ((Long) raw[1]) < 0) {
            throw new IllegalStateException("SEQUENCE candidate search must return raw long timestamp and PIT document ordinal");
        }
        return raw;
    }

    private static SequenceOrdinal ordinal(SearchHit hit, DataType timestampType) {
        Object[] raw = rawCandidateSortValues(hit);
        long timestamp = (Long) raw[0];
        long shardDoc = (Long) raw[1];
        // Decode the requested comparator units, independently of display formatting or the live field's original units.
        return switch (timestampType) {
            case DATETIME -> new SequenceOrdinal(timestamp, 0, shardDoc);
            case DATE_NANOS -> {
                Instant instant = DateUtils.toInstant(timestamp);
                yield new SequenceOrdinal(instant.toEpochMilli(), instant.getNano() % 1_000_000, shardDoc);
            }
            default -> throw new IllegalStateException("unexpected SEQUENCE timestamp sort type [" + timestampType + "]");
        };
    }

    /** Hydration must preserve the selected PIT document independently of any live event-time definition. */
    private static long physicalShardDoc(SearchHit hit) {
        Object[] sort = hit.getSortValues();
        if (sort.length != 1 || (sort[0] instanceof Long) == false || ((Long) sort[0]) < 0) {
            throw new IllegalStateException("SEQUENCE identity search must return one non-negative PIT document ordinal");
        }
        return (Long) sort[0];
    }

    private SequenceSourceScope scope(int stage, int branch) {
        return cursor(stage, branch).scope;
    }

    private Cursor cursor(int stage, int branch) {
        return cursors.stream().filter(cursor -> cursor.stage == stage && cursor.branch == branch).findFirst().orElseThrow();
    }

    static long precedingShardDoc(long shardDoc) {
        if (ShardDocSortField.decodeDoc(shardDoc) > 0) {
            return shardDoc - 1;
        }
        int shard = ShardDocSortField.decodeShardRequestIndex(shardDoc);
        return shard == 0 ? -1L : ShardDocSortField.encodeShardAndDoc(shard - 1, Integer.MAX_VALUE);
    }

    /** Copies only already evaluated cells; matcher handle copies then own the same ordinary byte accounting. */
    private EncodedHandle handle(Cursor cursor, int expansion, InputRow row, Page page, int position) {
        var retention = cursor.input.retention(cursor.branch);
        List<Attribute> retained = retention.retained();
        int[] channels = retained.isEmpty() ? null : cursor.retainedChannels();
        long containerBytes = 64L + 8L * retained.size();
        reserve(containerBytes);
        Block[] cells = null;
        try {
            cells = new Block[retained.size()];
            for (int column = 0; column < retained.size(); column++) {
                int channel = channels[column];
                try (var builder = PlannerUtils.toElementType(retained.get(column).dataType()).newBlockBuilder(1, blockFactory)) {
                    builder.copyFrom(page.getBlock(channel), position, position + 1);
                    cells[column] = builder.build();
                }
            }
            var identity = new CountingStreamOutput();
            writeIdentity(identity, cursor.branch, expansion, row);
            int identitySize = Math.toIntExact(identity.position());
            var count = new CountingStreamOutput();
            writeHandle(count, identitySize, cursor.branch, expansion, row, retention.direct(), cells);
            boolean complete = retention.direct();
            if (count.position() > retainedHandleCapacity) {
                // A local handle uses a contiguous BytesRef. Keep the original full replay losslessly available
                // when an otherwise eligible row cannot fit; never truncate a value or impose a result-size cap.
                complete = false;
                count.reset();
                writeHandle(count, identitySize, cursor.branch, expansion, row, false, cells);
            }
            int size = Math.toIntExact(count.position());
            long arrayBytes = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size);
            long permanentBytes = 64L + arrayBytes;
            long admission = 192L + 2L * arrayBytes;
            reserve(admission); // Both the exactly sized output buffer and toByteArray's copy are live together.
            boolean transferred = false;
            try {
                var buffer = new ByteArrayOutputStream(size);
                try (var out = new OutputStreamStreamOutput(buffer)) {
                    writeHandle(out, identitySize, cursor.branch, expansion, row, complete, cells);
                }
                if (buffer.size() != size) {
                    throw new IllegalStateException("SEQUENCE retained handle serialization size changed");
                }
                var encoded = new EncodedHandle(new BytesRef(buffer.toByteArray()), permanentBytes);
                transferred = true;
                return encoded;
            } finally {
                release(admission - (transferred ? permanentBytes : 0));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                Releasables.close(cells);
            } finally {
                release(containerBytes);
            }
        }
    }

    private static void writeIdentity(StreamOutput out, int branch, int expansion, InputRow row) throws IOException {
        out.writeVInt(branch);
        out.writeVInt(expansion);
        out.writeString(row.index);
        out.writeString(row.id);
        out.writeLong(row.ordinal.epochMillis());
        out.writeInt(row.ordinal.nanoOfMilli());
        out.writeLong(row.ordinal.shardDoc());
    }

    private static void writeHandle(
        StreamOutput out,
        int identitySize,
        int branch,
        int expansion,
        InputRow row,
        boolean complete,
        Block[] cells
    ) throws IOException {
        out.writeVInt(identitySize);
        writeIdentity(out, branch, expansion, row);
        out.writeBoolean(complete);
        out.writeVInt(complete ? cells.length : 0);
        if (complete) {
            for (Block cell : cells) {
                Block.writeTypedBlock(cell, out);
            }
        }
    }

    private static long handleIdentityBytes(BytesRef handle) throws IOException {
        try (var in = new BytesArray(handle).streamInput()) {
            int size = in.readVInt();
            if (size > in.available()) {
                throw new IllegalStateException("SEQUENCE selected handle has an invalid identity length");
            }
            return 8L * size;
        }
    }

    private static Handle readHandle(BytesRef handle) throws IOException {
        try (var in = new BytesArray(handle).streamInput()) {
            int identitySize = in.readVInt();
            int start = in.available();
            int branch = in.readVInt(), expansion = in.readVInt();
            String index = in.readString(), id = in.readString();
            SequenceOrdinal ordinal = new SequenceOrdinal(in.readLong(), in.readInt(), in.readLong());
            if (start - in.available() != identitySize) {
                throw new IllegalStateException("SEQUENCE selected handle has an invalid identity length");
            }
            boolean complete = in.readBoolean();
            int count = in.readVInt();
            int size = in.available();
            return new Handle(
                branch,
                expansion,
                index,
                id,
                ordinal,
                complete,
                count,
                new BytesRef(handle.bytes, handle.offset + handle.length - size, size)
            );
        }
    }

    private void reserve(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
        heldBytes += bytes;
    }

    private void release(long bytes) {
        heldBytes -= bytes;
        breaker.addWithoutBreaking(-bytes);
    }

    private static RuntimeException asRuntime(Exception e) {
        return e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
    }

    private record InputRow(SequenceOrdinal ordinal, String index, String id) {}

    private record EncodedHandle(BytesRef value, long bytes) {}

    private record Handle(
        int branch,
        int expansion,
        String index,
        String id,
        SequenceOrdinal ordinal,
        boolean complete,
        int count,
        BytesRef cells
    ) {
        private Handle forReplay() {
            // An empty slice of the original BytesRef would still retain its complete cell backing array.
            return new Handle(branch, expansion, index, id, ordinal, false, 0, new BytesRef());
        }
    }

    private record Completion(Runnable next, Runnable discard) {}

    /** Completed matches retain only bounded selected handles and admitted payload references until one output page. */
    private final class MatchBatch implements Releasable {
        private final int capacity;
        private final List<SequenceMatch> matches;
        private Block[][] cells;
        private PayloadGroup group;
        private int stage;
        private int branch;
        private long bytes;
        private boolean materializing;
        private boolean released;

        private MatchBatch(int capacity) {
            this.capacity = capacity;
            bytes = 128L + 24L * capacity;
            reserve(bytes);
            matches = new ArrayList<>(capacity);
        }

        private void prepare() {
            if (materializing == false) {
                long admission = 64L + 32L * matches.size() + 16L * matches.size() * sequence.output().size();
                reserve(admission);
                bytes += admission;
                cells = new Block[matches.size()][sequence.output().size()];
                materializing = true;
            }
        }

        private void closeCells() {
            if (cells != null) {
                // Close every row even if one block fails to release; the outer wrap also closes matches and hits.
                Iterable<Releasable> rows = Arrays.stream(cells).<Releasable>map(row -> () -> Releasables.close(row))::iterator;
                Releasables.close(rows);
            }
        }

        @Override
        public void close() {
            if (released == false) {
                released = true;
                try {
                    Releasables.close(group, this::closeCells, Releasables.wrap(matches));
                } finally {
                    group = null;
                    cells = null;
                    matches.clear();
                    release(bytes);
                    bytes = 0;
                }
            }
        }
    }

    /**
     * Fetches and replays selected events for one stage branch. Only the selected physical PIT ordinals enter fetch,
     * independently of live ID fields and routed copies.
     * Physical fetching does not reapply selection predicates. The finite retention proof can materialize directly;
     * all other branches and incomplete retained envelopes preserve the prepared stage's full payload replay.
     */
    private final class PayloadGroup implements Releasable {
        private final MatchBatch owner;
        private final int stage;
        private final int branch;
        private final Cursor cursor;
        private final TreeMap<Long, ReplayEvent> events;
        private final SequenceStageInput.Retention retention;
        private boolean direct;
        private long bytes;
        private Object[] after;
        private long previous = -1;
        private int found;
        private boolean fetching;
        private boolean replaying;
        private boolean released;

        private PayloadGroup(MatchBatch owner, int stage, int branch) throws IOException {
            this.owner = owner;
            this.stage = stage;
            this.branch = branch;
            cursor = SequenceSourceOperator.this.cursor(stage, branch);
            // Covers decoded handles, events/targets, map nodes, physical query containers, hit-reference arrays and
            // expansion counters. Payload blocks and retained SearchHits have independent admissions below.
            long admission = 1024L + 1024L * owner.matches.size();
            for (SequenceMatch match : owner.matches) {
                admission = Math.addExact(admission, handleIdentityBytes(match.handles().get(stage)));
            }
            breaker.addEstimateBytesAndMaybeBreak(admission, BREAKER_LABEL);
            bytes = admission;
            events = new TreeMap<>();
            boolean success = false;
            try {
                for (int row = 0; row < owner.matches.size(); row++) {
                    Handle handle = readHandle(owner.matches.get(row).handles().get(stage));
                    if (handle.branch != branch) {
                        continue;
                    }
                    long shardDoc = handle.ordinal.shardDoc();
                    ReplayEvent event = events.get(shardDoc);
                    if (event == null) {
                        event = new ReplayEvent(handle);
                        events.put(shardDoc, event);
                    } else if (event.handle.index.equals(handle.index) == false || event.handle.id.equals(handle.id) == false) {
                        throw new IllegalStateException("SEQUENCE selected PIT ordinal has conflicting source identity");
                    }
                    event.targets.add(new ReplayTarget(row, handle));
                }
                retention = events.isEmpty()
                    ? new SequenceStageInput.Retention(false, List.of(), List.of(), List.of())
                    : cursor.input.retention(branch);
                direct = retention.direct()
                    && events.values().stream().flatMap(event -> event.targets.stream()).allMatch(target -> target.handle.complete);
                if (direct == false) {
                    // A mixed overflow group replays every target. Drop all serialized cell backing before the
                    // async stage can outlive owner.matches and its full handle-byte admission on cancellation.
                    for (var entry : events.entrySet()) {
                        ReplayEvent original = entry.getValue();
                        var replay = new ReplayEvent(original.handle.forReplay());
                        for (ReplayTarget target : original.targets) {
                            replay.targets.add(new ReplayTarget(target.row, target.handle.forReplay()));
                        }
                        entry.setValue(replay);
                    }
                }
                if (events.isEmpty() == false) {
                    after = new Object[] { precedingShardDoc(events.firstKey()) };
                }
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private void fetch() {
            var input = payload.get(stage);
            RankDoc[] selected = new RankDoc[events.size()];
            int position = 0;
            for (long shardDoc : events.keySet()) {
                selected[position++] = new RankDoc(
                    ShardDocSortField.decodeDoc(shardDoc),
                    0f,
                    ShardDocSortField.decodeShardRequestIndex(shardDoc)
                );
            }
            // Filter by the selected physical PIT positions before fetch can evaluate any output-only field.
            // The filter context preserves the existing zero score when a stage demands _score.
            var membership = boolQuery().filter(new RankDocsQueryBuilder(selected, null, true));
            int size = Math.min(SEARCH_PAGE_SIZE, events.size());
            List<Attribute> fetched = direct ? retention.fetched() : input.source(branch).output();
            SearchSourceBuilder source = new SequenceHitReader(fetched, factory.configuration).identityFields(
                new SearchSourceBuilder().query(membership)
                    .size(size)
                    .trackTotalHits(false)
                    .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
                    .searchAfter(after)
            );
            fetching = true;
            search(stage, branch, source, true, response -> {
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    context.checkForEarlyTermination();
                    long shardDoc = physicalShardDoc(hit);
                    if (previous >= shardDoc) {
                        throw new IllegalStateException("SEQUENCE payload search did not advance strictly");
                    }
                    previous = shardDoc;
                    ReplayEvent event = events.get(shardDoc);
                    if (event != null) {
                        if (event.hit != null) {
                            throw new IllegalStateException("SEQUENCE selected event was fetched more than once");
                        }
                        if (event.handle.index.equals(hit.getIndex()) == false
                            || event.handle.id.equals(hit.getId()) == false
                            || scope(stage, branch).contains(hit) == false) {
                            throw new IllegalStateException("SEQUENCE selected PIT ordinal has conflicting source identity");
                        }
                        event.retain(hit);
                        found++;
                    }
                }
                if (found == events.size()) {
                    if (direct) {
                        materializeDirect();
                    } else {
                        replay(input);
                    }
                } else if (hits.length < size || previous >= events.lastKey()) {
                    throw new IllegalArgumentException("SEQUENCE could not reconstruct the selected event from its point-in-time snapshot");
                } else {
                    after = hits[hits.length - 1].getSortValues().clone();
                }
            }, () -> {
                fetching = false;
                if (released) {
                    close();
                }
            }, direct ? fetched : null);
        }

        private void materializeDirect() {
            ReplayEvent[] ordered = events.values().toArray(ReplayEvent[]::new);
            SearchHit[] hits = Arrays.stream(ordered).map(event -> event.hit).toArray(SearchHit[]::new);
            try (Page raw = new SequenceHitReader(retention.fetched(), factory.configuration).read(hits, blockFactory)) {
                for (int event = 0; event < ordered.length; event++) {
                    for (ReplayTarget target : ordered[event].targets) {
                        context.checkForEarlyTermination();
                        restoreRetained(target);
                        for (int column = 0; column < retention.bindings().size(); column++) {
                            var binding = retention.bindings().get(column);
                            switch (binding.kind()) {
                                case SELECTION -> {
                                }
                                case METADATA -> {
                                    if (binding.source().dataType() == DataType.SOURCE) {
                                        putFetchedCell(target, binding, column, raw, event);
                                    } else {
                                        String value = binding.source().name().equals("_id") ? target.handle.id : target.handle.index;
                                        try (var builder = blockFactory.newBytesRefBlockBuilder(1)) {
                                            builder.appendBytesRef(new BytesRef(value));
                                            try (Block cell = builder.build()) {
                                                putCell(target.row, binding.output(), cell);
                                            }
                                        }
                                    }
                                }
                                case FIELD -> putFetchedCell(target, binding, column, raw, event);
                            }
                        }
                        target.selected = true;
                    }
                }
            }
            owner.group = null;
            owner.branch++;
            close();
        }

        private void restoreRetained(ReplayTarget target) {
            if (target.handle.count != retention.retained().size()) {
                throw new IllegalStateException("SEQUENCE retained cell count does not match its selected branch");
            }
            try (var bytes = new BytesArray(target.handle.cells).streamInput(); var in = new BlockStreamInput(bytes, blockFactory)) {
                for (Attribute attribute : retention.retained()) {
                    try (Block cell = Block.readTypedBlock(in)) {
                        ElementType expected = PlannerUtils.toElementType(attribute.dataType());
                        if (cell.getPositionCount() != 1 || (cell.elementType() != expected && cell.elementType() != ElementType.NULL)) {
                            throw new IllegalStateException("SEQUENCE retained cell does not match its selected output type");
                        }
                        putCell(target.row, attribute, cell);
                    }
                }
                if (in.available() != 0) {
                    throw new IllegalStateException("SEQUENCE retained cells have trailing data");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void putFetchedCell(ReplayTarget target, SequenceStageInput.ValueBinding binding, int column, Page raw, int event) {
            int channel = cursor.fetchedChannels()[column];
            try (var builder = PlannerUtils.toElementType(binding.output().dataType()).newBlockBuilder(1, blockFactory)) {
                builder.copyFrom(raw.getBlock(channel), event, event + 1);
                try (Block cell = builder.build()) {
                    putCell(target.row, binding.output(), cell);
                }
            }
        }

        private void putCell(int row, Attribute attribute, Block cell) {
            for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                if (sequence.sourceStages().get(column) == stage && sequence.sourceAttributes().get(column).semanticEquals(attribute)) {
                    if (owner.cells[row][column] != null) {
                        throw new IllegalStateException("SEQUENCE selected cell was materialized more than once");
                    }
                    cell.incRef();
                    owner.cells[row][column] = cell;
                }
            }
        }

        private void replay(SequenceStageInput input) {
            ReplayEvent[] ordered = events.values().toArray(ReplayEvent[]::new);
            for (ReplayEvent event : ordered) {
                requireIdentityOnlyReplayHandle(event.handle);
                for (ReplayTarget target : event.targets) {
                    requireIdentityOnlyReplayHandle(target.handle);
                }
            }
            SearchHit[] hits = Arrays.stream(ordered).map(event -> event.hit).toArray(SearchHit[]::new);
            try (Page raw = new SequenceHitReader(input.source(branch).output(), factory.configuration).read(hits, blockFactory)) {
                boolean transferred = false;
                try {
                    replaying = true;
                    runStage(input, branch, raw, pages -> {
                        try {
                            select(ordered, pages);
                            owner.group = null;
                            owner.branch++;
                        } finally {
                            replaying = false;
                            close();
                        }
                    }, () -> {
                        replaying = false;
                        close();
                    });
                    transferred = true;
                } finally {
                    if (transferred == false) {
                        replaying = false;
                        close();
                    }
                }
            } finally {
                // The typed input binding now owns the copied values. Dropping selected hits before stage execution
                // completes bounds their retention independently of expansion and the collected compute pages.
                releaseHits();
            }
        }

        private void requireIdentityOnlyReplayHandle(Handle handle) {
            if (handle.complete || handle.count != 0 || handle.cells.bytes.length != 0) {
                throw new IllegalStateException("SEQUENCE replay must not retain unused selection cell backing");
            }
        }

        private void select(ReplayEvent[] ordered, List<Page> pages) {
            int[] expansions = new int[ordered.length];
            var logical = (SequenceStage) sequence.children().get(stage);
            var codec = cursor.replayCodec();
            int[] channels = cursor.replayChannels();
            for (Page page : pages) {
                LongBlock marker = page.getBlock(page.getBlockCount() - 1);
                for (int row = 0; row < page.getPositionCount(); row++) {
                    context.checkForEarlyTermination();
                    int source = Math.toIntExact(marker.getLong(marker.getFirstValueIndex(row)));
                    int expansion = expansions[source];
                    expansions[source] = Math.incrementExact(expansion);
                    for (ReplayTarget target : ordered[source].targets) {
                        if (target.handle.expansion != expansion) {
                            continue;
                        }
                        if (target.selected) {
                            throw new IllegalStateException("SEQUENCE selected expansion was replayed more than once");
                        }
                        try (var key = codec.encode(page, row, stage, logical.keys())) {
                            if (key == null || key.bytes().equals(owner.matches.get(target.row).key()) == false) {
                                throw new IllegalArgumentException(
                                    "SEQUENCE could not reconstruct the selected event: correlation key no longer matches"
                                );
                            }
                        }
                        for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                            if (sequence.sourceStages().get(column) == stage) {
                                int channel = channels[column];
                                Block block = page.getBlock(channel);
                                try (
                                    var builder = PlannerUtils.toElementType(sequence.output().get(column).dataType())
                                        .newBlockBuilder(1, blockFactory)
                                ) {
                                    builder.copyFrom(block, row, row + 1);
                                    owner.cells[target.row][column] = builder.build();
                                }
                            }
                        }
                        target.selected = true;
                    }
                }
            }
            for (ReplayEvent event : ordered) {
                for (ReplayTarget target : event.targets) {
                    if (target.selected == false) {
                        throw new IllegalArgumentException("SEQUENCE could not reconstruct the selected expansion during payload replay");
                    }
                }
            }
        }

        private void releaseHits() {
            Releasables.close(events.values());
        }

        @Override
        public void close() {
            try {
                if (released == false) {
                    released = true;
                    try {
                        releaseHits();
                    } finally {
                        events.clear();
                        after = null;
                        previous = -1;
                    }
                }
            } finally {
                // Search requests retain identity queries, and stage listeners retain ordered event/target arrays.
                // Their admission survives Source.close() until terminal cleanup, independently of heldBytes.
                if (fetching == false && replaying == false && bytes != 0) {
                    breaker.addWithoutBreaking(-bytes);
                    bytes = 0;
                }
            }
        }
    }

    /** A deduplicated selected source event owns one independent search-hit reference during bounded fetch paging. */
    private final class ReplayEvent implements Releasable {
        private final Handle handle;
        private final List<ReplayTarget> targets = new ArrayList<>();
        private SearchHit hit;
        private long bytes;

        private ReplayEvent(Handle handle) {
            this.handle = handle;
        }

        private void retain(SearchHit selected) {
            long admission = Math.addExact(64, SequenceHitReader.retainedBytes(selected));
            breaker.addEstimateBytesAndMaybeBreak(admission, BREAKER_LABEL);
            boolean retained = false;
            try {
                selected.incRef();
                hit = selected;
                bytes = admission;
                retained = true;
            } finally {
                if (retained == false) {
                    breaker.addWithoutBreaking(-admission);
                }
            }
        }

        @Override
        public void close() {
            if (hit != null) {
                try {
                    hit.decRef();
                } finally {
                    hit = null;
                    breaker.addWithoutBreaking(-bytes);
                    bytes = 0;
                }
            }
        }
    }

    /** Identifies the chosen expansion for one completed match within a deduplicated source event. */
    private static final class ReplayTarget {
        private final int row;
        private final Handle handle;
        private boolean selected;

        private ReplayTarget(int row, Handle handle) {
            this.row = row;
            this.handle = handle;
        }
    }

    /** Async stage output owns its container admission until completion, even when the source closes first. */
    private final class StagePages implements Releasable {
        private final List<Page> pages = new ArrayList<>();
        private long bytes;
        private boolean released;

        private void add(Page page) {
            page.allowPassingToDifferentDriver();
            synchronized (SequenceSourceOperator.this) {
                if (closed || finished || released) {
                    page.releaseBlocks();
                    return;
                }
                // Includes the Page, its block-reference array and both ArrayList arrays during growth. Blocks
                // themselves retain their own independent breaker admission through the block factory.
                long admission = 64L + 16L * page.getBlockCount();
                boolean admitted = false;
                boolean accepted = false;
                try {
                    breaker.addEstimateBytesAndMaybeBreak(admission, BREAKER_LABEL);
                    admitted = true;
                    pages.add(page);
                    bytes += admission;
                    accepted = true;
                } finally {
                    if (accepted == false) {
                        if (admitted) {
                            breaker.addWithoutBreaking(-admission);
                        }
                        page.releaseBlocks();
                    }
                }
            }
        }

        @Override
        public void close() {
            synchronized (SequenceSourceOperator.this) {
                if (released == false) {
                    released = true;
                    try {
                        Releasables.close(Releasables.wrap(pages));
                    } finally {
                        pages.clear();
                        breaker.addWithoutBreaking(-bytes);
                        bytes = 0;
                    }
                }
            }
        }
    }

    private final class Cursor implements Releasable {
        private final int stage;
        private final int branch;
        private final SequenceStageInput input;
        private final List<Attribute> keys;
        private final List<SequenceLatestValidConfig.ValueDemand> demands;
        private final String stageLabel;
        private SequenceKeyCodec codec;
        private SequenceKeyCodec replayCodec;
        private int[] retainedChannels;
        private int[] mandatoryChannels;
        private int[] replayChannels;
        private int[] fetchedChannels;
        private long channelBytes;
        private SequenceSourceScope scope;
        private List<String> authorizationExpressions;
        private List<Candidate> rows = new ArrayList<>();
        private long containerBytes;
        private Object[] after;
        private SequenceOrdinal previous;
        private int offset;
        private boolean exhausted;

        private Cursor(
            int stage,
            int branch,
            SequenceStageInput input,
            List<Attribute> keys,
            List<SequenceLatestValidConfig.ValueDemand> demands
        ) {
            this.stage = stage;
            this.branch = branch;
            this.input = input;
            this.keys = keys;
            this.demands = demands;
            stageLabel = "stage [" + (stage + 1) + "] branch [" + (branch + 1) + "]";
        }

        private SequenceKeyCodec codec() {
            if (codec == null) {
                codec = newCodec(input.output(branch));
            }
            return codec;
        }

        private SequenceKeyCodec replayCodec() {
            if (replayCodec == null) {
                replayCodec = newCodec(payload.get(stage).output(branch));
            }
            return replayCodec;
        }

        private SequenceKeyCodec newCodec(List<Attribute> attributes) {
            long bytes = channelArrayBytes(keys.size());
            reserve(bytes);
            try {
                int[] channels = new int[keys.size()];
                for (int key = 0; key < channels.length; key++) {
                    channels[key] = SequenceStageInput.outputChannel(attributes, keys.get(key), stageLabel);
                }
                // The codec charges and owns its copy; this temporary resolver array is released below.
                return new SequenceKeyCodec(keys.stream().map(Attribute::dataType).toList(), channels, breaker);
            } finally {
                release(bytes);
            }
        }

        private int[] retainedChannels() {
            if (retainedChannels == null) {
                List<Attribute> attributes = input.output(branch);
                List<Attribute> retained = input.retention(branch).retained();
                retainedChannels = channelArray(
                    retained.size(),
                    column -> SequenceStageInput.outputChannel(attributes, retained.get(column), stageLabel)
                );
            }
            return retainedChannels;
        }

        private int[] mandatoryChannels() {
            if (mandatoryChannels == null) {
                List<Attribute> attributes = input.output(branch);
                mandatoryChannels = channelArray(
                    demands.size(),
                    column -> SequenceStageInput.outputChannel(attributes, demands.get(column).attribute(), stageLabel)
                );
            }
            return mandatoryChannels;
        }

        private int[] replayChannels() {
            if (replayChannels == null) {
                List<Attribute> attributes = payload.get(stage).output(branch);
                replayChannels = channelArray(
                    sequence.sourceAttributes().size(),
                    column -> sequence.sourceStages().get(column) == stage
                        ? SequenceStageInput.outputChannel(attributes, sequence.sourceAttributes().get(column), stageLabel)
                        : -1
                );
            }
            return replayChannels;
        }

        private int[] fetchedChannels() {
            if (fetchedChannels == null) {
                var retention = input.retention(branch);
                fetchedChannels = channelArray(retention.bindings().size(), column -> {
                    var binding = retention.bindings().get(column);
                    return binding.kind() == SequenceStageInput.ValueKind.FIELD || binding.source().dataType() == DataType.SOURCE
                        ? SequenceStageInput.outputChannel(retention.fetched(), binding.source(), stageLabel)
                        : -1;
                });
            }
            return fetchedChannels;
        }

        /** Branch schemas are immutable; admission lasts until every cache reference is dropped in close(). */
        private int[] channelArray(int size, IntUnaryOperator resolve) {
            long bytes = channelArrayBytes(size);
            reserve(bytes);
            boolean success = false;
            try {
                int[] channels = new int[size];
                for (int column = 0; column < size; column++) {
                    channels[column] = resolve.applyAsInt(column);
                }
                channelBytes = Math.addExact(channelBytes, bytes);
                success = true;
                return channels;
            } finally {
                if (success == false) {
                    release(bytes);
                }
            }
        }

        private static long channelArrayBytes(int size) {
            return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * size);
        }

        private void add(Candidate candidate) {
            // Covers both backing arrays during ArrayList growth. Keep this admission until the backing array
            // is dropped, independently of consumed candidates' handle and key lifetimes.
            reserve(24);
            containerBytes += 24;
            rows.add(candidate);
        }

        private void resetRows() {
            rows = new ArrayList<>();
            release(containerBytes);
            containerBytes = 0;
            offset = 0;
        }

        @Override
        public void close() {
            try {
                Releasables.close(Releasables.wrap(rows), codec, replayCodec);
            } finally {
                scope = null;
                authorizationExpressions = null;
                codec = null;
                replayCodec = null;
                try {
                    resetRows();
                } finally {
                    // Consumption runs under the source monitor. Late search/stage completion discards its
                    // pages instead of invoking consumption; workers never capture these cached arrays.
                    retainedChannels = null;
                    mandatoryChannels = null;
                    replayChannels = null;
                    fetchedChannels = null;
                    release(channelBytes);
                    channelBytes = 0;
                }
            }
        }
    }

    private final class Candidate implements Releasable {
        private final SequenceKeyCodec.EncodedKey key;
        private final SequenceOrdinal ordinal;
        private final BytesRef handle;
        private final long bytes;
        private SequenceCandidateValues values;
        private boolean closed;

        private Candidate(
            SequenceKeyCodec.EncodedKey key,
            SequenceOrdinal ordinal,
            BytesRef handle,
            long bytes,
            SequenceCandidateValues values
        ) {
            this.key = key;
            this.ordinal = ordinal;
            this.handle = handle;
            this.bytes = bytes;
            this.values = values;
        }

        /** The matcher consumes this owner even if consumption fails. */
        private SequenceCandidateValues takeValues() {
            SequenceCandidateValues transferred = values;
            values = null;
            return transferred;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                try {
                    Releasables.close(values, key);
                } finally {
                    values = null;
                    release(bytes);
                }
            }
        }
    }
}
