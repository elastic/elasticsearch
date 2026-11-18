/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
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
import java.util.concurrent.TimeUnit;
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
 * retains keys and source handles with the demanded condition and output values from the first read.
 * Computed fields, selected expansion rows and original metadata are never read again after matching.
 * <p>
 * Matching and stage CPU work run on the owning driver. Callbacks hand off one completion and wake the driver.
 * Each search extends a fixed idle snapshot lease; a query that remains idle beyond it can lose its snapshot.
 * Closing waits for in-flight ownership through DriverContext and dispatches a close even for a late PIT open.
 * Searches use the caller's credentials and a fixed PIT shard enumeration.
 * Retained output uses the values from its authorized source read without an additional search.
 */
public final class SequenceSourceOperator extends SourceOperator {
    private static final Logger logger = LogManager.getLogger(SequenceSourceOperator.class);
    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    private static final int SEARCH_PAGE_SIZE = 256;
    private static final int OUTPUT_BATCH_SIZE = 64;
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

    /** Every source compiles the same bounded matcher while borrowing the planner's native registry. */
    public static FactoryProvider sourceFactoryProvider(BiFunction<SequenceExec, StagePlanner, Factory> dependencies) {
        Objects.requireNonNull(dependencies);
        return (plan, stagePlanner, analysisRegistry) -> new PreparedFactory(dependencies.apply(plan, stagePlanner), analysisRegistry);
    }

    /** The registry belongs to the node; per-source evaluators transfer to their owning source. */
    private record PreparedFactory(Factory factory, @Nullable AnalysisRegistry analysisRegistry) implements SourceOperatorFactory {
        private PreparedFactory {
            Objects.requireNonNull(factory);
        }

        @Override
        public SourceOperator get(DriverContext context) {
            return factory.getWithConditions(context, configuredBound(factory), analysisRegistry);
        }

        @Override
        public String describe() {
            return factory.describe();
        }
    }

    private static int configuredBound(Factory factory) {
        return factory.configuration.pragmas().sequenceMaxCandidates().orElse(SequenceLatestValidConfig.DEFAULT_MAX_CANDIDATES);
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
        @Nullable EsqlExecutionInfo executionInfo,
        long memoryLimitBytes
    ) implements SourceOperatorFactory {
        /** Callers without cluster settings use the same default; production passes a query-time snapshot. */
        public Factory(
            SequenceExec plan,
            Client client,
            Client cleanupClient,
            CancellableTask task,
            Configuration configuration,
            StagePlanner stagePlanner,
            StageExecutor stageExecutor,
            Supplier<ThreadContext.StoredContext> requestContext,
            @Nullable EsqlExecutionInfo executionInfo
        ) {
            this(
                plan,
                client,
                cleanupClient,
                task,
                configuration,
                stagePlanner,
                stageExecutor,
                requestContext,
                executionInfo,
                PlannerSettings.SEQUENCE_MAX_MEMORY.getDefault(Settings.EMPTY).getBytes()
            );
        }

        @Override
        public SourceOperator get(DriverContext context) {
            return getWithConditions(context, configuredBound(this), null);
        }

        /** Admits prepared production state without adding components to the public request factory record. */
        SourceOperator get(DriverContext context, SequenceLatestValidConfig latestValid) {
            return new SequenceSourceOperator(this, context, DEFAULT_KEEP_ALIVE, Objects.requireNonNull(latestValid), null);
        }

        /** Compiles conditions per source and transfers evaluator ownership after successful source construction. */
        SourceOperator getWithConditions(DriverContext context, int maxCandidates, @Nullable AnalysisRegistry analysisRegistry) {
            int stages = plan.sequence().children().size();
            SequenceLatestValidConfig.validateProductionStageCount(stages);
            SequenceLatestValidConfig.validateCandidateBound(maxCandidates, stages);
            MemoryAccounting accounting = memoryAccounting(this, context);
            var conditions = SequenceConditionCompiler.compile(
                plan.sequence(),
                maxCandidates,
                configuration.newFoldContext(),
                context,
                analysisRegistry,
                accounting.breaker(),
                accounting.budget()
            );
            try {
                return new SequenceSourceOperator(this, context, DEFAULT_KEEP_ALIVE, conditions.config(), conditions, accounting);
            } catch (RuntimeException | Error e) {
                try {
                    conditions.close();
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != e) {
                        e.addSuppressed(cleanup);
                    }
                }
                throw e;
            }
        }

        @Override
        public String describe() {
            return "SequenceSource[stages=" + plan.sequence().children().size() + "]";
        }
    }

    /** The always-active budget starts before condition preparation and survives asynchronous source cleanup. */
    private static MemoryAccounting memoryAccounting(Factory factory, DriverContext context) {
        SequenceMemoryBudget budget = new SequenceMemoryBudget(factory.memoryLimitBytes);
        return new MemoryAccounting(
            budget,
            budget.newScope().breaker(LocalCircuitBreaker.forAsyncIo(context.breaker())),
            budget.newScope().blockFactory(context.blockFactory().parent())
        );
    }

    private record MemoryAccounting(SequenceMemoryBudget budget, CircuitBreaker breaker, BlockFactory blockFactory) {}

    private final Factory factory;
    private final DriverContext context;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final Sequence sequence;
    private final SequencePairing matcher;
    private final SequenceRuntimeProfiler profiler;
    @Nullable
    private final SequenceKeyFilterProfile keyFilterProfile;
    private final SequenceMemoryBudget memoryBudget;
    private final Releasable conditionResources;
    private final SequenceExecutionInfo executionInfo;
    private final TimeValue keepAlive;
    private final int candidateBound;
    private final Warnings.Budget stageWarningsBudget = new Warnings.Budget();
    private final List<Cursor> cursors = new ArrayList<>();
    private SubscribableListener<Void> wakeup = new SubscribableListener<>();
    private Completion completion;
    private boolean pending;
    private boolean finished;
    private boolean closed;
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
    private long candidateRowsFetched;
    private long completedMatches;
    private long rowsEmitted;
    private long stageDrivers;
    private long stageDriverCpuNanos;
    private long stageDriverIterations;
    private long candidateSearchTookMillis;
    private long maxPendingCandidates;

    private SequenceSourceOperator(Factory factory, DriverContext context) {
        this(factory, context, DEFAULT_KEEP_ALIVE);
    }

    /** A short injected lease allows real expiry tests without changing production configuration. */
    SequenceSourceOperator(Factory factory, DriverContext context, TimeValue keepAlive) {
        this(factory, context, keepAlive, null, null);
    }

    /** The source owns compiled evaluators together with its matcher and retained candidate values. */
    SequenceSourceOperator(
        Factory factory,
        DriverContext context,
        TimeValue keepAlive,
        @Nullable SequenceLatestValidConfig latestValid,
        @Nullable Releasable conditionResources
    ) {
        this(factory, context, keepAlive, latestValid, conditionResources, memoryAccounting(factory, context));
    }

    /** Prepared production sources share compiler, matcher, serialized packets and block leases on one held estimate. */
    private SequenceSourceOperator(
        Factory factory,
        DriverContext context,
        TimeValue keepAlive,
        @Nullable SequenceLatestValidConfig latestValid,
        @Nullable Releasable conditionResources,
        MemoryAccounting accounting
    ) {
        if (keepAlive.millis() < 1) {
            throw new IllegalArgumentException("SEQUENCE snapshot keep-alive must be positive");
        }
        sequence = factory.plan.sequence();
        SequenceLatestValidConfig.validateProductionStageCount(sequence.children().size());
        profiler = new SequenceRuntimeProfiler(factory.configuration.profile(), sequence.children().size());
        keyFilterProfile = factory.configuration.profile()
            && factory.configuration.pragmas().sequenceKeyFilter()
            && factory.executionInfo != null ? factory.executionInfo.queryProfile().prepareSequenceKeyFilterProfile() : null;
        if (latestValid == null) {
            if (sequence.hasConditions()) {
                throw new IllegalStateException("SEQUENCE conditions require a prepared production configuration");
            }
            latestValid = SequenceLatestValidConfig.bounded(
                configuredBound(factory),
                java.util.Collections.nCopies(sequence.children().size(), List.of()),
                List.of()
            );
        }
        latestValid.withRememberedChains(factory.configuration.pragmas().sequenceRememberedChains());
        latestValid.validateForProduction(sequence.children().size());
        SequenceLatestValidConfig.validateCandidateBound(latestValid.maxCandidates().orElseThrow(), sequence.children().size());
        candidateBound = latestValid.maxCandidates().orElseThrow();
        if (factory.configuration.profile()) {
            latestValid = latestValid.withProfiledPredicates(profiler);
        }
        this.factory = factory;
        this.conditionResources = conditionResources;
        this.context = context;
        this.keepAlive = keepAlive;
        blockFactory = accounting.blockFactory();
        memoryBudget = accounting.budget();
        breaker = accounting.breaker();
        executionInfo = new SequenceExecutionInfo(factory.executionInfo());
        long[] maxGapNanos = null;
        if (sequence.hasLinkTimeLimits()) {
            maxGapNanos = new long[sequence.children().size()];
            for (int stage = 1; stage < maxGapNanos.length; stage++) {
                maxGapNanos[stage] = ((SequenceStage) sequence.children().get(stage)).maxGapNanos();
            }
        }
        matcher = new SequenceLatestValidMatcher(
            sequence.children().size(),
            new TimeValue(sequence.maxSpanNanos(), TimeUnit.NANOSECONDS),
            breaker,
            () -> factory.task != null && factory.task.isCancelled(),
            SequenceKeyCodec::compare,
            maxGapNanos,
            latestValid
        );
        try {
            reserve(1024L + 256L * sequence.children().size() + 16L * sequence.output().size());
            for (int stage = 0; stage < sequence.children().size(); stage++) {
                var logical = (SequenceStage) sequence.children().get(stage);
                List<Attribute> payloads = new ArrayList<>();
                for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                    if (sequence.sourceStages().get(column) == stage
                        && Sequence.isProvenanceAttribute(sequence.sourceAttributes().get(column)) == false) {
                        payloads.add(sequence.sourceAttributes().get(column));
                    }
                }
                List<SequenceLatestValidConfig.ValueDemand> demands = latestValid.mandatoryDemands().get(stage);
                // Includes the canonical demand projection and its temporary identity set/list overlap.
                reserve(256L + 128L * demands.size());
                var selection = SequenceStageInput.selection(logical, payloads, demands, this::reserve, stage + 1);
                for (int branch = 0; branch < selection.sourceCount(); branch++) {
                    reserve(1024);
                    cursors.add(new Cursor(stage, branch, selection, logical.keys(), demands));
                }
            }
        } catch (RuntimeException | Error e) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    /** Failure cleanup attempts every owner without replacing the operation's primary exception. */
    private static void closeOnFailure(Throwable failure, Releasable... resources) {
        for (Releasable resource : resources) {
            try {
                Releasables.close(resource);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
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
                    long started = profiler.start();
                    try {
                        selected = matcher.match(next.stage, row.key.bytes(), row.ordinal, row.handle, row.takeValues());
                    } finally {
                        profiler.finish(SequenceRuntimeProfiler.Phase.MATCHER, started);
                        if (factory.configuration.profile()) {
                            profiler.observeKeys(matcher.retainedKeys());
                        }
                    }
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
                            // LIMIT 1 builds only its own output. Later batches hold at most 64 matches and may
                            // flush earlier when candidate paging or the scheduler yield interrupts collection.
                            batch = new MatchBatch(rowsEmitted == 0 ? 1 : OUTPUT_BATCH_SIZE);
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
            try {
                finish();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure != null ? failure : asRuntime(e);
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
        // Only unused first-stage rows can keep another match possible.
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
        for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
            if (sequence.sourceStages().get(column) == stage
                && Sequence.isProvenanceAttribute(sequence.sourceAttributes().get(column)) == false) {
                return true;
            }
        }
        return false;
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
        search(cursor.stage, cursor.branch, source, response -> {
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
                long rawBytes = SequenceMemoryBudget.pageBytes(cursor.input.source(cursor.branch).output().size());
                reserve(rawBytes);
                try (
                    Page raw = new SequenceHitReader(cursor.input.source(cursor.branch).output(), factory.configuration).read(
                        hits,
                        blockFactory
                    )
                ) {
                    runStage(cursor.input, cursor.branch, raw, pages -> {
                        try {
                            candidates(cursor, identities, pages);
                        } finally {
                            breaker.addWithoutBreaking(-admitted);
                        }
                    }, () -> breaker.addWithoutBreaking(-admitted));
                } finally {
                    release(rawBytes);
                }
                identityBytes = 0; // The asynchronous stage owns identity admission now.
            } finally {
                breaker.addWithoutBreaking(-identityBytes);
            }
        });
    }

    private void candidates(Cursor cursor, List<InputRow> identities, List<Page> pages) {
        long started = profiler.start();
        try {
            buildCandidates(cursor, identities, pages);
        } finally {
            profiler.finish(SequenceRuntimeProfiler.Phase.CANDIDATE_BUILD, started);
        }
    }

    private void buildCandidates(Cursor cursor, List<InputRow> identities, List<Page> pages) {
        {
            for (Page page : pages) {
                LongBlock marker = page.getBlock(page.getBlockCount() - 1);
                for (int row = 0; row < page.getPositionCount(); row++) {
                    context.checkForEarlyTermination();
                    int source = Math.toIntExact(marker.getLong(marker.getFirstValueIndex(row)));
                    var key = cursor.codec().encode(page, row, cursor.stage, cursor.keys);
                    if (key == null) {
                        continue;
                    }
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
                        EncodedHandle encoded = handle(cursor, identity, page, row);
                        bytes = encoded.bytes;
                        long admission = 512L + 8L * (identity.index.length() + identity.id.length());
                        reserve(admission);
                        bytes += admission;
                        cursor.add(new Candidate(key, identity.ordinal, encoded.value, bytes, values));
                        values = null;
                    } catch (RuntimeException | Error e) {
                        long failedBytes = bytes;
                        closeOnFailure(e, values, key, () -> release(failedBytes));
                        throw e;
                    }
                    profiler.addCandidatesAdmitted(cursor.stage, 1);
                }
            }
            cursor.rows.sort(CANDIDATE_ORDER);
        }
    }

    private Page materialize() throws IOException {
        long started = profiler.start();
        try {
            return buildResult();
        } finally {
            profiler.finish(SequenceRuntimeProfiler.Phase.RESULT_BUILD, started);
        }
    }

    private Page buildResult() throws IOException {
        for (int row = 0; row < batch.matches.size(); row++) {
            for (int stage = 0; stage < sequence.children().size(); stage++) {
                if (needsPayload(stage)) {
                    restoreRetained(row, stage, batch.matches.get(row).handles().get(stage));
                }
            }
        }
        int count = batch.matches.size();
        var outputScope = memoryBudget.newScope();
        BlockFactory outputFactory = outputScope.blockFactory(context.blockFactory().parent());
        long outputBytes = SequenceMemoryBudget.pageBytes(sequence.output().size());
        reserve(outputBytes);
        Block[] blocks = null;
        boolean transferred = false;
        try {
            blocks = new Block[sequence.output().size()];
            for (int column = 0; column < blocks.length; column++) {
                try (
                    var builder = PlannerUtils.toElementType(sequence.output().get(column).dataType()).newBlockBuilder(count, outputFactory)
                ) {
                    for (int row = 0; row < count; row++) {
                        context.checkForEarlyTermination();
                        if (Sequence.isProvenanceAttribute(sequence.sourceAttributes().get(column))) {
                            try (Block value = provenance(batch.matches.get(row))) {
                                builder.copyFrom(value, 0, 1);
                            }
                            continue;
                        }
                        Block value = batch.cells[row][column];
                        if (value == null) {
                            throw new IllegalStateException("SEQUENCE retained output value is unavailable");
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
            outputScope.detach();
            transferred = true;
            return page;
        } finally {
            try {
                if (transferred == false) {
                    Releasables.close(blocks);
                }
            } finally {
                release(outputBytes);
            }
        }
    }

    /** Provenance reads only captured identities, independently of ordinary columns and released predicate values. */
    private Block provenance(SequenceMatch match) throws IOException {
        // Covers the JSON writer's byte/character scratch, output-stream growth/copy and decoded identity strings.
        // Only one writer is live at a time. The returned block and its builder use the budgeted factory separately.
        long admission = 64L * 1024;
        for (BytesRef handle : match.handles()) {
            admission = Math.addExact(admission, handleIdentityBytes(handle));
        }
        reserve(admission); // Decoded strings and escaped JSON coexist until the output builder copies each entry.
        try (var builder = blockFactory.newBytesRefBlockBuilder(1)) {
            builder.beginPositionEntry();
            for (int stage = 0; stage < match.handles().size(); stage++) {
                Handle handle = readHandle(match.handles().get(stage));
                try (var json = JsonXContent.contentBuilder()) {
                    json.startObject().field("stage", stage + 1).field("index", handle.index).field("id", handle.id).endObject();
                    builder.appendBytesRef(BytesReference.bytes(json).toBytesRef());
                }
            }
            builder.endPositionEntry();
            return builder.build();
        } finally {
            release(admission);
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
        long started = profiler.start();
        ActionListener<Void> timed = ActionListener.notifyOnce(
            ActionListener.runBefore(listener, () -> profiler.finish(SequenceRuntimeProfiler.Phase.STAGE_RUN, started))
        );
        try {
            factory.stageExecutor.execute(drivers, timed);
        } catch (Exception e) {
            try {
                Releasables.close(drivers);
            } finally {
                timed.onFailure(e);
            }
        }
    }

    private synchronized void recordStageCosts(List<Driver> drivers) {
        for (Driver driver : drivers) {
            stageDriverCpuNanos += driver.status().cpuNanos();
            stageDriverIterations += driver.status().iterations();
        }
    }

    /**
     * A filtered reader may reach EOF only after every start reader has finished and its final stage completion
     * has been applied. Include buffered future starts as well as matcher-owned unused starts, so this set can only
     * shrink after publication. Partial visitation, resource limits and uncertain raw identities keep ordinary reads.
     */
    private SequenceKeyFilter waitingKeyFilter(int stage, int branch) {
        if (stage == 0 || factory.configuration.pragmas().sequenceKeyFilter() == false || pending || completion != null) {
            return null;
        }
        var preference = factory.configuration.pragmas().fieldExtractPreference();
        if (preference != MappedFieldType.FieldExtractPreference.NONE && preference != MappedFieldType.FieldExtractPreference.DOC_VALUES) {
            recordKeyFilterFallback("field_extraction_preference");
            return null;
        }
        for (Cursor cursor : cursors) {
            if (cursor.stage == 0 && cursor.exhausted == false) {
                recordKeyFilterFallback("start_reader_open");
                return null;
            }
        }
        SequenceKeyFilter owner = null;
        boolean transferred = false;
        try {
            Cursor target = cursor(stage, branch);
            owner = SequenceKeyFilter.create(target.keys, target.input.source(branch).output(), breaker, context::checkForEarlyTermination);
            if (owner == null) {
                recordKeyFilterFallback("non_pushable_key");
                return null;
            }
            if (matcher.visitUnusedStartKeys(owner::add) == false) {
                recordKeyFilterFallback("key_snapshot_limit");
                return null;
            }
            for (Cursor cursor : cursors) {
                if (cursor.stage == 0) {
                    for (int row = cursor.offset; row < cursor.rows.size(); row++) {
                        context.checkForEarlyTermination();
                        if (owner.add(cursor.rows.get(row).key.bytes()) == false) {
                            recordKeyFilterFallback("key_snapshot_limit");
                            return null;
                        }
                    }
                }
            }
            if (owner.predicate() == null) {
                recordKeyFilterFallback("empty_or_unbuildable_predicate");
                return null;
            }
            transferred = true;
            return owner;
        } catch (CircuitBreakingException optionalLimit) {
            recordKeyFilterFallback("coordinator_breaker");
            return null;
        } finally {
            if (transferred == false) {
                Releasables.close(owner);
            }
        }
    }

    private void recordKeyFilterFallback(String reason) {
        if (keyFilterProfile != null) {
            keyFilterProfile.coordinatorFallback(reason);
        }
    }

    private void search(int stage, int branch, SearchSourceBuilder source, Consumer<SearchResponse> consume) {
        var readIndices = cursor(stage, branch).input.source(branch)
            .concreteQualifiedIndices()
            .stream()
            .filter(scope(stage, branch).indexNames()::contains)
            .toList();
        SequenceKeyFilter keyFilter = waitingKeyFilter(stage, branch);
        boolean transferred = false;
        try {
            QueryBuilder query = keyFilter == null
                ? source.query()
                : SequenceSourceQueryBuilder.withKeyFilter(source.query(), keyFilter.predicate());
            boolean profileFilter = keyFilter != null && keyFilterProfile != null;
            ThreadContext threadContext = profileFilter ? factory.client.threadPool().getThreadContext() : null;
            source.query(new SequenceSourceQueryBuilder(query, readIndices, threadContext));
            if (profileFilter) {
                source.profile(true);
            }
            candidateSearchRequests++;
            source.pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(keepAlive));
            begin();
            ActionListener<SearchResponse> listener = ActionListener.notifyOnce(ActionListener.wrap(response -> {
                profiler.addRowsFetched(stage, response.getHits().getHits().length);
                synchronized (this) {
                    if (response.pointInTimeId() != null) {
                        pit = response.pointInTimeId();
                    }
                }
                Releasables.close(keyFilter);
                if (profileFilter) {
                    keyFilterProfile.recordShardSearches(response);
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
                    failed(e);
                    return;
                }
                Runnable release = () -> {
                    try {
                        response.decRef();
                    } finally {
                        breaker.addWithoutBreaking(-admitted);
                    }
                };
                complete(() -> {
                    try {
                        executionInfo.onSearch(response);
                        candidateRowsFetched += response.getHits().getHits().length;
                        candidateSearchTookMillis += response.getTook().millis();
                        if (response.isTimedOut() || response.getFailedShards() != 0) {
                            throw new IllegalStateException("SEQUENCE requires a complete search response");
                        }
                        consume.accept(response);
                    } finally {
                        release.run();
                    }
                }, release);
            }, e -> {
                closeOnFailure(e, keyFilter);
                failed(e);
            }));
            transferred = true;
            try (var ignored = factory.requestContext.get()) {
                if (profileFilter) {
                    threadContext.putHeader(SequenceKeyFilterShardProfile.REQUEST_HEADER, "1");
                    keyFilterProfile.builtSearch();
                }
                // Can-match removes shards and renumbers shardRequestIndex, changing _shard_doc's high bits. Keep all PIT
                // shards in the same order, with stage membership expressed in the query rather than request indices.
                var request = new SearchRequest(factory.configuration.absoluteStartedTimeInMillis()).source(source)
                    .allowPartialSearchResults(false);
                request.setPointInTimeAuthorization(
                    pitAuthorizationIndices,
                    new OriginalIndices(
                        cursor(stage, branch).authorizationExpressions.toArray(String[]::new),
                        IndexResolver.DEFAULT_OPTIONS
                    ),
                    new SequenceSourceQueryBuilder(scope(stage, branch).query(), readIndices)
                );
                request.setPreFilterShardSize(Integer.MAX_VALUE);
                long started = profiler.start();
                ActionListener<SearchResponse> timed = ActionListener.notifyOnce(
                    ActionListener.runBefore(listener, () -> profiler.finish(SequenceRuntimeProfiler.Phase.SEARCH_WAIT, started))
                );
                try {
                    factory.client.search(request, timed);
                } catch (Exception e) {
                    timed.onFailure(e);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } finally {
            if (transferred == false) {
                Releasables.close(keyFilter);
            }
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
            e = snapshotFailure(e);
            if (failure == null) {
                executionInfo.onFailure(e);
                failure = asRuntime(e);
            } else if (failure != e) {
                failure.addSuppressed(e);
            }
        }
    }

    /** Missing contexts may reflect idle expiry, a lost node or an explicitly closed snapshot. */
    private static Exception snapshotFailure(Exception failure) {
        if (Arrays.stream(ElasticsearchException.guessRootCauses(failure))
            .anyMatch(root -> ExceptionsHelper.unwrap(root, SearchContextMissingException.class) != null)) {
            return new IllegalStateException(
                "SEQUENCE point-in-time snapshot is no longer available; snapshots are released after 5 minutes without a read. "
                    + "Retry the query, using a narrower time range if necessary.",
                failure
            );
        }
        return failure;
    }

    /** Cleanup callbacks are independent of the query so even an unanswered close cannot retain its resources. */
    private static final class PitCloseListener implements ActionListener<ClosePointInTimeResponse> {
        private static final PitCloseListener INSTANCE = new PitCloseListener();

        @Override
        public void onResponse(ClosePointInTimeResponse response) {
            if (response.isSucceeded() == false) {
                logger.warn(
                    "Could not confirm complete PIT cleanup for SEQUENCE query; reason [{}], exception type [{}]",
                    "incomplete_response",
                    "none"
                );
            }
        }

        @Override
        public void onFailure(Exception failure) {
            // A bounded type name is useful for diagnostics; the token, message and throwable may contain private data.
            String type = failure.getClass().getSimpleName();
            logger.warn(
                "Could not confirm complete PIT cleanup for SEQUENCE query; reason [{}], exception type [{}]",
                "close_failure",
                type.length() > 128 ? type.substring(0, 128) : type
            );
        }
    }

    private void closePit() {
        if (pending) {
            return;
        }
        if (pit == null) {
            releaseAsyncIfFinished();
            return;
        }
        BytesReference token = pit;
        pit = null;
        try (var ignored = factory.requestContext.get()) {
            // Cleanup survives parent-task cancellation with the original authorization, but does not retain the source.
            factory.cleanupClient.execute(
                TransportClosePointInTimeAction.TYPE,
                new ClosePointInTimeRequest(token),
                PitCloseListener.INSTANCE
            );
        } catch (Exception e) {
            PitCloseListener.INSTANCE.onFailure(e);
        } finally {
            releaseAsyncIfFinished();
        }
    }

    private void releaseAsyncIfFinished() {
        if (asyncRegistered && asyncReleased == false && (closed || finished || failure != null) && pending == false && pit == null) {
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
        if (factory.configuration.profile()) {
            profiler.observeBytesHeld(memoryBudget.peakBytes());
        }
        return new SequenceSourceStatus(
            candidateSearchRequests,
            0,
            candidateRowsFetched,
            0,
            completedMatches,
            rowsEmitted,
            stageDrivers,
            stageDriverCpuNanos,
            stageDriverIterations,
            candidateSearchTookMillis,
            0,
            maxPendingCandidates,
            matcher.latestValidCounters(),
            profiler.snapshot()
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
                publishLimitSummaries();
            } catch (RuntimeException | Error e) {
                closeOnFailure(e, this::clear, this::closePit);
                throw e;
            }
            try {
                clear();
            } catch (RuntimeException | Error e) {
                closeOnFailure(e, this::closePit);
                throw e;
            }
            closePit();
        }
    }

    /** Terminal publication is separate from expression warning budgets and never runs in a PIT-close callback. */
    private void publishLimitSummaries() {
        if (context.warningsMode() != DriverContext.WarningsMode.COLLECT || context.isFinished()) {
            return;
        }
        var counters = matcher.latestValidCounters();
        if (counters.candidatesDroppedAtBound() > 0) {
            context.addWarning(
                "SEQUENCE dropped ["
                    + counters.candidatesDroppedAtBound()
                    + "] candidates at the limit of ["
                    + candidateBound
                    + "] per stage and key; matches may be missing"
            );
        }
        if (counters.searchesAbandonedAtWorkLimit() > 0) {
            context.addWarning(
                "SEQUENCE abandoned ["
                    + counters.searchesAbandonedAtWorkLimit()
                    + "] searches at the work limit of ["
                    + SequenceLatestValidConfig.SEARCH_STEP_LIMIT
                    + "] steps; matches may be missing"
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

    private SequenceSourceScope scope(int stage, int branch) {
        return cursor(stage, branch).scope;
    }

    private Cursor cursor(int stage, int branch) {
        return cursors.stream().filter(cursor -> cursor.stage == stage && cursor.branch == branch).findFirst().orElseThrow();
    }

    /** Captures only demanded output cells; each candidate owns the values from this one authorized read. */
    private EncodedHandle handle(Cursor cursor, InputRow row, Page page, int position) {
        List<Attribute> retained = cursor.input.retained(cursor.branch);
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
            writeIdentity(identity, cursor.branch, row);
            int identitySize = Math.toIntExact(identity.position());
            var count = new CountingStreamOutput();
            writeHandle(count, identitySize, cursor.branch, row, cells);
            int size = Math.toIntExact(count.position());
            long arrayBytes = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size);
            long permanentBytes = 64L + arrayBytes;
            long admission = 192L + 2L * arrayBytes;
            reserve(admission); // Both the exactly sized output buffer and toByteArray's copy are live together.
            boolean transferred = false;
            try {
                var buffer = new ByteArrayOutputStream(size);
                try (var out = new OutputStreamStreamOutput(buffer)) {
                    writeHandle(out, identitySize, cursor.branch, row, cells);
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

    private static void writeIdentity(StreamOutput out, int branch, InputRow row) throws IOException {
        out.writeVInt(branch);
        out.writeString(row.index);
        out.writeString(row.id);
    }

    private static void writeHandle(StreamOutput out, int identitySize, int branch, InputRow row, Block[] cells) throws IOException {
        out.writeVInt(identitySize);
        writeIdentity(out, branch, row);
        out.writeVInt(cells.length);
        for (Block cell : cells) {
            Block.writeTypedBlock(cell, out);
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
            int branch = in.readVInt();
            String index = in.readString(), id = in.readString();
            if (start - in.available() != identitySize) {
                throw new IllegalStateException("SEQUENCE selected handle has an invalid identity length");
            }
            int count = in.readVInt();
            int size = in.available();
            return new Handle(branch, index, id, count, new BytesRef(handle.bytes, handle.offset + handle.length - size, size));
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

    private record Handle(int branch, String index, String id, int count, BytesRef cells) {}

    private record Completion(Runnable next, Runnable discard) {}

    /** Completed matches retain only bounded selected handles and admitted payload references until one output page. */
    private final class MatchBatch implements Releasable {
        private final int capacity;
        private final List<SequenceMatch> matches;
        private Block[][] cells;
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

        /** Takes one actual reference; its source-local block factory already owns the admission. */
        private void setCell(int row, int column, Block cell) {
            try {
                cells[row][column] = cell;
            } catch (RuntimeException | Error failure) {
                closeOnFailure(failure, cell);
                throw failure;
            }
        }

        @Override
        public void close() {
            if (released == false) {
                released = true;
                try {
                    Releasables.close(this::closeCells, Releasables.wrap(matches));
                } finally {
                    cells = null;
                    matches.clear();
                    release(bytes);
                    bytes = 0;
                }
            }
        }
    }

    /** Decodes captured cells without searching or running a stage again. */
    private void restoreRetained(int row, int stage, BytesRef encoded) throws IOException {
        long admission = 128L + handleIdentityBytes(encoded);
        reserve(admission);
        try {
            Handle handle = readHandle(encoded);
            List<Attribute> retained = cursor(stage, handle.branch).input.retained(handle.branch);
            if (handle.count != retained.size()) {
                throw new IllegalStateException("SEQUENCE retained cell count does not match its source branch");
            }
            try (var bytes = new BytesArray(handle.cells).streamInput(); var in = new BlockStreamInput(bytes, blockFactory)) {
                for (Attribute attribute : retained) {
                    try (Block cell = Block.readTypedBlock(in)) {
                        ElementType expected = PlannerUtils.toElementType(attribute.dataType());
                        if (cell.getPositionCount() != 1 || (cell.elementType() != expected && cell.elementType() != ElementType.NULL)) {
                            throw new IllegalStateException("SEQUENCE retained cell does not match its selected output type");
                        }
                        for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                            if (sequence.sourceStages().get(column) == stage
                                && sequence.sourceAttributes().get(column).semanticEquals(attribute)) {
                                if (batch.cells[row][column] != null) {
                                    throw new IllegalStateException("SEQUENCE retained cell was materialized more than once");
                                }
                                cell.incRef();
                                batch.setCell(row, column, cell);
                            }
                        }
                    }
                }
                if (in.available() != 0) {
                    throw new IllegalStateException("SEQUENCE retained cells have trailing data");
                }
            }
        } finally {
            release(admission);
        }
    }

    private final class StagePages implements Releasable {
        private final List<Page> pages = new ArrayList<>();
        private long bytes;
        private long foreignBlockBytes;
        private boolean released;

        private void add(Page page) {
            synchronized (SequenceSourceOperator.this) {
                if (closed || finished || released) {
                    page.releaseBlocks();
                    return;
                }
                long admission = SequenceMemoryBudget.pageBytes(page.getBlockCount());
                long foreign = 0;
                int previousSize = pages.size();
                boolean admitted = false;
                boolean foreignAdmitted = false;
                try {
                    page.allowPassingToDifferentDriver();
                    foreign = memoryBudget.foreignBytes(page);
                    memoryBudget.admitForeign(foreign);
                    foreignAdmitted = true;
                    breaker.addEstimateBytesAndMaybeBreak(admission, BREAKER_LABEL);
                    admitted = true;
                    pages.add(page);
                    bytes += admission;
                    foreignBlockBytes += foreign;
                } catch (RuntimeException | Error failure) {
                    long admittedBytes = admitted ? admission : 0;
                    long foreignBytes = foreignAdmitted ? foreign : 0;
                    closeOnFailure(failure, () -> {
                        if (pages.size() > previousSize) {
                            pages.remove(previousSize);
                        }
                    }, page, () -> breaker.addWithoutBreaking(-admittedBytes), () -> memoryBudget.releaseForeign(foreignBytes));
                    throw failure;
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
                        try {
                            breaker.addWithoutBreaking(-bytes);
                        } finally {
                            memoryBudget.releaseForeign(foreignBlockBytes);
                            bytes = 0;
                            foreignBlockBytes = 0;
                        }
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
        private int[] retainedChannels;
        private int[] mandatoryChannels;
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
                List<Attribute> retained = input.retained(branch);
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
                Releasables.close(Releasables.wrap(rows), codec);
            } finally {
                scope = null;
                authorizationExpressions = null;
                codec = null;
                try {
                    resetRows();
                } finally {
                    // Consumption runs under the source monitor. Late search/stage completion discards its
                    // pages instead of invoking consumption; workers never capture these cached arrays.
                    retainedChannels = null;
                    mandatoryChannels = null;
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
