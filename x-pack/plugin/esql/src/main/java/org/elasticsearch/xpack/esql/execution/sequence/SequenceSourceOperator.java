/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * Coordinator source of completed sequences, with bounded search pages and asynchronous stage execution. Selection
 * retains keys and compact source handles only. Payload is fetched under the same PIT and replayed through the
 * prepared stage, preserving selected expansion rows, computed fields and original metadata.
 * <p>
 * Matching and stage CPU work run on the owning driver. Callbacks hand off one completion and wake the driver;
 * snapshot renewal has separate synchronized ownership and never blocks a search or compute thread. Closing waits
 * for in-flight ownership through DriverContext and closes
 * even a PIT returned after early termination. Searches use the caller's credentials and a fixed PIT shard enumeration.
 */
public final class SequenceSourceOperator extends SourceOperator {
    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(2);
    private static final int SEARCH_PAGE_SIZE = 256;
    private static final int PAYLOAD_BATCH_SIZE = 64;
    private static final String BREAKER_LABEL = "esql_sequence_reader";
    private static final Comparator<Candidate> CANDIDATE_ORDER = Comparator.comparing((Candidate c) -> c.ordinal)
        .thenComparing(c -> c.key.bytes(), SequenceKeyCodec::compare);

    /** Prepares ordinary compute drivers for one local stage binding, with an ownership-transferring output consumer. */
    @FunctionalInterface
    public interface StagePlanner {
        List<Driver> plan(PhysicalPlan plan, Consumer<Page> output);
    }

    /** Runs prepared drivers as children of the query task, including cancellation and completion cleanup. */
    @FunctionalInterface
    public interface StageExecutor {
        void execute(List<Driver> drivers, ActionListener<Void> listener);
    }

    /** Supplies the native coordinator source while the local planner supplies ordinary stage driver preparation. */
    @FunctionalInterface
    public interface FactoryProvider {
        SourceOperatorFactory create(SequenceExec sequence, StagePlanner planner);
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
    private final SequenceMatcher matcher;
    private final SequenceExecutionInfo executionInfo;
    private final TimeValue keepAlive;
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
        if (keepAlive.millis() < 3) {
            throw new IllegalArgumentException("SEQUENCE snapshot keep-alive must be at least three milliseconds");
        }
        this.factory = factory;
        this.context = context;
        this.keepAlive = keepAlive;
        renewalInterval = TimeValue.timeValueMillis(keepAlive.millis() / 3);
        blockFactory = context.blockFactory().parent();
        breaker = LocalCircuitBreaker.forAsyncIo(context.breaker());
        sequence = factory.plan.sequence();
        executionInfo = new SequenceExecutionInfo(factory.executionInfo());
        matcher = new SequenceMatcher(
            sequence.children().size(),
            new TimeValue(sequence.maxSpanNanos(), TimeUnit.NANOSECONDS),
            breaker,
            () -> factory.task != null && factory.task.isCancelled(),
            SequenceKeyCodec::compare
        );
        boolean success = false;
        try {
            reserve(1024L + 256L * sequence.children().size() + 16L * sequence.output().size());
            for (int stage = 0; stage < sequence.children().size(); stage++) {
                var logical = (SequenceStage) sequence.children().get(stage);
                var selection = new SequenceStageInput(logical, List.of());
                List<Attribute> payloads = new ArrayList<>();
                for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                    if (sequence.sourceStages().get(column) == stage) {
                        payloads.add(sequence.sourceAttributes().get(column));
                    }
                }
                payload.add(new SequenceStageInput(logical, payloads));
                for (int branch = 0; branch < selection.sourceCount(); branch++) {
                    reserve(1024);
                    cursors.add(new Cursor(stage, branch, selection, logical.keys()));
                }
            }
            success = true;
        } finally {
            if (success == false) {
                close();
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
            // Yield to the normal driver scheduler even when many unmatched candidates are buffered locally.
            for (int iteration = 0; iteration < 128; iteration++) {
                context.checkForEarlyTermination();
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
                    inputExhausted = true;
                    if (batch != null) {
                        batch.prepare();
                        return materialize();
                    }
                    finish();
                    return null;
                }
                Candidate row = next.rows.set(next.offset++, null);
                SequenceMatcher.Match selected;
                try (row) {
                    selected = matcher.match(next.stage, row.key.bytes(), row.ordinal, row.handle);
                    maxPendingCandidates = Math.max(maxPendingCandidates, matcher.pendingCandidates());
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
        for (Cursor cursor : cursors) {
            var relation = cursor.input.source(cursor.branch);
            if (relation.originalIndices().keySet().stream().anyMatch(alias -> alias.isEmpty() == false)) {
                throw new IllegalArgumentException("cross-cluster SEQUENCE sources are not supported");
            }
            indices.addAll(SequenceSourceScope.positiveExpressions(cursor.authorizationExpressions));
            concreteIndices.addAll(cursor.scope.indexNames());
        }
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
            request.indexFilter(termsQuery("_index", concreteIndices));
            factory.client.execute(TransportOpenPointInTimeAction.TYPE, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private SearchSourceBuilder searchSource(SequenceStageInput input, int stage, int branch) {
        var relation = input.source(branch);
        var query = boolQuery().filter(termsQuery("_index", relation.concreteQualifiedIndices())).filter(existsQuery("@timestamp"));
        query.filter(scope(stage, branch).query());
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
        SearchSourceBuilder source = searchSource(cursor.input, cursor.stage, cursor.branch);
        if (cursor.after != null) {
            source.searchAfter(cursor.after);
        }
        search(cursor.stage, cursor.branch, source, false, response -> {
            SearchHit[] allHits = response.getHits().getHits();
            cursor.exhausted = allHits.length < SEARCH_PAGE_SIZE;
            if (allHits.length == 0) {
                return;
            }
            cursor.after = allHits[allHits.length - 1].getSortValues().clone();
            for (SearchHit hit : allHits) {
                SequenceOrdinal ordinal = ordinal(hit);
                if (cursor.previous != null && cursor.previous.compareTo(ordinal) >= 0) {
                    throw new IllegalStateException("SEQUENCE source search did not advance strictly");
                }
                cursor.previous = ordinal;
            }
            SearchHit[] hits = Arrays.stream(allHits).filter(cursor.scope::contains).toArray(SearchHit[]::new);
            if (hits.length == 0) {
                return;
            }
            List<InputRow> identities = new ArrayList<>(hits.length);
            long identityBytes = 0;
            try {
                for (SearchHit hit : hits) {
                    SequenceHitReader.ensureScalarTimestamp(hit);
                    SequenceOrdinal ordinal = ordinal(hit);
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
                    var key = cursor.codec.encode(page, row);
                    if (key == null) {
                        continue;
                    }
                    boolean success = false;
                    InputRow identity = identities.get(source);
                    long bytes = 512L + 8L * (identity.index.length() + identity.id.length());
                    try {
                        reserve(bytes);
                        try {
                            cursor.add(new Candidate(key, identity.ordinal, handle(cursor.branch, expansion, identity), bytes));
                            success = true;
                        } finally {
                            if (success == false) {
                                release(bytes);
                            }
                        }
                    } finally {
                        if (success == false) {
                            key.close();
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
            drivers = factory.stagePlanner.plan(binding.plan(), collected::add);
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
            request.setPointInTimeAuthorizationIndices(pitAuthorizationIndices);
            request.setPointInTimeAuthorizationScope(
                new OriginalIndices(cursor(stage, branch).authorizationExpressions.toArray(String[]::new), IndexResolver.DEFAULT_OPTIONS)
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
                request.setPointInTimeAuthorizationIndices(pitAuthorizationIndices);
                // Full-snapshot authorization is safe for a renewal which cannot retrieve documents. Retain every
                // shard position and singleton node requests, just as candidate/payload searches do.
                request.setPointInTimeAuthorizationScope(pitAuthorizationIndices);
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
        pit = null;
        closingPit = true;
        var listener = ActionListener.notifyOnce(ActionListener.<ClosePointInTimeResponse>wrap(unused -> pitClosed(), e -> pitClosed()));
        try (var ignored = factory.requestContext.get()) {
            // Cleanup must survive cancellation of the query's parent task, while retaining the same authorization.
            factory.cleanupClient.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(token), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private synchronized void pitClosed() {
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
            maxPendingCandidates
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
                clear();
            } finally {
                closePit();
            }
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
                () -> release(heldBytes)
            );
        } finally {
            batch = null;
            pitAuthorizationIndices = null;
        }
    }

    private SequenceKeyCodec codec(List<Attribute> keys, List<Attribute> attributes) {
        int[] channels = keys.stream().mapToInt(attributes::indexOf).toArray();
        return new SequenceKeyCodec(keys.stream().map(Attribute::dataType).toList(), channels, breaker);
    }

    private static SequenceOrdinal ordinal(SearchHit hit) {
        Object[] sort = hit.getSortValues();
        if (sort.length != 2) {
            throw new IllegalStateException("SEQUENCE search must return timestamp and PIT document ordinal");
        }
        Instant timestamp = Instant.parse(sort[0].toString());
        return new SequenceOrdinal(timestamp.toEpochMilli(), timestamp.getNano() % 1_000_000, ((Number) sort[1]).longValue());
    }

    private SequenceSourceScope scope(int stage, int branch) {
        return cursor(stage, branch).scope;
    }

    private Cursor cursor(int stage, int branch) {
        return cursors.stream().filter(cursor -> cursor.stage == stage && cursor.branch == branch).findFirst().orElseThrow();
    }

    private static Instant instant(SequenceOrdinal ordinal) {
        return Instant.ofEpochMilli(ordinal.epochMillis()).plusNanos(ordinal.nanoOfMilli());
    }

    static long precedingShardDoc(long shardDoc) {
        if (ShardDocSortField.decodeDoc(shardDoc) > 0) {
            return shardDoc - 1;
        }
        int shard = ShardDocSortField.decodeShardRequestIndex(shardDoc);
        return shard == 0 ? -1L : ShardDocSortField.encodeShardAndDoc(shard - 1, Integer.MAX_VALUE);
    }

    private static BytesRef handle(int branch, int expansion, InputRow row) {
        try (var out = new BytesStreamOutput()) {
            out.writeVInt(branch);
            out.writeVInt(expansion);
            out.writeString(row.index);
            out.writeString(row.id);
            out.writeLong(row.ordinal.epochMillis());
            out.writeInt(row.ordinal.nanoOfMilli());
            out.writeLong(row.ordinal.shardDoc());
            return BytesRef.deepCopyOf(out.bytes().toBytesRef());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Handle readHandle(BytesRef handle) throws IOException {
        try (var in = new BytesArray(handle).streamInput()) {
            return new Handle(
                in.readVInt(),
                in.readVInt(),
                in.readString(),
                in.readString(),
                new SequenceOrdinal(in.readLong(), in.readInt(), in.readLong())
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

    private record Handle(int branch, int expansion, String index, String id, SequenceOrdinal ordinal) {}

    private record Completion(Runnable next, Runnable discard) {}

    /** Completed matches retain only bounded selected handles and admitted payload references until one output page. */
    private final class MatchBatch implements Releasable {
        private final int capacity;
        private final List<SequenceMatcher.Match> matches;
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
     * Fetches and replays selected events for one stage branch. ID searches are paged because the same ID can exist
     * on several routed shards; only the exact selected PIT ordinals are retained, independently of each response.
     */
    private final class PayloadGroup implements Releasable {
        private final MatchBatch owner;
        private final int stage;
        private final int branch;
        private final TreeMap<SequenceOrdinal, ReplayEvent> events;
        private long bytes;
        private Object[] after;
        private SequenceOrdinal previous;
        private int found;
        private boolean fetching;
        private boolean replaying;
        private boolean released;

        private PayloadGroup(MatchBatch owner, int stage, int branch) throws IOException {
            this.owner = owner;
            this.stage = stage;
            this.branch = branch;
            // Covers decoded handles, events/targets, map nodes, ID query containers, hit-reference arrays and
            // expansion counters. Payload blocks and retained SearchHits have independent admissions below.
            long admission = 1024L + 1024L * owner.matches.size();
            for (SequenceMatcher.Match match : owner.matches) {
                admission = Math.addExact(admission, 8L * match.handles().get(stage).length);
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
                    ReplayEvent event = events.get(handle.ordinal);
                    if (event == null) {
                        event = new ReplayEvent(handle);
                        events.put(handle.ordinal, event);
                    } else if (event.handle.index.equals(handle.index) == false || event.handle.id.equals(handle.id) == false) {
                        throw new IllegalStateException("SEQUENCE selected PIT ordinal has conflicting source identity");
                    }
                    event.targets.add(new ReplayTarget(row, handle.expansion));
                }
                if (events.isEmpty() == false) {
                    SequenceOrdinal first = events.firstKey();
                    after = new Object[] { instant(first).toString(), precedingShardDoc(first.shardDoc()) };
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
            SearchSourceBuilder source = searchSource(input, stage, branch);
            Map<String, List<String>> identities = new LinkedHashMap<>();
            for (ReplayEvent event : events.values()) {
                identities.computeIfAbsent(event.handle.index, ignored -> new ArrayList<>()).add(event.handle.id);
            }
            var membership = boolQuery().minimumShouldMatch(1);
            for (var identity : identities.entrySet()) {
                membership.should(
                    boolQuery().filter(termQuery("_index", identity.getKey()))
                        .filter(idsQuery().addIds(identity.getValue().toArray(String[]::new)))
                );
            }
            source.query(
                boolQuery().filter(source.query())
                    .filter(membership)
                    .filter(rangeQuery("@timestamp").lte(instant(events.lastKey()).toString()).format("strict_date_optional_time_nanos"))
            );
            int size = Math.min(SEARCH_PAGE_SIZE, events.size());
            source.size(size).searchAfter(after);
            fetching = true;
            search(stage, branch, source, true, response -> {
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    context.checkForEarlyTermination();
                    SequenceOrdinal ordinal = ordinal(hit);
                    if (previous != null && previous.compareTo(ordinal) >= 0) {
                        throw new IllegalStateException("SEQUENCE payload search did not advance strictly");
                    }
                    previous = ordinal;
                    ReplayEvent event = events.get(ordinal);
                    if (event != null) {
                        if (event.hit != null
                            || event.handle.index.equals(hit.getIndex()) == false
                            || event.handle.id.equals(hit.getId()) == false
                            || scope(stage, branch).contains(hit) == false) {
                            throw new IllegalStateException("SEQUENCE selected event is unavailable in its snapshot");
                        }
                        SequenceHitReader.ensureScalarTimestamp(hit);
                        event.retain(hit);
                        found++;
                    }
                }
                if (found == events.size()) {
                    replay(input);
                } else if (hits.length < size || previous.compareTo(events.lastKey()) >= 0) {
                    throw new IllegalStateException("SEQUENCE selected event is unavailable in its snapshot");
                } else {
                    after = hits[hits.length - 1].getSortValues().clone();
                }
            }, () -> {
                fetching = false;
                if (released) {
                    close();
                }
            });
        }

        private void replay(SequenceStageInput input) {
            ReplayEvent[] ordered = events.values().toArray(ReplayEvent[]::new);
            SearchHit[] hits = Arrays.stream(ordered).map(event -> event.hit).toArray(SearchHit[]::new);
            try (Page raw = new SequenceHitReader(input.source(branch).output(), factory.configuration).read(hits, blockFactory)) {
                boolean transferred = false;
                try {
                    replaying = true;
                    runStage(input, branch, raw, pages -> {
                        try {
                            select(input, ordered, pages);
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

        private void select(SequenceStageInput input, ReplayEvent[] ordered, List<Page> pages) {
            int[] expansions = new int[ordered.length];
            List<Attribute> attributes = input.output();
            var logical = (SequenceStage) sequence.children().get(stage);
            try (var codec = codec(logical.keys(), attributes)) {
                for (Page page : pages) {
                    LongBlock marker = page.getBlock(page.getBlockCount() - 1);
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        context.checkForEarlyTermination();
                        int source = Math.toIntExact(marker.getLong(marker.getFirstValueIndex(row)));
                        int expansion = expansions[source];
                        expansions[source] = Math.incrementExact(expansion);
                        for (ReplayTarget target : ordered[source].targets) {
                            if (target.expansion != expansion) {
                                continue;
                            }
                            if (target.selected) {
                                throw new IllegalStateException("SEQUENCE selected expansion was replayed more than once");
                            }
                            try (var key = codec.encode(page, row)) {
                                if (key == null || key.bytes().equals(owner.matches.get(target.row).key()) == false) {
                                    throw new IllegalStateException("SEQUENCE selected expansion changed during payload replay");
                                }
                            }
                            for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
                                if (sequence.sourceStages().get(column) == stage) {
                                    int channel = attributes.indexOf(sequence.sourceAttributes().get(column));
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
            }
            for (ReplayEvent event : ordered) {
                for (ReplayTarget target : event.targets) {
                    if (target.selected == false) {
                        throw new IllegalStateException("SEQUENCE selected expansion is unavailable during payload replay");
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
                        previous = null;
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
        private final int expansion;
        private boolean selected;

        private ReplayTarget(int row, int expansion) {
            this.row = row;
            this.expansion = expansion;
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
        private final SequenceKeyCodec codec;
        private SequenceSourceScope scope;
        private List<String> authorizationExpressions;
        private List<Candidate> rows = new ArrayList<>();
        private long containerBytes;
        private Object[] after;
        private SequenceOrdinal previous;
        private int offset;
        private boolean exhausted;

        private Cursor(int stage, int branch, SequenceStageInput input, List<Attribute> keys) {
            this.stage = stage;
            this.branch = branch;
            this.input = input;
            codec = codec(keys, input.output());
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
                resetRows();
            }
        }
    }

    private final class Candidate implements Releasable {
        private final SequenceKeyCodec.EncodedKey key;
        private final SequenceOrdinal ordinal;
        private final BytesRef handle;
        private final long bytes;
        private boolean closed;

        private Candidate(SequenceKeyCodec.EncodedKey key, SequenceOrdinal ordinal, BytesRef handle, long bytes) {
            this.key = key;
            this.ordinal = ordinal;
            this.handle = handle;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                key.close();
                release(bytes);
            }
        }
    }
}
