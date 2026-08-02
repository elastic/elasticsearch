/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Drained;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Outcome;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Owns the node-local derived metrics state: it observes writes handed to it by {@link DerivedMetricsIndexingListener}, buffers them per
 * interval, and periodically flushes closed intervals into the managed destination streams.
 *
 * <p>All observation happens on the write thread, so the per-document path is kept to evaluating precompiled predicates and, only when
 * some metric actually needs it, reading a small filtered slice of the document's source. Everything expensive — building documents,
 * bulk indexing — happens on the flush thread.
 */
public class DerivedMetricsService implements Closeable {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsService.class);

    public static final Setting<TimeValue> FLUSH_INTERVAL = Setting.timeSetting(
        "data_streams.derived_metrics.flush_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * How long after an interval ends its bucket stays open, so that writes still in flight when the boundary passes are not lost.
     */
    public static final Setting<TimeValue> FLUSH_GRACE_PERIOD = Setting.timeSetting(
        "data_streams.derived_metrics.flush_grace_period",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * The buffer's own circuit breaker. Its limit is a fraction of the heap by default, so a node with a small heap gets a
     * proportionally small metrics budget without anyone configuring it.
     */
    public static final String BREAKER_NAME = "derived_metrics";
    public static final double DEFAULT_BREAKER_LIMIT_FRACTION = 0.05;
    public static final double DEFAULT_BREAKER_OVERHEAD = 1.0;

    /**
     * Five percent of the heap. Expressed as a fraction rather than a fixed size so that a node with a small heap gets a proportionally
     * small metrics budget without anyone having to configure one.
     */
    public static long defaultBreakerLimit() {
        return (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * DEFAULT_BREAKER_LIMIT_FRACTION);
    }

    public static final Setting<Integer> MAX_SERIES_PER_NODE = Setting.intSetting(
        "data_streams.derived_metrics.max_series_per_node",
        10_000,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Ceiling for any single source stream, so the node budget is not first-come-first-served. Defaults to the whole node budget, which
     * preserves today's behaviour until an operator chooses to divide it.
     */
    public static final Setting<Integer> MAX_SERIES_PER_STREAM = Setting.intSetting(
        "data_streams.derived_metrics.max_series_per_stream",
        MAX_SERIES_PER_NODE,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The bucket capacity of each histogram series. This is the knob that trades a histogram series' precision against its size, and a
     * histogram series is by far the most expensive kind: hundreds of buckets against the handful of primitives a scalar series needs.
     */
    public static final Setting<Integer> HISTOGRAM_BUCKETS = Setting.intSetting(
        "data_streams.derived_metrics.histogram_buckets",
        DerivedMetricsBuffer.DEFAULT_HISTOGRAM_BUCKETS,
        // The merger refuses anything below four. A lower minimum here was accepted at node scope and then threw on every observation:
        // listener exceptions are caught and logged rather than failing the write, so the only symptom was a metric that silently never
        // emitted and a log line per document.
        4,
        Setting.Property.NodeScope
    );

    /**
     * What to do when the buffer can take no more, either because a series cap was reached or because the circuit breaker refused the
     * memory a new series needed.
     */
    public enum MemoryPressurePolicy {
        /**
         * Emit what has been collected so far as a partial bucket, then carry on collecting. No observation is lost, because partials of
         * one bucket are reduced together at query time. The costs are more documents while the pressure lasts, and a partial whose
         * timestamp sits a few milliseconds after its bucket start rather than exactly on it — still inside the same
         * {@code date_histogram} bucket, but visible to anyone aligning windows by hand.
         */
        FLUSH_EARLY,
        /**
         * Drop the observation. Document volume stays perfectly flat and timestamps stay exactly on bucket boundaries, at the cost of
         * losing data. This is what Micrometer and the Prometheus Java client do.
         */
        DROP
    }

    public static final Setting<MemoryPressurePolicy> MEMORY_PRESSURE_POLICY = Setting.enumSetting(
        MemoryPressurePolicy.class,
        "data_streams.derived_metrics.memory_pressure_policy",
        MemoryPressurePolicy.FLUSH_EARLY,
        Setting.Property.NodeScope
    );

    /**
     * The share of the node's indexing pressure budget above which derived metrics stop emitting.
     *
     * <p>Emitted bulks are charged to the same node-wide budget as the user writes that produced them — the {@code derived_metrics} origin
     * buys a security context, not a separate allowance — so without this a busy node can have its own writes rejected to make room for
     * metrics about those writes. Declining early is the isolation we can actually get: we cannot have our own pool of bytes, but we can
     * refuse to compete for the shared one when the node is already under strain. Set to 1.0 to never decline.
     */
    public static final Setting<Double> INDEXING_PRESSURE_CEILING = Setting.doubleSetting(
        "data_streams.derived_metrics.indexing_pressure_ceiling",
        0.7,
        0.0,
        1.0,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> BULK_SIZE = Setting.intSetting(
        "data_streams.derived_metrics.bulk_size",
        1_000,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Ceiling on emission outstanding at once, expressed as a number of full bulks. Emission is fire and forget, so a destination that
     * cannot keep up would otherwise let every flush add to a queue with nothing bounding it.
     *
     * <p>What is actually bounded is documents rather than requests, this many times {@link #BULK_SIZE}. Counting requests would make the
     * ceiling depend on how the documents happened to be divided up: flushing early under memory pressure emits a handful of documents at
     * a time, and a request-based ceiling would shed almost all of them while barely any memory was in flight.
     */
    public static final Setting<Integer> MAX_IN_FLIGHT_BULKS = Setting.intSetting(
        "data_streams.derived_metrics.max_in_flight_bulks",
        8,
        1,
        Setting.Property.NodeScope
    );

    private final Client client;
    private final ThreadPool threadPool;
    private final DerivedMetricsBuffer buffer;
    private final TimeValue flushInterval;
    private final long graceMillis;
    private final int bulkSize;
    private final int maxInFlightDocuments;
    private final MemoryPressurePolicy memoryPressurePolicy;
    /**
     * The node's persistent ID, which is what identifies a partial. See {@link DerivedMetricsDestination#NODE_FIELD} for why this rather
     * than the node name.
     */
    private final String nodeId;
    private final String nodeName;
    private final AtomicInteger inFlightDocuments = new AtomicInteger();
    private final AtomicLong droppedForBackpressure = new AtomicLong();
    private final AtomicLong earlyFlushes = new AtomicLong();
    private final AtomicLong droppedForIndexingPressure = new AtomicLong();
    private final AtomicLong lostSeries = new AtomicLong();
    private final AtomicLong emissionFailures = new AtomicLong();
    /**
     * Documents the destination itself rejected. Distinct from {@link #emissionFailures}, which counts whole requests that never landed:
     * this is where a duplicate {@code _id} or a mapping conflict shows up, and both are invisible without it.
     */
    private final AtomicLong emissionRejections = new AtomicLong();
    /** Observations skipped because the metric reads a field the document did not have, or had as something non-numeric. */
    /** Documents whose values came from the already-parsed document rather than from a second parse of {@code _source}. */
    private final AtomicLong documentsReadFromIndex = new AtomicLong();
    /** Documents that had to be parsed again because some configured path could not be recovered from the index. */
    private final AtomicLong documentsReadFromSource = new AtomicLong();
    private final AtomicLong skippedForMissingValue = new AtomicLong();
    /** Observations skipped because the metric needs a predicate evaluated against a {@code _source} that could not be read. */
    private final AtomicLong skippedForUnreadableSource = new AtomicLong();
    /** Observations the buffer refused once and accepted after an early flush; the buffer counted a drop that did not happen. */
    private final AtomicLong recoveredAfterRelief = new AtomicLong();
    /** Observations the buffer refused twice; the buffer counted two drops for one lost observation. */
    private final AtomicLong doubleCountedDrops = new AtomicLong();
    private final IndexingPressure indexingPressure;
    private final double indexingPressureCeiling;
    private final Executor executor;
    private final MeterRegistry meterRegistry;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    private final ThreadLocal<RecordingScratch> scratches = ThreadLocal.withInitial(RecordingScratch::new);

    private volatile Scheduler.Cancellable scheduled;
    private volatile boolean closed;
    private long lastReportedDrops;
    private final long[] reportedBackpressureDrops = new long[1];
    private final long[] reportedIndexingPressureDrops = new long[1];
    private final long[] reportedEarlyFlushes = new long[1];

    public DerivedMetricsService(
        Settings settings,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        IndexingPressure indexingPressure,
        MeterRegistry meterRegistry,
        String nodeId,
        String nodeName
    ) {
        this.client = new OriginSettingClient(client, DataStreamDerivedMetrics.DERIVED_METRICS_ORIGIN);
        this.threadPool = threadPool;
        this.executor = threadPool.executor(DataStreamsPlugin.DERIVED_METRICS_THREAD_POOL);
        this.indexingPressure = indexingPressure;
        this.indexingPressureCeiling = INDEXING_PRESSURE_CEILING.get(settings);
        this.meterRegistry = meterRegistry;
        this.buffer = new DerivedMetricsBuffer(
            bigArrays,
            MAX_SERIES_PER_NODE.get(settings),
            MAX_SERIES_PER_STREAM.get(settings),
            HISTOGRAM_BUCKETS.get(settings),
            // Vary the first partial's offset per service instance, so a node that restarts inside a bucket it had already emitted for
            // does not stamp a second partial at the same timestamp and have it silently rejected as a duplicate _id.
            Math.floorMod(threadPool.absoluteTimeInMillis(), PARTIAL_SEED_RANGE),
            // A low-cardinality bucket is striped once per thread that can be inside an observation, which is the size of the write pool.
            EsExecutors.allocatedProcessors(settings)
        );
        this.flushInterval = FLUSH_INTERVAL.get(settings);
        this.graceMillis = FLUSH_GRACE_PERIOD.get(settings).millis();
        this.bulkSize = BULK_SIZE.get(settings);
        this.maxInFlightDocuments = MAX_IN_FLIGHT_BULKS.get(settings) * this.bulkSize;
        this.memoryPressurePolicy = MEMORY_PRESSURE_POLICY.get(settings);
        this.nodeId = nodeId;
        this.nodeName = nodeName;
    }

    /**
     * How many distinct starting offsets a partial can take. Small enough to leave the rest of the interval for actual partials, large
     * enough that two restarts inside one bucket are very unlikely to pick the same one.
     */
    private static final int PARTIAL_SEED_RANGE = 128;

    public void init() {
        registerMetrics();
        scheduled = threadPool.scheduleWithFixedDelay(this::flush, flushInterval, executor);
    }

    /**
     * Publishes what the feature is shedding, because every one of these was previously only a log line — which means nobody sees it until
     * they go looking, and nobody can alert on it at all.
     */
    private void registerMetrics() {
        register("es.derived_metrics.series.current", "series currently buffered on this node", "count", () -> (long) buffer.size());
        register(
            "es.derived_metrics.series.dropped.total",
            "observations dropped because a series cap or the circuit breaker refused them",
            "count",
            // the retry after an early flush is counted by the buffer as a drop whether or not it succeeded, so both outcomes are
            // subtracted back out here: one for an observation that was not lost at all, one for the second count of one that was
            () -> buffer.droppedSeries() - recoveredAfterRelief.get() - doubleCountedDrops.get()
        );
        register(
            "es.derived_metrics.series.dropped.node_cap.total",
            "series refused because this node had already spent its budget",
            "count",
            buffer::droppedSeriesAtNodeCap
        );
        register(
            "es.derived_metrics.series.dropped.stream_cap.total",
            "series refused because one source data stream had already taken its share",
            "count",
            buffer::droppedSeriesAtStreamCap
        );
        register(
            "es.derived_metrics.series.dropped.breaker.total",
            "series refused because the circuit breaker would not give them the memory they needed",
            "count",
            buffer::droppedSeriesAtBreaker
        );
        register(
            "es.derived_metrics.observations.skipped.missing_value.total",
            "observations skipped because the metric's value field was absent or not numeric",
            "count",
            skippedForMissingValue::get
        );
        register(
            "es.derived_metrics.observations.skipped.unreadable_source.total",
            "observations skipped because the metric's predicate needed a _source that could not be read",
            "count",
            skippedForUnreadableSource::get
        );
        register(
            "es.derived_metrics.documents.read.from_index.total",
            "documents whose values were read from the already-parsed document rather than by parsing _source again",
            "count",
            documentsReadFromIndex::get
        );
        register(
            "es.derived_metrics.documents.read.from_source.total",
            "documents that had to be parsed again because a configured path could not be recovered from the index",
            "count",
            documentsReadFromSource::get
        );
        register(
            "es.derived_metrics.documents.rejected.total",
            "documents the destination rejected, which is where a duplicate _id or a mapping conflict appears",
            "count",
            emissionRejections::get
        );
        register(
            "es.derived_metrics.partials.exhausted.total",
            "buckets that could not be flushed early because no timestamp offset was left inside the interval",
            "count",
            buffer::partialsExhausted
        );
        register(
            "es.derived_metrics.series.lost.total",
            "buffered series lost because they could not be flushed before this node stopped observing",
            "count",
            lostSeries::get
        );
        register(
            "es.derived_metrics.documents.dropped.backpressure.total",
            "documents dropped because the destination was not keeping up",
            "count",
            droppedForBackpressure::get
        );
        register(
            "es.derived_metrics.documents.dropped.indexing_pressure.total",
            "documents dropped because the node was above its indexing pressure ceiling",
            "count",
            droppedForIndexingPressure::get
        );
        register("es.derived_metrics.documents.failed.total", "documents whose bulk request failed", "count", emissionFailures::get);
        register("es.derived_metrics.flushes.early.total", "buckets flushed early because the buffer was full", "count", earlyFlushes::get);
    }

    private void register(String name, String description, String unit, LongSupplier value) {
        metrics.add(meterRegistry.registerLongAsyncCounter(name, description, unit, () -> new LongWithAttributes(value.getAsLong())));
    }

    /**
     * Records one write against the derived metrics of its source data stream.
     *
     * @param succeeded whether the document was indexed. Failed writes only feed metrics triggered by failures, but they still carry
     *                  their dimensions, so a failure series is broken down the same way a success series is.
     */
    public void record(
        ProjectId project,
        String sourceDataStream,
        CompiledDerivedMetrics compiled,
        ParsedDocument parsedDocument,
        boolean succeeded
    ) {
        record(project, sourceDataStream, compiled, parsedDocument, succeeded, null);
    }

    /**
     * @param strategies how each configured path can be read back from the already-parsed document, or null when that is not known or not
     *                   possible. Reading the parsed document avoids re-parsing {@code _source}, which is the great majority of what
     *                   observing a write costs; see {@link DerivedMetricsDocumentReader}.
     */
    public void record(
        ProjectId project,
        String sourceDataStream,
        CompiledDerivedMetrics compiled,
        ParsedDocument parsedDocument,
        boolean succeeded,
        @Nullable DerivedMetricsDocumentReader.Strategies strategies
    ) {
        Trigger trigger = succeeded ? Trigger.SUCCESS : Trigger.FAILURE;
        if (compiled.triggers().contains(trigger) == false) {
            return;
        }
        long now = threadPool.absoluteTimeInMillis();
        long documentSize = parsedDocument == null ? 0L : parsedDocument.source().estimatedSizeInBytes();
        RecordingScratch scratch = scratches.get();
        Object[] source = scratch.startDocument(compiled);
        boolean haveSource = false;
        if (compiled.needsSource() && parsedDocument != null) {
            // Prefer the document Elasticsearch has already parsed. It only serves when every configured path can be recovered exactly,
            // so a fallback is a performance difference and never a difference in what gets emitted.
            if (strategies != null && DerivedMetricsDocumentReader.read(parsedDocument, strategies, source)) {
                haveSource = true;
                documentsReadFromIndex.incrementAndGet();
            } else {
                if (strategies != null) {
                    scratch.clearSource();
                }
                haveSource = DerivedMetricsSourceReader.read(parsedDocument, compiled.sourcePaths(), source);
                documentsReadFromSource.incrementAndGet();
            }
        }

        for (CompiledMetric metric : compiled.metrics()) {
            if (metric.trigger() != trigger) {
                continue;
            }
            if (haveSource && metric.predicate().test(source) == false) {
                continue;
            }
            if (haveSource == false && metric.predicate() != DerivedMetricsPredicate.MATCH_ALL) {
                // the predicate needs a source we could not read, so the metric cannot be said to match
                skippedForUnreadableSource.incrementAndGet();
                continue;
            }
            double value = valueOf(metric, source, haveSource, documentSize);
            if (Double.isNaN(value)) {
                // The configured value field is absent or not a number. This is what a misspelled field name looks like, and left
                // uncounted it produces a metric that silently emits nothing forever with no signal anywhere.
                skippedForMissingValue.incrementAndGet();
                continue;
            }
            String[] values = scratch.dimensionValues(metric, source, haveSource);
            Interval interval = metric.interval();
            long bucketStart = DerivedMetricsBuffer.bucketStart(now, interval.millis());
            TableKey key = scratch.tableKey(project, sourceDataStream, metric, bucketStart, interval.millis());
            Outcome outcome = buffer.record(key, values, scratch.encoding, value);
            if (outcome.recorded() == false && memoryPressurePolicy == MemoryPressurePolicy.FLUSH_EARLY) {
                // Make room by emitting what is already collected, then take this observation rather than losing it. One retry only: if
                // the buffer still refuses after a drain the node is over its budget for reasons a second attempt will not change.
                //
                // The buffer counts a drop on each refusal, so a retry that succeeds has already been counted as a loss and a retry that
                // fails has been counted twice. Neither is true, so the retry's answer is used to put the count back.
                relievePressure(outcome, key);
                if (buffer.record(key, values, scratch.encoding, value).recorded()) {
                    recoveredAfterRelief.incrementAndGet();
                } else {
                    doubleCountedDrops.incrementAndGet();
                }
            }
        }
    }

    /**
     * Frees the bucket that just refused an observation, so the write path can carry on collecting into a fresh one.
     *
     * <p>Deliberately scoped to the one bucket. This runs on the indexing thread, inside the shard's operation permit — anything that
     * needs every permit, including relocation hand-off and shard close, waits behind it — so it must be bounded work rather than a walk
     * of every bucket the node holds. Draining one table is enough for the retry to succeed, which is the whole point of
     * {@code flush_early}; a wider drain would free memory this observation does not need. Building and sending the documents is handed
     * to the derived metrics pool.
     */
    private void relievePressure(Outcome outcome, TableKey key) {
        // Give up whichever bucket is holding the most rather than the one that happened to ask, so the node frees the memory actually
        // filling it. Scoped to the refusing stream when it was the per-stream cap that bit, because freeing another stream's memory
        // would not give this one any of its share back.
        Drained largest = switch (outcome) {
            case REFUSED_STREAM_CAP -> buffer.drainLargest(buffer.streamOf(key));
            case REFUSED_NODE_CAP, REFUSED_BREAKER -> buffer.drainLargest(null);
            case RECORDED -> throw new AssertionError("relieving pressure for an observation that was recorded");
        };
        // nothing could be given up, so fall back to the bucket that refused: it may still have room for another partial
        final Drained drained = largest != null ? largest : buffer.drainForPressure(key);
        if (drained == null) {
            return;
        }
        earlyFlushes.incrementAndGet();
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                emit(List.of(drained));
            }

            @Override
            public void onFailure(Exception e) {
                // emit closes every table it consumes and close is idempotent, so this only has to catch the tables it never reached
                drained.table().close();
                logger.warn("failed to emit derived metrics flushed early under memory pressure", e);
            }

            @Override
            public void onRejection(Exception e) {
                // the pool is bounded on purpose: shedding here is the designed behaviour, not a failure, but it must be counted
                droppedForBackpressure.addAndGet(drained.table().size());
                drained.table().close();
            }
        });
    }

    /**
     * Per-thread reusable state for the write path. The dimension buffer, the encoding buffer and the table key are all reused, so
     * recording an observation against a series that already exists allocates nothing at all — which matters because this runs on the
     * indexing thread for every document times every metric.
     */
    private static final class RecordingScratch {
        private final DerivedMetricsDimensionCodec.Scratch encoding = new DerivedMetricsDimensionCodec.Scratch();
        private TableKey key;

        /**
         * One row of resolved values per distinct dimension list, and the document generation each row was filled for. Metrics sharing a
         * dimension list therefore read {@code _source} once between them rather than once each.
         */
        private String[][] resolved = new String[0][];
        private int[] resolvedFor = new int[0];
        private int generation;
        private CompiledDerivedMetrics compiled;
        /** The document's values indexed by slot, reused across documents so extraction allocates nothing for the array itself. */
        private Object[] source = new Object[0];

        /**
         * Invalidates every cached row, because a new document is being observed. Sized against the configuration, and reset outright when
         * the configuration changes, since slot numbers only mean anything within one compilation and one thread serves many streams.
         */
        Object[] startDocument(CompiledDerivedMetrics compiled) {
            if (this.compiled != compiled) {
                this.compiled = compiled;
                resolved = new String[compiled.dimensionSets()][];
                resolvedFor = new int[compiled.dimensionSets()];
                source = new Object[compiled.sourcePaths().size()];
                generation = 0;
            }
            generation++;
            // the extractor only writes the paths the document actually has, so anything left from the previous document has to go
            java.util.Arrays.fill(source, null);
            return source;
        }

        /**
         * Discards whatever a partial read left behind, so that falling back to a source parse starts from the same blank slate a fresh
         * document would.
         */
        void clearSource() {
            java.util.Arrays.fill(source, null);
        }

        /**
         * The dimension values for one metric, resolved at most once per document per distinct dimension list. The returned array is
         * caller-read-only: the buffer encodes it and does not retain it.
         */
        String[] dimensionValues(CompiledMetric metric, Object[] source, boolean haveSource) {
            int set = metric.dimensionSet();
            int size = metric.dimensions().size();
            String[] values = resolved[set];
            if (values == null || values.length < size) {
                values = new String[size];
                resolved[set] = values;
            }
            if (resolvedFor[set] != generation) {
                resolvedFor[set] = generation;
                for (int i = 0; i < size; i++) {
                    values[i] = null;
                }
                resolveDimensions(metric.dimensionSlots(), source, haveSource, values);
            }
            return values;
        }

        /**
         * A table key changes only when the bucket rolls or the metric differs, so the previous one is reused whenever it still matches.
         */
        TableKey tableKey(ProjectId project, String sourceDataStream, CompiledMetric metric, long bucketStart, long intervalMillis) {
            TableKey previous = key;
            if (previous != null
                && previous.bucketStartMillis() == bucketStart
                && previous.metric() == metric
                && previous.sourceDataStream().equals(sourceDataStream)
                && previous.project().equals(project)) {
                return previous;
            }
            key = new TableKey(project, sourceDataStream, metric, bucketStart, intervalMillis);
            return key;
        }
    }

    /**
     * What this document contributes to the metric, or {@code NaN} when it contributes nothing — because the field is absent, is not
     * numeric, or the source could not be read. A sentinel rather than a {@code Double} because this runs once per metric per document and
     * boxing there is a real allocation on the indexing thread; a NaN observation would be meaningless anyway.
     */
    private static double valueOf(CompiledMetric metric, Object[] source, boolean haveSource, long documentSize) {
        return switch (metric.source()) {
            case CompiledDerivedMetrics.Source.Constant constant -> constant.value();
            case CompiledDerivedMetrics.Source.DocumentSize unused -> (double) documentSize;
            case CompiledDerivedMetrics.Source.Field field -> haveSource
                ? DerivedMetricsSourceReader.numericValue(source, field.slot())
                : Double.NaN;
        };
    }

    /**
     * Fills the caller's reusable array with the value of each configured dimension, leaving null where the document did not have one.
     * A document missing a dimension forms its own series rather than sharing a placeholder.
     */
    private static void resolveDimensions(int[] slots, Object[] source, boolean haveSource, String[] values) {
        if (slots.length == 0 || haveSource == false) {
            return;
        }
        for (int i = 0; i < slots.length; i++) {
            values[i] = DerivedMetricsSourceReader.stringValue(source, slots[i]);
        }
    }

    /**
     * Emits every bucket whose interval has closed. Intervals with no observations hold no buckets and therefore emit nothing, which is
     * what keeps a quiet stream from producing a steady trickle of zeroes.
     */
    void flush() {
        emit(buffer.drainClosed(threadPool.absoluteTimeInMillis(), graceMillis));
        reportDrops();
    }

    /**
     * Emits everything buffered, including intervals that are still open, because something is about to make this node stop observing
     * the writes those intervals cover.
     *
     * <p>This is the bounded-durability half of the contract. Nothing is persisted, so a hard kill still loses the open interval, but
     * every loss the node can see coming — a shard leaving, an orderly shutdown — is avoided rather than left to chance.
     */
    void flushEverything(String reason) {
        if (closed) {
            return;
        }
        List<Drained> drained = buffer.drainAll();
        if (drained.isEmpty()) {
            return;
        }
        logger.debug("flushing [{}] buffered derived metric series because {}", drained.size(), reason);
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                emit(drained);
            }

            @Override
            public void onFailure(Exception e) {
                drained.forEach(entry -> entry.table().close());
                logger.warn(() -> "failed to flush derived metrics because " + reason, e);
            }

            @Override
            public void onRejection(Exception e) {
                long lost = drained.stream().mapToLong(entry -> entry.table().size()).sum();
                lostSeries.addAndGet(lost);
                drained.forEach(entry -> entry.table().close());
                logger.warn("lost [{}] buffered derived metric series: the derived metrics pool was full while {}", lost, reason);
            }
        });
    }

    /**
     * Converts and sends in bulk-sized chunks rather than materialising every drained bucket first, so peak memory during a flush is one
     * bulk rather than the whole drained set. Every drained table is closed, which is what returns its memory to the circuit breaker.
     */
    private void emit(List<Drained> drained) {
        if (drained.isEmpty()) {
            return;
        }
        // Group by project only: a bulk can address any number of destinations, but it is scoped to one project.
        Map<ProjectId, BulkRequest> pending = new HashMap<>();
        BytesRef spare = new BytesRef();
        for (Drained entry : drained) {
            TableKey key = entry.key();
            DerivedMetricsSeriesTable table = entry.table();
            try {
                long series = table.size();
                for (long ordinal = 0; ordinal < series; ordinal++) {
                    ProjectId project = key.project();
                    BulkRequest bulk = pending.computeIfAbsent(project, unused -> new BulkRequest());
                    bulk.add(DerivedMetricsEmitter.toIndexRequest(key, table, ordinal, spare, nodeId, nodeName, entry.partial()));
                    if (bulk.numberOfActions() >= bulkSize) {
                        pending.remove(project);
                        send(project, bulk);
                    }
                }
            } finally {
                table.close();
            }
        }
        pending.forEach(this::send);
    }

    /**
     * Sends one bulk, keeping a ceiling on how many are outstanding. Emission is fire and forget, so without a ceiling a destination
     * that cannot keep up would let every flush add to an unbounded queue of in-flight requests.
     */
    private void send(ProjectId project, BulkRequest bulk) {
        int documents = bulk.numberOfActions();
        if (nodeIsUnderIndexingPressure()) {
            droppedForIndexingPressure.addAndGet(documents);
            return;
        }
        if (inFlightDocuments.addAndGet(documents) > maxInFlightDocuments) {
            inFlightDocuments.addAndGet(-documents);
            droppedForBackpressure.addAndGet(documents);
            return;
        }
        client.projectClient(project).bulk(bulk, ActionListener.runAfter(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                if (response.hasFailures() == false) {
                    return;
                }
                boolean reported = false;
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        emissionRejections.incrementAndGet();
                        if (reported == false) {
                            // one message is enough to diagnose a systematic problem, and the destination is managed so failures are not
                            // expected to be per-document — but every rejection is still counted, because this is where a duplicate _id
                            // would land and that is the failure the partial offset scheme exists to prevent
                            reported = true;
                            logger.warn(() -> "failed to write derived metrics to [" + item.getIndex() + "]", item.getFailure().getCause());
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                emissionFailures.addAndGet(documents);
                logger.warn(() -> "failed to write [" + documents + "] derived metric documents for project [" + project + "]", e);
            }
        }, () -> inFlightDocuments.addAndGet(-documents)));
    }

    /**
     * Whether the node is already spending enough of its indexing budget that derived metrics should get out of the way. Checked once per
     * bulk rather than per document, so the cost of reading the stats is irrelevant next to the bulk it guards.
     */
    private boolean nodeIsUnderIndexingPressure() {
        if (indexingPressure == null || indexingPressureCeiling >= 1.0) {
            return false;
        }
        IndexingPressureStats stats = indexingPressure.stats();
        long limit = stats.getMemoryLimit();
        return limit > 0 && stats.getCurrentCombinedCoordinatingAndPrimaryBytes() > limit * indexingPressureCeiling;
    }

    /**
     * Logs what has been shed since the last flush. The counters themselves are cumulative, because they are also published as metrics;
     * this only reports the delta so the logs stay readable.
     */
    private void reportDrops() {
        long shed = delta(droppedForBackpressure, reportedBackpressureDrops);
        if (shed > 0) {
            logger.warn(
                "derived metrics dropped [{}] documents because [{}] were already in flight; the destination is not keeping up",
                shed,
                maxInFlightDocuments
            );
        }
        long shedForPressure = delta(droppedForIndexingPressure, reportedIndexingPressureDrops);
        if (shedForPressure > 0) {
            logger.warn(
                "derived metrics dropped [{}] documents because the node was above [{}] of its indexing pressure budget; user writes take "
                    + "precedence, raise [{}] to change that",
                shedForPressure,
                indexingPressureCeiling,
                INDEXING_PRESSURE_CEILING.getKey()
            );
        }
        long early = delta(earlyFlushes, reportedEarlyFlushes);
        if (early > 0) {
            logger.warn(
                "derived metrics flushed [{}] times early because the buffer was full; the affected buckets are emitted as several "
                    + "partials, which costs documents but loses nothing. Reduce the configured dimensions, raise [{}], or set [{}] to "
                    + "[drop] to shed observations instead",
                early,
                MAX_SERIES_PER_NODE.getKey(),
                MEMORY_PRESSURE_POLICY.getKey()
            );
        }
        long dropped = buffer.droppedSeries();
        if (dropped > lastReportedDrops) {
            logger.warn(
                "derived metrics dropped [{}] observations because this node is already tracking [{}] series; "
                    + "reduce the configured dimensions or raise [{}]",
                dropped - lastReportedDrops,
                buffer.size(),
                MAX_SERIES_PER_NODE.getKey()
            );
            lastReportedDrops = dropped;
        }
    }

    /**
     * How much a cumulative counter has moved since it was last reported. The holder is a single-element array because these are only
     * touched from the flush thread and a field per counter would be four more fields.
     */
    private static long delta(AtomicLong counter, long[] lastReported) {
        long total = counter.get();
        long moved = total - lastReported[0];
        lastReported[0] = total;
        return moved;
    }

    /** Observations skipped because the metric's value field was absent or not numeric — what a misspelled field name looks like. */
    /** Documents read from the already-parsed document rather than by parsing {@code _source} again. */
    public long documentsReadFromIndex() {
        return documentsReadFromIndex.get();
    }

    /** Documents that fell back to a second parse of {@code _source}. */
    public long documentsReadFromSource() {
        return documentsReadFromSource.get();
    }

    public long skippedForMissingValue() {
        return skippedForMissingValue.get();
    }

    /** Documents the destination rejected, as opposed to whole requests that failed. */
    public long emissionRejections() {
        return emissionRejections.get();
    }

    /**
     * Observations the buffer refused and then accepted after an early flush. The buffer counted a drop for the first refusal that did
     * not turn out to be one, which is why the published drop total subtracts this.
     */
    public long recoveredAfterRelief() {
        return recoveredAfterRelief.get();
    }

    // visible for testing
    DerivedMetricsBuffer buffer() {
        return buffer;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Scheduler.Cancellable cancellable = scheduled;
        if (cancellable != null) {
            cancellable.cancel();
        }
        // By the time plugins are closed the cluster service, the indices service and the transport service are already down, so a bulk
        // sent from here cannot land. Rather than firing one and pretending, report what is being lost. The flushes on shard close and on
        // node shutdown are what make this set small; see flushEverything.
        List<Drained> lost = buffer.drainAll();
        long series = 0;
        for (Drained drained : lost) {
            series += drained.table().size();
            drained.table().close();
        }
        if (series > 0) {
            lostSeries.addAndGet(series);
            logger.warn(
                "lost [{}] buffered derived metric series that had not been flushed when the node shut down; this is the open interval, "
                    + "which is not persisted",
                series
            );
        }
        buffer.close();
        for (AutoCloseable metric : metrics) {
            try {
                metric.close();
            } catch (Exception e) {
                logger.debug("failed to deregister a derived metrics metric", e);
            }
        }
    }
}
