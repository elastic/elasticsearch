/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;

/**
 * Breaker that will check a parent's when incrementing
 */
public class ChildMemoryCircuitBreaker implements CircuitBreaker {

    private volatile LimitAndOverhead limitAndOverhead;
    private final Durability durability;
    private final AtomicLong used;
    private final AtomicLong trippedCount;
    private final Logger logger;
    private final HierarchyCircuitBreakerService parent;
    private final String name;
    private final LongCounter trippedCountMeter;
    private final LongUpDownCounter memoryHeldMeter;
    private final Map<String, Object> uncategorizedHeldAttributes;
    private final Map<String, Map<String, Object>> categoryAttributeCache;

    /**
     * Attribute key identifying the breaker on the legacy {@link CircuitBreakerMetrics#ES_BREAKER_TRIP_COUNT_TOTAL} counter.
     * Retained as {@code "type"} for backwards compatibility with existing trip-count dashboards.
     */
    public static final String CIRCUIT_BREAKER_TYPE_ATTRIBUTE = "type";

    /**
     * Attribute key identifying the breaker on the memory gauges ({@code es.breaker.memory.*}). Equivalent in meaning to
     * {@link #CIRCUIT_BREAKER_TYPE_ATTRIBUTE} but namespaced as {@code "es_breaker_type"} to follow the newer metric naming convention.
     */
    public static final String BREAKER_METRIC_TYPE_ATTRIBUTE = "es_breaker_type";

    /**
     * Attribute key on the memory gauges identifying the (bounded) category a charge was admitted/released under. The value is
     * derived from the caller-supplied label via {@link #categoryFor(String)} and is always one of {@link #KNOWN_CATEGORIES} or
     * {@link #CATEGORY_UNCATEGORIZED}, so this dimension can never explode the metric's time-series cardinality.
     */
    public static final String CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE = "es_breaker_category";

    /**
     * Category value for any charge or release that does not map to one of the {@link #KNOWN_CATEGORIES}: the fallback for
     * unrecognized labels and the bucket used by the unlabeled {@link #addWithoutBreaking(long)} path.
     */
    public static final String CATEGORY_UNCATEGORIZED = "uncategorized";

    /** Per-phase retained query memory charged by {@link org.elasticsearch.index.query.AbstractQueryBuilder#toQuery}. */
    public static final String CATEGORY_QUERY = "query";

    /** Query-construction reservation for compiled wildcard automata. */
    public static final String CATEGORY_WILDCARD = "wildcard";

    /** Query-construction reservation for compiled regexp automata. */
    public static final String CATEGORY_REGEXP = "regexp";

    /** Range-query retained memory (label is {@code range:<field>}; the field suffix is stripped for the metric). */
    public static final String CATEGORY_RANGE = "range";

    /** Up-front reservation made by {@link PreallocatedCircuitBreakerService} (label is {@code preallocate[<detail>]}). */
    public static final String CATEGORY_PREALLOCATE = "preallocate";

    private static final Set<String> KNOWN_CATEGORIES = Set.of(
        CATEGORY_QUERY,
        CATEGORY_WILDCARD,
        CATEGORY_REGEXP,
        CATEGORY_RANGE,
        CATEGORY_PREALLOCATE
    );

    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. Uses the given oldBreaker to initialize
     * the starting offset.
     * @param metrics the metrics container used to report trip count and held-bytes metrics
     * @param settings settings to configure this breaker
     * @param parent parent circuit breaker service to delegate tripped breakers to
     * @param name the name of the breaker
     */
    public ChildMemoryCircuitBreaker(
        CircuitBreakerMetrics metrics,
        BreakerSettings settings,
        Logger logger,
        HierarchyCircuitBreakerService parent,
        String name
    ) {
        this.name = name;
        this.limitAndOverhead = new LimitAndOverhead(settings.getLimit(), settings.getOverhead());
        this.durability = settings.getDurability();
        this.used = new AtomicLong(0);
        this.trippedCount = new AtomicLong(0);
        this.logger = logger;
        logger.trace(() -> format("creating ChildCircuitBreaker with settings %s", settings));
        this.parent = parent;
        this.trippedCountMeter = metrics.getTripCount();
        this.memoryHeldMeter = metrics.getMemoryHeld();
        this.uncategorizedHeldAttributes = Map.of(
            BREAKER_METRIC_TYPE_ATTRIBUTE,
            this.name,
            CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE,
            CATEGORY_UNCATEGORIZED
        );
        final Map<String, Map<String, Object>> cache = new HashMap<>(KNOWN_CATEGORIES.size() + 1);
        for (String category : KNOWN_CATEGORIES) {
            cache.put(category, Map.of(BREAKER_METRIC_TYPE_ATTRIBUTE, this.name, CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE, category));
        }
        cache.put(CATEGORY_UNCATEGORIZED, this.uncategorizedHeldAttributes);
        this.categoryAttributeCache = Map.copyOf(cache);
    }

    /**
     * Backwards-compatible constructor preserving the pre-telemetry-overhaul signature for out-of-tree callers that only have a
     * trip {@link LongCounter}.
     */
    @Deprecated(forRemoval = true)
    public ChildMemoryCircuitBreaker(
        LongCounter trippedCountMeter,
        BreakerSettings settings,
        Logger logger,
        HierarchyCircuitBreakerService parent,
        String name
    ) {
        this(CircuitBreakerMetrics.fromTripCount(trippedCountMeter), settings, logger, parent, name);
    }

    /**
     * Method used to trip the breaker, delegates to the parent to determine whether to trip the breaker or not.
     */
    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        final long memoryBytesLimit = this.limitAndOverhead.limit;
        this.trippedCount.incrementAndGet();
        this.trippedCountMeter.incrementBy(1L, Map.of(CIRCUIT_BREAKER_TYPE_ATTRIBUTE, this.name));
        final String message = "["
            + this.name
            + "] Data too large, data for ["
            + fieldName
            + "]"
            + " would be ["
            + bytesNeeded
            + "/"
            + ByteSizeValue.ofBytes(bytesNeeded)
            + "]"
            + ", which is larger than the limit of ["
            + memoryBytesLimit
            + "/"
            + ByteSizeValue.ofBytes(memoryBytesLimit)
            + "]; for more information, see "
            + ReferenceDocs.CIRCUIT_BREAKER_ERRORS;
        logger.debug(() -> format("%s", message));
        throw new CircuitBreakingException(message, bytesNeeded, memoryBytesLimit, durability);
    }

    /**
     * Add a number of bytes, tripping the circuit breaker if the aggregated estimates are above the limit. Automatically trips the breaker
     * if the memory limit is set to 0. Will never trip the breaker if the limit is set to -1, but can still be used to aggregate
     * estimations.
     * <p>
     * Only positive {@code bytes} values are recorded on the {@code es.breaker.memory.held.usage} gauge. Callers that need to release
     * memory and keep the gauge balanced must use {@link #addWithoutBreaking(long, String)} with the same label as the original admit.
     *
     * @param bytes number of bytes to add to the breaker
     */
    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        final LimitAndOverhead limitAndOverhead = this.limitAndOverhead;
        final long memoryBytesLimit = limitAndOverhead.limit;
        final double overheadConstant = limitAndOverhead.overhead;
        // short-circuit on no data allowed, immediately throwing an exception
        if (memoryBytesLimit == 0) {
            circuitBreak(label, bytes);
        }

        long newUsed;
        // If there is no limit (-1), we can optimize a bit by using
        // .addAndGet() instead of looping (because we don't have to check a
        // limit), which makes the RamAccountingTermsEnum case faster.
        if (memoryBytesLimit == -1) {
            newUsed = noLimit(bytes, label);
        } else {
            newUsed = limit(bytes, label, overheadConstant, memoryBytesLimit);
        }

        // Additionally, we need to check that we haven't exceeded the parent's limit
        try {
            parent.checkParentLimit((long) (bytes * overheadConstant), label);
        } catch (CircuitBreakingException e) {
            // If the parent breaker is tripped, this breaker has to be
            // adjusted back down because the allocation is "blocked" but the
            // breaker has already been incremented
            this.adjustUsedBytes(-bytes);
            throw e;
        }
        if (bytes > 0) {
            this.memoryHeldMeter.add(bytes, heldAttributes(label));
        }
        assert newUsed >= 0 : "Used bytes: [" + newUsed + "] must be >= 0";
    }

    private long noLimit(long bytes, String label) {
        long newUsed;
        newUsed = this.used.addAndGet(bytes);
        logger.trace(
            () -> format(
                "[%s] Adding [%s][%s] to used bytes [new used: [%s], limit: [-1b]]",
                this.name,
                ByteSizeValue.ofBytes(bytes),
                label,
                ByteSizeValue.ofBytes(newUsed)
            )
        );
        return newUsed;
    }

    private long limit(long bytes, String label, double overheadConstant, long memoryBytesLimit) {
        long newUsed;// Otherwise, check the addition and commit the addition, looping if
        // there are conflicts. May result in additional logging, but it's
        // trace logging and shouldn't be counted on for additions.
        long currentUsed;
        do {
            currentUsed = this.used.get();
            newUsed = currentUsed + bytes;
            long newUsedWithOverhead = (long) (newUsed * overheadConstant);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[{}] Adding [{}][{}] to used bytes [new used: [{}], limit: {} [{}], estimate: {} [{}]]",
                    this.name,
                    ByteSizeValue.ofBytes(bytes),
                    label,
                    ByteSizeValue.ofBytes(newUsed),
                    memoryBytesLimit,
                    ByteSizeValue.ofBytes(memoryBytesLimit),
                    newUsedWithOverhead,
                    ByteSizeValue.ofBytes(newUsedWithOverhead)
                );
            }
            if (memoryBytesLimit > 0 && newUsedWithOverhead > memoryBytesLimit) {
                logger.warn(
                    "[{}] New used memory {} [{}] for data of [{}] would be larger than configured breaker: {} [{}], breaking",
                    this.name,
                    newUsedWithOverhead,
                    ByteSizeValue.ofBytes(newUsedWithOverhead),
                    label,
                    memoryBytesLimit,
                    ByteSizeValue.ofBytes(memoryBytesLimit)
                );
                circuitBreak(label, newUsedWithOverhead);
            }
            // Attempt to set the new used value, but make sure it hasn't changed
            // underneath us, if it has, keep trying until we are able to set it
        } while (this.used.compareAndSet(currentUsed, newUsed) == false);
        return newUsed;
    }

    /**
     * Add an <b>exact</b> number of bytes, not checking for tripping the
     * circuit breaker. This bypasses the overheadConstant multiplication.
     *
     * Also does not check with the parent breaker to see if the parent limit
     * has been exceeded.
     * <p>
     * Updates {@code es.breaker.memory.held.usage} under {@code category="uncategorized"}.
     * Use {@link #addWithoutBreaking(long, String)} when the corresponding admit was labeled so that the per-category gauge stays balanced.
     *
     * @param bytes number of bytes to add to the breaker
     */
    @Override
    public void addWithoutBreaking(long bytes) {
        adjustUsedBytes(bytes);
        this.memoryHeldMeter.add(bytes, uncategorizedHeldAttributes);
    }

    /**
     * Category-aware variant of {@link #addWithoutBreaking(long)} - records the delta on {@code es.breaker.memory.held.usage}
     * under the bounded {@code es_breaker_category} that {@code label} maps to (see {@link #categoryFor(String)}), so that an
     * admit / release pair sharing the same label cancels out on the per-category gauge.
     */
    @Override
    public void addWithoutBreaking(long bytes, String label) {
        adjustUsedBytes(bytes);
        this.memoryHeldMeter.add(bytes, heldAttributes(label));
    }

    /**
     * Builds the {@code es.breaker.memory.held.usage} attributes for {@code label}, mapping the free-text label to a bounded
     * {@code es_breaker_category} via {@link #categoryFor(String)}. Returns a pre-computed cached map for every valid category
     * (including {@link #CATEGORY_UNCATEGORIZED}) so no allocation occurs on the hot path.
     */
    private Map<String, Object> heldAttributes(String label) {
        return categoryAttributeCache.get(categoryFor(label));
    }

    /**
     * Maps a free-text breaker label to one of the bounded {@link #KNOWN_CATEGORIES}, or {@link #CATEGORY_UNCATEGORIZED} when the
     * label is unrecognized or {@code null}. For composite labels of the form {@code <category>[<detail>]} (e.g.
     * {@code preallocate[aggregations]}) or {@code <category>:<detail>} (e.g. {@code range:my_field}) only the {@code <category>}
     * prefix - the text before the first {@code '['} or {@code ':'} - is matched, so the per-detail suffix adds no cardinality.
     */
    static String categoryFor(String label) {
        if (label == null) {
            return CATEGORY_UNCATEGORIZED;
        }
        final int separator = firstSeparator(label);
        final String base = separator >= 0 ? label.substring(0, separator) : label;
        return KNOWN_CATEGORIES.contains(base) ? base : CATEGORY_UNCATEGORIZED;
    }

    /** Index of the first {@code '['} or {@code ':'} in {@code label}, or {@code -1} when neither is present. */
    private static int firstSeparator(String label) {
        final int bracket = label.indexOf('[');
        final int colon = label.indexOf(':');
        if (bracket < 0) {
            return colon;
        }
        if (colon < 0) {
            return bracket;
        }
        return Math.min(bracket, colon);
    }

    void recordHeldDelta(long bytes, String label) {
        this.memoryHeldMeter.add(bytes, heldAttributes(label));
    }

    private void adjustUsedBytes(long bytes) {
        long u = used.addAndGet(bytes);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Adjusted breaker by [{}] bytes, now [{}]", this.name, bytes, u);
        }
        assert u >= 0 : "Used bytes: [" + u + "] must be >= 0";
    }

    /**
     * @return the number of aggregated "used" bytes so far
     */
    @Override
    public long getUsed() {
        return this.used.get();
    }

    /**
     * @return the number of bytes that can be added before the breaker trips
     */
    @Override
    public long getLimit() {
        return this.limitAndOverhead.limit;
    }

    /**
     * @return the constant multiplier the breaker uses for aggregations
     */
    @Override
    public double getOverhead() {
        return this.limitAndOverhead.overhead;
    }

    /**
     * @return the number of times the breaker has been tripped
     */
    @Override
    public long getTrippedCount() {
        return this.trippedCount.get();
    }

    /**
     * @return the name of the breaker
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * @return whether a tripped circuit breaker will reset itself (transient) or requires manual intervention (permanent).
     */
    @Override
    public Durability getDurability() {
        return this.durability;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        this.limitAndOverhead = new LimitAndOverhead(limit, overhead);
    }

    private static class LimitAndOverhead {

        private final long limit;
        private final double overhead;

        LimitAndOverhead(long limit, double overhead) {
            this.limit = limit;
            this.overhead = overhead;
        }
    }
}
