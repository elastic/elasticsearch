/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import java.util.Locale;

/**
 * Interface for an object that can be incremented, breaking after some
 * configured limit has been reached.
 */
public interface CircuitBreaker {

    /**
     * The parent breaker is a sum of all the following breakers combined. With
     * this we allow a single breaker to have a significant amount of memory
     * available while still having a "total" limit for all breakers. Note that
     * it's not a "real" breaker in that it cannot be added to or subtracted
     * from by itself.
     */
    String PARENT = "parent";
    /**
     * The fielddata breaker tracks data used for fielddata (on fields) as well
     * as the id cached used for parent/child queries.
     */
    String FIELDDATA = "fielddata";
    /**
     * The request breaker tracks memory used for particular requests. This
     * includes allocations for things like the cardinality aggregation, and
     * accounting for the number of buckets used in an aggregation request.
     * Generally the amounts added to this breaker are released after a request
     * is finished.
     */
    String REQUEST = "request";
    /**
     * The in-flight request breaker tracks bytes allocated for reading requests
     * on the network layer.
     */
    String IN_FLIGHT_REQUESTS = "inflight_requests";

    enum Type {
        // A regular or ChildMemoryCircuitBreaker
        MEMORY,
        // A special parent-type for the hierarchy breaker service
        PARENT,
        // A breaker where every action is a noop, it never breaks
        NOOP;

        public static Type parseValue(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "noop" -> Type.NOOP;
                case "parent" -> Type.PARENT;
                case "memory" -> Type.MEMORY;
                default -> throw new IllegalArgumentException("No CircuitBreaker with type: " + value);
            };
        }
    }

    enum Durability {
        // The condition that tripped the circuit breaker fixes itself eventually.
        TRANSIENT,
        // The condition that tripped the circuit breaker requires manual intervention.
        PERMANENT
    }

    /**
     * Trip the circuit breaker
     * @param fieldName name of the field responsible for tripping the breaker
     * @param bytesNeeded bytes asked for but unable to be allocated
     */
    void circuitBreak(String fieldName, long bytesNeeded);

    /**
     * Add bytes to the breaker and trip if the that puts breaker over the limit.
     * <p>
     * The {@code label} is used both as the human-readable identifier embedded in the resulting {@link CircuitBreakingException} and as
     * the {@code category} attribute on the {@code es.breaker.memory.reserved.total}, {@code es.breaker.trip.total} and
     * {@code es.breaker.memory.held} metrics. Callers should keep the label low-cardinality (avoid embedding field names, shard ids, or
     * other dynamic content) so those metrics stay aggregatable.
     *
     * @param bytes number of bytes to add
     * @param label thing requesting the bytes being added that is included in
     *              the exception if the breaker is tripped
     */
    void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException;

    /**
     * Add bytes to the circuit breaker without tripping. Releases (negative {@code bytes}) made through this overload are reported on the
     * {@code es.breaker.memory.held} gauge under {@code category="uncategorized"}. Prefer {@link #addWithoutBreaking(long, String)} when
     * the caller knows the label that was used at admit time, so the per-category gauge stays balanced.
     */
    void addWithoutBreaking(long bytes);

    /**
     * Category-aware variant of {@link #addWithoutBreaking(long)}. The {@code label} is used as the {@code category} attribute on the
     * {@code es.breaker.memory.held} gauge so that admit and release can balance out per sub-system.
     * <p>
     * The default implementation drops the label and delegates to {@link #addWithoutBreaking(long)}, which is safe for breakers that do
     * not maintain the held gauge (e.g. noop or test breakers).
     */
    default void addWithoutBreaking(long bytes, String label) {
        addWithoutBreaking(bytes);
    }

    /**
     * @return the currently used bytes the breaker is tracking
     */
    long getUsed();

    /**
     * @return maximum number of bytes the circuit breaker can track before tripping
     */
    long getLimit();

    /**
     * @return overhead of circuit breaker
     */
    double getOverhead();

    /**
     * @return the number of times the circuit breaker has been tripped
     */
    long getTrippedCount();

    /**
     * @return the name of the breaker
     */
    String getName();

    /**
     * @return whether a tripped circuit breaker will reset itself (transient) or requires manual intervention (permanent).
     */
    Durability getDurability();

    /**
     * sets the new limit and overhead values for the circuit breaker.
     * The resulting write should be readable by other threads.
     * @param limit the desired limit
     * @param overhead the desired overhead constant
     */
    void setLimitAndOverhead(long limit, double overhead);
}
