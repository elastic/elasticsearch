/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
     * The in-flight request breaker tracks bytes allocated for reading and
     * writing requests on the network layer.
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
     * @param bytes number of bytes to add
     * @param label thing requesting the bytes being added that is included in
     *              the exception if the breaker is tripped
     */
    void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException;

    /**
     * Add bytes to the circuit breaker without tripping.
     */
    void addWithoutBreaking(long bytes);

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
