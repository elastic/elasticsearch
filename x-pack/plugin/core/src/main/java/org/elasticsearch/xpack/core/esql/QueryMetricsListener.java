/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

import org.elasticsearch.ExceptionsHelper;

import java.util.List;
import java.util.Map;

/**
 * Called once per ES|QL query that read federated sources, on the coordinating node, with the {@link Outcome} of the query. At
 * the moment only the queries that succeeded are reported.
 * Implementations must be cheap and non-blocking — no I/O, no locks, only atomic operations.
 * <p>
 * The metrics collected are raw profile-based metrics, no computation is performed on them at collection time.
 * </p>
 * <p>
 * A listener that only implements {@link #onQueryCompleted(Map)} is called for the queries that succeeded and for no others, so
 * that work that was not delivered is not counted by a consumer that does not know about outcomes, such as billing.
 * A listener that wants every outcome implements {@link #onQueryCompleted(Outcome, Map)}, and implements
 * {@link #onQueryCompleted(Map)} as well, with nothing to do or an assertion that it is not called: it stays abstract so that
 * the interface remains a functional interface.
 * </p>
 */
public interface QueryMetricsListener {

    QueryMetricsListener NOOP = metrics -> {};

    /**
     * How a query ended.
     */
    enum Outcome {
        /** The query returned its results. */
        SUCCESS,
        /** The query failed, for any reason other than a cancellation. */
        FAILED,
        /** The query was cancelled: by the user, because its task was cancelled, or because it expired. */
        CANCELLED
    }

    /**
     * A listener that calls every listener in {@code listeners}, as it is when this is called, in turn, or {@link #NOOP} if there
     * are none. A listener that throws does not keep the ones after it from being called, which are not to be skipped because of
     * a listener they have nothing to do with, such as billing: the first exception is thrown at the end, with the others
     * suppressed.
     */
    static QueryMetricsListener composite(List<QueryMetricsListener> listeners) {
        if (listeners.isEmpty()) {
            return NOOP;
        }
        List<QueryMetricsListener> all = List.copyOf(listeners);
        return new QueryMetricsListener() {
            @Override
            public void onQueryCompleted(Map<String, Long> metrics) {
                onQueryCompleted(Outcome.SUCCESS, metrics);
            }

            @Override
            public void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
                RuntimeException failure = null;
                for (QueryMetricsListener listener : all) {
                    try {
                        listener.onQueryCompleted(outcome, metrics);
                    } catch (RuntimeException e) {
                        failure = ExceptionsHelper.useOrSuppress(failure, e);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        };
    }

    /**
     * Wall-clock time spent on planning, from queryProfile.planning(). Includes I/O and transport waits: object-store
     * listings and metadata reads, field caps, enrich and inference round trips.
     */
    String PLANNING_NANOS = "planning_nanos";
    /**
     * CPU time spent on planning (no I/O wait), from queryProfile.planningCpuNanos()
     */
    String PLANNING_CPU_NANOS = "planning_cpu_nanos";
    /**
     * Thread CPU time spent by drivers on compute, from result.completionInfo().cpuNanos(). Includes CPU time
     * operators meter on other threads for their driver, such as parallel aggregation workers and the connector producer
     * threads of {@code AsyncConnectorSourceOperatorFactory}. File-based format-reader producer threads are counted in
     * {@link #READ_CPU_NANOS} instead.
     */
    String CPU_NANOS = "cpu_nanos";
    /**
     * Time drivers held a thread, from result.completionInfo().driverNanos(). Unlike {@link #CPU_NANOS} this includes
     * time waiting for a core, GC pauses and time blocked inside an operator.
     */
    String DRIVER_NANOS = "driver_nanos";
    /**
     * Time spent by drivers on reading data, from result.completionInfo().readNanos()
     */
    String READ_NANOS = "read_nanos";
    /**
     * CPU time spent by drivers on reading data (no IO wait), from result.completionInfo().readCpuNanos()
     */
    String READ_CPU_NANOS = "read_cpu_nanos";
    /**
     * Time spent on discovering splits, from queryProfile.splitDiscoveryNanos()
     */
    String SPLIT_DISCOVERY_NANOS = "split_discovery_nanos";
    /**
     * CPU time spent discovering splits (no IO wait), from queryProfile.splitDiscoveryCpuNanos()
     */
    String SPLIT_DISCOVERY_CPU_NANOS = "split_discovery_cpu_nanos";
    /**
     * Bytes read by the query, both from external sources and Lucene.
     */
    String BYTES_READ = "bytes_read";

    /**
     * A query succeeded. Called by the default implementation of {@link #onQueryCompleted(Outcome, Map)}, and only for queries that
     * succeeded.
     */
    void onQueryCompleted(Map<String, Long> metrics);

    /**
     * A query ended with {@code outcome}. The default implementation passes the queries that succeeded to
     * {@link #onQueryCompleted(Map)} and ignores the others.
     */
    default void onQueryCompleted(Outcome outcome, Map<String, Long> metrics) {
        if (outcome == Outcome.SUCCESS) {
            onQueryCompleted(metrics);
        }
    }
}
