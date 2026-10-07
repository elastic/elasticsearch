/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Maps a failure of an external-source query (or of a discovery attempt) to the closed {@code error_type} vocabulary
 * ({@link DataSourceUsageAccumulator#ERROR_TYPE_NAMES}) and to an HTTP status, so on-call can tell <em>why</em> an
 * external-source query failed rather than only <em>that</em> it did. The counterpart of
 * {@link ConfigChangeTelemetry#rejectedReason} for queries.
 * <p>
 * Classification is done on exception <em>types</em> and on {@link ExternalException#condition()}, never on messages,
 * and only closed tokens are ever returned: class names and messages are unbounded label values and are not stable.
 * A failure that none of the rules recognise, including a plain {@link IllegalArgumentException} (bad glob, bad
 * configuration, a limit being exceeded), is {@link DataSourceUsageAccumulator#ERROR_TYPE_OTHER}; the status still
 * tells a client error from a server error.
 */
public final class QueryFailureTelemetry {

    /**
     * The outcome of {@link #classify}: a closed {@code errorType} and the HTTP {@code status} code as a string, ready
     * to be used as metric attribute values.
     */
    public record Failure(String errorType, String status) {}

    private static final Logger logger = LogManager.getLogger(QueryFailureTelemetry.class);

    private QueryFailureTelemetry() {}

    /**
     * Classifies {@code failure}, looking through wrappers in its cause chain. The first rule that matches wins, from
     * the most specific cause to the generic fallback; the status is the one of the exception that matched (so a
     * typed failure hidden behind a wrapper still reports its own status), or of the unwrapped failure when nothing
     * matched. Never throws: the callers run on the failure path, ahead of the response listener, so a bug here must not
     * stop the query from reporting its own failure.
     */
    public static Failure classify(Throwable failure) {
        try {
            return doClassify(failure);
        } catch (RuntimeException e) {
            logger.debug("telemetry: failed to classify a query failure", e);
            return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_OTHER, String.valueOf(RestStatus.INTERNAL_SERVER_ERROR.getStatus()));
        }
    }

    private static Failure doClassify(Throwable failure) {
        Throwable breaker = ExceptionsHelper.unwrap(failure, CircuitBreakingException.class);
        if (breaker != null) {
            return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_CIRCUIT_BREAKER, status(breaker));
        }
        Throwable external = unwrapCause(failure, ExternalException::isExternalFailure);
        if (external != null) {
            return new Failure(errorType(ExternalException.conditionOf(external)), status(external));
        }
        Throwable rejected = ExceptionsHelper.unwrap(failure, EsRejectedExecutionException.class);
        if (rejected != null) {
            return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_RESOURCE_LIMIT, status(rejected));
        }
        // Only the engine's own timeout (e.g. an exchange sink that stayed inactive). The timeouts of the external-source code are
        // modelled elsewhere: a store or network timeout is a storage failure, and waiting for a concurrency or budget permit is a
        // resource limit. A raw java.util.concurrent.TimeoutException never reaches this point, and would carry no usable status.
        Throwable timeout = ExceptionsHelper.unwrap(failure, ElasticsearchTimeoutException.class);
        if (timeout != null) {
            return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_TIMEOUT, status(timeout));
        }
        Throwable analysis = ExceptionsHelper.unwrap(failure, EsqlClientException.class);
        if (analysis != null) {
            return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_VERIFICATION, status(analysis));
        }
        return new Failure(DataSourceUsageAccumulator.ERROR_TYPE_OTHER, status(ExceptionsHelper.unwrapCause(failure)));
    }

    /**
     * The first throwable in {@code failure}'s cause chain (itself included) that matches {@code predicate}, or {@code null}.
     * Suppressed exceptions are deliberately not searched, like every other rule here: the compute layer reports the failure
     * it prefers (a client error over a server error) and attaches the others as suppressed, so a failure that is only
     * suppressed is not the one the client sees, and labelling the query with it would disagree with the returned status.
     */
    private static Throwable unwrapCause(Throwable failure, Predicate<Throwable> predicate) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            if (predicate.test(t)) {
                return t;
            }
        }
        return null;
    }

    private static String status(Throwable t) {
        return String.valueOf(ExceptionsHelper.status(t).getStatus());
    }

    /**
     * The category of an external-source failure, from its {@link ExternalException.Condition} when it has one (the
     * switch is exhaustive on purpose, so a new condition forces a decision here), otherwise
     * {@link DataSourceUsageAccumulator#ERROR_TYPE_OTHER}.
     */
    static String errorType(ExternalException.Condition condition) {
        if (condition == null) {
            // Legacy free-text constructors carry no condition (ExternalFailures#classify builds them for raw I/O and
            // SDK failures); there is nothing reliable to tell why such a failure happened.
            return DataSourceUsageAccumulator.ERROR_TYPE_OTHER;
        }
        return switch (condition) {
            case ACCESS_DENIED, CREDENTIALS_EXPIRED, CLOCK_SKEW -> DataSourceUsageAccumulator.ERROR_TYPE_STORAGE_AUTH;
            case OBJECT_NOT_FOUND, OBJECT_ARCHIVED -> DataSourceUsageAccumulator.ERROR_TYPE_STORAGE_NOT_FOUND;
            case STORE_THROTTLED -> DataSourceUsageAccumulator.ERROR_TYPE_STORAGE_THROTTLED;
            case STORE_UNAVAILABLE, OBJECT_CHANGED -> DataSourceUsageAccumulator.ERROR_TYPE_STORAGE_UNAVAILABLE;
            case MALFORMED_DATA -> DataSourceUsageAccumulator.ERROR_TYPE_FORMAT;
            case LOCAL_CAPACITY -> DataSourceUsageAccumulator.ERROR_TYPE_RESOURCE_LIMIT;
            case METADATA_UNAVAILABLE, LISTING_FAILED -> DataSourceUsageAccumulator.ERROR_TYPE_DISCOVERY;
            case CLIENT_BUG -> DataSourceUsageAccumulator.ERROR_TYPE_OTHER;
        };
    }
}
