/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportException;

import java.util.Set;

import static org.elasticsearch.ExceptionsHelper.status;

/**
 * Classifies exceptions as recoverable (transient, safe to retry) or irrecoverable (permanent, should not retry).
 *
 * Design: defaults to irrecoverable (explicit allowlist). Only exceptions that are known to be transient
 * infrastructure failures are classified as recoverable. Unknown exception types fail fast to ensure
 * clear diagnostics rather than masking real bugs with silent retries.
 *
 * Layers:
 *   Layer 1 - Data Plane: shard/search failures that resolve when shards recover
 *   Layer 2 - Control Plane: master election transients
 *   Layer 3 - Resource Plane: circuit breakers and thread pool saturation (conditional)
 *   Layer 4 - Transport Plane: connection and timeout failures
 */
public final class MlRecoverableErrorClassifier {

    private static final Set<RestStatus> IRRECOVERABLE_REST_STATUSES = Set.of(
        RestStatus.GONE,
        RestStatus.NOT_IMPLEMENTED,
        RestStatus.NOT_FOUND,
        RestStatus.BAD_REQUEST,
        RestStatus.UNAUTHORIZED,
        RestStatus.FORBIDDEN,
        RestStatus.METHOD_NOT_ALLOWED,
        RestStatus.NOT_ACCEPTABLE
    );

    private MlRecoverableErrorClassifier() {}

    /**
     * Returns {@code true} if the exception is a transient infrastructure failure that is safe to retry.
     * Returns {@code false} for any unknown exception type (irrecoverable by default).
     *
     * @param e the exception to classify; wrapping exceptions are unwrapped before classification
     * @return true if the error is recoverable via retry
     */
    public static boolean isRecoverable(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);

        // TaskCancelledException: the persistent task was removed from cluster state; retrying fights user intent
        if (cause instanceof TaskCancelledException) {
            return false;
        }

        // IllegalIndexShardStateException: shard in wrong state (e.g. RECOVERING) – transient; retry when shard is ready.
        // Overrides NOT_FOUND status; TransportActions treats it as shard-not-available (retryable).
        if (cause instanceof IllegalIndexShardStateException) {
            return true;
        }

        // Status-based irrecoverable check: covers all ElasticsearchStatusException variants with bad statuses
        RestStatus restStatus = status(cause);
        if (IRRECOVERABLE_REST_STATUSES.contains(restStatus)) {
            return false;
        }

        // Layer 1: Data Plane -- shard/search failures that resolve when shards recover
        if (cause instanceof SearchPhaseExecutionException) {
            return true;
        }
        if (cause instanceof NoShardAvailableActionException) {
            return true;
        }
        if (cause instanceof IndexPrimaryShardNotAllocatedException) {
            return true;
        }

        // Layer 2: Control Plane -- master election transients
        if (cause instanceof MasterNotDiscoveredException) {
            return true;
        }
        if (cause instanceof NotMasterException) {
            return true;
        }
        if (cause instanceof ClusterBlockException cbe) {
            return cbe.retryable();
        }

        // Layer 3: Resource Plane -- conditional checks
        if (cause instanceof CircuitBreakingException cbe) {
            return cbe.getDurability() == CircuitBreaker.Durability.TRANSIENT;
        }
        if (cause instanceof EsRejectedExecutionException ere) {
            return ere.isExecutorShutdown() == false;
        }
        if (cause instanceof VersionConflictEngineException) {
            return true;
        }

        // Layer 3 continued: TOO_MANY_REQUESTS covers circuit breakers and thread pool rejections
        // that reach us as generic ElasticsearchStatusException (e.g. from ProcessContext.tryLock)
        if (restStatus == RestStatus.TOO_MANY_REQUESTS) {
            return true;
        }

        // Layer 3 continued: SERVICE_UNAVAILABLE (503) is a transient cluster unavailability signal
        if (restStatus == RestStatus.SERVICE_UNAVAILABLE) {
            return true;
        }

        // Layer 4: Transport Plane -- all TransportException subclasses (connect, timeout, receive, send)
        if (cause instanceof TransportException) {
            return true;
        }
        if (cause instanceof NodeClosedException) {
            return true;
        }
        if (cause instanceof NoNodeAvailableException) {
            return true;
        }
        // ElasticsearchTimeoutException has no specific RestStatus; check by class hierarchy
        if (cause instanceof ElasticsearchTimeoutException) {
            return true;
        }

        // Default: irrecoverable. Unknown exception types fail fast for clear diagnostics.
        return false;
    }
}
