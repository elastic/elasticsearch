/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * Base class for bidirectional batch exchange handlers.
 * Contains common fields and exchange ID construction logic shared by
 * {@link BidirectionalBatchExchangeServer} and {@link BidirectionalBatchExchangeClient}.
 */
public abstract class BidirectionalBatchExchangeBase implements Releasable {

    protected final String sessionId;
    protected final ExchangeService exchangeService;
    protected final Executor executor;
    protected final int maxBufferSize;
    protected final TransportService transportService;
    protected final Task task;
    protected final Settings settings;

    /**
     * Logs an exchange failure at {@code nonCancellationLevel}, unless it is one of the expected, transient
     * conditions that are downgraded to keep genuine failures visible:
     * <ul>
     *     <li>Cancellations - expected teardown (for example the query reached its LIMIT and the exchange was
     *     closed early via a synthesized "client stopped" error, or the task was cancelled) - are logged at DEBUG.</li>
     *     <li>{@link IndexNotFoundException} - the lookup target index was not available on the routed node, for
     *     example during a rolling restart, or before the index has recovered/propagated - is logged at DEBUG. This
     *     is a transient, self-healing condition and the failure is still propagated to the coordinator, so logging
     *     it at a higher level only produces spurious noise (and, in Serverless, trips promotion quality-gate
     *     log-error-rate checks during routine node churn).</li>
     *     <li>{@link CircuitBreakingException} circuit breaker trips - expected backpressure (the client receives a
     *     429) rather than a bug - are logged at DEBUG so the client, server, and operator do not triple-log the same
     *     trip. Use {@link #logExchangeFailure(Logger, Level, boolean, Exception, String, Object...)} with
     *     {@code reportCircuitBreakerAtWarn} set to log them at WARN from a single layer.</li>
     * </ul>
     * Shared by the client, the server, and the operator driving them, so the caller supplies its own logger.
     *
     * @param logger               the logger to log to (the caller's own logger)
     * @param nonCancellationLevel the level to log at when the failure is not a cancellation or a missing index
     * @param failure              the failure that decides the log level (cancellations and missing index at DEBUG)
     * @param message              a parameterized log message template
     * @param params               the parameters for the message template; a trailing {@link Throwable} is logged with its stack trace
     */
    @SuppressLoggerChecks(reason = "safely delegates to logger with a caller-supplied message and params")
    public static void logExchangeFailure(Logger logger, Level nonCancellationLevel, Exception failure, String message, Object... params) {
        logExchangeFailure(logger, nonCancellationLevel, false, failure, message, params);
    }

    /**
     * @param reportCircuitBreakerAtWarn when true, circuit breaker trips are logged at WARN; otherwise DEBUG
     */
    @SuppressLoggerChecks(reason = "safely delegates to logger with a caller-supplied message and params")
    public static void logExchangeFailure(
        Logger logger,
        Level nonCancellationLevel,
        boolean reportCircuitBreakerAtWarn,
        Exception failure,
        String message,
        Object... params
    ) {
        if (failure != null
            && (ExceptionsHelper.isTaskCancelledException(failure)
                || ExceptionsHelper.unwrap(failure, IndexNotFoundException.class) != null)) {
            logger.debug(message, params);
        } else if (failure != null && ExceptionsHelper.unwrapCause(failure) instanceof CircuitBreakingException) {
            if (reportCircuitBreakerAtWarn) {
                logger.warn(message, params);
            } else {
                logger.debug(message, params);
            }
        } else {
            logger.log(nonCancellationLevel, message, params);
        }
    }

    /**
     * Constructs the client-to-server exchange ID from the session ID.
     */
    protected static String buildClientToServerId(String sessionId) {
        return sessionId + "/clientToServer";
    }

    /**
     * Constructs the server-to-client exchange ID from the session ID.
     */
    protected static String buildServerToClientId(String sessionId) {
        return sessionId + "/serverToClient";
    }

    /**
     * Base constructor for bidirectional batch exchange handlers.
     *
     * @param sessionId the session ID (used for logging and identification)
     */
    protected BidirectionalBatchExchangeBase(
        String sessionId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        Settings settings
    ) {
        this.sessionId = sessionId;
        this.exchangeService = exchangeService;
        this.executor = executor;
        this.maxBufferSize = maxBufferSize;
        this.transportService = transportService;
        this.task = task;
        this.settings = settings;
    }

    /**
     * Connects a remote sink to a source handler via transport.
     * This is a common pattern used by both server and client to establish
     * transport-based connections for bidirectional exchange.
     * <p>
     * Always uses failFast=true so the source handler aborts immediately on sink failure.
     * The caller collects the real error via the listener and an {@link org.elasticsearch.compute.EsqlRefCountingListener}
     * whose {@link org.elasticsearch.compute.operator.FailureCollector} picks it over the generic
     * {@link org.elasticsearch.tasks.TaskCancelledException} thrown by the aborted source.
     */
    protected void connectRemoteSink(
        DiscoveryNode node,
        String exchangeId,
        ExchangeSourceHandler sourceHandler,
        ActionListener<Void> listener,
        String errorMessagePrefix
    ) {
        try {
            Transport.Connection connection = transportService.getConnection(node);
            RemoteSink remoteSink = exchangeService.newRemoteSink(task, exchangeId, transportService, connection);
            int concurrentClients = ExchangeSourceHandler.getConcurrentClients(settings);
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, concurrentClients, listener);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to " + errorMessagePrefix + " for exchange [" + exchangeId + "]", e);
        }
    }
}
