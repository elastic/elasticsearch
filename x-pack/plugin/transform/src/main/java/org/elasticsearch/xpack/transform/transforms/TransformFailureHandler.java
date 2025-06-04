/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.common.notifications.Level.INFO;
import static org.elasticsearch.xpack.core.common.notifications.Level.WARNING;

/**
 * Handles all failures a transform can run into when searching, indexing as well as internal
 * state handling.
 *
 * TODO:
 *
 *  - the settings have to be passed as parameter - because they can change at runtime - longer term the necessary
 *    parts should be read from the context object instead
 */
class TransformFailureHandler {
    private static final Logger logger = LogManager.getLogger(TransformFailureHandler.class);
    public static final int LOG_FAILURE_EVERY = 10;
    private final TransformAuditor auditor;
    private final String transformId;
    private final TransformContext context;

    TransformFailureHandler(TransformAuditor auditor, TransformContext context, String transformId) {
        this.auditor = auditor;
        this.transformId = transformId;
        this.context = context;
    }

    /**
     * Handle a search or indexing failure
     *
     * @param exception the exception caught
     * @param settingsConfig The settings
     */
    void handleIndexerFailure(Exception exception, SettingsConfig settingsConfig) {
        // more detailed reporting in the handlers and below
        logger.atDebug().withThrowable(exception).log("[{}] transform encountered an exception", transformId);
        Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(exception);
        boolean unattended = TransformEffectiveSettings.isUnattended(settingsConfig);
        int numFailureRetries = TransformEffectiveSettings.getNumFailureRetries(settingsConfig, context.getNumFailureRetries());

        if (unwrappedException instanceof CircuitBreakingException e) {
            handleCircuitBreakingException(e, unattended);
        } else if (unwrappedException instanceof ScriptException e) {
            handleScriptException(e, unattended);
        } else if (unwrappedException instanceof BulkIndexingException e) {
            handleBulkIndexingException(e, unattended, numFailureRetries);
        } else if (unwrappedException instanceof ClusterBlockException e) {
            // gh#89802 always retry for a cluster block exception, because a cluster block should be temporary.
            retry(e, e.getDetailedMessage(), unattended, numFailureRetries);
        } else if (unwrappedException instanceof SearchPhaseExecutionException e) {
            // The reason of a SearchPhaseExecutionException unfortunately contains a full stack trace.
            // Instead of displaying that to the user, get the cause's message instead.
            retry(e, e.getCause() != null ? e.getCause().getMessage() : null, unattended, numFailureRetries);
        } else if (unwrappedException instanceof ElasticsearchException e) {
            handleElasticsearchException(e, unattended, numFailureRetries);
        } else if (unwrappedException instanceof IllegalArgumentException e) {
            handleIllegalArgumentException(e, unattended);
        } else {
            retry(unwrappedException, ExceptionRootCauseFinder.getDetailedMessage(unwrappedException), unattended, numFailureRetries);
        }
    }

    /**
     * Handle failures persisting internal state
     *
     * @param e the exception caught
     * @param settingsConfig The settings
     * @return true if there is at least one more retry to be made, false otherwise
     */
    boolean handleStatePersistenceFailure(Exception e, SettingsConfig settingsConfig) {
        // we use the same setting for retries, however a separate counter, because the failure
        // counter for search/index gets reset after a successful bulk index request
        int numFailureRetries = TransformEffectiveSettings.getNumFailureRetries(settingsConfig, context.getNumFailureRetries());

        int failureCount = context.incrementAndGetStatePersistenceFailureCount(e);

        if (numFailureRetries != -1 && failureCount > numFailureRetries) {
            fail(
                e,
                "task encountered more than " + numFailureRetries + " failures updating internal state; latest failure: " + e.getMessage()
            );
            return false;
        }
        return true;
    }

    /**
     * Handle the circuit breaking case: A search consumed too much memory and got aborted.
     * <p>
     * Going out of memory we smoothly reduce the page size which reduces memory consumption.
     * <p>
     * Implementation details: We take the values from the circuit breaker as a hint and reduce
     * either based on the circuitbreaker value or a log-scale value.
     *
     * @param circuitBreakingException CircuitBreakingException thrown
     * @param unattended whether the transform runs in unattended mode
     */
    private void handleCircuitBreakingException(CircuitBreakingException circuitBreakingException, boolean unattended) {
        final int pageSize = context.getPageSize();
        double reducingFactor = Math.min(
            (double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
            1 - (Math.log10(pageSize) * 0.1)
        );

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < TransformIndexer.MINIMUM_PAGE_SIZE) {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            if (unattended) {
                retry(circuitBreakingException, message, true, -1);
            } else {
                fail(circuitBreakingException, message);
            }
        } else {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize, newPageSize);
            auditor.info(transformId, message);
            logger.info("[{}] {}", transformId, message);
            context.setPageSize(newPageSize);
        }
    }

    /**
     * Handle script exception case. This is error is irrecoverable.
     *
     * @param scriptException ScriptException thrown
     * @param unattended whether the transform runs in unattended mode
     */
    private void handleScriptException(ScriptException scriptException, boolean unattended) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_SCRIPT_ERROR,
            scriptException.getDetailedMessage(),
            scriptException.getScriptStack()
        );
        if (unattended) {
            retry(scriptException, message, true, -1);
        } else {
            fail(scriptException, message);
        }
    }

    /**
     * Handle bulk indexing exception case. This is error can be irrecoverable.
     *
     * @param bulkIndexingException BulkIndexingException thrown
     * @param unattended whether the transform runs in unattended mode
     * @param numFailureRetries the number of configured retries
     */
    private void handleBulkIndexingException(BulkIndexingException bulkIndexingException, boolean unattended, int numFailureRetries) {
        if (bulkIndexingException.getCause() instanceof ClusterBlockException) {
            context.setIsWaitingForIndexToUnblock(true);
            retryWithoutIncrementingFailureCount(
                bulkIndexingException,
                bulkIndexingException.getDetailedMessage(),
                unattended,
                numFailureRetries
            );
        } else if (unattended == false && bulkIndexingException.isIrrecoverable()) {
            String message = TransformMessages.getMessage(
                TransformMessages.LOG_TRANSFORM_PIVOT_IRRECOVERABLE_BULK_INDEXING_ERROR,
                bulkIndexingException.getDetailedMessage()
            );
            fail(bulkIndexingException, message);
        } else {
            retry(bulkIndexingException, bulkIndexingException.getDetailedMessage(), unattended, numFailureRetries);
        }
    }

    /**
     * Handle a generic elasticsearch exception. This is error can be irrecoverable.
     * <p>
     * The failure is classified using the http status code from the exception.
     *
     * @param elasticsearchException ElasticsearchException thrown
     * @param unattended whether the transform runs in unattended mode
     * @param numFailureRetries the number of configured retries
     */
    private void handleElasticsearchException(ElasticsearchException elasticsearchException, boolean unattended, int numFailureRetries) {
        if (unattended == false && ExceptionRootCauseFinder.isExceptionIrrecoverable(elasticsearchException)) {
            String message = "task encountered irrecoverable failure: " + elasticsearchException.getDetailedMessage();
            fail(elasticsearchException, message);
        } else {
            retry(elasticsearchException, elasticsearchException.getDetailedMessage(), unattended, numFailureRetries);
        }
    }

    /**
     * Handle a generic illegal argument exception. This is error is irrecoverable.
     * <p>
     * If this exception is caught it is likely a bug somewhere.
     *
     * @param illegalArgumentException IllegalArgumentException thrown
     * @param unattended whether the transform runs in unattended mode
     */
    private void handleIllegalArgumentException(IllegalArgumentException illegalArgumentException, boolean unattended) {
        if (unattended) {
            retry(illegalArgumentException, illegalArgumentException.getMessage(), true, -1);
        } else {
            String message = "task encountered irrecoverable failure: " + illegalArgumentException.getMessage();
            fail(illegalArgumentException, message);
        }
    }

    /**
     * Terminate failure handling with a retry.
     * <p>
     * In case the number of retries are exhausted - and the transform does not run as unattended - the transform
     * might be set to failed.
     *
     * @param unwrappedException The exception caught
     * @param message error message to log/audit
     * @param unattended whether the transform runs in unattended mode
     * @param numFailureRetries the number of configured retries
     */
    private void retry(Throwable unwrappedException, String message, boolean unattended, int numFailureRetries) {
        // group failures to decide whether to report it below
        final boolean repeatedFailure = context.getLastFailure() != null
            && unwrappedException.getClass().equals(context.getLastFailure().getClass());

        final int failureCount = context.incrementAndGetFailureCount(unwrappedException);
        if (unattended == false && numFailureRetries != -1 && failureCount > numFailureRetries) {
            fail(unwrappedException, "task encountered more than " + numFailureRetries + " failures; latest failure: " + message);
            return;
        }

        logRetry(unwrappedException, message, unattended, numFailureRetries, failureCount, repeatedFailure);
    }

    /**
     * Terminate failure handling without incrementing the retries used
     * <p>
     * This is used when there is an ongoing recoverable issue and we want to retain
     * retries for any issues that may occur after the issue is resolved
     *
     * @param unwrappedException The exception caught
     * @param message error message to log/audit
     * @param unattended whether the transform runs in unattended mode
     * @param numFailureRetries the number of configured retries
     */
    private void retryWithoutIncrementingFailureCount(
        Throwable unwrappedException,
        String message,
        boolean unattended,
        int numFailureRetries
    ) {
        // group failures to decide whether to report it below
        final boolean repeatedFailure = context.getLastFailure() != null
            && unwrappedException.getClass().equals(context.getLastFailure().getClass());

        logRetry(unwrappedException, message, unattended, numFailureRetries, context.getFailureCount(), repeatedFailure);
    }

    private void logRetry(
        Throwable unwrappedException,
        String message,
        boolean unattended,
        int numFailureRetries,
        int failureCount,
        boolean repeatedFailure
    ) {
        // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
        // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
        // and if the number of retries is about to exceed
        if (repeatedFailure == false || failureCount % LOG_FAILURE_EVERY == 0 || failureCount == numFailureRetries) {
            String retryMessage = format(
                "Transform encountered an exception: [%s]; Will automatically retry [%d/%d]",
                message,
                failureCount,
                numFailureRetries
            );

            logger.atLevel(unattended ? Level.INFO : Level.WARN)
                .withThrowable(unwrappedException)
                .log("[{}] {}", transformId, retryMessage);
            auditor.audit(unattended ? INFO : WARNING, transformId, retryMessage);
        }
    }

    /**
     * Terminate failure handling by failing the transform.
     * <p>
     * This should be called if the transform does not run unattended and the failure is permanent or after the
     * configured number of retries.
     *
     * @param failureMessage the reason of the failure
     */
    private void fail(Throwable exception, String failureMessage) {
        // note: logging and audit is done as part of context.markAsFailed
        context.markAsFailed(exception, failureMessage);
    }
}
