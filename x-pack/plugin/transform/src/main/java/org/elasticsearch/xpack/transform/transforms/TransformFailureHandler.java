/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import java.util.Optional;

import static org.elasticsearch.core.Strings.format;

class TransformFailureHandler {
    private static final Logger logger = LogManager.getLogger(TransformFailureHandler.class);

    private final TransformAuditor auditor;
    private final String jobId;
    private final TransformContext context;

    // protected for unit tests
    TransformFailureHandler(TransformAuditor auditor, TransformContext context, String jobId) {
        this.auditor = auditor;
        this.jobId = jobId;
        this.context = context;
    }

    void handleSearchOrIndexingFailure(Exception e, SettingsConfig settingsConfig) {
        // more detailed reporting in the handlers and below
        logger.debug(() -> "[" + jobId + "] transform encountered an exception: ", e);
        Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);

        if (unwrappedException instanceof CircuitBreakingException) {
            handleCircuitBreakingException((CircuitBreakingException) unwrappedException);
            return;
        }

        if (unwrappedException instanceof ScriptException) {
            handleScriptException((ScriptException) unwrappedException);
            return;
        }

        if (unwrappedException instanceof BulkIndexingException && ((BulkIndexingException) unwrappedException).isIrrecoverable()) {
            handleIrrecoverableBulkIndexingException((BulkIndexingException) unwrappedException);
            return;
        }

        // irrecoverable error without special handling
        if (unwrappedException instanceof ElasticsearchException elasticsearchException) {
            if (ExceptionRootCauseFinder.IRRECOVERABLE_REST_STATUSES.contains(elasticsearchException.status())) {
                failIndexer("task encountered irrecoverable failure: " + elasticsearchException.getDetailedMessage());
                return;
            }
        }

        if (unwrappedException instanceof IllegalArgumentException) {
            failIndexer("task encountered irrecoverable failure: " + e.getMessage());
            return;
        }

        handleRetry(unwrappedException, settingsConfig);
    }

    boolean handleStatePersistenceFailure(Exception e, SettingsConfig settingsConfig) {
        // we use the same setting for retries, however a separate counter, because the failure
        // counter for search/index gets reset after a successful bulk index request
        int numFailureRetries = Optional.ofNullable(settingsConfig.getNumFailureRetries()).orElse(context.getNumFailureRetries());

        final int failureCount = context.incrementAndGetStatePersistenceFailureCount();

        if (numFailureRetries != -1 && failureCount > numFailureRetries) {
            failIndexer(
                "task encountered more than " + numFailureRetries + " failures updating internal state; latest failure: " + e.getMessage()
            );
            return true;
        }
        return false;
    }

    /**
     * Handle the circuit breaking case: A search consumed too much memory and got aborted.
     * <p>
     * Going out of memory we smoothly reduce the page size which reduces memory consumption.
     * <p>
     * Implementation details: We take the values from the circuit breaker as a hint, but
     * note that it breaks early, that's why we also reduce using
     *
     * @param circuitBreakingException CircuitBreakingException thrown
     */
    private void handleCircuitBreakingException(CircuitBreakingException circuitBreakingException) {
        final int pageSize = context.getPageSize();
        double reducingFactor = Math.min(
            (double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
            1 - (Math.log10(pageSize) * 0.1)
        );

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < TransformIndexer.MINIMUM_PAGE_SIZE) {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            failIndexer(message);
        } else {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize, newPageSize);
            auditor.info(jobId, message);
            logger.info("[{}] {}", jobId, message);
            context.setPageSize(newPageSize);
        }
    }

    /**
     * Handle script exception case. This is error is irrecoverable.
     *
     * @param scriptException ScriptException thrown
     */
    private void handleScriptException(ScriptException scriptException) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_SCRIPT_ERROR,
            scriptException.getDetailedMessage(),
            scriptException.getScriptStack()
        );
        failIndexer(message);
    }

    /**
     * Handle permanent bulk indexing exception case. This is error is irrecoverable.
     *
     * @param bulkIndexingException BulkIndexingException thrown
     */
    private void handleIrrecoverableBulkIndexingException(BulkIndexingException bulkIndexingException) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_IRRECOVERABLE_BULK_INDEXING_ERROR,
            bulkIndexingException.getDetailedMessage()
        );
        failIndexer(message);
    }

    private void handleRetry(Throwable unwrappedException, SettingsConfig settingsConfig) {
        int numFailureRetries = Optional.ofNullable(settingsConfig.getNumFailureRetries()).orElse(context.getNumFailureRetries());

        // group failures to decide whether to report it below
        final String thisFailureClass = unwrappedException.getClass().toString();
        final String lastFailureClass = context.getLastFailure();
        final int failureCount = context.incrementAndGetFailureCount(thisFailureClass);

        if (numFailureRetries != -1 && failureCount > numFailureRetries) {
            failIndexer(
                "task encountered more than "
                    + numFailureRetries
                    + " failures; latest failure: "
                    + ExceptionRootCauseFinder.getDetailedMessage(unwrappedException)
            );
            return;
        }

        // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
        // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
        // and if the number of retries is about to exceed
        if (thisFailureClass.equals(lastFailureClass) == false || failureCount == numFailureRetries) {
            String message = format(
                "Transform encountered an exception: [%s]; Will automatically retry [%d/%d]",
                ExceptionRootCauseFinder.getDetailedMessage(unwrappedException),
                failureCount,
                numFailureRetries
            );
            logger.warn(() -> "[" + jobId + "] " + message);
            auditor.warning(jobId, message);
        }
    }

    protected void failIndexer(String failureMessage) {
        // note: logging and audit is done as part of context.markAsFailed
        context.markAsFailed(failureMessage);
    }
}
