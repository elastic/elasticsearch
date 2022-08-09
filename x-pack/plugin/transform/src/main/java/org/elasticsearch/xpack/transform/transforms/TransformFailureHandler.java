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
    public static final int LOG_FAILURE_EVERY = 10;
    private final TransformAuditor auditor;
    private final String jobId;
    private final TransformContext context;

    // protected for unit tests
    TransformFailureHandler(TransformAuditor auditor, TransformContext context, String jobId) {
        this.auditor = auditor;
        this.jobId = jobId;
        this.context = context;
    }

    void handleIndexerFailure(Exception e, SettingsConfig settingsConfig) {
        // more detailed reporting in the handlers and below
        logger.debug(() -> "[" + jobId + "] transform encountered an exception: ", e);
        Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);
        boolean unattended = Boolean.TRUE.equals(settingsConfig.getUnattended());

        if (unwrappedException instanceof CircuitBreakingException circuitBreakingException) {
            handleCircuitBreakingException(circuitBreakingException, unattended);
        } else if (unwrappedException instanceof ScriptException scriptException) {
            handleScriptException(scriptException, unattended);
        } else if (unwrappedException instanceof BulkIndexingException bulkIndexingException && bulkIndexingException.isIrrecoverable()) {
            handleIrrecoverableBulkIndexingException(bulkIndexingException, unattended);
        } else if (unwrappedException instanceof ElasticsearchException elasticsearchException) {
            handleElasticsearchException(elasticsearchException, unattended, getNumFailureRetries(settingsConfig));
        } else if (unwrappedException instanceof IllegalArgumentException illegalArgumentException) {
            handleIllegalArgumentException(illegalArgumentException, unattended);
        } else {
            retry(
                unwrappedException,
                ExceptionRootCauseFinder.getDetailedMessage(unwrappedException),
                unattended,
                getNumFailureRetries(settingsConfig)
            );
        }
    }

    boolean handleStatePersistenceFailure(Exception e, SettingsConfig settingsConfig) {
        // we use the same setting for retries, however a separate counter, because the failure
        // counter for search/index gets reset after a successful bulk index request
        int numFailureRetries = getNumFailureRetries(settingsConfig);

        final int failureCount = context.incrementAndGetStatePersistenceFailureCount();

        if (numFailureRetries != -1 && failureCount > numFailureRetries) {
            fail(
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
                fail(message);
            }
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
    private void handleScriptException(ScriptException scriptException, boolean unattended) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_SCRIPT_ERROR,
            scriptException.getDetailedMessage(),
            scriptException.getScriptStack()
        );
        if (unattended) {
            retry(scriptException, message, true, -1);
        } else {
            fail(message);
        }
    }

    /**
     * Handle permanent bulk indexing exception case. This is error is irrecoverable.
     *
     * @param bulkIndexingException BulkIndexingException thrown
     */
    private void handleIrrecoverableBulkIndexingException(BulkIndexingException bulkIndexingException, boolean unattended) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_IRRECOVERABLE_BULK_INDEXING_ERROR,
            bulkIndexingException.getDetailedMessage()
        );
        if (unattended) {
            retry(bulkIndexingException, message, true, -1);
        } else {
            fail(message);
        }
    }

    private void handleElasticsearchException(ElasticsearchException elasticsearchException, boolean unattended, int numFailureRetries) {
        if (unattended == false && ExceptionRootCauseFinder.IRRECOVERABLE_REST_STATUSES.contains(elasticsearchException.status())) {
            String message = "task encountered irrecoverable failure: " + elasticsearchException.getDetailedMessage();
            fail(message);
        } else {
            retry(elasticsearchException, elasticsearchException.getDetailedMessage(), unattended, numFailureRetries);
        }
    }

    private void handleIllegalArgumentException(IllegalArgumentException illegalArgumentException, boolean unattended) {
        if (unattended) {
            retry(illegalArgumentException, illegalArgumentException.getMessage(), true, -1);
        } else {
            String message = "task encountered irrecoverable failure: " + illegalArgumentException.getMessage();
            fail(message);
        }
    }

    private void retry(Throwable unwrappedException, String message, boolean unattended, int numFailureRetries) {
        // group failures to decide whether to report it below
        final String thisFailureClass = unwrappedException.getClass().toString();
        final String lastFailureClass = context.getLastFailure();
        final int failureCount = context.incrementAndGetFailureCount(thisFailureClass);

        if (unattended == false && numFailureRetries != -1 && failureCount > numFailureRetries) {
            fail("task encountered more than " + numFailureRetries + " failures; latest failure: " + message);
            return;
        }

        // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
        // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
        // and if the number of retries is about to exceed
        if (thisFailureClass.equals(lastFailureClass) == false
            || failureCount % LOG_FAILURE_EVERY == 0
            || failureCount == numFailureRetries) {
            String retryMessage = format(
                "Transform encountered an exception: [%s]; Will automatically retry [%d/%d]",
                message,
                failureCount,
                numFailureRetries
            );
            logger.warn(() -> "[" + jobId + "] " + retryMessage);
            auditor.warning(jobId, retryMessage);
        }
    }

    private void fail(String failureMessage) {
        // note: logging and audit is done as part of context.markAsFailed
        context.markAsFailed(failureMessage);
    }

    private int getNumFailureRetries(SettingsConfig settingsConfig) {
        return Boolean.TRUE.equals(settingsConfig.getUnattended())
            ? -1
            : Optional.ofNullable(settingsConfig.getNumFailureRetries()).orElse(context.getNumFailureRetries());
    }
}
