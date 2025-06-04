/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Collection;
import java.util.Set;

/**
 * Set of static utils to find the cause of a search exception.
 */
public final class ExceptionRootCauseFinder {

    /**
     * List of rest statuses that we consider irrecoverable
     */
    static final Set<RestStatus> IRRECOVERABLE_REST_STATUSES = Set.of(
        RestStatus.GONE,
        RestStatus.NOT_IMPLEMENTED,
        RestStatus.NOT_FOUND,
        RestStatus.BAD_REQUEST,
        RestStatus.UNAUTHORIZED,
        RestStatus.FORBIDDEN,
        RestStatus.METHOD_NOT_ALLOWED,
        RestStatus.NOT_ACCEPTABLE
    );

    /**
     * Return the best error message possible given a already unwrapped exception.
     *
     * @param t the throwable
     * @return the message string of the given throwable
     */
    public static String getDetailedMessage(Throwable t) {
        if (t instanceof ElasticsearchException) {
            return ((ElasticsearchException) t).getDetailedMessage();
        }

        return t.getMessage();
    }

    /**
     * Return the first irrecoverableException from a collection of bulk responses if there are any.
     *
     * @param failures a collection of bulk item responses with failures
     * @return The first exception considered irrecoverable if there are any, null if no irrecoverable exception found
     */
    public static Throwable getFirstIrrecoverableExceptionFromBulkResponses(Collection<BulkItemResponse> failures) {
        for (BulkItemResponse failure : failures) {
            Throwable unwrappedThrowable = ExceptionsHelper.unwrapCause(failure.getFailure().getCause());
            if (unwrappedThrowable instanceof IllegalArgumentException) {
                return unwrappedThrowable;
            }

            if (unwrappedThrowable instanceof ElasticsearchException elasticsearchException) {
                if (isExceptionIrrecoverable(elasticsearchException) && isNotIndexNotFoundException(elasticsearchException)) {
                    return elasticsearchException;
                }
            }
        }

        return null;
    }

    /**
     * We can safely recover from IndexNotFoundExceptions on Bulk responses.
     * If the transform is running, the next checkpoint will recreate the index.
     * If the transform is not running, the next start request will recreate the index.
     */
    private static boolean isNotIndexNotFoundException(ElasticsearchException elasticsearchException) {
        return elasticsearchException instanceof IndexNotFoundException == false;
    }

    public static boolean isExceptionIrrecoverable(ElasticsearchException elasticsearchException) {
        if (IRRECOVERABLE_REST_STATUSES.contains(elasticsearchException.status())) {

            // Even if the status indicates the exception is irrecoverable, some exceptions
            // with these status are worth retrying on.

            // A TaskCancelledException occurs if a sub-action of a search encounters a circuit
            // breaker exception. In this case the overall search task is cancelled.
            if (elasticsearchException instanceof TaskCancelledException) {
                return false;
            }
            // We can safely retry SearchContextMissingException instead of failing the transform.
            if (elasticsearchException instanceof SearchContextMissingException) {
                return false;
            }
            return true;
        }
        return false;
    }

    private ExceptionRootCauseFinder() {}
}
