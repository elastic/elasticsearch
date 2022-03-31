/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Set of static utils to find the cause of a search exception.
 */
public final class ExceptionRootCauseFinder {

    /**
     * List of rest statuses that we consider irrecoverable
     */
    public static final Set<RestStatus> IRRECOVERABLE_REST_STATUSES = new HashSet<>(
        Arrays.asList(
            RestStatus.GONE,
            RestStatus.NOT_IMPLEMENTED,
            RestStatus.NOT_FOUND,
            RestStatus.BAD_REQUEST,
            RestStatus.UNAUTHORIZED,
            RestStatus.FORBIDDEN,
            RestStatus.METHOD_NOT_ALLOWED,
            RestStatus.NOT_ACCEPTABLE
        )
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
            Throwable unwrappedThrowable = org.elasticsearch.ExceptionsHelper.unwrapCause(failure.getFailure().getCause());
            if (unwrappedThrowable instanceof IllegalArgumentException) {
                return unwrappedThrowable;
            }

            if (unwrappedThrowable instanceof ElasticsearchException elasticsearchException) {
                if (IRRECOVERABLE_REST_STATUSES.contains(elasticsearchException.status())) {
                    return elasticsearchException;
                }
            }
        }

        return null;
    }

    private ExceptionRootCauseFinder() {}

}
