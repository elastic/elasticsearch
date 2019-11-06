/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;

/**
 * Set of static utils to find the cause of a search exception.
 */
public final class SearchExceptionRootCauseFinder {

    /**
     * Unwrap the Exceptionstack and return the most likely cause.
     *
     * @param e raw Exception
     * @return unwrapped elasticsearch exception
     */
    public static ElasticsearchException getRootCauseElasticsearchException(Exception e) {
        // circuit breaking exceptions are at the bottom
        Throwable unwrappedThrowable = org.elasticsearch.ExceptionsHelper.unwrapCause(e);

        if (unwrappedThrowable instanceof SearchPhaseExecutionException) {
            SearchPhaseExecutionException searchPhaseException = (SearchPhaseExecutionException) e;
            for (ShardSearchFailure shardFailure : searchPhaseException.shardFailures()) {
                Throwable unwrappedShardFailure = org.elasticsearch.ExceptionsHelper.unwrapCause(shardFailure.getCause());

                if (unwrappedShardFailure instanceof ElasticsearchException) {
                    return (ElasticsearchException) unwrappedShardFailure;
                }
            }
        } else if (unwrappedThrowable instanceof ElasticsearchException) {
            return (ElasticsearchException) unwrappedThrowable;
        }

        return null;
    }

    private SearchExceptionRootCauseFinder() {}

}
