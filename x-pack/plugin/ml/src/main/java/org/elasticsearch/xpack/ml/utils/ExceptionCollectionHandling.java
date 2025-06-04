/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by generative AI
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class ExceptionCollectionHandling {

    /**
     * Given an array of exceptions, return a single ElasticsearchStatusException.
     * Return the first exception if all exceptions have 4XX status.
     * Otherwise, return a generic 500 error.
     *
     * @param  failures must not be empty or null
     * @param  message the message to use for the ElasticsearchStatusException
     */
    public static ElasticsearchStatusException exceptionArrayToStatusException(AtomicArray<Exception> failures, String message) {

        List<Exception> caughtExceptions = failures.asList();
        if (caughtExceptions.isEmpty()) {
            assert false : "method to combine exceptions called with no exceptions";
            return new ElasticsearchStatusException("No exceptions caught", RestStatus.INTERNAL_SERVER_ERROR);
        } else {

            boolean allElasticsearchException = true;
            boolean allStatus4xx = true;

            for (Exception exception : caughtExceptions) {
                if (exception instanceof ElasticsearchException elasticsearchException) {
                    if (elasticsearchException.status().getStatus() < 400 || elasticsearchException.status().getStatus() >= 500) {
                        allStatus4xx = false;
                    }
                } else {
                    allElasticsearchException = false;
                    break;
                }
            }

            if (allElasticsearchException && allStatus4xx) {
                return new ElasticsearchStatusException(
                    message,
                    ((ElasticsearchException) caughtExceptions.get(0)).status(),
                    caughtExceptions.get(0)
                );
            } else {
                return new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
            }

        }

    }
}
