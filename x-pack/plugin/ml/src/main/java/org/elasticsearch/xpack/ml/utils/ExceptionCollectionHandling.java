/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by generative AI
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class ExceptionCollectionHandling {

    public static ElasticsearchStatusException exceptionArrayToStatusException(AtomicArray<Exception> failures, String message) {

        List<Exception> caughtExceptions = failures.asList();
        if (caughtExceptions.isEmpty()) {
            throw new ElasticsearchStatusException("No exceptions caught", RestStatus.INTERNAL_SERVER_ERROR);
        } else {

            boolean allElasticsearchStatusException = true;
            boolean allElasticsearchStatusException4xx = true;
            boolean allSameCode = true;
            int firstCode = -1;

            for (Exception exception : caughtExceptions) {
                if (exception instanceof ElasticsearchStatusException == false) {
                    allElasticsearchStatusException = false;
                    break;
                }
                ElasticsearchStatusException elasticsearchStatusException = (ElasticsearchStatusException) exception;
                if (firstCode == -1) {
                    firstCode = elasticsearchStatusException.status().getStatus();
                } else if (false == (firstCode == elasticsearchStatusException.status().getStatus())) {
                    allSameCode = false;
                }

                if (elasticsearchStatusException.status().getStatus() < 400 || elasticsearchStatusException.status().getStatus() >= 500) {
                    allElasticsearchStatusException4xx = false;
                }
            }

            assert allElasticsearchStatusException; // TODO Remove this

            if (allElasticsearchStatusException && allElasticsearchStatusException4xx) {
                if (allSameCode) {
                    return new ElasticsearchStatusException(
                        message,
                        ((ElasticsearchStatusException) caughtExceptions.get(0)).status(),
                        caughtExceptions.get(0)
                    );
                } else {
                    return new ElasticsearchStatusException(message, RestStatus.REQUEST_TIMEOUT);
                }
            } else {
                return new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
            }

        }

    }
}
