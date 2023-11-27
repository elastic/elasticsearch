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

    public static ElasticsearchStatusException exceptionCollectionToSingleWith429OrMatchingStatus(
        AtomicArray<Exception> failures,
        String message
    ) {

        List<Exception> caughtExceptions = failures.asList();
        if (caughtExceptions.isEmpty()) {
            throw new ElasticsearchStatusException("No exceptions caught", RestStatus.INTERNAL_SERVER_ERROR);
        } else {
            return new ElasticsearchStatusException(
                message,
                caughtExceptions.get(0) instanceof ElasticsearchStatusException // TODO check if getting zero is correct
                    ? ((ElasticsearchStatusException) caughtExceptions.get(0)).status()
                    : RestStatus.TOO_MANY_REQUESTS,
                caughtExceptions.get(0)
            );
        }

    }
}
