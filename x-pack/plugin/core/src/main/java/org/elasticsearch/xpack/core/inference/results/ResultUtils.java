/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class ResultUtils {

    public static ElasticsearchStatusException createInvalidChunkedResultException(String receivedResultName) {
        return new ElasticsearchStatusException(
            "Expected a chunked inference [{}] received [{}]",
            RestStatus.INTERNAL_SERVER_ERROR,
            ChunkedTextEmbeddingResults.NAME,
            receivedResultName
        );
    }

    private ResultUtils() {}
}
