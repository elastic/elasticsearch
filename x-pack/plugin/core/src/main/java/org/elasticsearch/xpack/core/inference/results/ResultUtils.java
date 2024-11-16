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

    public static ElasticsearchStatusException createInvalidChunkedResultException(String expectedResultName, String receivedResultName) {
        return new ElasticsearchStatusException(
            "Received incompatible results. Check that your model_id matches the task_type of this endpoint. "
                + "Expected chunked output of type [{}] but received [{}].",
            RestStatus.CONFLICT,
            expectedResultName,
            receivedResultName
        );
    }

    private ResultUtils() {}
}
