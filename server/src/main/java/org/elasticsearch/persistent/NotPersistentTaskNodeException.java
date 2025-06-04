/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception which indicates that an operation failed because the node stopped being the node on which the PersistentTask is allocated.
 */
public class NotPersistentTaskNodeException extends ElasticsearchException {

    public NotPersistentTaskNodeException(String nodeId, String persistentTaskName) {
        super("Node [{}] is not hosting PersistentTask [{}]", nodeId, persistentTaskName);
    }

    public NotPersistentTaskNodeException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
