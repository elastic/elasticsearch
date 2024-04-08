/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception which indicates that the PersistentTask node has not been assigned yet.
 */
public class PersistentTaskNodeNotAssignedException extends ElasticsearchException {

    public PersistentTaskNodeNotAssignedException(String persistentTaskName) {
        super("PersistentTask [{}] has not been yet assigned to a node on this cluster", persistentTaskName);
    }

    public PersistentTaskNodeNotAssignedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
