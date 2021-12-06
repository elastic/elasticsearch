/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A generic exception that can be thrown by a task when it's cancelled by the task manager API
 */
public class TaskCancelledException extends ElasticsearchException {

    public TaskCancelledException(String msg) {
        super(msg);
    }

    public TaskCancelledException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        // Tasks are typically cancelled at the request of the client, so a 4xx status code is more accurate than the default of 500 (and
        // means we don't log every cancellation at WARN level). There's no perfect match for cancellation in the available status codes,
        // but (quoting RFC 7213) 400 Bad Request "indicates that the server cannot or will not process the request due to something that is
        // perceived to be a client error" which is broad enough to be acceptable here.
        return RestStatus.BAD_REQUEST;
    }
}
