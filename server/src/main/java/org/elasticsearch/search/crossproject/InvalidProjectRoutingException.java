/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public final class InvalidProjectRoutingException extends ElasticsearchException {

    public static final TransportVersion INVALID_PROJECT_ROUTING_EXCEPTION_VERSION = TransportVersion.fromName(
        "invalid_project_routing_exception"
    );

    public InvalidProjectRoutingException(String msg, Object... args) {
        super(msg, args);
    }

    public InvalidProjectRoutingException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public InvalidProjectRoutingException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
