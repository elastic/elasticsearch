/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;

/**
 * A base {@link Exception}s for all exceptions thrown by routing related operations.
 */
public class RoutingException extends ElasticsearchException {

    public RoutingException(String message, Throwable cause) {
        super(message, cause);
    }

    public RoutingException(StreamInput in) throws IOException {
        super(in);
    }
}
