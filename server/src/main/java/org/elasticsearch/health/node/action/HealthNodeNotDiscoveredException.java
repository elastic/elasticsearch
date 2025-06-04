/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception which indicates that no health node is selected in this cluster, aka the
 * health node persistent task is not assigned.
 */
public class HealthNodeNotDiscoveredException extends ElasticsearchException {

    public HealthNodeNotDiscoveredException() {
        super("No health node was discovered");
    }

    public HealthNodeNotDiscoveredException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
