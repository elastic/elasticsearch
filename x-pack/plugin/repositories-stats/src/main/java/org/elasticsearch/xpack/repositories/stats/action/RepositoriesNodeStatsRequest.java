/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

final class RepositoriesNodeStatsRequest extends TransportRequest {
    RepositoriesNodeStatsRequest() {}

    RepositoriesNodeStatsRequest(StreamInput in) throws IOException {
        super(in);
    }
}
