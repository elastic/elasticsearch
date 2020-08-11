/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public final class RepositoriesMetricsRequest extends BaseNodesRequest<RepositoriesMetricsRequest> {
    public RepositoriesMetricsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public RepositoriesMetricsRequest(String... nodesIds) {
        super(nodesIds);
    }
}
