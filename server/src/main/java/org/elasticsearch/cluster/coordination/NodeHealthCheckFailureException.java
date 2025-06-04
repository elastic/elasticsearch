/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This exception is thrown if the File system is reported unhealthy by @{@link org.elasticsearch.monitor.fs.FsHealthService}
 * and this nodes needs to be removed from the cluster
 */

public class NodeHealthCheckFailureException extends ElasticsearchException {

    public NodeHealthCheckFailureException(String msg, Object... args) {
        super(msg, args);
    }

    public NodeHealthCheckFailureException(StreamInput in) throws IOException {
        super(in);
    }
}
