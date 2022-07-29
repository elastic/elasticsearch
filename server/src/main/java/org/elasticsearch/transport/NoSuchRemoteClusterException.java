/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * An exception that remote cluster is missing or
 * connectivity to the remote connection is failing
 */
public final class NoSuchRemoteClusterException extends ResourceNotFoundException {

    public NoSuchRemoteClusterException(String clusterName) {
        // No node available for cluster
        super("no such remote cluster: [" + clusterName + "]");
    }

    public NoSuchRemoteClusterException(StreamInput in) throws IOException {
        super(in);
    }

}
