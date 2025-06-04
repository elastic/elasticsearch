/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an error that occurred on a remote node.
 * It allows capturing some context such as the cluster alias that encountered the error.
 */
public class RemoteException extends ElasticsearchException {

    /**
     * @param clusterAlias Name of the cluster.
     * @param cause Error that was encountered.
     */
    public RemoteException(String clusterAlias, Throwable cause) {
        super("Remote [" + clusterAlias + "] encountered an error", cause);
        Objects.requireNonNull(cause);
    }

    public RemoteException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        // This is similar to what we do in SearchPhaseExecutionException.
        return ExceptionsHelper.status(getCause());
    }
}
