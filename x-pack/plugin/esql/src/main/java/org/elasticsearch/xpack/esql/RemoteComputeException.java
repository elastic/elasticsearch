/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;

import java.util.Objects;

/**
 * Represents an error that occurred when starting compute on a remote node.
 * It allows capturing some context such as the cluster alias that encountered the error.
 */
public class RemoteComputeException extends ElasticsearchException {

    /**
     * @param clusterAlias Name of the cluster.
     * @param cause Error that was encountered.
     */
    public RemoteComputeException(String clusterAlias, Throwable cause) {
        super("Remote [" + clusterAlias + "] encountered an error", cause);
        Objects.requireNonNull(cause);
    }

    @Override
    public RestStatus status() {
        // This is similar to what we do in SearchPhaseExecutionException.
        return ExceptionsHelper.status(getCause());
    }
}
