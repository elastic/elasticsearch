/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.local;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A base request for actions that are executed locally on the node that receives the request.
 */
public abstract class LocalClusterStateRequest extends ActionRequest {

    /**
     * The timeout for waiting until the cluster is unblocked.
     * We use the name <code>masterTimeout</code> to be consistent with the master node actions.
     */
    private final TimeValue masterTimeout;

    protected LocalClusterStateRequest(TimeValue masterTimeout) {
        this.masterTimeout = Objects.requireNonNull(masterTimeout);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    public TimeValue masterTimeout() {
        return masterTimeout;
    }
}
