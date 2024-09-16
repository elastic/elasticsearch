/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get cluster level stats from the remote cluster.
 */
public class RemoteClusterStatsRequest extends ActionRequest {
    private final String[] nodesIds;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public RemoteClusterStatsRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    public RemoteClusterStatsRequest(StreamInput in) throws IOException {
        this.nodesIds = in.readStringArray();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.CCS_TELEMETRY_STATS)) {
            throw new UnsupportedOperationException("RemoteClusterStatsRequest is not supported by the remote cluster");
        }
        out.writeStringArrayNullable(nodesIds);
    }

    public String[] nodesIds() {
        return nodesIds;
    }

}
