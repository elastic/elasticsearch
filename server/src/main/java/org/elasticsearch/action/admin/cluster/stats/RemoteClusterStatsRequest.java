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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get cluster level stats from the remote cluster.
 */
public class RemoteClusterStatsRequest extends LegacyActionRequest {
    public RemoteClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public RemoteClusterStatsRequest() {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
            : "RemoteClusterStatsRequest is not supported by the remote cluster";
        if (out.getTransportVersion().before(TransportVersions.V_8_16_0)) {
            throw new UnsupportedOperationException("RemoteClusterStatsRequest is not supported by the remote cluster");
        }
        super.writeTo(out);
    }
}
