/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

public class GetSecurityStatsNodeRequest extends AbstractTransportRequest {

    public GetSecurityStatsNodeRequest() {}

    public GetSecurityStatsNodeRequest(final StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.SECURITY_STATS_ENDPOINT)) { // shouldn't happen
            throw new UnsupportedOperationException("node doesn't support security stats endpoint");
        }
        super.writeTo(out);
    }
}
