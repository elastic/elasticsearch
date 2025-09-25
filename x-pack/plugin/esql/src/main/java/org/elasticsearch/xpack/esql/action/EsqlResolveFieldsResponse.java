/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class EsqlResolveFieldsResponse extends ActionResponse {
    private static final TransportVersion CREATED = TransportVersion.fromName("esql_resolve_fields_response_created");

    private final FieldCapabilitiesResponse caps;
    private final TransportVersion minTransportVersion;

    public EsqlResolveFieldsResponse(FieldCapabilitiesResponse caps, TransportVersion minTransportVersion) {
        this.caps = caps;
        this.minTransportVersion = minTransportVersion;
    }

    public EsqlResolveFieldsResponse(StreamInput in) throws IOException {
        caps = new FieldCapabilitiesResponse(in);
        if (in.getTransportVersion().supports(CREATED) && in.readBoolean()) {
            minTransportVersion = TransportVersion.readVersion(in);
        } else {
            minTransportVersion = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        caps.writeTo(out);
        if (out.getTransportVersion().supports(CREATED)) {
            out.writeBoolean(minTransportVersion != null);
            if (minTransportVersion != null) {
                TransportVersion.writeVersion(minTransportVersion, out);
            }
        }
    }

    public FieldCapabilitiesResponse caps() {
        return caps;
    }

    public TransportVersion minTransportVersion() {
        return minTransportVersion;
    }
}
