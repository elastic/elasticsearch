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
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class EsqlResolveFieldsResponse extends ActionResponse {
    public static final TransportVersion RESOLVE_FIELDS_RESPONSE_CREATED_TV = TransportVersion.fromName(
        "esql_resolve_fields_response_created"
    );

    private final FieldCapabilitiesResponse caps;
    private final TransportVersion minTransportVersion;

    public EsqlResolveFieldsResponse(FieldCapabilitiesResponse caps, TransportVersion minTransportVersion) {
        this.caps = caps;
        this.minTransportVersion = minTransportVersion;
    }

    public EsqlResolveFieldsResponse(StreamInput in) throws IOException {
        caps = new FieldCapabilitiesResponse(in);
        if (in.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_CREATED_TV) && in.readBoolean()) {
            minTransportVersion = TransportVersion.readVersion(in);
        } else {
            minTransportVersion = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        caps.writeTo(out);
        if (out.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_CREATED_TV)) {
            out.writeBoolean(minTransportVersion != null);
            if (minTransportVersion != null) {
                TransportVersion.writeVersion(minTransportVersion, out);
            }
        }
    }

    public FieldCapabilitiesResponse caps() {
        return caps;
    }

    /**
     * The minimum {@link TransportVersion} of all clusters against which we resolved
     * indices.
     * <p>
     *     If this is {@code null} then one of the nodes is before {@link #RESOLVE_FIELDS_RESPONSE_CREATED_TV} but
     *     we have no idea how early it is. Could be back in {@code 8.19.0}.
     * </p>
     */
    @Nullable
    public TransportVersion minTransportVersion() {
        return minTransportVersion;
    }
}
