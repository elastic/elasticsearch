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
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

public class EsqlResolveFieldsResponse extends ActionResponse {
    public static final TransportVersion RESOLVE_FIELDS_RESPONSE_CREATED_TV = TransportVersion.fromName(
        "esql_resolve_fields_response_created"
    );

    /**
     * Marks when we started using the minimum transport version to determine whether a data type is supported on all nodes.
     * This is about the coordinator - data nodes will be able to respond with the correct data as long as they're on the
     * transport version required for the respective data types. See {@link DataType#supportedVersion()}.
     * <p>
     * Note: this is in 9.2.1, but not 9.2.0 - in 9.2.0 we resorted to workarounds to sometimes enable {@link DataType#DENSE_VECTOR} and
     * {@link DataType#AGGREGATE_METRIC_DOUBLE}, even though 9.2.0 nodes already support these types.
     * <p>
     * This means that mixed clusters with a 9.2.1 coordinator and 9.2.0 data nodes will properly support these types,
     * but a 9.2.0 coordinator with 9.2.1+ nodes will still require the workaround.
     */
    public static final TransportVersion RESOLVE_FIELDS_RESPONSE_USED_TV = TransportVersion.fromName("esql_resolve_fields_response_used");

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
