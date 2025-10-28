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
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

public class EsqlResolveFieldsResponse extends ActionResponse {
    private static final TransportVersion RESOLVE_FIELDS_RESPONSE_CREATED_TV = TransportVersion.fromName(
        "esql_resolve_fields_response_created"
    );
    public static final TransportVersion RESOLVE_FIELDS_RESPONSE_REMOVED_MIN_TV = TransportVersion.fromName(
        "esql_resolve_fields_response_removed_min_tv"
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

    public EsqlResolveFieldsResponse(FieldCapabilitiesResponse caps) {
        this.caps = caps;
    }

    public EsqlResolveFieldsResponse(StreamInput in) throws IOException {
        this.caps = readMinTransportVersion(new FieldCapabilitiesResponse(in), in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        caps.writeTo(out);
        writeMinTransportVersion(out);
    }

    private static FieldCapabilitiesResponse readMinTransportVersion(FieldCapabilitiesResponse caps, StreamInput in) throws IOException {
        /*
         * minTransportVersion has a bit of a story.
         * 1. First we created it to `EsqlResolveFieldsResponse` in
         *    `EsqlResolveFieldsResponse.RESOLVE_FIELDS_RESPONSE_CREATED_TV`.
         * 2. We then added it to `FieldCapsResponse` in `FieldCapsResponse.MIN_TRANSPORT_VERSION`.
         *    Now we send two copies!
         * 3. Finally, we removed it from `EsqlResolveFieldsResponse` in
         *    `EsqlResolveFieldsResponse.RESOLVE_FIELDS_RESPONSE_REMOVED_MIN_TV`. Now we send one copy.
         * We handle the transport version that was part of EsqlResolveFieldsResponse here by checking
         * *our* transport versions in reverse order.
         */
        if (in.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_REMOVED_MIN_TV)) {
            // Transport version not sent as part of EsqlResolveFieldResponse, use the one in caps.
            return caps;
        }
        if (in.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_CREATED_TV) && in.readBoolean()) {
            /*
             * Transport version sent as part of EsqlResolveFieldResponse.
             * There could be one in caps if we're after FieldCapsResponse.MIN_TRANSPORT_VERSION.
             * But we need to read the one that is in EsqlResolveFieldResponse off of the wire anyway, so we just use it.
             */
            return caps.withMinTransportVersion(TransportVersion.readVersion(in));
        }
        // No transport version sent.
        return caps;
    }

    private void writeMinTransportVersion(StreamOutput out) throws IOException {
        // See big comment in readMinTransportVersion for the story
        if (out.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_REMOVED_MIN_TV)) {
            // Remote does not expect an optional min transport version so we don't send anything
            return;
        }
        if (out.getTransportVersion().supports(RESOLVE_FIELDS_RESPONSE_CREATED_TV)) {
            // Remote expects to read an optional transport version from EsqlResolveFieldsResponse
            out.writeBoolean(caps.minTransportVersion() != null);
            if (caps.minTransportVersion() != null) {
                TransportVersion.writeVersion(caps.minTransportVersion(), out);
            }
            return;
        }
        // Remote does not expect an optional min transport version so we don't send anything
    }

    public FieldCapabilitiesResponse caps() {
        return caps;
    }
}
