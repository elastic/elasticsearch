/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Request for the {@code POST /{index}/_semantic_cleanup} endpoint.
 *
 * <p>Optionally scopes the cleanup to a single {@code field}, and optionally
 * overrides the cluster-level staged TTL for this run via {@code maxAge}.
 */
public class StagedSemanticCleanupRequest extends BroadcastRequest<StagedSemanticCleanupRequest> {

    /**
     * Transport version that introduced the {@code field} and {@code maxAge} fields on this request,
     * and the {@code cleared} and {@code failed} fields on the corresponding response.
     */
    static final TransportVersion STAGED_SEMANTIC_CLEANUP_FIELDS_ADDED = TransportVersion.fromName("staged_semantic_cleanup_fields_added");

    @Nullable
    private final String field;

    @Nullable
    private final TimeValue maxAge;

    public StagedSemanticCleanupRequest(String[] indices, @Nullable String field, @Nullable TimeValue maxAge) {
        super(indices);
        this.field = field;
        this.maxAge = maxAge;
    }

    public StagedSemanticCleanupRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(STAGED_SEMANTIC_CLEANUP_FIELDS_ADDED)) {
            this.field = in.readOptionalString();
            this.maxAge = in.readOptionalTimeValue();
        } else {
            this.field = null;
            this.maxAge = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(STAGED_SEMANTIC_CLEANUP_FIELDS_ADDED)) {
            out.writeOptionalString(field);
            out.writeOptionalTimeValue(maxAge);
        }
    }

    /** Returns the optional field name to scope cleanup to, or {@code null} for all fields. */
    @Nullable
    public String field() {
        return field;
    }

    /** Returns the optional TTL override for this run, or {@code null} to use the cluster setting. */
    @Nullable
    public TimeValue maxAge() {
        return maxAge;
    }
}
