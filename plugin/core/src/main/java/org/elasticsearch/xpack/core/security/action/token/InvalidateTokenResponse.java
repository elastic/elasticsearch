/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response for a invalidation of a token.
 */
public final class InvalidateTokenResponse extends ActionResponse {

    private boolean created;

    public InvalidateTokenResponse() {}

    public InvalidateTokenResponse(boolean created) {
        this.created = created;
    }

    /**
     * If the token is already invalidated then created will be <code>false</code>
     */
    public boolean isCreated() {
        return created;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(created);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        created = in.readBoolean();
    }
}
