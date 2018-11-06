/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;

import java.io.IOException;

/**
 * Response for a invalidation of one or multiple tokens.
 */
public final class InvalidateTokenResponse extends ActionResponse {

    private TokensInvalidationResult result;

    public InvalidateTokenResponse() {}

    public InvalidateTokenResponse(TokensInvalidationResult result) {
        this.result = result;
    }

    public TokensInvalidationResult getResult() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        TokensInvalidationResult.writeTo(result, out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = TokensInvalidationResult.readFrom(in);
    }
}
