/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * Response for a invalidation of one or multiple tokens.
 */
public final class InvalidateTokenResponse extends ActionResponse implements ToXContent {

    private TokensInvalidationResult result;

    public InvalidateTokenResponse() {}

    public InvalidateTokenResponse(TokensInvalidationResult result) {
        this.result = result;
    }

    public TokensInvalidationResult getResult() {
        return result;
    }

    private boolean isCreated() {
        return result.getInvalidatedTokens().size() > 0
            && result.getPreviouslyInvalidatedTokens().isEmpty()
            && result.getErrors().isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_0_0)) {
            out.writeBoolean(isCreated());
        } else {
            result.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_7_0_0)) {
            final boolean created = in.readBoolean();
            if (created) {
                result = new TokensInvalidationResult(Arrays.asList(""), Collections.emptyList(), Collections.emptyList(), 0);
            } else {
                result = new TokensInvalidationResult(Collections.emptyList(), Arrays.asList(""), Collections.emptyList(), 0);
            }
        } else {
            result = new TokensInvalidationResult(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        result.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvalidateTokenResponse that = (InvalidateTokenResponse) o;
        return Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }
}
