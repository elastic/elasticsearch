/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The result of attempting to invalidate one or multiple tokens. The result contains information about:
 * <ul>
 * <li>how many of the tokens were actually invalidated</li>
 * <li>how many tokens are not invalidated in this request because they were already invalidated</li>
 * <li>how many tokens were not invalidated because of an error and what the error was</li>
 * </ul>
 */
public class TokensInvalidationResult implements ToXContentObject {

    private final String[] invalidatedTokens;
    private final String[] prevInvalidatedTokens;
    private final String[] errors;
    private final int attemptCounter;


    public TokensInvalidationResult(String[] invalidatedTokens, String[] notInvalidatedTokens,
                                    @Nullable String[] errors, int attemptCounter) {
        Objects.requireNonNull(invalidatedTokens, "invalidated_tokens must be provided");
        this.invalidatedTokens = invalidatedTokens;
        Objects.requireNonNull(notInvalidatedTokens, "not_invalidated_must be provided");
        this.prevInvalidatedTokens = notInvalidatedTokens;
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = new String[0];
        }
        this.attemptCounter = attemptCounter;
    }

    public static TokensInvalidationResult emptyResult(){
        return new TokensInvalidationResult(new String[0], new String[0], new String[0], 0);
    }

    public String[] getInvalidatedTokens() {
        return invalidatedTokens;
    }

    public String[] getPrevInvalidatedTokens() {
        return prevInvalidatedTokens;
    }

    public String[] getErrors() {
        return errors;
    }

    public int getAttemptCounter() {
        return attemptCounter;
    }

    public static void writeTo(TokensInvalidationResult result, StreamOutput out) throws IOException {
        out.writeVInt(result.getInvalidatedTokens().length);
        out.writeStringArray(result.getInvalidatedTokens());
        out.writeVInt(result.getPrevInvalidatedTokens().length);
        out.writeStringArray(result.getPrevInvalidatedTokens());
        out.writeVInt(result.getErrors().length);
        out.writeStringArray(result.getErrors());
        out.writeVInt(result.getAttemptCounter());
    }

    public static TokensInvalidationResult readFrom(StreamInput in) throws IOException {
        int invalidatedTokensSize = in.readVInt();
        String[] invalidatedTokens = in.readStringArray();
        int prevInvalidatedTokensSize = in.readVInt();
        String[] prevUnvalidatedTokens = in.readStringArray();
        int errorsSize = in.readVInt();
        String[] errors = in.readStringArray();
        int attemptCounter = in.readVInt();
        return new TokensInvalidationResult(invalidatedTokens, prevUnvalidatedTokens, errors, attemptCounter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("invalidated_tokens", invalidatedTokens.length)
            .field("prev_invalidated_tokens", prevInvalidatedTokens.length)
            .startObject("errors")
            .field("size", errors.length);
        if (errors.length > 0) {
            builder.field("error_messages");
            builder.startArray();
            for (String error : errors) {
                builder.value(error);
            }
            builder.endArray();
        }
        builder.endObject()
            .endObject();
        return builder;
    }
}
