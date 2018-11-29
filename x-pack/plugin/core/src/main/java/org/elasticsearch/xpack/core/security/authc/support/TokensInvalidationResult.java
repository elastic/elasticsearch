/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    private final String[] previouslyInvalidatedTokens;
    private final List<ElasticsearchException> errors;
    private final int attemptCount;

    public TokensInvalidationResult(String[] invalidatedTokens, String[] previouslyInvalidatedTokens,
                                    @Nullable List<ElasticsearchException> errors, int attemptCount) {
        Objects.requireNonNull(invalidatedTokens, "invalidated_tokens must be provided");
        this.invalidatedTokens = invalidatedTokens;
        Objects.requireNonNull(previouslyInvalidatedTokens, "previously_invalidated_tokens must be provided");
        this.previouslyInvalidatedTokens = previouslyInvalidatedTokens;
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = Collections.emptyList();
        }
        this.attemptCount = attemptCount;
    }

    public static TokensInvalidationResult emptyResult() {
        return new TokensInvalidationResult(new String[0], new String[0], Collections.emptyList(), 0);
    }

    public static TokensInvalidationResult emptyResultWithCounter(int attemptCount) {
        return new TokensInvalidationResult(new String[0], new String[0], Collections.emptyList(), attemptCount);
    }

    public String[] getInvalidatedTokens() {
        return invalidatedTokens;
    }

    public String[] getPreviouslyInvalidatedTokens() {
        return previouslyInvalidatedTokens;
    }

    public List<ElasticsearchException> getErrors() {
        return errors;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public static void writeTo(TokensInvalidationResult result, StreamOutput out) throws IOException {
        out.writeVInt(result.getInvalidatedTokens().length);
        out.writeStringArray(result.getInvalidatedTokens());
        out.writeVInt(result.getPreviouslyInvalidatedTokens().length);
        out.writeStringArray(result.getPreviouslyInvalidatedTokens());
        out.writeVInt(result.getErrors().size());
        for (Exception e : result.getErrors()) {
            out.writeException(e);
        }
        out.writeVInt(result.getAttemptCount());
    }

    public static TokensInvalidationResult readFrom(StreamInput in) throws IOException {
        int invalidatedTokensSize = in.readVInt();
        String[] invalidatedTokens = in.readStringArray();
        int previouslyInvalidatedTokensSize = in.readVInt();
        String[] previouslyUnvalidatedTokens = in.readStringArray();
        int errorsSize = in.readVInt();
        List<ElasticsearchException> errors = new ArrayList<>(errorsSize);
        for (int i = 0; i < errorsSize; i++) {
            errors.add(in.readException());
        }
        int attemptCounter = in.readVInt();
        return new TokensInvalidationResult(invalidatedTokens, previouslyUnvalidatedTokens, errors, attemptCounter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("invalidated_tokens", invalidatedTokens.length)
            .field("previously_invalidated_tokens", previouslyInvalidatedTokens.length)
            .startObject("errors")
            .field("size", errors.size());
        if (errors.isEmpty() == false) {
            builder.field("error_details");
            builder.startArray();
            for (ElasticsearchException e : errors) {
                builder.startObject();
                e.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokensInvalidationResult that = (TokensInvalidationResult) o;
        if (errors.size() != that.errors.size()) {
            return false;
        }
        for (Exception e : errors) {
            // ElasticsearchException#toString contains the class name and the detailed message
            if (that.errors.stream().anyMatch(e1 -> e1.toString().equals(e.toString())) == false) {
                return false;
            }
        }
        return attemptCount == that.attemptCount &&
            Arrays.equals(invalidatedTokens, that.invalidatedTokens) &&
            Arrays.equals(previouslyInvalidatedTokens, that.previouslyInvalidatedTokens);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(errors, attemptCount);
        result = 31 * result + Arrays.hashCode(invalidatedTokens);
        result = 31 * result + Arrays.hashCode(previouslyInvalidatedTokens);
        return result;
    }
}
