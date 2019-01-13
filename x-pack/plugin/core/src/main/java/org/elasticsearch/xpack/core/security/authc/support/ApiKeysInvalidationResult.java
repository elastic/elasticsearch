/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The result of attempting to invalidate one or multiple api keys. The result contains information about:
 * <ul>
 * <li>API key ids those that were actually invalidated</li>
 * <li>API key ids those that were not invalidated in this request because they were already invalidated</li>
 * <li>how many errors were encountered while invalidating API keys and the error details</li>
 * </ul>
 */
public class ApiKeysInvalidationResult implements ToXContentObject, Writeable {

    private final List<String> invalidatedApiKeys;
    private final List<String> previouslyInvalidatedApiKeys;
    private final List<ElasticsearchException> errors;
    private final int attemptCount;

    /**
     * Constructor for API keys in validation result
     * @param invalidatedApiKeys list of invalidated API key ids
     * @param previouslyInvalidatedApiKeys list of previously invalidated API key ids
     * @param errors list of encountered errors while invalidating API keys
     * @param attemptCount no of times the invalidation was attempted.
     */
    public ApiKeysInvalidationResult(List<String> invalidatedApiKeys, List<String> previouslyInvalidatedApiKeys,
                                    @Nullable List<ElasticsearchException> errors, int attemptCount) {
        Objects.requireNonNull(invalidatedApiKeys, "invalidated_api_keys must be provided");
        this.invalidatedApiKeys = invalidatedApiKeys;
        Objects.requireNonNull(previouslyInvalidatedApiKeys, "previously_invalidated_api_keys must be provided");
        this.previouslyInvalidatedApiKeys = previouslyInvalidatedApiKeys;
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = Collections.emptyList();
        }
        this.attemptCount = attemptCount;
    }

    public ApiKeysInvalidationResult(StreamInput in) throws IOException {
        this.invalidatedApiKeys = in.readList(StreamInput::readString);
        this.previouslyInvalidatedApiKeys = in.readList(StreamInput::readString);
        this.errors = in.readList(StreamInput::readException);
        this.attemptCount = in.readVInt();
    }

    public static ApiKeysInvalidationResult emptyResult() {
        return new ApiKeysInvalidationResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 0);
    }


    public List<String> getInvalidatedApiKeys() {
        return invalidatedApiKeys;
    }

    public List<String> getPreviouslyInvalidatedApiKeys() {
        return previouslyInvalidatedApiKeys;
    }

    public List<ElasticsearchException> getErrors() {
        return errors;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .array("invalidated_api_keys", invalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
            .array("previously_invalidated_api_keys", previouslyInvalidatedApiKeys.toArray(Strings.EMPTY_ARRAY))
            .field("error_count", errors.size());
        if (errors.isEmpty() == false) {
            builder.field("error_details");
            builder.startArray();
            for (ElasticsearchException e : errors) {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, e);
                builder.endObject();
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringList(invalidatedApiKeys);
        out.writeStringList(previouslyInvalidatedApiKeys);
        out.writeCollection(errors, StreamOutput::writeException);
        out.writeVInt(attemptCount);
    }

    @Override
    public String toString() {
        return "ApiKeysInvalidationResult [invalidatedApiKeys=" + invalidatedApiKeys + ", previouslyInvalidatedApiKeys="
                + previouslyInvalidatedApiKeys + ", errors=" + errors + ", attemptCount=" + attemptCount + "]";
    }

}
