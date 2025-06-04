/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for invalidation of one or more API keys result.<br>
 * The result contains information about:
 * <ul>
 * <li>API key ids that were actually invalidated</li>
 * <li>API key ids that were not invalidated in this request because they were already invalidated</li>
 * <li>how many errors were encountered while invalidating API keys and the error details</li>
 * </ul>
 */
public final class InvalidateApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final List<String> invalidatedApiKeys;
    private final List<String> previouslyInvalidatedApiKeys;
    private final List<ElasticsearchException> errors;

    public InvalidateApiKeyResponse(StreamInput in) throws IOException {
        this.invalidatedApiKeys = in.readCollectionAsList(StreamInput::readString);
        this.previouslyInvalidatedApiKeys = in.readCollectionAsList(StreamInput::readString);
        this.errors = in.readCollectionAsList(StreamInput::readException);
    }

    /**
     * Constructor for API keys invalidation response
     * @param invalidatedApiKeys list of invalidated API key ids
     * @param previouslyInvalidatedApiKeys list of previously invalidated API key ids
     * @param errors list of encountered errors while invalidating API keys
     */
    public InvalidateApiKeyResponse(
        List<String> invalidatedApiKeys,
        List<String> previouslyInvalidatedApiKeys,
        @Nullable List<ElasticsearchException> errors
    ) {
        this.invalidatedApiKeys = Objects.requireNonNull(invalidatedApiKeys, "invalidated_api_keys must be provided");
        this.previouslyInvalidatedApiKeys = Objects.requireNonNull(
            previouslyInvalidatedApiKeys,
            "previously_invalidated_api_keys must be provided"
        );
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = Collections.emptyList();
        }
    }

    public static InvalidateApiKeyResponse emptyResponse() {
        return new InvalidateApiKeyResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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
        out.writeStringCollection(invalidatedApiKeys);
        out.writeStringCollection(previouslyInvalidatedApiKeys);
        out.writeCollection(errors, StreamOutput::writeException);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<InvalidateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "invalidate_api_key_response",
        args -> {
            return new InvalidateApiKeyResponse((List<String>) args[0], (List<String>) args[1], (List<ElasticsearchException>) args[3]);
        }
    );
    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("invalidated_api_keys"));
        PARSER.declareStringArray(constructorArg(), new ParseField("previously_invalidated_api_keys"));
        // we parse error_count but ignore it while constructing response
        PARSER.declareInt(constructorArg(), new ParseField("error_count"));
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField("error_details")
        );
    }

    public static InvalidateApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "InvalidateApiKeyResponse [invalidatedApiKeys="
            + invalidatedApiKeys
            + ", previouslyInvalidatedApiKeys="
            + previouslyInvalidatedApiKeys
            + ", errors="
            + errors
            + "]";
    }

}
