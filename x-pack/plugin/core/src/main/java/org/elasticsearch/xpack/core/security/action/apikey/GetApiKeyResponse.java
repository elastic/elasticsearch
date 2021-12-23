/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for get API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class GetApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final ApiKey[] foundApiKeysInfo;

    public GetApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.foundApiKeysInfo = in.readArray(ApiKey::new, ApiKey[]::new);
    }

    public GetApiKeyResponse(Collection<ApiKey> foundApiKeysInfo) {
        Objects.requireNonNull(foundApiKeysInfo, "found_api_keys_info must be provided");
        this.foundApiKeysInfo = foundApiKeysInfo.toArray(new ApiKey[0]);
    }

    public static GetApiKeyResponse emptyResponse() {
        return new GetApiKeyResponse(Collections.emptyList());
    }

    public ApiKey[] getApiKeyInfos() {
        return foundApiKeysInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().array("api_keys", (Object[]) foundApiKeysInfo);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(foundApiKeysInfo);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_api_key_response",
        args -> { return (args[0] == null) ? GetApiKeyResponse.emptyResponse() : new GetApiKeyResponse((List<ApiKey>) args[0]); }
    );
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ApiKey.fromXContent(p), new ParseField("api_keys"));
    }

    public static GetApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "GetApiKeyResponse [foundApiKeysInfo=" + foundApiKeysInfo + "]";
    }

}
