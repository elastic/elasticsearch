/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for get API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class GetApiKeyResponse extends ActionResponse implements ToXContentObject {

    public static final GetApiKeyResponse EMPTY = new GetApiKeyResponse(List.of());

    private final List<Item> foundApiKeyInfoList;

    public GetApiKeyResponse(Collection<ApiKey> foundApiKeyInfos) {
        Objects.requireNonNull(foundApiKeyInfos, "found_api_keys_info must be provided");
        this.foundApiKeyInfoList = foundApiKeyInfos.stream().map(Item::new).toList();
    }

    public List<Item> getApiKeyInfoList() {
        return foundApiKeyInfoList;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("api_keys", foundApiKeyInfoList);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetApiKeyResponse that = (GetApiKeyResponse) o;
        return Objects.equals(foundApiKeyInfoList, that.foundApiKeyInfoList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(foundApiKeyInfoList);
    }

    @Override
    public String toString() {
        return "GetApiKeyResponse{foundApiKeysInfo=" + foundApiKeyInfoList + "}";
    }

    public record Item(ApiKey apiKeyInfo) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            apiKeyInfo.innerToXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Item{apiKeyInfo=" + apiKeyInfo + "}";
        }
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_api_key_response",
        args -> (args[0] == null) ? GetApiKeyResponse.EMPTY : new GetApiKeyResponse((List<ApiKey>) args[0])
    );
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ApiKey.fromXContent(p), new ParseField("api_keys"));
    }

    public static GetApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
