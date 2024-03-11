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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response for get API keys.<br>
 * The result contains information about the API keys that were found as well as the profile uid of the key owners, if available.
 */
public final class GetApiKeyResponse extends ActionResponse implements ToXContentObject {

    public static final GetApiKeyResponse EMPTY = new GetApiKeyResponse(List.of(), null);

    private final ApiKey[] foundApiKeysInfo;
    @Nullable
    private final String[] ownerProfileUids;

    public GetApiKeyResponse(Collection<ApiKey> foundApiKeysInfo, @Nullable Collection<String> ownerProfileUids) {
        Objects.requireNonNull(foundApiKeysInfo, "found_api_keys_info must be provided");
        this.foundApiKeysInfo = foundApiKeysInfo.toArray(new ApiKey[0]);
        if (ownerProfileUids != null) {
            if (foundApiKeysInfo.size() != ownerProfileUids.size()) {
                throw new IllegalArgumentException("Every api key info must be associated to an (optional) owner profile id");
            }
            this.ownerProfileUids = ownerProfileUids.toArray(new String[0]);
        } else {
            this.ownerProfileUids = null;
        }
    }

    private GetApiKeyResponse(ApiKey[] foundApiKeysInfo, @Nullable String[] ownerProfileUids) {
        assert foundApiKeysInfo != null;
        assert ownerProfileUids == null || ownerProfileUids.length == foundApiKeysInfo.length;
        this.foundApiKeysInfo = foundApiKeysInfo;
        this.ownerProfileUids = ownerProfileUids;
    }

    public ApiKey[] getApiKeyInfos() {
        return foundApiKeysInfo;
    }

    public String[] getOwnerProfileUids() {
        return ownerProfileUids;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("api_keys");
        for (int i = 0; i < foundApiKeysInfo.length; i++) {
            builder.startObject();
            foundApiKeysInfo[i].innerToXContent(builder, params);
            if (ownerProfileUids != null && ownerProfileUids[i] != null) {
                builder.field("profile_uid", ownerProfileUids[i]);
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "GetApiKeyResponse [foundApiKeysInfo="
            + Arrays.toString(foundApiKeysInfo)
            + ", ownerProfileUids="
            + Arrays.toString(ownerProfileUids)
            + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    static final ConstructingObjectParser<Tuple<ApiKey, String>, Void> KEY_PARSER;
    static final ConstructingObjectParser<GetApiKeyResponse, Void> RESPONSE_PARSER;
    static {
        int nFieldsForParsingApiKeys = 13; // this must be changed whenever ApiKey#initializeParser is changed for the number of parsers
        KEY_PARSER = new ConstructingObjectParser<>(
            "api_key_with_profile_uid",
            true,
            args -> new Tuple<>(new ApiKey(args), (String) args[nFieldsForParsingApiKeys])
        );
        int nParsedFields = ApiKey.initializeParser(KEY_PARSER);
        if (nFieldsForParsingApiKeys != nParsedFields) {
            throw new IllegalStateException("Unexpected fields for parsing API Keys");
        }
        KEY_PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("profile_uid"));
        RESPONSE_PARSER = new ConstructingObjectParser<>("get_api_key_response", args -> {
            if (args[0] == null) {
                return GetApiKeyResponse.EMPTY;
            } else {
                @SuppressWarnings("unchecked")
                List<Tuple<ApiKey, String>> apiKeysWithProfileUids = (List<Tuple<ApiKey, String>>) args[0];
                return new GetApiKeyResponse(
                    apiKeysWithProfileUids.stream().map(Tuple::v1).toArray(ApiKey[]::new),
                    apiKeysWithProfileUids.stream().map(Tuple::v2).toArray(String[]::new)
                );
            }
        });
        RESPONSE_PARSER.declareObjectArray(optionalConstructorArg(), KEY_PARSER, new ParseField("api_keys"));
    }

    public static GetApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return RESPONSE_PARSER.parse(parser, null);
    }
}
