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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

    public GetApiKeyResponse(Collection<Item> foundApiKeysInfos) {
        Objects.requireNonNull(foundApiKeysInfos, "found_api_keys_info must be provided");
        if (foundApiKeysInfos instanceof List<Item>) {
            this.foundApiKeyInfoList = (List<Item>) foundApiKeysInfos;
        } else {
            this.foundApiKeyInfoList = new ArrayList<>(foundApiKeysInfos);
        }
    }

    public GetApiKeyResponse(Collection<ApiKey> foundApiKeysInfos, @Nullable Collection<String> ownerProfileUids) {
        Objects.requireNonNull(foundApiKeysInfos, "found_api_keys_info must be provided");
        if (ownerProfileUids == null) {
            this.foundApiKeyInfoList = foundApiKeysInfos.stream().map(Item::new).toList();
        } else {
            if (foundApiKeysInfos.size() != ownerProfileUids.size()) {
                throw new IllegalStateException("Each api key info must be associated to a (nullable) owner profile uid");
            }
            int size = foundApiKeysInfos.size();
            this.foundApiKeyInfoList = new ArrayList<>(size);
            Iterator<ApiKey> apiKeyIterator = foundApiKeysInfos.iterator();
            Iterator<String> profileUidIterator = ownerProfileUids.iterator();
            while (apiKeyIterator.hasNext()) {
                if (false == profileUidIterator.hasNext()) {
                    throw new IllegalStateException("Each api key info must be associated to a (nullable) owner profile uid");
                }
                this.foundApiKeyInfoList.add(new Item(apiKeyIterator.next(), profileUidIterator.next()));
            }
        }
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

    public record Item(ApiKey apiKeyInfo, @Nullable String ownerProfileUid) implements ToXContentObject {

        public Item(ApiKey apiKeyInfo) {
            this(apiKeyInfo, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            apiKeyInfo.innerToXContent(builder, params);
            if (ownerProfileUid != null) {
                builder.field("profile_uid", ownerProfileUid);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Item{apiKeyInfo=" + apiKeyInfo + ", ownerProfileUid=" + ownerProfileUid + "}";
        }
    }

    static final ConstructingObjectParser<GetApiKeyResponse, Void> RESPONSE_PARSER;
    static {
        int nFieldsForParsingApiKeyInfo = 13; // this must be changed whenever ApiKey#initializeParser is changed for the number of parsers
        ConstructingObjectParser<Item, Void> keyInfoParser = new ConstructingObjectParser<>(
            "api_key_with_profile_uid",
            true,
            args -> new Item(new ApiKey(args), (String) args[nFieldsForParsingApiKeyInfo])
        );
        int nParsedFields = ApiKey.initializeParser(keyInfoParser);
        if (nFieldsForParsingApiKeyInfo != nParsedFields) {
            throw new IllegalStateException("Unexpected fields for parsing API Keys");
        }
        keyInfoParser.declareStringOrNull(optionalConstructorArg(), new ParseField("profile_uid"));
        RESPONSE_PARSER = new ConstructingObjectParser<>("get_api_key_response", args -> {
            if (args[0] == null) {
                return GetApiKeyResponse.EMPTY;
            } else {
                @SuppressWarnings("unchecked")
                List<Item> apiKeysWithProfileUids = (List<Item>) args[0];
                return new GetApiKeyResponse(apiKeysWithProfileUids);
            }
        });
        RESPONSE_PARSER.declareObjectArray(optionalConstructorArg(), keyInfoParser, new ParseField("api_keys"));
    }

    public static GetApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return RESPONSE_PARSER.parse(parser, null);
    }
}
