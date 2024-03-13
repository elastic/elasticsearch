/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ApiKeyWithProfileUid extends ApiKey {

    @Nullable
    private final String profileUid;

    ApiKeyWithProfileUid(ApiKey apiKey, @Nullable String profileUid) {
        super(apiKey);
        this.profileUid = profileUid;
    }

    @Nullable
    public String getProfileUid() {
        return profileUid;
    }

    @Override
    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerToXContent(builder, params);
        if (profileUid != null) {
            builder.field("profile_uid", profileUid);
        }
    }

    static final ConstructingObjectParser<ApiKeyWithProfileUid, Void> PARSER;
    static {
        int nFieldsForParsingApiKeys = 13; // this must be changed whenever ApiKey#initializeParser is changed for the number of parsers
        PARSER = new ConstructingObjectParser<>(
                "api_key_with_profile_uid",
                true,
                args -> new ApiKeyWithProfileUid(new ApiKey(args), (String) args[nFieldsForParsingApiKeys])
        );
        int nParsedFields = ApiKey.initializeParser(PARSER);
        if (nFieldsForParsingApiKeys != nParsedFields) {
            throw new IllegalStateException("Unexpected fields for parsing API Keys");
        }
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("profile_uid"));
    }

    public static ApiKeyWithProfileUid fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
