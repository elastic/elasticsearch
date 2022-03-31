/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class QueryApiKeyResponse {

    private final long total;
    private final List<ApiKey> apiKeys;

    public QueryApiKeyResponse(long total, List<ApiKey> apiKeys) {
        this.total = total;
        this.apiKeys = apiKeys;
    }

    public long getTotal() {
        return total;
    }

    public int getCount() {
        return apiKeys.size();
    }

    public List<ApiKey> getApiKeys() {
        return apiKeys;
    }

    public static QueryApiKeyResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    static final ConstructingObjectParser<QueryApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "query_api_key_response",
        args -> {
            final long total = (long) args[0];
            final int count = (int) args[1];
            @SuppressWarnings("unchecked")
            final List<ApiKey> items = (List<ApiKey>) args[2];
            if (count != items.size()) {
                throw new IllegalArgumentException("count [" + count + "] is not equal to number of items [" + items.size() + "]");
            }
            return new QueryApiKeyResponse(total, items);
        }
    );

    static {
        PARSER.declareLong(constructorArg(), new ParseField("total"));
        PARSER.declareInt(constructorArg(), new ParseField("count"));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ApiKey.fromXContent(p), new ParseField("api_keys"));
    }
}
