/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ServiceTokenInfo;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response when requesting credentials of a service account.
 */
public final class GetServiceAccountCredentialsResponse {

    private final String principal;
    private final List<ServiceTokenInfo> indexTokenInfos;
    private final ServiceAccountCredentialsNodesResponse nodesResponse;

    public GetServiceAccountCredentialsResponse(String principal,
                                                List<ServiceTokenInfo> indexTokenInfos,
                                                ServiceAccountCredentialsNodesResponse nodesResponse) {
        this.principal = Objects.requireNonNull(principal, "principal is required");
        this.indexTokenInfos = List.copyOf(Objects.requireNonNull(indexTokenInfos, "service token infos are required"));
        this.nodesResponse = Objects.requireNonNull(nodesResponse, "nodes response is required");
    }

    public String getPrincipal() {
        return principal;
    }

    public List<ServiceTokenInfo> getIndexTokenInfos() {
        return indexTokenInfos;
    }

    public ServiceAccountCredentialsNodesResponse getNodesResponse() {
        return nodesResponse;
    }

    @SuppressWarnings("unchecked")
    static ConstructingObjectParser<GetServiceAccountCredentialsResponse, Void> PARSER =
        new ConstructingObjectParser<>("get_service_account_credentials_response",
            args -> {
                final int count = (int) args[1];
                final List<ServiceTokenInfo> indexTokenInfos = (List<ServiceTokenInfo>) args[2];
                final ServiceAccountCredentialsNodesResponse fileTokensResponse = (ServiceAccountCredentialsNodesResponse) args[3];
                if (count != indexTokenInfos.size() + fileTokensResponse.getFileTokenInfos().size()) {
                    throw new IllegalArgumentException("number of tokens do not match");
                }
                return new GetServiceAccountCredentialsResponse((String) args[0], indexTokenInfos, fileTokensResponse);
            });

    static {
        PARSER.declareString(constructorArg(), new ParseField("service_account"));
        PARSER.declareInt(constructorArg(), new ParseField("count"));
        PARSER.declareObject(constructorArg(),
            (p, c) -> GetServiceAccountCredentialsResponse.parseIndexTokenInfos(p), new ParseField("tokens"));
        PARSER.declareObject(constructorArg(),
            (p, c) -> ServiceAccountCredentialsNodesResponse.fromXContent(p), new ParseField("nodes_credentials"));
    }

    public static GetServiceAccountCredentialsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    static List<ServiceTokenInfo> parseIndexTokenInfos(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        final List<ServiceTokenInfo> indexTokenInfos = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            indexTokenInfos.add(new ServiceTokenInfo(parser.currentName(), "index"));
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
        return indexTokenInfos;
    }
}
