/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ServiceTokenInfo;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response when requesting credentials of a service account.
 */
public final class GetServiceAccountCredentialsResponse {

    private final String principal;
    private final String nodeName;
    private final List<ServiceTokenInfo> serviceTokenInfos;

    public GetServiceAccountCredentialsResponse(
        String principal, String nodeName, List<ServiceTokenInfo> serviceTokenInfos) {
        this.principal = Objects.requireNonNull(principal, "principal is required");
        this.nodeName = Objects.requireNonNull(nodeName, "nodeName is required");
        this.serviceTokenInfos = List.copyOf(Objects.requireNonNull(serviceTokenInfos, "service token infos are required)"));
    }

    public String getPrincipal() {
        return principal;
    }

    public String getNodeName() {
        return nodeName;
    }

    public List<ServiceTokenInfo> getServiceTokenInfos() {
        return serviceTokenInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountCredentialsResponse that = (GetServiceAccountCredentialsResponse) o;
        return principal.equals(that.principal) && nodeName.equals(that.nodeName) && serviceTokenInfos.equals(that.serviceTokenInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, nodeName, serviceTokenInfos);
    }

    static ConstructingObjectParser<GetServiceAccountCredentialsResponse, Void> PARSER =
        new ConstructingObjectParser<>("get_service_account_credentials_response",
            args -> {
                @SuppressWarnings("unchecked")
                final List<ServiceTokenInfo> tokenInfos = Stream.concat(
                    ((Map<String, Object>) args[3]).keySet().stream().map(name -> new ServiceTokenInfo(name, "index")),
                    ((Map<String, Object>) args[4]).keySet().stream().map(name -> new ServiceTokenInfo(name, "file")))
                    .collect(Collectors.toList());
                assert tokenInfos.size() == (int) args[2] : "number of tokens do not match";
                return new GetServiceAccountCredentialsResponse((String) args[0], (String) args[1], tokenInfos);
            });

    static {
        PARSER.declareString(constructorArg(), new ParseField("service_account"));
        PARSER.declareString(constructorArg(), new ParseField("node_name"));
        PARSER.declareInt(constructorArg(), new ParseField("count"));
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("tokens"));
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("file_tokens"));
    }

    public static GetServiceAccountCredentialsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
