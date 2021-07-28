/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableList;

public class GetServiceAccountCredentialsResponse extends ActionResponse implements ToXContentObject {

    private final String principal;
    private final String nodeName;
    private final List<TokenInfo> tokenInfos;

    public GetServiceAccountCredentialsResponse(String principal, String nodeName, Collection<TokenInfo> tokenInfos) {
        this.principal = principal;
        this.nodeName = nodeName;
        this.tokenInfos = tokenInfos == null ? List.of() : tokenInfos.stream().sorted().collect(toUnmodifiableList());
    }

    public GetServiceAccountCredentialsResponse(StreamInput in) throws IOException {
        super(in);
        this.principal = in.readString();
        this.nodeName = in.readString();
        this.tokenInfos = in.readList(TokenInfo::new);
    }

    public String getPrincipal() {
        return principal;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Collection<TokenInfo> getTokenInfos() {
        return tokenInfos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        out.writeString(nodeName);
        out.writeList(tokenInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final Map<TokenInfo.TokenSource, List<TokenInfo>> tokenInfosBySource =
            tokenInfos.stream().collect(groupingBy(TokenInfo::getSource, toUnmodifiableList()));
        builder.startObject()
            .field("service_account", principal)
            .field("node_name", nodeName)
            .field("count", tokenInfos.size())
            .field("tokens").startObject();
        for (TokenInfo info : tokenInfosBySource.getOrDefault(TokenInfo.TokenSource.INDEX, List.of())) {
            info.toXContent(builder, params);
        }
        builder.endObject().field("file_tokens").startObject();
        for (TokenInfo info : tokenInfosBySource.getOrDefault(TokenInfo.TokenSource.FILE, List.of())) {
            info.toXContent(builder, params);
        }
        builder.endObject().endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountCredentialsResponse that = (GetServiceAccountCredentialsResponse) o;
        return Objects.equals(principal, that.principal) && Objects.equals(nodeName, that.nodeName) && Objects.equals(
            tokenInfos, that.tokenInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, nodeName, tokenInfos);
    }
}
