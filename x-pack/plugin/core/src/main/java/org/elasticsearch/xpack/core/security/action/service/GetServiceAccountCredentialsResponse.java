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
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class GetServiceAccountCredentialsResponse extends ActionResponse implements ToXContentObject {

    private final String principal;
    private final List<TokenInfo> indexTokenInfos;
    private final GetServiceAccountCredentialsNodesResponse nodesResponse;

    public GetServiceAccountCredentialsResponse(
        String principal,
        Collection<TokenInfo> indexTokenInfos,
        GetServiceAccountCredentialsNodesResponse nodesResponse
    ) {
        this.principal = principal;
        this.indexTokenInfos = indexTokenInfos == null ? List.of() : indexTokenInfos.stream().sorted().toList();
        this.nodesResponse = nodesResponse;
    }

    public GetServiceAccountCredentialsResponse(StreamInput in) throws IOException {
        this.principal = in.readString();
        this.indexTokenInfos = in.readCollectionAsList(TokenInfo::new);
        this.nodesResponse = new GetServiceAccountCredentialsNodesResponse(in);
    }

    public String getPrincipal() {
        return principal;
    }

    public List<TokenInfo> getIndexTokenInfos() {
        return indexTokenInfos;
    }

    public GetServiceAccountCredentialsNodesResponse getNodesResponse() {
        return nodesResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        out.writeCollection(indexTokenInfos);
        nodesResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final List<TokenInfo> fileTokenInfos = nodesResponse.getFileTokenInfos();

        builder.startObject()
            .field("service_account", principal)
            .field("count", indexTokenInfos.size() + fileTokenInfos.size())
            .field("tokens")
            .startObject();
        for (TokenInfo info : indexTokenInfos) {
            info.toXContent(builder, params);
        }
        builder.endObject().field("nodes_credentials").startObject();
        RestActions.buildNodesHeader(builder, params, nodesResponse);
        builder.startObject("file_tokens");
        for (TokenInfo info : fileTokenInfos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject().endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "GetServiceAccountCredentialsResponse{"
            + "principal='"
            + principal
            + '\''
            + ", indexTokenInfos="
            + indexTokenInfos
            + ", nodesResponse="
            + nodesResponse
            + '}';
    }
}
