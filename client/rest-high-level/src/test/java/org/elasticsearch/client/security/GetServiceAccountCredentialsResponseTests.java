/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesResponse;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountCredentialsResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse,
    GetServiceAccountCredentialsResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse createServerTestInstance(
        XContentType xContentType) {
        final String[] fileTokenNames = randomArray(3, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final GetServiceAccountCredentialsNodesResponse nodesResponse = new GetServiceAccountCredentialsNodesResponse(
            new ClusterName(randomAlphaOfLength(12)),
            List.of(new GetServiceAccountCredentialsNodesResponse.Node(new DiscoveryNode(randomAlphaOfLength(10),
                new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                Version.CURRENT), fileTokenNames)),
            List.of(new FailedNodeException(randomAlphaOfLength(11), "error", new NoSuchFieldError("service_tokens"))));
        return new org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse(
            randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8),
            randomList(0, 5, () -> TokenInfo.indexToken(randomAlphaOfLengthBetween(3, 8))),
            nodesResponse);
    }

    @Override
    protected GetServiceAccountCredentialsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetServiceAccountCredentialsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse serverTestInstance,
        GetServiceAccountCredentialsResponse clientInstance) {
        assertThat(serverTestInstance.getPrincipal(), equalTo(clientInstance.getPrincipal()));

        assertThat(
            Stream.concat(serverTestInstance.getIndexTokenInfos().stream(),
                serverTestInstance.getNodesResponse().getFileTokenInfos().stream())
                .map(tokenInfo -> new Tuple<>(tokenInfo.getName(), tokenInfo.getSource().name().toLowerCase(Locale.ROOT)))
                .collect(Collectors.toSet()),
            equalTo(Stream.concat(clientInstance.getIndexTokenInfos().stream(),
                clientInstance.getNodesResponse().getFileTokenInfos().stream())
                .map(info -> new Tuple<>(info.getName(), info.getSource()))
                .collect(Collectors.toSet())));

        assertThat(
            serverTestInstance.getNodesResponse().failures().size(),
            equalTo(clientInstance.getNodesResponse().getHeader().getFailures().size()));
    }
}
