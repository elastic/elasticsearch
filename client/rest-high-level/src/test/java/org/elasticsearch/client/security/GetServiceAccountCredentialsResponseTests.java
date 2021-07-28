/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;

import java.io.IOException;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountCredentialsResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse,
    GetServiceAccountCredentialsResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse(
            randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8), randomList(
            1,
            5,
            () -> randomBoolean() ?
                TokenInfo.fileToken(randomAlphaOfLengthBetween(3, 8)) :
                TokenInfo.indexToken(randomAlphaOfLengthBetween(3, 8)))
        );
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
        assertThat(serverTestInstance.getNodeName(), equalTo(clientInstance.getNodeName()));

        assertThat(
            serverTestInstance.getTokenInfos().stream()
                .map(tokenInfo -> new Tuple<>(tokenInfo.getName(), tokenInfo.getSource().name().toLowerCase(Locale.ROOT)))
                .collect(Collectors.toSet()),
            equalTo(clientInstance.getServiceTokenInfos().stream()
                .map(info -> new Tuple<>(info.getName(), info.getSource()))
                .collect(Collectors.toSet())));
    }
}
