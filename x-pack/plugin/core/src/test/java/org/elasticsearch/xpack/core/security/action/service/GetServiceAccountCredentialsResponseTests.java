/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetServiceAccountCredentialsResponseTests extends AbstractWireSerializingTestCase<GetServiceAccountCredentialsResponse> {

    @Override
    protected Writeable.Reader<GetServiceAccountCredentialsResponse> instanceReader() {
        return GetServiceAccountCredentialsResponse::new;
    }

    @Override
    protected GetServiceAccountCredentialsResponse createTestInstance() {
        final String principal = randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final List<TokenInfo> tokenInfos = IntStream.range(0, randomIntBetween(0, 10))
            .mapToObj(i -> randomTokenInfo())
            .collect(Collectors.toUnmodifiableList());
        return new GetServiceAccountCredentialsResponse(principal, nodeName, tokenInfos);
    }

    @Override
    protected GetServiceAccountCredentialsResponse mutateInstance(GetServiceAccountCredentialsResponse instance) throws IOException {

        switch (randomIntBetween(0, 2)) {
            case 0:
                return new GetServiceAccountCredentialsResponse(randomValueOtherThan(instance.getPrincipal(),
                    () -> randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)),
                    instance.getNodeName(), instance.getTokenInfos());
            case 1:
                return new GetServiceAccountCredentialsResponse(instance.getPrincipal(),
                    randomValueOtherThan(instance.getNodeName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    instance.getTokenInfos());
            default:
                final ArrayList<TokenInfo> tokenInfos = new ArrayList<>(instance.getTokenInfos());
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        if (false == tokenInfos.isEmpty()) {
                            tokenInfos.remove(randomIntBetween(0, tokenInfos.size() - 1));
                        } else {
                            tokenInfos.add(randomTokenInfo());
                        }
                        break;
                    case 1:
                        tokenInfos.add(randomIntBetween(0, tokenInfos.isEmpty() ? 0 : tokenInfos.size() - 1), randomTokenInfo());
                        break;
                    default:
                        if (false == tokenInfos.isEmpty()) {
                            for (int i = 0; i < randomIntBetween(1, tokenInfos.size()); i++) {
                                final int j = randomIntBetween(0, tokenInfos.size() - 1);
                                tokenInfos.set(j, randomValueOtherThan(tokenInfos.get(j), this::randomTokenInfo));
                            }
                        } else {
                            tokenInfos.add(randomTokenInfo());
                        }
                }
                return new GetServiceAccountCredentialsResponse(instance.getPrincipal(), instance.getNodeName(),
                    tokenInfos.stream().collect(Collectors.toUnmodifiableList()));
        }
    }

    public void testEquals() {
        final GetServiceAccountCredentialsResponse response = createTestInstance();
        final ArrayList<TokenInfo> tokenInfos = new ArrayList<>(response.getTokenInfos());
        Collections.shuffle(tokenInfos, random());
        assertThat(new GetServiceAccountCredentialsResponse(
            response.getPrincipal(), response.getNodeName(), tokenInfos.stream().collect(Collectors.toUnmodifiableList())),
            equalTo(response));
    }

    public void testToXContent() throws IOException {
        final GetServiceAccountCredentialsResponse response = createTestInstance();
        final Map<String, TokenInfo> nameToTokenInfos = response.getTokenInfos().stream()
            .collect(Collectors.toMap(TokenInfo::getName, Function.identity()));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder),
            false, builder.contentType()).v2();

        assertThat(responseMap.get("service_account"), equalTo(response.getPrincipal()));
        assertThat(responseMap.get("node_name"), equalTo(response.getNodeName()));
        assertThat(responseMap.get("count"), equalTo(response.getTokenInfos().size()));
        @SuppressWarnings("unchecked")
        final Map<String, Object> tokens = (Map<String, Object>) responseMap.get("tokens");
        assertNotNull(tokens);
        tokens.keySet().forEach(k -> assertThat(nameToTokenInfos.remove(k).getSource(), equalTo(TokenInfo.TokenSource.INDEX)));

        @SuppressWarnings("unchecked")
        final Map<String, Object> fileTokens = (Map<String, Object>) responseMap.get("file_tokens");
        assertNotNull(fileTokens);
        fileTokens.keySet().forEach(k -> assertThat(nameToTokenInfos.remove(k).getSource(), equalTo(TokenInfo.TokenSource.FILE)));

        assertThat(nameToTokenInfos, is(anEmptyMap()));
    }

    private TokenInfo randomTokenInfo() {
        return randomBoolean() ?
            TokenInfo.fileToken(randomAlphaOfLengthBetween(3, 8)) :
            TokenInfo.indexToken(randomAlphaOfLengthBetween(3, 8));
    }
}
