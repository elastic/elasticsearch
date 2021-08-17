/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class QueryApiKeyResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse, QueryApiKeyResponse> {

    @Override
    protected org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse createServerTestInstance(XContentType xContentType) {
        final int count = randomIntBetween(0, 5);
        final int total = randomIntBetween(count, count + 5);
        final int nSortValues = randomIntBetween(0, 3);
        return new org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse(total,
            IntStream.range(0, count)
                .mapToObj(i -> new org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse.Item(
                    randomApiKeyInfo(),
                    randSortValues(nSortValues)))
                .collect(Collectors.toList()));
    }

    @Override
    protected QueryApiKeyResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return QueryApiKeyResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse serverTestInstance, QueryApiKeyResponse clientInstance) {
        assertThat(serverTestInstance.getTotal(), equalTo(clientInstance.getTotal()));
        assertThat(serverTestInstance.getCount(), equalTo(clientInstance.getCount()));
        for (int i = 0; i < serverTestInstance.getItems().length; i++) {
            assertApiKeyInfo(serverTestInstance.getItems()[i], clientInstance.getApiKeys().get(i));
        }
    }

    private void assertApiKeyInfo(
        org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse.Item serverItem, ApiKey clientApiKeyInfo) {
        assertThat(serverItem.getApiKey().getId(), equalTo(clientApiKeyInfo.getId()));
        assertThat(serverItem.getApiKey().getName(), equalTo(clientApiKeyInfo.getName()));
        assertThat(serverItem.getApiKey().getUsername(), equalTo(clientApiKeyInfo.getUsername()));
        assertThat(serverItem.getApiKey().getRealm(), equalTo(clientApiKeyInfo.getRealm()));
        assertThat(serverItem.getApiKey().getCreation(), equalTo(clientApiKeyInfo.getCreation()));
        assertThat(serverItem.getApiKey().getExpiration(), equalTo(clientApiKeyInfo.getExpiration()));
        assertThat(serverItem.getApiKey().getMetadata(), equalTo(clientApiKeyInfo.getMetadata()));
        assertThat(serverItem.getSortValues(), equalTo(clientApiKeyInfo.getSortValues()));
    }

    private org.elasticsearch.xpack.core.security.action.ApiKey randomApiKeyInfo() {
        final Instant creation = Instant.now();
        return new org.elasticsearch.xpack.core.security.action.ApiKey(randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLength(20),
            creation,
            randomFrom(creation.plus(randomLongBetween(1, 10), ChronoUnit.DAYS), null),
            randomBoolean(),
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            CreateApiKeyRequestTests.randomMetadata()
        );
    }

    private Object[] randSortValues(int nSortValues) {
        if (nSortValues > 0) {
            return randomArray(nSortValues, nSortValues, Object[]::new,
                () -> randomFrom(randomInt(Integer.MAX_VALUE), randomAlphaOfLength(8), randomBoolean()));
        } else {
            return null;
        }
    }
}
