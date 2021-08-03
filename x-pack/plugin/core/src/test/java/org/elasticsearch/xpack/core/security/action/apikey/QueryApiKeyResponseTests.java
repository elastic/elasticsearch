/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.elasticsearch.xpack.core.security.action.ApiKeyTests;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryApiKeyResponseTests extends AbstractWireSerializingTestCase<QueryApiKeyResponse> {

    @Override
    protected Writeable.Reader<QueryApiKeyResponse> instanceReader() {
        return QueryApiKeyResponse::new;
    }

    @Override
    protected QueryApiKeyResponse createTestInstance() {
        final List<ApiKey> apiKeys = randomList(0, 3, this::randomApiKeyInfo);
        return new QueryApiKeyResponse(randomIntBetween(apiKeys.size(), 100), apiKeys);
    }

    @Override
    protected QueryApiKeyResponse mutateInstance(QueryApiKeyResponse instance) throws IOException {
        final ArrayList<ApiKey> apiKeyInfos =
            Arrays.stream(instance.getApiKeyInfos()).collect(Collectors.toCollection(ArrayList::new));
        switch (randomIntBetween(0, 3)) {
            case 0:
                apiKeyInfos.add(randomApiKeyInfo());
                return new QueryApiKeyResponse(instance.getTotal(), apiKeyInfos);
            case 1:
                if (false == apiKeyInfos.isEmpty()) {
                    return new QueryApiKeyResponse(instance.getTotal(), apiKeyInfos.subList(1, apiKeyInfos.size()));
                } else {
                    apiKeyInfos.add(randomApiKeyInfo());
                    return new QueryApiKeyResponse(instance.getTotal(), apiKeyInfos);
                }
            case 2:
                if (false == apiKeyInfos.isEmpty()) {
                    final int index = randomIntBetween(0, apiKeyInfos.size() - 1);
                    apiKeyInfos.set(index, randomApiKeyInfo());
                } else {
                    apiKeyInfos.add(randomApiKeyInfo());
                }
                return new QueryApiKeyResponse(instance.getTotal(), apiKeyInfos);
            default:
                return new QueryApiKeyResponse(instance.getTotal() + 1, apiKeyInfos);
        }
    }

    private ApiKey randomApiKeyInfo() {
        final String name = randomAlphaOfLengthBetween(3, 8);
        final String id = randomAlphaOfLength(22);
        final String username = randomAlphaOfLengthBetween(3, 8);
        final String realm_name = randomAlphaOfLengthBetween(3, 8);
        final Instant creation = Instant.ofEpochMilli(randomMillisUpToYear9999());
        final Instant expiration = randomBoolean() ? Instant.ofEpochMilli(randomMillisUpToYear9999()) : null;
        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        return new ApiKey(name, id, creation, expiration, false, username, realm_name, metadata);
    }
}
