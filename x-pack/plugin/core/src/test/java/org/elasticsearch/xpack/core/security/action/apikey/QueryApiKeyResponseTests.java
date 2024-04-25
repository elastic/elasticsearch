/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class QueryApiKeyResponseTests extends ESTestCase {

    public void testMismatchApiKeyInfoAndProfileData() {
        List<ApiKey> apiKeys = randomList(
            0,
            3,
            () -> new ApiKey(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomFrom(ApiKey.Type.values()),
                Instant.now(),
                Instant.now(),
                randomBoolean(),
                null,
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                null,
                null,
                null,
                null
            )
        );
        List<Object[]> sortValues = new ArrayList<>(apiKeys.size());
        for (int i = 0; i < apiKeys.size(); i++) {
            sortValues.add(new String[] { "dummy sort value" });
        }
        List<String> profileUids = randomList(0, 5, () -> randomFrom(randomAlphaOfLength(4), null));
        if (apiKeys.size() != profileUids.size()) {
            IllegalStateException iae = expectThrows(
                IllegalStateException.class,
                () -> new QueryApiKeyResponse(100, apiKeys, sortValues, profileUids, null)
            );
            assertThat(iae.getMessage(), containsString("Each api key info must be associated to a (nullable) owner profile uid"));
        }
    }

    public void testMismatchApiKeyInfoAndSortValues() {
        List<ApiKey> apiKeys = randomList(
            0,
            3,
            () -> new ApiKey(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomFrom(ApiKey.Type.values()),
                Instant.now(),
                Instant.now(),
                randomBoolean(),
                null,
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                null,
                null,
                null,
                null
            )
        );
        List<String> profileUids = new ArrayList<>(apiKeys.size());
        for (int i = 0; i < apiKeys.size(); i++) {
            profileUids.add(randomFrom(randomAlphaOfLength(8), null));
        }
        List<Object[]> sortValues = randomList(0, 6, () -> new String[] { "dummy sort value" });
        if (apiKeys.size() != sortValues.size()) {
            IllegalStateException iae = expectThrows(
                IllegalStateException.class,
                () -> new QueryApiKeyResponse(100, apiKeys, sortValues, profileUids, null)
            );
            assertThat(iae.getMessage(), containsString("Each api key info must be associated to a (nullable) sort value"));
        }
    }
}
