/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link Authentication} is immutable, and every inbound transport request decodes its own copy
 * from the request header. Each copy is then retained for the life of that request by the thread
 * context captured into the outbound response handler, so N concurrent requests from one client
 * hold N private copies of an identical value.
 *
 * <p>Observed on a serverless index node that OOM'd: 14,151 {@code Authentication} objects
 * retaining 176MB across only <em>seven</em> distinct principals. The dominant one was a
 * Fleet-server API key retaining 13,616 bytes per copy, because an API key carries its role
 * descriptors inline in metadata.
 *
 * <p>Both tests assert the behaviour we want, so they fail on current main and pass once
 * {@code decode} shares instances for identical headers.
 */
public class AuthenticationDecodeSharingTests extends ESTestCase {

    /** Shaped like a Fleet-server key: role descriptors serialised into metadata. */
    private static String encodedApiKeyHeader(int descriptorBytes) throws IOException {
        final BytesArray descriptors = new BytesArray("{\"role\":{\"indices\":[\"" + "x".repeat(descriptorBytes) + "\"]}}");
        return AuthenticationTestHelper.builder()
            .apiKey("fleet-server-key")
            .metadata(
                Map.of(
                    AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
                    descriptors,
                    AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    descriptors
                )
            )
            .build(false)
            .encode();
    }

    public void testIdenticalHeadersDecodeToTheSameInstance() throws Exception {
        final String header = encodedApiKeyHeader(512);

        assertThat(AuthenticationContextSerializer.decode(header), sameInstance(AuthenticationContextSerializer.decode(header)));
    }

    public void testRetainedHeapDoesNotScaleWithConcurrentRequests() throws Exception {
        final String header = encodedApiKeyHeader(4096);
        final int inFlightRequests = 100;

        final Authentication[] retained = new Authentication[inFlightRequests];
        for (int i = 0; i < inFlightRequests; i++) {
            retained[i] = AuthenticationContextSerializer.decode(header);
        }

        // RamUsageTester counts each distinct object once, so a shared instance is charged
        // once no matter how many slots reference it.
        final long retainedByAll = RamUsageTester.ramUsed(retained);
        final long retainedByOne = RamUsageTester.ramUsed(AuthenticationContextSerializer.decode(header));

        // One identity in flight on N requests should cost one copy, not N.
        assertThat(retainedByAll, lessThan(retainedByOne * 2));
    }
}
