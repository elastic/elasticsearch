/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Checks that {@link ApiKeyAuthenticationBenchmark} measures what it claims to: successful API key authentications that are
 * served from the doc cache and complete on the calling thread.
 */
public class ApiKeyAuthenticationBenchmarkTests extends ESTestCase {

    public void testAuthenticatesEveryApiKeyFromTheDocCache() throws Exception {
        final ApiKeyAuthenticationBenchmark benchmark = new ApiKeyAuthenticationBenchmark();
        benchmark.numApiKeys = between(1, 100);
        benchmark.setup();
        try {
            final long docCacheMisses = benchmark.docCacheMisses();

            final ApiKeyAuthenticationBenchmark.ResultListener listener = new ApiKeyAuthenticationBenchmark.ResultListener();
            for (int apiKey = 0; apiKey < benchmark.numApiKeys; apiKey++) {
                final User user = benchmark.authenticateApiKey(apiKey, listener).getValue().v1();
                assertThat(user.principal(), equalTo(ApiKeyAuthenticationBenchmark.principal(apiKey)));
            }

            final ApiKeyAuthenticationBenchmark.PerThread perThread = new ApiKeyAuthenticationBenchmark.PerThread();
            perThread.init(benchmark.numApiKeys, randomLong());
            final Set<Integer> apiKeys = new HashSet<>();
            for (int i = 0; i < benchmark.numApiKeys; i++) {
                apiKeys.add(perThread.nextApiKey());
            }
            assertThat(apiKeys.size(), equalTo(benchmark.numApiKeys));
            for (int i = 0; i < 2 * benchmark.numApiKeys; i++) {
                benchmark.authenticate(perThread);
            }

            assertThat(benchmark.docCacheMisses(), equalTo(docCacheMisses));
        } finally {
            benchmark.teardown();
        }
    }
}
