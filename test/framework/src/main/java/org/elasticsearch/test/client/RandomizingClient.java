/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.client;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** A {@link Client} that randomizes request parameters. */
public class RandomizingClient extends FilterClient {

    private final SearchType defaultSearchType;
    private final String defaultPreference;
    private final int batchedReduceSize;
    private final int maxConcurrentShardRequests;
    private final int preFilterShardSize;
    private final boolean doTimeout;

    public RandomizingClient(Client client, Random random) {
        super(client);
        defaultSearchType = RandomPicks.randomFrom(random, Arrays.asList(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        if (random.nextInt(10) == 0) {
            defaultPreference = Preference.LOCAL.type();
        } else if (random.nextInt(10) == 0) {
            String s = TestUtil.randomRealisticUnicodeString(random, 1, 10);
            defaultPreference = s.startsWith("_") ? null : s; // '_' is a reserved character
        } else {
            defaultPreference = null;
        }
        this.batchedReduceSize = 2 + random.nextInt(10);
        if (random.nextBoolean()) {
            this.maxConcurrentShardRequests = 1 + random.nextInt(1 << random.nextInt(8));
        } else {
            this.maxConcurrentShardRequests = -1; // randomly use the default
        }
        if (random.nextBoolean()) {
            preFilterShardSize = 1 + random.nextInt(1 << random.nextInt(7));
        } else {
            preFilterShardSize = -1;
        }
        doTimeout = random.nextBoolean();
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        SearchRequestBuilder searchRequestBuilder = in.prepareSearch(indices)
            .setSearchType(defaultSearchType)
            .setPreference(defaultPreference)
            .setBatchedReduceSize(batchedReduceSize);
        if (maxConcurrentShardRequests != -1) {
            searchRequestBuilder.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }
        if (preFilterShardSize != -1) {
            searchRequestBuilder.setPreFilterShardSize(preFilterShardSize);
        }
        if (doTimeout) {
            searchRequestBuilder.setTimeout(new TimeValue(1, TimeUnit.DAYS));
        }
        return searchRequestBuilder;
    }

    @Override
    public String toString() {
        return "randomized(" + super.toString() + ")";
    }

    public Client in() {
        return super.in();
    }

}
