/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class TransportClientTest extends RandomizedTest {

    @Test
    public void testName() throws Exception {
        TransportAddress transportAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
        try (TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(transportAddress)) {
            ActionFuture<SearchResponse> future = client
                    .search(new SearchRequest().source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));

            SearchResponse get = future.actionGet(10, TimeUnit.SECONDS);
        }
    }
}
