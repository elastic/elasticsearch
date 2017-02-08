/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class InternalClientIntegTests extends ESSingleNodeTestCase {

    public void testFetchAllEntities() throws ExecutionException, InterruptedException {
        Client client = client();
        int numDocs = randomIntBetween(5, 30);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex("foo", "bar").setSource(Collections.singletonMap("number", i)).get();
        }
        client.admin().indices().prepareRefresh("foo").get();
        SearchRequest request = client.prepareSearch()
                .setScroll(TimeValue.timeValueHours(10L))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(randomIntBetween(1, 10))
                .setFetchSource(true)
                .request();
        request.indicesOptions().ignoreUnavailable();
        PlainActionFuture<Collection<Integer>> future = new PlainActionFuture<>();
        InternalClient.fetchAllByEntity(client(), request, future,
                (hit) -> Integer.parseInt(hit.getSourceAsMap().get("number").toString()));
        Collection<Integer> integers = future.actionGet();
        ArrayList<Integer> list = new ArrayList<>(integers);
        CollectionUtil.timSort(list);
        assertEquals(numDocs, list.size());
        for (int i = 0; i < numDocs; i++) {
            assertEquals(list.get(i).intValue(), i);
        }
    }
}
