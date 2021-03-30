/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PointInTimeSearchHelperSingleNodeTests extends ESSingleNodeTestCase {

    public void testFetchAll() {
        Client client = client();
        int numDocs = randomIntBetween(5, 30);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex("foo").setSource(Collections.singletonMap("number", i)).get();
        }
        client.admin().indices().prepareRefresh("foo").get();
        SearchRequest request = client.prepareSearch()
            .setIndices("foo")
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(randomIntBetween(1, 10))
            .setFetchSource(true)
            .request();
        if (randomBoolean()) {
            request.source().sort("number");
        }
        request.indicesOptions().ignoreUnavailable();
        PlainActionFuture<Collection<Integer>> future = new PlainActionFuture<>();
        PointInTimeSearchHelper.fetchAll(client(), request,
            (hit) -> Integer.parseInt(hit.getSourceAsMap().get("number").toString()),
            future);
        Collection<Integer> integers = future.actionGet();
        List<Integer> list = new ArrayList<>(integers);
        CollectionUtil.timSort(list);
        assertEquals(numDocs, list.size());
        for (int i = 0; i < numDocs; i++) {
            assertEquals(list.get(i).intValue(), i);
        }
    }
}
