/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.kibana;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexThreadPoolTests;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class KibanaThreadPoolTests extends SystemIndexThreadPoolTests {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Set.of(KibanaPlugin.class);
    }

    public void testKibanaThreadPool() {
        runWithBlockedThreadPools(() -> {
            // index documents
            String idToDelete = client().prepareIndex(".kibana").setSource(Map.of("foo", "delete me!")).get().getId();
            String idToUpdate = client().prepareIndex(".kibana").setSource(Map.of("foo", "update me!")).get().getId();

            // bulk index, delete, and update
            Client bulkClient = client();
            BulkResponse response = bulkClient.prepareBulk(".kibana")
                .add(bulkClient.prepareIndex(".kibana").setSource(Map.of("foo", "search me!")))
                .add(bulkClient.prepareDelete(".kibana", idToDelete))
                .add(bulkClient.prepareUpdate().setId(idToUpdate).setDoc(Map.of("foo", "I'm updated!")))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertNoFailures(response);

            // match-all search
            assertHitCount(client().prepareSearch(".kibana").setQuery(QueryBuilders.matchAllQuery()), 2);
        });
    }
}
