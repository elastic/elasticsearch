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
import org.elasticsearch.indices.SystemIndexThreadPoolTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class KibanaThreadPoolIT extends SystemIndexThreadPoolTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Set.of(KibanaPlugin.class);
    }

    public void testKibanaThreadPool() {
        List<String> kibanaSystemIndices = Stream.of(
            KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.REPORTING_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.APM_AGENT_CONFIG_INDEX_DESCRIPTOR.getIndexPattern(),
            KibanaPlugin.APM_CUSTOM_LINK_INDEX_DESCRIPTOR.getIndexPattern()
        ).map(s -> s.replace("*", randomAlphaOfLength(8).toLowerCase(Locale.ROOT))).toList();

        runWithBlockedThreadPools(() -> {
            for (String index : kibanaSystemIndices) {
                // index documents
                String idToDelete = client().prepareIndex(index).setSource(Map.of("foo", "delete me!")).get().getId();
                String idToUpdate = client().prepareIndex(index).setSource(Map.of("foo", "update me!")).get().getId();

                // bulk index, delete, and update
                Client bulkClient = client();
                BulkResponse response = bulkClient.prepareBulk(index)
                    .add(bulkClient.prepareIndex(index).setSource(Map.of("foo", "search me!")))
                    .add(bulkClient.prepareDelete(index, idToDelete))
                    .add(bulkClient.prepareUpdate().setId(idToUpdate).setDoc(Map.of("foo", "I'm updated!")))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
                assertNoFailures(response);

                // match-all search
                assertHitCount(client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), 2);
            }
        });
    }
}
