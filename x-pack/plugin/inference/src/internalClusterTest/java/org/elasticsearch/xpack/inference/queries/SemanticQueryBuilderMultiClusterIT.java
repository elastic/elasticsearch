/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class SemanticQueryBuilderMultiClusterIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(InferencePlugin.class);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    private void createIndexOnAllClusters(String index) {
        for (String clusterAlias : clusters().keySet()) {
            assertAcked(client(clusterAlias).admin().indices().prepareCreate(index));
        }
    }

    public void testSemanticQueryFailsOnCCS() {
        final String indexName = "test_index";
        createIndexOnAllClusters(indexName);

        SearchRequestBuilder requestBuilder = client().prepareSearch(indexName, REMOTE_CLUSTER + ":" + indexName)
            .setQuery(new SemanticQueryBuilder("field", "query"));

        try {
            assertResponse(requestBuilder, r -> fail("Request should have failed"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("semantic query does not support cross-cluster search"));
        }
    }
}
