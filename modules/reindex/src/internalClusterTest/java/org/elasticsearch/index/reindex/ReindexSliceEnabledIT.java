/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies local reindex can paginate slice-enabled sources with point-in-time search.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexSliceEnabledIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    public void testReindexFromSliceEnabledSourceUsesPit() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assumeTrue(
            "reindex PIT search feature must be enabled on the cluster",
            internalCluster().getCurrentMasterNodeInstance(FeatureService.class)
                .clusterHasFeature(clusterService().state(), ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE)
        );

        String source = "slice-source";
        String destination = "reindex-destination";
        assertAcked(
            prepareCreate(source).setSettings(
                Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 1).put("number_of_replicas", 0)
            ).get()
        );
        assertAcked(prepareCreate(destination).setSettings(Settings.builder().put("number_of_replicas", 0)).get());

        client().index(new IndexRequest(source).id("1").routing("tenant-a").setRoutingFromSlice(true).source("value", "test")).get();
        indicesAdmin().prepareRefresh(source).get();

        BulkByPaginatedSearchResponse response = client().execute(
            ReindexAction.INSTANCE,
            new ReindexRequest().setSourceIndices(source).setDestIndex(destination)
        ).actionGet();

        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getCreated(), equalTo(1L));
        assertBusy(() -> assertHitCount(prepareSearch(destination).setSize(0), 1L));
    }
}
