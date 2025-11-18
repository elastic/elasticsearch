/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class CanMatchDataTierCoordinatorRewriteIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateCompositeXPackPlugin.class);
    }

    @Before
    public void setUpMasterNode() {
        internalCluster().startMasterOnlyNode();
    }

    public void testTierFiledCoordinatorRewrite() throws Exception {
        startHotOnlyNode();
        String warmNode = startWarmOnlyNode();
        ensureGreen();

        String hotIndex = "hot-index";
        String warmIndex = "warm-index";
        createIndexWithTierPreference(hotIndex, DataTier.DATA_HOT);
        createIndexWithTierPreference(warmIndex, DataTier.DATA_WARM);

        ensureGreen(hotIndex, warmIndex);
        // index 2 docs in the hot index and 1 doc in the warm index
        indexDoc(hotIndex, "1", "field", "value");
        indexDoc(hotIndex, "2", "field", "value");
        indexDoc(warmIndex, "3", "field2", "valuee");

        refresh(hotIndex, warmIndex);

        internalCluster().stopNode(warmNode);

        ensureRed(warmIndex);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(CoordinatorRewriteContext.TIER_FIELD_NAME, "data_hot"));

        final SearchRequest searchRequest = new SearchRequest();
        // we set the pre filter shard size to 1 automatically for mounted indices however,
        // we do have to explicitly make sure the can_match phase runs for hot/warm indices by lowering
        // the threshold for the pre filter shard size
        searchRequest.setPreFilterShardSize(1);
        searchRequest.indices(hotIndex, warmIndex);
        searchRequest.source(SearchSourceBuilder.searchSource().query(boolQueryBuilder));

        assertResponse(client().search(searchRequest), searchResponse -> {
            // we're only querying the hot tier which is available so we shouldn't get any failures
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            // we should be receiving the 2 docs from the index that's in the data_hot tier
            assertNotNull(searchResponse.getHits().getTotalHits());
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        });
    }

    public String startHotOnlyNode() {
        Settings.Builder nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_hot", "ingest"))
            .put("node.attr.box", "hot");

        return internalCluster().startNode(nodeSettings.build());
    }

    public String startWarmOnlyNode() {
        Settings.Builder nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_warm", "ingest"))
            .put("node.attr.box", "warm");

        return internalCluster().startNode(nodeSettings.build());
    }

    private void createIndexWithTierPreference(String indexName, String tierPreference) {

        indicesAdmin().prepareCreate(indexName)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder().put(DataTier.TIER_PREFERENCE, tierPreference).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0))
            .get();
    }
}
