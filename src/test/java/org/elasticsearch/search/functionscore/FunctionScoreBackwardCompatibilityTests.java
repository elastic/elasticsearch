/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.functionscore;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

/**
 */
@TestLogging("index.translog.fs:TRACE")
public class FunctionScoreBackwardCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    /**
     * Simple upgrade test for function score
     */
    @Test
    @TestLogging("org.elasticsearch.index.gateway.local:TRACE")
    public void testSimpleFunctionScoreParsingWorks() throws IOException, ExecutionException, InterruptedException {

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("text")
                        .field("type", "string")
                        .endObject()
                        .startObject("loc")
                        .field("type", "geo_point")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()));
        ensureYellow();

        int numDocs = 10;
        String[] ids = new String[numDocs];
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            indexBuilders.add(client().prepareIndex()
                    .setType("type1").setId(id).setIndex("test")
                    .setSource(
                            jsonBuilder().startObject()
                                    .field("text", "value " + (i < 5 ? "boosted" : ""))
                                    .startObject("loc")
                                    .field("lat", 10 + i)
                                    .field("lon", 20)
                                    .endObject()
                                    .endObject()));
            ids[i] = id;
        }
        indexRandom(true, indexBuilders);
        checkFunctionScoreStillWorks(ids);
        logClusterState();
        // prevent any kind of allocation during the upgrade we recover from gateway
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "none")).get();
        boolean upgraded;
        int upgradedNodesCounter = 1;
        do {
            logger.debug("function_score bwc: upgrading {}st node", upgradedNodesCounter++);
            upgraded = backwardsCluster().upgradeOneNode();
            ensureYellow();
            logClusterState();
            checkFunctionScoreStillWorks(ids);
        } while (upgraded);
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "all")).get();
        logger.debug("done function_score while upgrading");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //enable scripting on the internal nodes
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal)).put("script.inline", "on").build();
    }

    @Override
    protected Settings externalNodeSettings(int nodeOrdinal) {
        //enable scripting on the external nodes using the proper setting depending on the bwc version
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(super.externalNodeSettings(nodeOrdinal));
        if (compatibilityVersion().before(Version.V_1_6_0)) {
            builder.put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, false);
        } else {
            builder.put("script.inline", "on");
        }
        return builder.build();
    }

    private void checkFunctionScoreStillWorks(String... ids) throws ExecutionException, InterruptedException, IOException {
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(termFilter("text", "value"))
                                        .add(gaussDecayFunction("loc", new GeoPoint(10, 20), "1000km"))
                                        .add(scriptFunction("_index['text']['value'].tf()"))
                                        .add(termFilter("text", "boosted"), factorFunction(5))
                        ))).actionGet();
        assertSearchResponse(response);
        assertOrderedSearchHits(response, ids);
    }
}
