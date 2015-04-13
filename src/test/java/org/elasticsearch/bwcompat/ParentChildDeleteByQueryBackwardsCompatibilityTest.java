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

package org.elasticsearch.bwcompat;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.core.Is.is;

/**
 */
public class ParentChildDeleteByQueryBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @BeforeClass
    public static void checkVersion() {
        assumeTrue("parent child queries in delete by query is forbidden from 1.1.2 and up", globalCompatibilityVersion().onOrBefore(Version.V_1_1_1));
    }

    @Override
    public void assertAllShardsOnNodes(String index, String pattern) {
        super.assertAllShardsOnNodes(index, pattern);
    }

    @Override
    protected Settings externalNodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.externalNodeSettings(nodeOrdinal))
                .put("index.translog.disable_flush", true)
                .build();
    }

    @Test
    public void testHasChild() throws Exception {
        assertAcked(prepareCreate("idx")
                .setSettings(ImmutableSettings.builder()
                                .put(indexSettings())
                                .put("index.refresh_interval", "-1")
                                .put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern())
                )
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("idx", "parent", "1").setSource("{}"));
        requests.add(client().prepareIndex("idx", "child", "1").setParent("1").setSource("{}"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("idx")
                .setQuery(hasChildQuery("child", matchAllQuery()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        client().prepareDeleteByQuery("idx")
                .setQuery(hasChildQuery("child", matchAllQuery()))
                .get();
        refresh();

        response = client().prepareSearch("idx")
                .setQuery(hasChildQuery("child", matchAllQuery()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 0);

        client().prepareIndex("idx", "type", "1").setSource("{}").get();
        assertThat(client().prepareGet("idx", "type", "1").get().isExists(), is(true));

        backwardsCluster().upgradeAllNodes();
        backwardsCluster().allowOnAllNodes("idx");
        ensureGreen("idx");

        response = client().prepareSearch("idx")
                .setQuery(hasChildQuery("child", matchAllQuery()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1); // The delete by query has failed on recovery so that parent doc is still there

        // But the rest of the recovery did execute, we just skipped over the delete by query with the p/c query.
        assertThat(client().prepareGet("idx", "type", "1").get().isExists(), is(true));
        response = client().prepareSearch("idx").setTypes("type").get();
        assertNoFailures(response);
        assertHitCount(response, 1);
    }

}
