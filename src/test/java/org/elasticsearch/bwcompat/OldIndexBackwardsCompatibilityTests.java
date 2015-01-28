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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.action.admin.indices.upgrade.UpgradeTest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.index.merge.NoMergePolicyProvider;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.hamcrest.Matchers;

import java.lang.reflect.Modifier;
import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class OldIndexBackwardsCompatibilityTests extends StaticIndexBackwardCompatibilityTest {
    
    List<String> indexes = Arrays.asList(
        "index-0.20.6.zip",
        "index-0.90.0.Beta1.zip",
        "index-0.90.0.RC1.zip",
        "index-0.90.0.RC2.zip",
        "index-0.90.0.zip",
        "index-0.90.1.zip",
        "index-0.90.2.zip",
        "index-0.90.3.zip",
        "index-0.90.4.zip",
        "index-0.90.5.zip",
        "index-0.90.6.zip",
        "index-0.90.7.zip",
        "index-0.90.8.zip",
        "index-0.90.9.zip",
        "index-0.90.10.zip",
        "index-0.90.11.zip",
        "index-0.90.12.zip",
        "index-0.90.13.zip",
        "index-1.0.0.Beta1.zip",
        "index-1.0.0.Beta2.zip",
        "index-1.0.0.RC1.zip",
        "index-1.0.0.RC2.zip",
        "index-1.0.0.zip",
        "index-1.0.1.zip",
        "index-1.0.2.zip",
        "index-1.0.3.zip",
        "index-1.1.0.zip",
        "index-1.1.1.zip",
        "index-1.1.2.zip",
        "index-1.2.0.zip",
        "index-1.2.1.zip",
        "index-1.2.2.zip",
        "index-1.2.3.zip",
        "index-1.2.4.zip",
        "index-1.3.0.zip",
        "index-1.3.1.zip",
        "index-1.3.2.zip",
        "index-1.3.3.zip",
        "index-1.3.4.zip",
        "index-1.3.5.zip",
        "index-1.3.6.zip",
        "index-1.3.7.zip",
        "index-1.4.0.Beta1.zip",
        "index-1.4.0.zip",
        "index-1.4.1.zip",
        "index-1.4.2.zip"
    );
    
    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (java.lang.reflect.Field field : Version.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
                Version v = (Version)field.get(Version.class);
                if (v.snapshot()) continue;
                if (v.onOrBefore(Version.V_0_20_5)) continue;

                expectedVersions.add("index-" + v.toString() + ".zip");
            }
        }
        
        for (String index : indexes) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: " + index);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old index tests are missing indexes:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    public void testOldIndexes() throws Exception {
        Collections.shuffle(indexes, getRandom());
        for (String index : indexes) {
            logger.info("Testing old index " + index);
            assertOldIndexWorks(index);
        }
    }

    void assertOldIndexWorks(String index) throws Exception {
        Settings settings = ImmutableSettings.builder()
            .put(InternalNode.HTTP_ENABLED, true) // for _upgrade
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class) // disable merging so no segments will be upgraded
                .build();
        loadIndex(index, settings);
        logMemoryStats();
        assertBasicSearchWorks();
        assertRealtimeGetWorks();
        assertNewReplicasWork();
        assertUpgradeWorks(isLatestLuceneVersion(index));
        unloadIndex();
    }
    
    Version extractVersion(String index) {
        return Version.fromString(index.substring(index.indexOf('-') + 1, index.lastIndexOf('.')));
    }
    
    boolean isLatestLuceneVersion(String index) {
        Version version = extractVersion(index);
        return version.luceneVersion.major == Version.CURRENT.luceneVersion.major &&
               version.luceneVersion.minor == Version.CURRENT.luceneVersion.minor;
    }

    void assertBasicSearchWorks() {
        SearchRequestBuilder searchReq = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        long numDocs = searchRsp.getHits().getTotalHits();
        logger.info("Found " + numDocs + " in old index");
        
        searchReq.addSort("long_sort", SortOrder.ASC);
        ElasticsearchAssertions.assertNoFailures(searchReq.get());
    }

    void assertRealtimeGetWorks() {
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder()
            .put("refresh_interval", -1)
            .build()));
        SearchRequestBuilder searchReq = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery());
        SearchHit hit = searchReq.get().getHits().getAt(0);
        String docId = hit.getId();
        // foo is new, it is not a field in the generated index
        client().prepareUpdate("test", "doc", docId).setDoc("foo", "bar").get();
        GetResponse getRsp = client().prepareGet("test", "doc", docId).get();
        Map<String, Object> source = getRsp.getSourceAsMap();
        assertThat(source, Matchers.hasKey("foo"));

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder()
            .put("refresh_interval", "1s")
            .build()));
    }

    void assertNewReplicasWork() throws Exception {
        final int numReplicas = randomIntBetween(2, 3);
        for (int i = 0; i < numReplicas; ++i) {
            logger.debug("Creating another node for replica " + i);
            internalCluster().startNode(ImmutableSettings.builder()
                .put("data.node", true)
                .put("master.node", false)
                .put(InternalNode.HTTP_ENABLED, true) // for _upgrade
                .build());
        }
        client().admin().cluster().prepareHealth("test").setWaitForNodes("" + (numReplicas + 1));
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder()
            .put("number_of_replicas", numReplicas)).execute().actionGet());
        // This can take a while when the number of replicas is greater than cluster.routing.allocation.node_concurrent_recoveries
        // (which defaults to 2).  We could override that setting, but running this test on a busy box could
        // still result in taking a long time to finish starting replicas, so instead we have an increased timeout
        ensureGreen(TimeValue.timeValueMinutes(1), "test");

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder()
            .put("number_of_replicas", 0))
            .execute().actionGet());
        waitNoPendingTasksOnAll(); // make sure the replicas are removed before going on
    }
    
    void assertUpgradeWorks(boolean alreadyLatest) throws Exception {
        HttpRequestBuilder httpClient = httpClient();

        if (alreadyLatest == false) {
            UpgradeTest.assertNotUpgraded(httpClient, "test");
        }
        UpgradeTest.runUpgrade(httpClient, "test", "wait_for_completion", "true");
        UpgradeTest.assertUpgraded(httpClient, "test");
    }
}
