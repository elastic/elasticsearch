/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.ttl;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope=Scope.TEST)
public class SimpleTTLTests extends ElasticsearchIntegrationTest {

    static private final long PURGE_INTERVAL = 200;
    
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("indices.ttl.interval", PURGE_INTERVAL)
                .put("index.number_of_shards", 2) // 2 shards to test TTL purge with routing properly
                .put("cluster.routing.operation.use_type", false) // make sure we control the shard computation
                .put("cluster.routing.operation.hash.type", "djb")
                .build();
    }

    @Test
    public void testSimpleTTL() throws Exception {

        client().admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                        .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                        .endObject()
                        .endObject())
                .addMapping("type2", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type2")
                        .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                        .startObject("_ttl").field("enabled", true).field("store", "yes").field("default", "1d").endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        long providedTTLValue = 3000;
        logger.info("--> checking ttl");
        // Index one doc without routing, one doc with routing, one doc with not TTL and no default and one doc with default TTL
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTTL(providedTTLValue).setRefresh(true).execute().actionGet();
        long now = System.currentTimeMillis();
        client().prepareIndex("test", "type1", "with_routing").setSource("field1", "value1").setTTL(providedTTLValue).setRouting("routing").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "no_ttl").setSource("field1", "value1").execute().actionGet();
        client().prepareIndex("test", "type2", "default_ttl").setSource("field1", "value1").execute().actionGet();

        // realtime get check
        long currentTime = System.currentTimeMillis();
        GetResponse getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        long ttl0;
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, greaterThan(-PURGE_INTERVAL));
            assertThat(ttl0, lessThan(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThan(0l));
        }
        // verify the ttl is still decreasing when going to the replica
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, greaterThan(-PURGE_INTERVAL));
            assertThat(ttl0, lessThan(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThan(0l));
        }
        // non realtime get (stored)
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, greaterThan(-PURGE_INTERVAL));
            assertThat(ttl0, lessThan(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThan(0l));
        }
        // non realtime get going the replica
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, greaterThan(-PURGE_INTERVAL));
            assertThat(ttl0, lessThan(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThan(0l));
        }

        // no TTL provided so no TTL fetched
        getResponse = client().prepareGet("test", "type1", "no_ttl").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.getField("_ttl"), nullValue());
        // no TTL provided make sure it has default TTL
        getResponse = client().prepareGet("test", "type2", "default_ttl").setFields("_ttl").setRealtime(true).execute().actionGet();
        ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl0, greaterThan(0L));

        // make sure the purger has done its job for all indexed docs that are expired
        long shouldBeExpiredDate = now + providedTTLValue + PURGE_INTERVAL + 2000;
        currentTime = System.currentTimeMillis();
        if (shouldBeExpiredDate - currentTime > 0) {
            Thread.sleep(shouldBeExpiredDate - currentTime);
        }

        // We can't assume that after waiting for ttl + purgeInterval (waitTime) that the document have actually been deleted.
        // The ttl purging happens in the background in a different thread, and might not have been completed after waiting for waitTime.
        // But we can use index statistics' delete count to be sure that deletes have been executed, that must be incremented before
        // ttl purging has finished.
        logger.info("--> checking purger");
        long currentDeleteCount;
        do {
            if (rarely()) {
                client().admin().indices().prepareFlush("test").setFull(true).execute().actionGet();
            } else if (rarely()) {
                client().admin().indices().prepareOptimize("test").setMaxNumSegments(1).execute().actionGet();
            }
            IndicesStatsResponse response = client().admin().indices().prepareStats("test")
                    .clear().setIndexing(true)
                    .execute().actionGet();
            currentDeleteCount = response.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount();
        } while (currentDeleteCount < 4); // TTL deletes two docs, but it is indexed in the primary shard and replica shard.
        assertThat(currentDeleteCount, equalTo(4l));

        // realtime get check
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        getResponse = client().prepareGet("test", "type1", "with_routing").setRouting("routing").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        // replica realtime get check
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        getResponse = client().prepareGet("test", "type1", "with_routing").setRouting("routing").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        // Need to run a refresh, in order for the non realtime get to work.
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        // non realtime get (stored) check
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        getResponse = client().prepareGet("test", "type1", "with_routing").setRouting("routing").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        // non realtime get going the replica check
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
        getResponse = client().prepareGet("test", "type1", "with_routing").setRouting("routing").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));
    }
}
