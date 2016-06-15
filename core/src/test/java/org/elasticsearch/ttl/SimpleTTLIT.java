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

package org.elasticsearch.ttl;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope= Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class SimpleTTLIT extends ESIntegTestCase {

    static private final long PURGE_INTERVAL = 200;

    @Override
    protected int numberOfShards() {
        return 2;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("indices.ttl.interval", PURGE_INTERVAL, TimeUnit.MILLISECONDS)
                .build();
    }

    public void testSimpleTTL() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).endObject()
                        .endObject()
                        .endObject())
                .addMapping("type2", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type2")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).field("default", "1d").endObject()
                        .endObject()
                        .endObject()));
        ensureYellow("test");

        final NumShards test = getNumShards("test");

        long providedTTLValue = 3000;
        logger.info("--> checking ttl");
        // Index one doc without routing, one doc with routing, one doc with not TTL and no default and one doc with default TTL
        long now = System.currentTimeMillis();
        IndexResponse indexResponse = client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setTimestamp(String.valueOf(now)).setTTL(providedTTLValue).setRefreshPolicy(IMMEDIATE).get();
        assertThat(indexResponse.isCreated(), is(true));
        indexResponse = client().prepareIndex("test", "type1", "with_routing").setSource("field1", "value1")
                .setTimestamp(String.valueOf(now)).setTTL(providedTTLValue).setRouting("routing").setRefreshPolicy(IMMEDIATE).get();
        assertThat(indexResponse.isCreated(), is(true));
        indexResponse = client().prepareIndex("test", "type1", "no_ttl").setSource("field1", "value1").get();
        assertThat(indexResponse.isCreated(), is(true));
        indexResponse = client().prepareIndex("test", "type2", "default_ttl").setSource("field1", "value1").get();
        assertThat(indexResponse.isCreated(), is(true));

        // realtime get check
        long currentTime = System.currentTimeMillis();
        GetResponse getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").get();
        long ttl0;
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, lessThanOrEqualTo(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThanOrEqualTo(0L));
        }
        // verify the ttl is still decreasing when going to the replica
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").get();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, lessThanOrEqualTo(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThanOrEqualTo(0L));
        }
        // non realtime get (stored)
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).get();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, lessThanOrEqualTo(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThanOrEqualTo(0L));
        }
        // non realtime get going the replica
        currentTime = System.currentTimeMillis();
        getResponse = client().prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).get();
        if (getResponse.isExists()) {
            ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
            assertThat(ttl0, lessThanOrEqualTo(providedTTLValue - (currentTime - now)));
        } else {
            assertThat(providedTTLValue - (currentTime - now), lessThanOrEqualTo(0L));
        }

        // no TTL provided so no TTL fetched
        getResponse = client().prepareGet("test", "type1", "no_ttl").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.getField("_ttl"), nullValue());
        // no TTL provided make sure it has default TTL
        getResponse = client().prepareGet("test", "type2", "default_ttl").setFields("_ttl").setRealtime(true).execute().actionGet();
        ttl0 = ((Number) getResponse.getField("_ttl").getValue()).longValue();
        assertThat(ttl0, greaterThan(0L));

        IndicesStatsResponse response = client().admin().indices().prepareStats("test").clear().setIndexing(true).get();
        assertThat(response.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount(), equalTo(0L));

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
        assertTrue(awaitBusy(() -> {
                    if (rarely()) {
                        client().admin().indices().prepareFlush("test").get();
                    } else if (rarely()) {
                        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();
                    }
                    IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").clear().setIndexing(true).get();
                    // TTL deletes two docs, but it is indexed in the primary shard and replica shard.
                    return indicesStatsResponse.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount() == 2L * test.dataCopies;
                },
                5, TimeUnit.SECONDS
        ));

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

    // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveTTLConfiguration() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_ttl").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertTTLMappingEnabled(index, type);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("properties").startObject("otherField").field("type", "text").endObject().endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure timestamp field is still in mapping
        assertTTLMappingEnabled(index, type);
    }

    /**
     * Test that updates with detect_noop set to true (the default) that don't
     * change the source don't change the ttl. This is unexpected behavior and
     * documented in ttl-field.asciidoc. If this behavior changes it is safe to
     * rewrite this test to reflect the new behavior and to change the
     * documentation.
     */
    public void testNoopUpdate() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).endObject()
                        .startObject("_ttl").field("enabled", true).endObject()
                        .endObject()
                        .endObject()));
        ensureYellow("test");

        long aLongTime = 10000000;
        long firstTtl = aLongTime * 3;
        long secondTtl = aLongTime * 2;
        long thirdTtl = aLongTime * 1;
        IndexResponse indexResponse = client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setTTL(firstTtl).setRefreshPolicy(IMMEDIATE).get();
        assertTrue(indexResponse.isCreated());
        assertThat(getTtl("type1", 1), both(lessThanOrEqualTo(firstTtl)).and(greaterThan(secondTtl)));

        // Updating with the default detect_noop without a change to the document doesn't change the ttl.
        UpdateRequestBuilder update = client().prepareUpdate("test", "type1", "1").setDoc("field1", "value1").setTtl(secondTtl);
        assertThat(updateAndGetTtl(update), both(lessThanOrEqualTo(firstTtl)).and(greaterThan(secondTtl)));

        // Updating with the default detect_noop with a change to the document does change the ttl.
        update = client().prepareUpdate("test", "type1", "1").setDoc("field1", "value2").setTtl(secondTtl);
        assertThat(updateAndGetTtl(update), both(lessThanOrEqualTo(secondTtl)).and(greaterThan(thirdTtl)));

        // Updating with detect_noop=true without a change to the document doesn't change the ttl.
        update = client().prepareUpdate("test", "type1", "1").setDoc("field1", "value2").setTtl(secondTtl).setDetectNoop(true);
        assertThat(updateAndGetTtl(update), both(lessThanOrEqualTo(secondTtl)).and(greaterThan(thirdTtl)));

        // Updating with detect_noop=false without a change to the document does change the ttl.
        update = client().prepareUpdate("test", "type1", "1").setDoc("field1", "value2").setTtl(thirdTtl).setDetectNoop(false);
        assertThat(updateAndGetTtl(update), lessThanOrEqualTo(thirdTtl));
    }

    private long updateAndGetTtl(UpdateRequestBuilder update) {
        UpdateResponse updateResponse = update.setFields("_ttl").get();
        assertThat(updateResponse.getShardInfo().getFailed(), equalTo(0));
        // You can't actually fetch _ttl from an update so we use a get.
        return getTtl(updateResponse.getType(), updateResponse.getId());
    }

    private long getTtl(String type, Object id) {
        GetResponse getResponse = client().prepareGet("test", type, id.toString()).setFields("_ttl").setRealtime(true).execute()
                .actionGet();
        return ((Number) getResponse.getField("_ttl").getValue()).longValue();
    }

    private void assertTTLMappingEnabled(String index, String type) throws IOException {
        String errMsg = String.format(Locale.ROOT, "Expected ttl field mapping to be enabled for %s/%s", index, type);

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).addTypes(type).get();
        Map<String, Object> mappingSource = getMappingsResponse.getMappings().get(index).get(type).getSourceAsMap();
        assertThat(errMsg, mappingSource, hasKey("_ttl"));
        String ttlAsString = mappingSource.get("_ttl").toString();
        assertThat(ttlAsString, is(notNullValue()));
        assertThat(errMsg, ttlAsString, is("{enabled=true}"));
    }
}
