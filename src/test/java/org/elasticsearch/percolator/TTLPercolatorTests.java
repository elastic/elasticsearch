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

package org.elasticsearch.percolator;

import com.google.common.base.Predicate;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.AlreadyExpiredException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.percolator.PercolatorTests.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class TTLPercolatorTests extends ElasticsearchIntegrationTest {

    private static final long PURGE_INTERVAL = 200;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("indices.ttl.interval", PURGE_INTERVAL)
                .build();
    }

    @Test
    public void testPercolatingWithTimeToLive() throws Exception {
        final Client client = client();
        ensureGreen();

        String percolatorMapping = XContentFactory.jsonBuilder().startObject().startObject(PercolatorService.TYPE_NAME)
                .startObject("_ttl").field("enabled", true).endObject()
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject().string();

        String typeMapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_ttl").field("enabled", true).endObject()
                .startObject("_timestamp").field("enabled", true).endObject()
                .startObject("properties").startObject("field1").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping(PercolatorService.TYPE_NAME, percolatorMapping)
                .addMapping("type1", typeMapping)
                .execute().actionGet();
        ensureGreen();

        final NumShards test = getNumShards("test");

        long ttl = 1500;
        long now = System.currentTimeMillis();
        client.prepareIndex("test", PercolatorService.TYPE_NAME, "kuku").setSource(jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("term")
                .field("field1", "value1")
                .endObject()
                .endObject()
                .endObject()
        ).setRefresh(true).setTTL(ttl).execute().actionGet();

        IndicesStatsResponse response = client.admin().indices().prepareStats("test")
                .clear().setIndexing(true)
                .execute().actionGet();
        assertThat(response.getIndices().get("test").getTotal().getIndexing().getTotal().getIndexCount(), equalTo((long)test.dataCopies));

        PercolateResponse percolateResponse = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("doc")
                        .field("field1", "value1")
                        .endObject()
                        .endObject()
                ).execute().actionGet();
        assertNoFailures(percolateResponse);
        if (percolateResponse.getMatches().length == 0) {
            // OK, ttl + purgeInterval has passed (slow machine or many other tests were running at the same time
            GetResponse getResponse = client.prepareGet("test", PercolatorService.TYPE_NAME, "kuku").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(false));
            response = client.admin().indices().prepareStats("test")
                    .clear().setIndexing(true)
                    .execute().actionGet();
            long currentDeleteCount = response.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount();
            assertThat(currentDeleteCount, equalTo((long)test.dataCopies));
            return;
        }

        assertThat(convertFromTextArray(percolateResponse.getMatches(), "test"), arrayContaining("kuku"));
        long timeSpent = System.currentTimeMillis() - now;
        long waitTime = ttl + PURGE_INTERVAL - timeSpent;
        if (waitTime >= 0) {
            Thread.sleep(waitTime); // Doesn't make sense to check the deleteCount before ttl has expired
        }

        // See comment in SimpleTTLTests
        logger.info("Checking if the ttl purger has run");
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats("test").clear().setIndexing(true).get();
                // TTL deletes one doc, but it is indexed in the primary shard and replica shards
                return indicesStatsResponse.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount() == test.dataCopies;
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));

        percolateResponse = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("doc")
                        .field("field1", "value1")
                        .endObject()
                        .endObject()
                ).execute().actionGet();
        assertMatchCount(percolateResponse, 0l);
        assertThat(percolateResponse.getMatches(), emptyArray());
    }


    @Test
    public void testEnsureTTLDoesNotCreateIndex() throws IOException, InterruptedException {
        ensureGreen();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put("indices.ttl.interval", 60) // 60 sec
                .build()).get();

        String typeMapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_ttl").field("enabled", true).endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1))
                .addMapping("type1", typeMapping)
                .execute().actionGet();
        ensureGreen();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder()
                .put("indices.ttl.interval", 1) // 60 sec
                .build()).get();

        for (int i = 0; i < 100; i++) {
            logger.debug("index doc {} ", i);
            try {
                client().prepareIndex("test", "type1", "" + i).setSource(jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term")
                        .field("field1", "value1")
                        .endObject()
                        .endObject()
                        .endObject()
                ).setTTL(randomIntBetween(1, 500)).execute().actionGet();
            } catch (MapperParsingException e) {
                logger.info("failed indexing {}", i, e);
                // if we are unlucky the TTL is so small that we see the expiry date is already in the past when
                // we parse the doc ignore those...
                assertThat(e.getCause(), Matchers.instanceOf(AlreadyExpiredException.class));
            }

        }
        refresh();
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").clear().setIndexing(true).get();
                logger.debug("delete count [{}]", indicesStatsResponse.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount());
                // TTL deletes one doc, but it is indexed in the primary shard and replica shards
                return indicesStatsResponse.getIndices().get("test").getTotal().getIndexing().getTotal().getDeleteCount() != 0;
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
        internalCluster().wipeIndices("test");
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", typeMapping)
                .execute().actionGet();


    }

}
