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

package org.elasticsearch.river;

import com.google.common.base.Predicate;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.river.dummy.DummyRiverModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
@AwaitsFix(bugUrl="occasionally fails apparently due to synchronous mappings updates")
public class RiverTests extends ElasticsearchIntegrationTest {

    @Test
    public void testRiverStart() throws Exception {
        startAndCheckRiverIsStarted("dummy-river-test");
    }

    @Test
    public void testMultipleRiversStart() throws Exception {
        int nbRivers = between(2,10);
        logger.info("-->  testing with {} rivers...", nbRivers);
        Thread[] riverCreators = new Thread[nbRivers];
        final CountDownLatch latch = new CountDownLatch(nbRivers);
        final MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet();
        for (int i = 0; i < nbRivers; i++) {
            final String riverName = "dummy-river-test-" + i;
            riverCreators[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        startRiver(riverName);
                    } catch (Throwable t) {
                        logger.warn("failed to register river {}", t, riverName);
                    } finally {
                        latch.countDown();
                    }
                }
            };
            riverCreators[i].start();
            multiGetRequestBuilder.add(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName, "_status");
        }

        latch.await();

        logger.info("-->  checking that all rivers were created");
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object obj) {
                MultiGetResponse multiGetItemResponse = multiGetRequestBuilder.get();
                for (MultiGetItemResponse getItemResponse : multiGetItemResponse) {
                    if (getItemResponse.isFailed() || !getItemResponse.getResponse().isExists()) {
                        return false;
                    }
                }
                return true;
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

    /**
     * Test case for https://github.com/elasticsearch/elasticsearch/issues/4577
     * River does not start when using config/templates files
     */
    @Test
    public void startDummyRiverWithDefaultTemplate() throws Exception {
        logger.info("--> create empty template");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("*")
                .setOrder(0)
                .addMapping(MapperService.DEFAULT_MAPPING,
                        JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                                .endObject().endObject())
                .get();

        startAndCheckRiverIsStarted("dummy-river-default-template-test");
    }

    /**
     * Test case for https://github.com/elasticsearch/elasticsearch/issues/4577
     * River does not start when using config/templates files
     */
    @Test
    public void startDummyRiverWithSomeTemplates() throws Exception {
        logger.info("--> create some templates");
        client().admin().indices().preparePutTemplate("template_1")
                .setTemplate("*")
                .setOrder(0)
                .addMapping(MapperService.DEFAULT_MAPPING,
                        JsonXContent.contentBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                                .endObject().endObject())
                .get();
        client().admin().indices().preparePutTemplate("template_2")
                .setTemplate("*")
                .setOrder(0)
                .addMapping("atype",
                        JsonXContent.contentBuilder().startObject().startObject("atype")
                                .endObject().endObject())
                .get();

        startAndCheckRiverIsStarted("dummy-river-template-test");
    }

    /**
     * Create a Dummy river then check it has been started. We will fail after 5 seconds.
     * @param riverName Dummy river needed to be started
     */
    private void startAndCheckRiverIsStarted(final String riverName) throws InterruptedException {
        startRiver(riverName);
        checkRiverIsStarted(riverName);
    }

    private void startRiver(final String riverName) {
        logger.info("-->  starting river [{}]", riverName);
        IndexResponse indexResponse = client().prepareIndex(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName, "_meta")
                .setSource("type", DummyRiverModule.class.getCanonicalName()).get();
        assertTrue(indexResponse.isCreated());
        ensureGreen();
    }

    private void checkRiverIsStarted(final String riverName) throws InterruptedException {
        logger.info("-->  checking that river [{}] was created", riverName);
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object obj) {
                GetResponse response = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, riverName, "_status").get();
                return response.isExists();
            }
        }, 5, TimeUnit.SECONDS), equalTo(true));
    }

}
