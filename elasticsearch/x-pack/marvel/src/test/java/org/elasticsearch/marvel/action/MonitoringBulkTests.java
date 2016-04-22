/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.resolver.bulk.MonitoringBulkResolver;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@TestLogging("_root:DEBUG")
public class MonitoringBulkTests extends MarvelIntegTestCase {

    @Override
    protected Settings transportClientSettings() {
        return super.transportClientSettings();
    }

    public void testMonitoringBulkIndexing() throws Exception {
        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();
        String[] types = {"type1", "type2", "type3"};

        int numDocs = scaledRandomIntBetween(100, 5000);
        for (int i = 0; i < numDocs; i++) {
            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), Version.CURRENT.toString());
            doc.setType(randomFrom(types));
            doc.setSource(jsonBuilder().startObject().field("num", numDocs).endObject().bytes());
            requestBuilder.add(doc);
        }

        MonitoringBulkResponse response = requestBuilder.get();
        assertThat(response.getError(), is(nullValue()));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setTypes(types).setSize(numDocs).get();
        assertHitCount(searchResponse, numDocs);

        for (SearchHit searchHit : searchResponse.getHits()) {
            Map<String, Object> source = searchHit.sourceAsMap();
            assertNotNull(source.get(MonitoringBulkResolver.Fields.CLUSTER_UUID));
            assertNotNull(source.get(MonitoringBulkResolver.Fields.TIMESTAMP));
            assertNotNull(source.get(MonitoringBulkResolver.Fields.SOURCE_NODE));
        }
    }

    /**
     * This test creates N threads that execute a random number of monitoring bulk requests.
     */
    public void testConcurrentRequests() throws Exception {
        final int numberThreads = randomIntBetween(3, 5);
        final Thread[] threads = new Thread[numberThreads];
        final CountDownLatch latch = new CountDownLatch(numberThreads + 1);
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        AtomicLong total = new AtomicLong(0);

        logger.info("--> using {} concurrent clients to execute requests", threads.length);
        for (int i = 0; i < threads.length; i++) {
            final int nbRequests = randomIntBetween(1, 5);

            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error in exporting thread", t);
                    exceptions.add(t);
                }

                @Override
                protected void doRun() throws Exception {
                    latch.countDown();
                    latch.await();
                    for (int j = 0; j < nbRequests; j++) {
                        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();

                        int numDocs = scaledRandomIntBetween(10, 50);
                        for (int k = 0; k < numDocs; k++) {
                            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), Version.CURRENT.toString());
                            doc.setType("concurrent");
                            doc.setSource(jsonBuilder().startObject().field("num", k).endObject().bytes());
                            requestBuilder.add(doc);
                        }

                        total.addAndGet(numDocs);
                        MonitoringBulkResponse response = requestBuilder.get();
                        assertNull (response.getError());
                    }
                }
            }, "export_thread_" + i);
            threads[i].start();
        }

        // wait for all threads to be ready
        latch.countDown();
        latch.await();

        // wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(exceptions, empty());
        awaitMarvelDocsCount(greaterThanOrEqualTo(total.get()), "concurrent");
    }

    public void testUnsupportedSystem() throws Exception {
        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();
        String[] types = {"type1", "type2", "type3"};

        int totalDocs = randomIntBetween(10, 1000);
        int unsupportedDocs = 0;

        for (int i = 0; i < totalDocs; i++) {
            MonitoringBulkDoc doc;
            if (randomBoolean()) {
                doc = new MonitoringBulkDoc("unknown", Version.CURRENT.toString());
                unsupportedDocs++;
            } else {
                doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), Version.CURRENT.toString());
            }
            doc.setType(randomFrom(types));
            doc.setSource(jsonBuilder().startObject().field("num", i).endObject().bytes());
            requestBuilder.add(doc);
        }

        MonitoringBulkResponse response = requestBuilder.get();
        if (unsupportedDocs == 0) {
            assertThat(response.getError(), is(nullValue()));
        } else {
            assertThat(response.getError(), is(notNullValue()));
        }
        refresh();

        SearchResponse countResponse = client().prepareSearch().setTypes(types).setSize(0).get();
        assertHitCount(countResponse, totalDocs - unsupportedDocs);
    }
}
