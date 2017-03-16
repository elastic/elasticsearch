/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.bulk.MonitoringBulkTimestampedResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;

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

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/353")
public class MonitoringBulkTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(NetworkModule.HTTP_ENABLED.getKey(), securityEnabled).build();
    }

    @Override
    protected Settings transportClientSettings() {
        return super.transportClientSettings();
    }

    public void testMonitoringBulkIndexing() throws Exception {
        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();
        String[] types = {"type1", "type2", "type3"};

        int numDocs = scaledRandomIntBetween(100, 5000);
        for (int i = 0; i < numDocs; i++) {
            MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), MonitoringTemplateUtils.TEMPLATE_VERSION);
            doc.setType(randomFrom(types));
            doc.setSource(jsonBuilder().startObject().field("num", numDocs).endObject().bytes(), XContentType.JSON);
            requestBuilder.add(doc);
        }

        MonitoringBulkResponse response = requestBuilder.get();
        assertThat(response.getError(), is(nullValue()));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setTypes(types).setSize(numDocs).get();
        assertHitCount(searchResponse, numDocs);

        for (SearchHit searchHit : searchResponse.getHits()) {
            Map<String, Object> source = searchHit.getSourceAsMap();
            assertNotNull(source.get(MonitoringBulkTimestampedResolver.Fields.CLUSTER_UUID));
            assertNotNull(source.get(MonitoringBulkTimestampedResolver.Fields.TIMESTAMP));
            assertNotNull(source.get(MonitoringBulkTimestampedResolver.Fields.SOURCE_NODE));
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
                public void onFailure(Exception e) {
                    logger.error("unexpected error in exporting thread", e);
                    exceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    latch.countDown();
                    latch.await();
                    for (int j = 0; j < nbRequests; j++) {
                        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();

                        int numDocs = scaledRandomIntBetween(10, 50);
                        for (int k = 0; k < numDocs; k++) {
                            MonitoringBulkDoc doc =
                                    new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), MonitoringTemplateUtils.TEMPLATE_VERSION);
                            doc.setType("concurrent");
                            doc.setSource(jsonBuilder().startObject().field("num", k).endObject().bytes(), XContentType.JSON);
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
        awaitMonitoringDocsCount(greaterThanOrEqualTo(total.get()), "concurrent");
    }

    public void testUnsupportedSystem() throws Exception {
        MonitoringBulkRequestBuilder requestBuilder = monitoringClient().prepareMonitoringBulk();
        String[] types = {"type1", "type2", "type3"};

        int totalDocs = randomIntBetween(10, 1000);
        int unsupportedDocs = 0;

        for (int i = 0; i < totalDocs; i++) {
            MonitoringBulkDoc doc;
            if (randomBoolean()) {
                doc = new MonitoringBulkDoc("unknown", MonitoringTemplateUtils.TEMPLATE_VERSION);
                unsupportedDocs++;
            } else {
                doc = new MonitoringBulkDoc(MonitoredSystem.KIBANA.getSystem(), MonitoringTemplateUtils.TEMPLATE_VERSION);
            }
            doc.setType(randomFrom(types));
            doc.setSource(jsonBuilder().startObject().field("num", i).endObject().bytes(), XContentType.JSON);
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

    public void testOverHttp() throws Exception {
        final String contentType = randomFrom("application/json", "application/x-ndjson");
        assumeTrue("security needs to be enabled for the http transport type to be set", securityEnabled);
        RestClient restClient = getRestClient();
        final Header authorization = new BasicHeader(HttpHeaders.AUTHORIZATION,
                    UsernamePasswordToken.basicAuthHeaderValue(SecuritySettings.TEST_USERNAME,
                            new SecuredString(SecuritySettings.TEST_PASSWORD.toCharArray())));
        Response response = restClient.performRequest("POST", "/_xpack/monitoring/_bulk",
                MapBuilder.<String, String>newMapBuilder().put("system_id", MonitoredSystem.KIBANA.getSystem())
                        .put("system_api_version", MonitoringTemplateUtils.TEMPLATE_VERSION)
                        .put("interval", "10s").immutableMap(),
                new StringEntity("{\"index\":{\"_index\":\"\",\"_type\":\"kibana\",\"_id\":\"1\"} }\n" +
                "{ \"field1\" : \"value1\" }\n", ContentType.create(contentType)), authorization);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }
}
