/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.bulk;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@LuceneTestCase.AwaitsFix(bugUrl = "")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class WriteMemoryLimitsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Need at least two threads because we are going to block one
            .put("thread_pool.write.size", 2)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testRejectionDueToRequestOverMemoryLimit() {
        final String index = "test";
        assertAcked(prepareCreate(index));
        ensureGreen(index);
        final BulkRequest lessThan1KB = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            lessThan1KB.add(new IndexRequest(index).source(Collections.singletonMap("key", "value" + i)));
        }

        final BulkRequest moreThan1KB = new BulkRequest();
        for (int i = 0; i < 4; ++i) {
            moreThan1KB.add(new IndexRequest(index).source(Collections.singletonMap("key", "value" + i)));
        }
//        assertThat(DocWriteRequest.writeSizeInBytes(lessThan1KB.requests.stream()), lessThan(1024L));
        assertFalse(client().bulk(lessThan1KB).actionGet().hasFailures());

//        assertThat(DocWriteRequest.writeSizeInBytes(moreThan1KB.requests.stream()), greaterThan(1024L));
        final ActionFuture<BulkResponse> bulkFuture2 = client().bulk(moreThan1KB);
        final BulkResponse failedResponses = bulkFuture2.actionGet();
        for (BulkItemResponse response : failedResponses) {
            assertEquals(RestStatus.TOO_MANY_REQUESTS, response.getFailure().getStatus());
        }
    }

    public void testRejectionDueToConcurrentRequestsOverMemoryLimit() throws InterruptedException {
        final String index = "test";
        assertAcked(prepareCreate(index));
        ensureGreen();

        final CountDownLatch replicationStarted = new CountDownLatch(1);
        final CountDownLatch replicationBlock = new CountDownLatch(1);

        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            final MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportShardBulkAction.ACTION_NAME + "[r]")) {
                    try {
                        replicationStarted.countDown();
                        replicationBlock.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final BulkRequest lessThan1KB = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            lessThan1KB.add(new IndexRequest(index).source(Collections.singletonMap("key", "value" + i)));
        }
//        assertThat(DocWriteRequest.writeSizeInBytes(lessThan1KB.requests.stream()), lessThan(1024L));

        final BulkRequest rejectedRequest = new BulkRequest();
        for (int i = 0; i < 2; ++i) {
            rejectedRequest.add(new IndexRequest(index).source(Collections.singletonMap("key", "value" + i)));
        }
//        assertThat(DocWriteRequest.writeSizeInBytes(rejectedRequest.requests.stream()), lessThan(1024L));

        try {
            final ActionFuture<BulkResponse> successFuture = client().bulk(lessThan1KB);
            replicationStarted.await();

            final ActionFuture<BulkResponse> rejectedFuture = client().bulk(rejectedRequest);
            final BulkResponse failedResponses = rejectedFuture.actionGet();
            assertTrue(failedResponses.hasFailures());
            for (BulkItemResponse response : failedResponses) {
                assertEquals(RestStatus.TOO_MANY_REQUESTS, response.getFailure().getStatus());
            }

            replicationBlock.countDown();

            final BulkResponse successResponses = successFuture.actionGet();
            assertFalse(successResponses.hasFailures());
        } finally {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                final MockTransportService mockTransportService = (MockTransportService) transportService;
                mockTransportService.clearAllRules();
            }
        }

    }
}
