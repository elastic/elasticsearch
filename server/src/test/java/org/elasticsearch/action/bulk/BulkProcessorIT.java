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

package org.elasticsearch.action.bulk;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BulkProcessorIT extends ESIntegTestCase {
    public void testThatBulkProcessorCountIsCorrect() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessorTestListener listener = new BulkProcessorTestListener(latch);

        int numDocs = randomIntBetween(10, 100);
        try (BulkProcessor processor = BulkProcessor.builder(client(), listener)
                //let's make sure that the bulk action limit trips, one single execution will index all the documents
                .setConcurrentRequests(randomIntBetween(0, 1)).setBulkActions(numDocs)
                .setFlushInterval(TimeValue.timeValueHours(24)).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .build()) {

            MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(1));
            assertThat(listener.afterCounts.get(), equalTo(1));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertResponseItems(listener.bulkItems, numDocs);
            assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
        }
    }

    public void testBulkProcessorFlush() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessorTestListener listener = new BulkProcessorTestListener(latch);

        int numDocs = randomIntBetween(10, 100);

        try (BulkProcessor processor = BulkProcessor.builder(client(), listener)
                //let's make sure that this bulk won't be automatically flushed
                .setConcurrentRequests(randomIntBetween(0, 10)).setBulkActions(numDocs + randomIntBetween(1, 100))
                .setFlushInterval(TimeValue.timeValueHours(24)).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)).build()) {

            MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            assertThat(latch.await(randomInt(500), TimeUnit.MILLISECONDS), equalTo(false));
            //we really need an explicit flush as none of the bulk thresholds was reached
            processor.flush();
            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(1));
            assertThat(listener.afterCounts.get(), equalTo(1));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertResponseItems(listener.bulkItems, numDocs);
            assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
        }
    }

    public void testBulkProcessorConcurrentRequests() throws Exception {
        int bulkActions = randomIntBetween(10, 100);
        int numDocs = randomIntBetween(bulkActions, bulkActions + 100);
        int concurrentRequests = randomIntBetween(0, 7);

        int expectedBulkActions = numDocs / bulkActions;

        final CountDownLatch latch = new CountDownLatch(expectedBulkActions);
        int totalExpectedBulkActions = numDocs % bulkActions == 0 ? expectedBulkActions : expectedBulkActions + 1;
        final CountDownLatch closeLatch = new CountDownLatch(totalExpectedBulkActions);

        BulkProcessorTestListener listener = new BulkProcessorTestListener(latch, closeLatch);

        MultiGetRequestBuilder multiGetRequestBuilder;

        try (BulkProcessor processor = BulkProcessor.builder(client(), listener)
                .setConcurrentRequests(concurrentRequests).setBulkActions(bulkActions)
                //set interval and size to high values
                .setFlushInterval(TimeValue.timeValueHours(24)).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)).build()) {

            multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(expectedBulkActions));
            assertThat(listener.afterCounts.get(), equalTo(expectedBulkActions));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertThat(listener.bulkItems.size(), equalTo(numDocs - numDocs % bulkActions));
        }

        closeLatch.await();

        assertThat(listener.beforeCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.afterCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.bulkFailures.size(), equalTo(0));
        assertThat(listener.bulkItems.size(), equalTo(numDocs));

        Set<String> ids = new HashSet<>();
        for (BulkItemResponse bulkItemResponse : listener.bulkItems) {
            assertThat(bulkItemResponse.getFailureMessage(), bulkItemResponse.isFailed(), equalTo(false));
            assertThat(bulkItemResponse.getIndex(), equalTo("test"));
            assertThat(bulkItemResponse.getType(), equalTo("test"));
            //with concurrent requests > 1 we can't rely on the order of the bulk requests
            assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(numDocs)));
            //we do want to check that we don't get duplicate ids back
            assertThat(ids.add(bulkItemResponse.getId()), equalTo(true));
        }

        assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
    }

    public void testBulkProcessorWaitOnClose() throws Exception {
        BulkProcessorTestListener listener = new BulkProcessorTestListener();

        int numDocs = randomIntBetween(10, 100);
        BulkProcessor processor = BulkProcessor.builder(client(), listener)
                //let's make sure that the bulk action limit trips, one single execution will index all the documents
                .setConcurrentRequests(randomIntBetween(0, 1)).setBulkActions(numDocs)
                .setFlushInterval(TimeValue.timeValueHours(24)).setBulkSize(new ByteSizeValue(randomIntBetween(1, 10),
                        RandomPicks.randomFrom(random(), ByteSizeUnit.values())))
                .build();

        MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);
        assertThat(processor.isOpen(), is(true));
        assertThat(processor.awaitClose(1, TimeUnit.MINUTES), is(true));
        if (randomBoolean()) { // check if we can call it multiple times
            if (randomBoolean()) {
                assertThat(processor.awaitClose(1, TimeUnit.MINUTES), is(true));
            } else {
                processor.close();
            }
        }
        assertThat(processor.isOpen(), is(false));

        assertThat(listener.beforeCounts.get(), greaterThanOrEqualTo(1));
        assertThat(listener.afterCounts.get(), greaterThanOrEqualTo(1));
        assertThat(listener.bulkFailures.size(), equalTo(0));
        assertResponseItems(listener.bulkItems, numDocs);
        assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
    }

    public void testBulkProcessorConcurrentRequestsReadOnlyIndex() throws Exception {
        createIndex("test-ro");
        assertAcked(client().admin().indices().prepareUpdateSettings("test-ro")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true)));
        ensureGreen();

        int bulkActions = randomIntBetween(10, 100);
        int numDocs = randomIntBetween(bulkActions, bulkActions + 100);
        int concurrentRequests = randomIntBetween(0, 10);

        int expectedBulkActions = numDocs / bulkActions;

        final CountDownLatch latch = new CountDownLatch(expectedBulkActions);
        int totalExpectedBulkActions = numDocs % bulkActions == 0 ? expectedBulkActions : expectedBulkActions + 1;
        final CountDownLatch closeLatch = new CountDownLatch(totalExpectedBulkActions);

        int testDocs = 0;
        int testReadOnlyDocs = 0;
        MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet();
        BulkProcessorTestListener listener = new BulkProcessorTestListener(latch, closeLatch);

        try (BulkProcessor processor = BulkProcessor.builder(client(), listener)
                .setConcurrentRequests(concurrentRequests).setBulkActions(bulkActions)
                        //set interval and size to high values
                .setFlushInterval(TimeValue.timeValueHours(24)).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)).build()) {

            for (int i = 1; i <= numDocs; i++) {
                if (randomBoolean()) {
                    testDocs++;
                    processor.add(new IndexRequest("test", "test", Integer.toString(testDocs))
                        .source(Requests.INDEX_CONTENT_TYPE, "field", "value"));
                    multiGetRequestBuilder.add("test", "test", Integer.toString(testDocs));
                } else {
                    testReadOnlyDocs++;
                    processor.add(new IndexRequest("test-ro", "test", Integer.toString(testReadOnlyDocs))
                        .source(Requests.INDEX_CONTENT_TYPE, "field", "value"));
                }
            }
        }

        closeLatch.await();

        assertThat(listener.beforeCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.afterCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.bulkFailures.size(), equalTo(0));
        assertThat(listener.bulkItems.size(), equalTo(testDocs + testReadOnlyDocs));

        Set<String> ids = new HashSet<>();
        Set<String> readOnlyIds = new HashSet<>();
        for (BulkItemResponse bulkItemResponse : listener.bulkItems) {
            assertThat(bulkItemResponse.getIndex(), either(equalTo("test")).or(equalTo("test-ro")));
            assertThat(bulkItemResponse.getType(), equalTo("test"));
            if (bulkItemResponse.getIndex().equals("test")) {
                assertThat(bulkItemResponse.isFailed(), equalTo(false));
                //with concurrent requests > 1 we can't rely on the order of the bulk requests
                assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(testDocs)));
                //we do want to check that we don't get duplicate ids back
                assertThat(ids.add(bulkItemResponse.getId()), equalTo(true));
            } else {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                //with concurrent requests > 1 we can't rely on the order of the bulk requests
                assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(testReadOnlyDocs)));
                //we do want to check that we don't get duplicate ids back
                assertThat(readOnlyIds.add(bulkItemResponse.getId()), equalTo(true));
            }
        }

        assertMultiGetResponse(multiGetRequestBuilder.get(), testDocs);
    }

    private static MultiGetRequestBuilder indexDocs(Client client, BulkProcessor processor, int numDocs) throws Exception {
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        for (int i = 1; i <= numDocs; i++) {
            if (randomBoolean()) {
                processor.add(new IndexRequest("test", "test", Integer.toString(i))
                    .source(Requests.INDEX_CONTENT_TYPE, "field", randomRealisticUnicodeOfLengthBetween(1, 30)));
            } else {
                final String source = "{ \"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"" + Integer.toString(i) + "\"} }\n"
                    + Strings.toString(JsonXContent.contentBuilder()
                        .startObject().field("field", randomRealisticUnicodeOfLengthBetween(1, 30)).endObject()) + "\n";
                processor.add(new BytesArray(source), null, null, XContentType.JSON);
            }
            multiGetRequestBuilder.add("test", "test", Integer.toString(i));
        }
        return multiGetRequestBuilder;
    }

    private static void assertResponseItems(List<BulkItemResponse> bulkItemResponses, int numDocs) {
        assertThat(bulkItemResponses.size(), is(numDocs));
        int i = 1;
        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
            assertThat(bulkItemResponse.getIndex(), equalTo("test"));
            assertThat(bulkItemResponse.getType(), equalTo("test"));
            assertThat(bulkItemResponse.getId(), equalTo(Integer.toString(i++)));
            assertThat("item " + i + " failed with cause: " + bulkItemResponse.getFailureMessage(),
                bulkItemResponse.isFailed(), equalTo(false));
        }
    }

    private static void assertMultiGetResponse(MultiGetResponse multiGetResponse, int numDocs) {
        assertThat(multiGetResponse.getResponses().length, equalTo(numDocs));
        int i = 1;
        for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
            assertThat(multiGetItemResponse.getIndex(), equalTo("test"));
            assertThat(multiGetItemResponse.getType(), equalTo("test"));
            assertThat(multiGetItemResponse.getId(), equalTo(Integer.toString(i++)));
        }
    }

    private static class BulkProcessorTestListener implements BulkProcessor.Listener {

        private final CountDownLatch[] latches;
        private final AtomicInteger beforeCounts = new AtomicInteger();
        private final AtomicInteger afterCounts = new AtomicInteger();
        private final List<BulkItemResponse> bulkItems = new CopyOnWriteArrayList<>();
        private final List<Throwable> bulkFailures = new CopyOnWriteArrayList<>();

        private BulkProcessorTestListener(CountDownLatch... latches) {
            this.latches = latches;
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            beforeCounts.incrementAndGet();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            bulkItems.addAll(Arrays.asList(response.getItems()));
            afterCounts.incrementAndGet();
            for (CountDownLatch latch : latches) {
                latch.countDown();
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            bulkFailures.add(failure);
            afterCounts.incrementAndGet();
            for (CountDownLatch latch : latches) {
                latch.countDown();
            }
        }
    }
}
