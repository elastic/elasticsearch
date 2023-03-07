/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BulkProcessor2IT extends ESIntegTestCase {

    public void testThatBulkProcessor2CountIsCorrect() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessor2TestListener listener = new BulkProcessor2TestListener(latch);

        int numDocs = randomIntBetween(10, 100);
        BulkProcessor2 processor = BulkProcessor2.builder(client()::bulk, listener, client().threadPool())
            // let's make sure that the bulk action limit trips, one single execution will index all the documents
            .setBulkActions(numDocs)
            .setFlushInterval(TimeValue.timeValueHours(24))
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
        try {

            MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(1));
            assertThat(listener.afterCounts.get(), equalTo(1));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertResponseItems(listener.bulkItems, numDocs);
            assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
            assertThat(processor.getTotalBytesInFlight(), equalTo(0L));
        } finally {
            processor.awaitClose(5, TimeUnit.SECONDS);
        }
    }

    public void testBulkProcessor2ConcurrentRequests() throws Exception {
        int bulkActions = randomIntBetween(10, 100);
        int numDocs = randomIntBetween(bulkActions, bulkActions + 100);

        int expectedBulkActions = numDocs / bulkActions;

        final CountDownLatch latch = new CountDownLatch(expectedBulkActions);
        int totalExpectedBulkActions = numDocs % bulkActions == 0 ? expectedBulkActions : expectedBulkActions + 1;
        final CountDownLatch closeLatch = new CountDownLatch(totalExpectedBulkActions);

        BulkProcessor2TestListener listener = new BulkProcessor2TestListener(latch, closeLatch);

        MultiGetRequestBuilder multiGetRequestBuilder;
        BulkProcessor2 processor = BulkProcessor2.builder(client()::bulk, listener, client().threadPool())
            .setBulkActions(bulkActions)
            // set interval and size to high values
            .setFlushInterval(TimeValue.timeValueHours(24))
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
        try {

            multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(expectedBulkActions));
            assertThat(listener.afterCounts.get(), equalTo(expectedBulkActions));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertThat(listener.bulkItems.size(), equalTo(numDocs - numDocs % bulkActions));
        } finally {
            processor.awaitClose(5, TimeUnit.SECONDS);
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
            // with concurrent requests > 1 we can't rely on the order of the bulk requests
            assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(numDocs)));
            // we do want to check that we don't get duplicate ids back
            assertThat(ids.add(bulkItemResponse.getId()), equalTo(true));
        }

        assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
        assertThat(processor.getTotalBytesInFlight(), equalTo(0L));
    }

    public void testBulkProcessor2WaitOnClose() throws Exception {
        BulkProcessor2TestListener listener = new BulkProcessor2TestListener();

        int numDocs = randomIntBetween(10, 100);
        BulkProcessor2 processor = BulkProcessor2.builder(client()::bulk, listener, client().threadPool())
            // let's make sure that the bulk action limit trips, one single execution will index all the documents
            .setBulkActions(numDocs)
            .setFlushInterval(TimeValue.timeValueHours(24))
            .setBulkSize(new ByteSizeValue(randomIntBetween(1, 10), RandomPicks.randomFrom(random(), ByteSizeUnit.values())))
            .build();

        MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);
        processor.close();
        assertThat(listener.beforeCounts.get(), greaterThanOrEqualTo(1));
        assertThat(listener.afterCounts.get(), greaterThanOrEqualTo(1));
        assertThat(listener.bulkFailures.size(), equalTo(0));
        assertResponseItems(listener.bulkItems, numDocs);
        assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
    }

    public void testBulkProcessor2ConcurrentRequestsReadOnlyIndex() throws Exception {
        createIndex("test-ro");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "test-ro");
        ensureGreen();

        int bulkActions = randomIntBetween(10, 100);
        int numDocs = randomIntBetween(bulkActions, bulkActions + 100);

        int expectedBulkActions = numDocs / bulkActions;

        final CountDownLatch latch = new CountDownLatch(expectedBulkActions);
        int totalExpectedBulkActions = numDocs % bulkActions == 0 ? expectedBulkActions : expectedBulkActions + 1;
        final CountDownLatch closeLatch = new CountDownLatch(totalExpectedBulkActions);

        int testDocs = 0;
        int testReadOnlyDocs = 0;
        MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet();
        BulkProcessor2TestListener listener = new BulkProcessor2TestListener(latch, closeLatch);

        BulkProcessor2 processor = BulkProcessor2.builder(client()::bulk, listener, client().threadPool())
            .setBulkActions(bulkActions)
            // set interval and size to high values
            .setFlushInterval(TimeValue.timeValueHours(24))
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
        try {

            for (int i = 1; i <= numDocs; i++) {
                if (randomBoolean()) {
                    testDocs++;
                    processor.add(
                        new IndexRequest("test").id(Integer.toString(testDocs)).source(Requests.INDEX_CONTENT_TYPE, "field", "value")
                    );
                    multiGetRequestBuilder.add("test", Integer.toString(testDocs));
                } else {
                    testReadOnlyDocs++;
                    processor.add(
                        new IndexRequest("test-ro").id(Integer.toString(testReadOnlyDocs))
                            .source(Requests.INDEX_CONTENT_TYPE, "field", "value")
                    );
                }
            }
        } finally {
            processor.awaitClose(5, TimeUnit.SECONDS);
        }

        closeLatch.await();

        assertThat(listener.beforeCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.afterCounts.get(), equalTo(totalExpectedBulkActions));
        assertThat(listener.bulkFailures.size(), equalTo(0));
        assertThat(listener.bulkItems.size(), equalTo(testDocs + testReadOnlyDocs));
        assertThat(processor.getTotalBytesInFlight(), equalTo(0L));
        Set<String> ids = new HashSet<>();
        Set<String> readOnlyIds = new HashSet<>();
        for (BulkItemResponse bulkItemResponse : listener.bulkItems) {
            assertThat(bulkItemResponse.getIndex(), either(equalTo("test")).or(equalTo("test-ro")));
            if (bulkItemResponse.getIndex().equals("test")) {
                assertThat(bulkItemResponse.isFailed(), equalTo(false));
                // with concurrent requests > 1 we can't rely on the order of the bulk requests
                assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(testDocs)));
                // we do want to check that we don't get duplicate ids back
                assertThat(ids.add(bulkItemResponse.getId()), equalTo(true));
            } else {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                // with concurrent requests > 1 we can't rely on the order of the bulk requests
                assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(testReadOnlyDocs)));
                // we do want to check that we don't get duplicate ids back
                assertThat(readOnlyIds.add(bulkItemResponse.getId()), equalTo(true));
            }
        }

        assertMultiGetResponse(multiGetRequestBuilder.get(), testDocs);
    }

    private static MultiGetRequestBuilder indexDocs(Client client, BulkProcessor2 processor, int numDocs) throws Exception {
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        for (int i = 1; i <= numDocs; i++) {
            processor.add(
                new IndexRequest("test").id(Integer.toString(i))
                    .source(Requests.INDEX_CONTENT_TYPE, "field", randomRealisticUnicodeOfLengthBetween(1, 30))
            );
            multiGetRequestBuilder.add("test", Integer.toString(i));
        }
        return multiGetRequestBuilder;
    }

    private static void assertResponseItems(List<BulkItemResponse> bulkItemResponses, int numDocs) {
        assertThat(bulkItemResponses.size(), is(numDocs));
        int i = 1;
        List<BulkItemResponse> sortedResponses = bulkItemResponses.stream()
            .sorted(Comparator.comparing(o -> Integer.valueOf(o.getId())))
            .toList();
        for (BulkItemResponse bulkItemResponse : sortedResponses) {
            assertThat(bulkItemResponse.getIndex(), equalTo("test"));
            assertThat(bulkItemResponse.getId(), equalTo(Integer.toString(i++)));
            assertThat(
                "item " + i + " failed with cause: " + bulkItemResponse.getFailureMessage(),
                bulkItemResponse.isFailed(),
                equalTo(false)
            );
        }
    }

    private static void assertMultiGetResponse(MultiGetResponse multiGetResponse, int numDocs) {
        assertThat(multiGetResponse.getResponses().length, equalTo(numDocs));
        int i = 1;
        for (MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
            assertThat(multiGetItemResponse.getIndex(), equalTo("test"));
            assertThat(multiGetItemResponse.getId(), equalTo(Integer.toString(i++)));
        }
    }

    private static class BulkProcessor2TestListener implements BulkProcessor2.Listener {

        private final CountDownLatch[] latches;
        private final AtomicInteger beforeCounts = new AtomicInteger();
        private final AtomicInteger afterCounts = new AtomicInteger();
        private final List<BulkItemResponse> bulkItems = new CopyOnWriteArrayList<>();
        private final List<Throwable> bulkFailures = new CopyOnWriteArrayList<>();

        private BulkProcessor2TestListener(CountDownLatch... latches) {
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
        public void afterBulk(long executionId, BulkRequest request, Exception failure) {
            bulkFailures.add(failure);
            afterCounts.incrementAndGet();
            for (CountDownLatch latch : latches) {
                latch.countDown();
            }
        }
    }
}
