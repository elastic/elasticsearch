/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
        try (
            BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
                // let's make sure that the bulk action limit trips, one single execution will index all the documents
                .setConcurrentRequests(randomIntBetween(0, 1))
                .setBulkActions(numDocs)
                .setFlushInterval(TimeValue.timeValueHours(24))
                .setBulkSize(ByteSizeValue.of(1, ByteSizeUnit.GB))
                .build()
        ) {

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

        try (
            BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
                // let's make sure that this bulk won't be automatically flushed
                .setConcurrentRequests(randomIntBetween(0, 10))
                .setBulkActions(numDocs + randomIntBetween(1, 100))
                .setFlushInterval(TimeValue.timeValueHours(24))
                .setBulkSize(ByteSizeValue.of(1, ByteSizeUnit.GB))
                .build()
        ) {

            MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);

            assertThat(latch.await(randomInt(500), TimeUnit.MILLISECONDS), equalTo(false));
            // we really need an explicit flush as none of the bulk thresholds was reached
            processor.flush();
            latch.await();

            assertThat(listener.beforeCounts.get(), equalTo(1));
            assertThat(listener.afterCounts.get(), equalTo(1));
            assertThat(listener.bulkFailures.size(), equalTo(0));
            assertResponseItems(listener.bulkItems, numDocs);
            assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
        }
    }

    public void testBulkProcessorFlushDisabled() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessorTestListener listener = new BulkProcessorTestListener(latch);

        int numDocs = randomIntBetween(10, 100);

        AtomicBoolean flushEnabled = new AtomicBoolean(false);
        try (
            BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
                // let's make sure that this bulk won't be automatically flushed
                .setConcurrentRequests(randomIntBetween(0, 10))
                .setBulkActions(numDocs + randomIntBetween(1, 100))
                .setFlushInterval(TimeValue.timeValueHours(24))
                .setBulkSize(ByteSizeValue.of(1, ByteSizeUnit.GB))
                .setFlushCondition(flushEnabled::get)
                .build()
        ) {

            MultiGetRequestBuilder multiGetRequestBuilder = indexDocs(client(), processor, numDocs);
            assertThat(latch.await(randomInt(500), TimeUnit.MILLISECONDS), equalTo(false));
            // no documents will be indexed here
            processor.flush();

            flushEnabled.set(true);
            processor.flush();
            latch.await();

            // disabled flush resulted in listener being triggered only once
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

        try (
            BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
                .setConcurrentRequests(concurrentRequests)
                .setBulkActions(bulkActions)
                // set interval and size to high values
                .setFlushInterval(TimeValue.timeValueHours(24))
                .setBulkSize(ByteSizeValue.of(1, ByteSizeUnit.GB))
                .build()
        ) {

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
            // with concurrent requests > 1 we can't rely on the order of the bulk requests
            assertThat(Integer.valueOf(bulkItemResponse.getId()), both(greaterThan(0)).and(lessThanOrEqualTo(numDocs)));
            // we do want to check that we don't get duplicate ids back
            assertThat(ids.add(bulkItemResponse.getId()), equalTo(true));
        }

        assertMultiGetResponse(multiGetRequestBuilder.get(), numDocs);
    }

    public void testBulkProcessorWaitOnClose() throws Exception {
        BulkProcessorTestListener listener = new BulkProcessorTestListener();

        int numDocs = randomIntBetween(10, 100);
        BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
            // let's make sure that the bulk action limit trips, one single execution will index all the documents
            .setConcurrentRequests(randomIntBetween(0, 1))
            .setBulkActions(numDocs)
            .setFlushInterval(TimeValue.timeValueHours(24))
            .setBulkSize(ByteSizeValue.of(randomIntBetween(1, 10), RandomPicks.randomFrom(random(), ByteSizeUnit.values())))
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
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "test-ro");
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

        try (
            BulkProcessor processor = BulkProcessor.builder(client()::bulk, listener, "BulkProcessorIT")
                .setConcurrentRequests(concurrentRequests)
                .setBulkActions(bulkActions)
                // set interval and size to high values
                .setFlushInterval(TimeValue.timeValueHours(24))
                .setBulkSize(ByteSizeValue.of(1, ByteSizeUnit.GB))
                .build()
        ) {

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

    private static MultiGetRequestBuilder indexDocs(Client client, BulkProcessor processor, int numDocs) throws Exception {
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
        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
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
