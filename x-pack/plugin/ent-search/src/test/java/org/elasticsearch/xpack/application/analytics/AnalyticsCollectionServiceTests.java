/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class AnalyticsCollectionServiceTests extends AnalyticsTestCase {
    private static final int NUM_COLLECTIONS = 10;

    @Before
    public void setupDataStreams() throws Exception {
        for (int i = 0; i < NUM_COLLECTIONS; i++) {
            createDataStream(new AnalyticsCollection("collection_" + i).getEventDataStream());
        }
    }

    public void testGetExistingAnalyticsCollection() throws Exception {
        String collectionName = "collection_" + random().nextInt(NUM_COLLECTIONS);
        List<AnalyticsCollection> collections = awaitGetAnalyticsCollections(collectionName);
        assertThat(collections, hasSize(1));
        assertThat(collections.get(0).getName(), equalTo(collectionName));
    }

    public void testGetMissingAnalyticsCollection() throws Exception {
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> awaitGetAnalyticsCollections("not-a-collection-name")
        );

        assertThat(e.getMessage(), equalTo("not-a-collection-name"));
    }

    public void testCreateAnalyticsCollection() throws Exception {
        String collectionName = randomIdentifier();

        PutAnalyticsCollectionAction.Response response = awaitPutAnalyticsCollection(collectionName);
        assertThat(response.isAcknowledged(), equalTo(true));
        assertThat(response.getName(), equalTo(collectionName));

        // Checking a data stream has been created for the analytics collection.
        assertThat(
            clusterService().state().metadata().dataStreams(),
            hasKey(new AnalyticsCollection(response.getName()).getEventDataStream())
        );

        // Checking we can get the collection we have just created.
        assertThat(awaitGetAnalyticsCollections(collectionName), hasSize(1));
    }

    public void testCreateAlreadyExistingAnalyticsCollection() throws Exception {
        String collectionName = "collection_" + random().nextInt(NUM_COLLECTIONS);

        ResourceAlreadyExistsException e = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> awaitPutAnalyticsCollection(collectionName)
        );

        assertThat(e.getMessage(), equalTo(collectionName));
    }

    public void testDeleteAnalyticsCollection() throws Exception {
        String collectionName = "collection_" + random().nextInt(NUM_COLLECTIONS);
        String dataStreamName = new AnalyticsCollection(collectionName).getEventDataStream();

        AcknowledgedResponse response = awaitDeleteAnalyticsCollection(collectionName);
        assertThat(response.isAcknowledged(), equalTo(true));

        // Checking that the underlying data stream has been deleted too.
        assertThat(clusterService().state().metadata().dataStreams(), not(hasKey(dataStreamName)));

        // Checking that the analytics collection is not accessible anymore.
        expectThrows(ResourceNotFoundException.class, () -> awaitGetAnalyticsCollections(collectionName));
    }

    public void testDeleteMissingAnalyticsCollection() throws Exception {
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> { awaitDeleteAnalyticsCollection("not-a-collection-name"); }
        );

        assertThat(e.getMessage(), equalTo("not-a-collection-name"));
    }

    private void createDataStream(String dataStreamName) throws ExecutionException, InterruptedException {
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get();
    }

    private List<AnalyticsCollection> awaitGetAnalyticsCollections(String collectionName) throws Exception {
        GetAnalyticsCollectionAction.Request request = new GetAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(analyticsCollectionService()::getAnalyticsCollection).execute(request).getAnalyticsCollections();
    }

    private PutAnalyticsCollectionAction.Response awaitPutAnalyticsCollection(String collectionName) throws Exception {
        PutAnalyticsCollectionAction.Request request = new PutAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(analyticsCollectionService()::putAnalyticsCollection).execute(request);
    }

    private AcknowledgedResponse awaitDeleteAnalyticsCollection(String collectionName) throws Exception {
        DeleteAnalyticsCollectionAction.Request request = new DeleteAnalyticsCollectionAction.Request(collectionName);
        return new Executor<>(analyticsCollectionService()::deleteAnalyticsCollection).execute(request);
    }

    private static class Executor<T, R> {
        BiConsumer<T, ActionListener<R>> f;

        Executor(BiConsumer<T, ActionListener<R>> f) {
            this.f = f;
        }

        public R execute(T param) throws Exception {
            CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<R> resp = new AtomicReference<>(null);
            final AtomicReference<Exception> exc = new AtomicReference<>(null);

            f.accept(param, new ActionListener<R>() {
                @Override
                public void onResponse(R r) {
                    resp.set(r);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exc.set(e);
                    latch.countDown();
                }
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            if (exc.get() != null) {
                throw exc.get();
            }
            assertNotNull(resp.get());
            return resp.get();
        }
    }
}
