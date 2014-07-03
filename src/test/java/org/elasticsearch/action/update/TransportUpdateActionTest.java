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

package org.elasticsearch.action.update;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests to demonstrate the bug discussed in issue #6355 and pull request #6724. The tests validate the fix for that bug
 * and similar situations.
 */
public class TransportUpdateActionTest extends ElasticsearchIntegrationTest {

    private static final String TYPE_NAME = "some-type";
    private static final TimeValue WAIT_FOR_RESPONSE_TIMEOUT = TimeValue.timeValueSeconds(5);

    private static InternalEngineWithControllableTimingForTesting internalEngine;

    private final String indexName = "issue-6355-test-index-" + UUID.randomUUID();
    private final String documentId = UUID.randomUUID().toString();

    /**
     * Creates an index which uses an implementation of {@link Engine} that allows the test to enforce specific timing
     * (and, if needed, exceptional behavior) for request handling.
     */
    @Before
    public void createTestIndexAndInitTestEngine() {
        client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                        ImmutableSettings
                                .settingsBuilder()
                                .put(IndexEngineModule.EngineSettings.ENGINE_TYPE,
                                        InternalEngineWithControllableTimingForTestingModule.class.getName()).put("number_of_shards", 1)
                                .put("number_of_replicas", 0).build()).execute().actionGet();
        internalEngine = InternalEngineWithControllableTimingForTesting.currentTestInstance;
    }

    @After
    public void resetTestEngine() {
        internalEngine.resetSemaphores();
    }

    @AfterClass
    public static void clearStaticFields() {
        internalEngine = null;
    }

    /**
     * Checks correct behavior in case of an update request that does not include a retry_on_conflict parameter.
     */
    @Test
    public void shouldReceiveUpdateResponseWhenNotRetryingOnConflict() {
        sendAddRequestAndWaitUntilCompletelyDone(documentId);

        ListenableActionFuture<UpdateResponse> updateFuture = executeUpdateAndDeleteRequestsWithIntendedTiming(documentId, 0);

        verifyUpdateResponseCompletesExceptionally(updateFuture, VersionConflictEngineException.class);
    }

    /**
     * Checks correct behavior in case of an update request that includes a retry_on_conflict parameter set to 1. This
     * test demonstrates the bug described in issue #6355 and validates the fix. The scenario is based on real client
     * requests that happen with a particular timing.
     */
    @Test
    public void shouldReceiveUpdateResponseWhenRetryingOnConflict() {
        sendAddRequestAndWaitUntilCompletelyDone(documentId);

        ListenableActionFuture<UpdateResponse> updateFuture = executeUpdateAndDeleteRequestsWithIntendedTiming(documentId, 1);

        verifyUpdateResponseCompletesExceptionally(updateFuture, DocumentMissingException.class);
    }

    /**
     * Checks correct behavior in case of an upsert request that does not include a retry_on_conflict parameter.
     */
    @Test
    public void shouldReceiveUpsertResponseWhenNotRetryingOnConflict() {
        ListenableActionFuture<UpdateResponse> upsertFuture = updateDocument(documentId, true, 0);

        internalEngine.letNextGetThrowException();

        verifyUpdateResponseCompletesExceptionally(upsertFuture, VersionConflictEngineException.class);
    }

    /**
     * Checks correct behavior in case of an upsert request that includes a retry_on_conflict parameter set to 1. This
     * was not originally covered by issue #6355, but the discussion for pull request #6724 revealed that the same bug
     * may appear here as well. The scenario is based on a fake exception thrown when getting a document from the
     * {@link Engine}, because unlike for the "update" case it turns out to be quite difficult to set up real client
     * requests to trigger an exception in {@link UpdateHelper#prepare(UpdateRequest)} for the "upsert" case.
     */
    @Test
    public void shouldReceiveUpsertResponseWhenRetryingOnConflict() {
        // send an upsert request and make sure the corresponding create operation has arrived in the internal engine
        ListenableActionFuture<UpdateResponse> upsertFuture = updateDocument(documentId, true, 1);
        internalEngine.waitUntilCreateOperationReceived();

        // let another request cause a version conflict, triggering a retry of the upsert request
        sendAddRequestAndWaitUntilCompletelyDone(documentId);

        // now complete the update request, but make sure the retry is going to fail when getting the document
        internalEngine.letCreateOperationBegin();
        internalEngine.waitUntilCreateOperationFinished();

        internalEngine.letNextGetThrowException();

        internalEngine.letCreateOperationReturn();

        verifyUpdateResponseCompletesExceptionally(upsertFuture, VersionConflictEngineException.class);
    }

    private void verifyUpdateResponseCompletesExceptionally(ListenableActionFuture<UpdateResponse> updateFuture,
            Class<? extends Exception> expectedExceptionClass) {
        try {
            updateFuture.actionGet(WAIT_FOR_RESPONSE_TIMEOUT);
            fail("Future for update request did not complete exceptionally, but should.");
        } catch (ElasticsearchTimeoutException e) {
            fail("Future for update request did not complete within " + WAIT_FOR_RESPONSE_TIMEOUT);
        } catch (Exception e) {
            assertTrue("Future for update request completed with an unexpected exception.", expectedExceptionClass.isInstance(e));
        }
    }

    private ListenableActionFuture<UpdateResponse> executeUpdateAndDeleteRequestsWithIntendedTiming(String documentId, int retryOnConflict) {
        // send an update request and make sure the corresponding index operation has arrived in the internal engine
        ListenableActionFuture<UpdateResponse> updateFuture = updateDocument(documentId, false, retryOnConflict);
        internalEngine.waitUntilIndexOperationReceived();

        sendDeleteRequestAndWaitUntilCompletelyDone(documentId);

        // now complete the index operation of the update request
        internalEngine.letIndexOperationBegin();
        internalEngine.waitUntilIndexOperationFinished();
        internalEngine.letIndexOperationReturn();

        return updateFuture;
    }

    private void sendAddRequestAndWaitUntilCompletelyDone(String documentId) {
        ListenableActionFuture<IndexResponse> addFuture = addDocument(documentId);

        internalEngine.waitUntilIndexOperationReceived();
        internalEngine.letIndexOperationBegin();
        internalEngine.waitUntilIndexOperationFinished();
        internalEngine.letIndexOperationReturn();

        waitUntilResponseReceived(addFuture);
    }

    private void sendDeleteRequestAndWaitUntilCompletelyDone(String documentId) {
        ListenableActionFuture<DeleteResponse> deleteFuture = deleteDocument(documentId);

        internalEngine.waitUntilDeleteOperationReceived();
        internalEngine.letDeleteOperationBegin();
        internalEngine.waitUntilDeleteOperationFinished();
        internalEngine.letDeleteOperationReturn();

        waitUntilResponseReceived(deleteFuture);
    }

    private void waitUntilResponseReceived(ListenableActionFuture<?> responseFuture) {
        responseFuture.actionGet(WAIT_FOR_RESPONSE_TIMEOUT);
    }

    private ListenableActionFuture<IndexResponse> addDocument(String id) {
        return client().prepareIndex(indexName, TYPE_NAME, id).setSource(buildJson(id)).execute();
    }

    private ListenableActionFuture<UpdateResponse> updateDocument(String id, boolean upsert, int retryOnConflict) {
        return client().prepareUpdate(indexName, TYPE_NAME, id).setDoc(buildJson(id)).setDocAsUpsert(upsert)
                .setRetryOnConflict(retryOnConflict).execute();
    }

    private ListenableActionFuture<DeleteResponse> deleteDocument(String id) {
        return client().prepareDelete(indexName, TYPE_NAME, id).execute();
    }

    private XContentBuilder buildJson(String id) {
        try {
            return XContentFactory.jsonBuilder().startObject().field("id", id).endObject();
        } catch (IOException e) {
            throw new RuntimeException("Not going to happen!");
        }
    }
}
