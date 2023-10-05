/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModelRegistryTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testGetUnparsedModelMap_ThrowsResourceNotFound_WhenNoHitsReturned() {
        var client = mockClient();
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[0]));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        registry.getUnparsedModelMap("1", listener);

        ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), is("Model not found [1]"));
    }

    public void testGetUnparsedModelMap_ThrowsIllegalArgumentException_WhenInvalidIndexReceived() {
        var client = mockClient();
        var unknownIndexHit = SearchHit.createFromMap(Map.of("_index", "unknown_index"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { unknownIndexHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        registry.getUnparsedModelMap("1", listener);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Invalid result while loading model [1] index: [unknown_index]. Try deleting and reinitializing the service")
        );
    }

    public void testGetUnparsedModelMap_ThrowsIllegalStateException_WhenUnableToFindInferenceEntry() {
        var client = mockClient();
        var inferenceSecretsHit = SearchHit.createFromMap(Map.of("_index", ".secrets-inference"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceSecretsHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        registry.getUnparsedModelMap("1", listener);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Failed to load model, model [1] is in an invalid state. Try deleting and reinitializing the service")
        );
    }

    public void testGetUnparsedModelMap_ThrowsIllegalStateException_WhenUnableToFindInferenceSecretsEntry() {
        var client = mockClient();
        var inferenceHit = SearchHit.createFromMap(Map.of("_index", ".inference"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        registry.getUnparsedModelMap("1", listener);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Failed to load model, model [1] is in an invalid state. Try deleting and reinitializing the service")
        );
    }

    public void testGetUnparsedModelMap_ReturnsModelConfigMap_WhenBothInferenceAndSecretsHitsAreFound() {
        var client = mockClient();
        var inferenceHit = SearchHit.createFromMap(Map.of("_index", ".inference"));
        var inferenceSecretsHit = SearchHit.createFromMap(Map.of("_index", ".secrets-inference"));

        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceHit, inferenceSecretsHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        registry.getUnparsedModelMap("1", listener);

        var modelConfig = listener.actionGet(TIMEOUT);
        assertThat(modelConfig.config(), nullValue());
        assertThat(modelConfig.secrets(), nullValue());
    }

    public void testStoreModel_ReturnsTrue_WhenNoFailuresOccur() {
        var client = mockBulkClient();

        var bulkItem = mock(BulkItemResponse.class);
        when(bulkItem.isFailed()).thenReturn(false);
        var bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[] { bulkItem });

        mockClientExecuteBulk(client, bulkResponse);

        var model = TestModel.createRandomInstance();
        var registry = new ModelRegistry(client);
        var listener = new PlainActionFuture<Boolean>();

        registry.storeModel(model, listener);

        assertTrue(listener.actionGet(TIMEOUT));
    }

    public void testStoreModel_ThrowsException_WhenBulkResponseIsEmpty() {
        var client = mockBulkClient();

        var bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[0]);

        mockClientExecuteBulk(client, bulkResponse);

        var model = TestModel.createRandomInstance();
        var registry = new ModelRegistry(client);
        var listener = new PlainActionFuture<Boolean>();

        registry.storeModel(model, listener);

        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is(
                format(
                    "Failed to store inference model [%s], invalid bulk response received. Try reinitializing the service",
                    model.getConfigurations().getModelId()
                )
            )
        );
    }

    public void testStoreModel_ThrowsResourceAlreadyExistsException_WhenFailureIsAVersionConflict() {
        var client = mockBulkClient();

        var bulkItem = mock(BulkItemResponse.class);
        when(bulkItem.isFailed()).thenReturn(true);

        var failure = new BulkItemResponse.Failure("index", "id", mock(VersionConflictEngineException.class));
        when(bulkItem.getFailure()).thenReturn(failure);
        var bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[] { bulkItem });

        mockClientExecuteBulk(client, bulkResponse);

        var model = TestModel.createRandomInstance();
        var registry = new ModelRegistry(client);
        var listener = new PlainActionFuture<Boolean>();

        registry.storeModel(model, listener);

        ResourceAlreadyExistsException exception = expectThrows(ResourceAlreadyExistsException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), is(format("Inference model [%s] already exists", model.getConfigurations().getModelId())));
    }

    public void testStoreModel_ThrowsException_WhenFailureIsNotAVersionConflict() {
        var client = mockBulkClient();

        var bulkItem = mock(BulkItemResponse.class);
        when(bulkItem.isFailed()).thenReturn(true);

        var failure = new BulkItemResponse.Failure("index", "id", mock(IllegalStateException.class));
        when(bulkItem.getFailure()).thenReturn(failure);
        var bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.getItems()).thenReturn(new BulkItemResponse[] { bulkItem });

        mockClientExecuteBulk(client, bulkResponse);

        var model = TestModel.createRandomInstance();
        var registry = new ModelRegistry(client);
        var listener = new PlainActionFuture<Boolean>();

        registry.storeModel(model, listener);

        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), is(format("Failed to store inference model [%s]", model.getConfigurations().getModelId())));
    }

    private Client mockBulkClient() {
        var client = mockClient();
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client, BulkAction.INSTANCE));

        return client;
    }

    private Client mockClient() {
        var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        return client;
    }

    private static void mockClientExecuteSearch(Client client, SearchResponse searchResponse) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(searchResponse);
            return Void.TYPE;
        }).when(client).execute(any(), any(), any());
    }

    private static void mockClientExecuteBulk(Client client, BulkResponse bulkResponse) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(bulkResponse);
            return Void.TYPE;
        }).when(client).execute(any(), any(), any());
    }

    private static SearchResponse mockSearchResponse(SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1);

        var searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);

        return searchResponse;
    }
}
