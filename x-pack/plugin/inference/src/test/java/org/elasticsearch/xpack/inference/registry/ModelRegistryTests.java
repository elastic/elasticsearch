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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.After;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
        mockClientExecuteSearch(client, mockSearchResponse(SearchHits.EMPTY));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), is("Inference endpoint not found [1]"));
    }

    public void testGetUnparsedModelMap_ThrowsIllegalArgumentException_WhenInvalidIndexReceived() {
        var client = mockClient();
        var unknownIndexHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", "unknown_index"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { unknownIndexHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Invalid result while loading inference endpoint [1] index: [unknown_index]. Try deleting and reinitializing the service")
        );
    }

    public void testGetUnparsedModelMap_ThrowsIllegalStateException_WhenUnableToFindInferenceEntry() {
        var client = mockClient();
        var inferenceSecretsHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", ".secrets-inference"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceSecretsHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Failed to load inference endpoint [1]. Endpoint is in an invalid state, try deleting and reinitializing the service")
        );
    }

    public void testGetUnparsedModelMap_ThrowsIllegalStateException_WhenUnableToFindInferenceSecretsEntry() {
        var client = mockClient();
        var inferenceHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", ".inference"));
        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("Failed to load inference endpoint [1]. Endpoint is in an invalid state, try deleting and reinitializing the service")
        );
    }

    public void testGetModelWithSecrets() {
        var client = mockClient();
        String config = """
            {
              "model_id": "1",
              "task_type": "sparse_embedding",
              "service": "foo"
            }
            """;
        String secrets = """
            {
              "api_key": "secret"
            }
            """;

        var inferenceHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", ".inference"));
        inferenceHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(Strings.toUTF8Bytes(config))));
        var inferenceSecretsHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", ".secrets-inference"));
        inferenceSecretsHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(Strings.toUTF8Bytes(secrets))));

        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceHit, inferenceSecretsHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModelWithSecrets("1", listener);

        var modelConfig = listener.actionGet(TIMEOUT);
        assertEquals("1", modelConfig.inferenceEntityId());
        assertEquals("foo", modelConfig.service());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelConfig.taskType());
        assertThat(modelConfig.settings().keySet(), empty());
        assertThat(modelConfig.secrets().keySet(), hasSize(1));
        assertEquals("secret", modelConfig.secrets().get("api_key"));
    }

    public void testGetModelNoSecrets() {
        var client = mockClient();
        String config = """
            {
              "model_id": "1",
              "task_type": "sparse_embedding",
              "service": "foo"
            }
            """;

        var inferenceHit = SearchResponseUtils.searchHitFromMap(Map.of("_index", ".inference"));
        inferenceHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(Strings.toUTF8Bytes(config))));

        mockClientExecuteSearch(client, mockSearchResponse(new SearchHit[] { inferenceHit }));

        var registry = new ModelRegistry(client);

        var listener = new PlainActionFuture<UnparsedModel>();
        registry.getModel("1", listener);

        registry.getModel("1", listener);
        var modelConfig = listener.actionGet(TIMEOUT);
        assertEquals("1", modelConfig.inferenceEntityId());
        assertEquals("foo", modelConfig.service());
        assertEquals(TaskType.SPARSE_EMBEDDING, modelConfig.taskType());
        assertThat(modelConfig.settings().keySet(), empty());
        assertThat(modelConfig.secrets().keySet(), empty());
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
                    "Failed to store inference endpoint [%s], invalid bulk response received. Try reinitializing the service",
                    model.getConfigurations().getInferenceEntityId()
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
        assertThat(
            exception.getMessage(),
            is(format("Inference endpoint [%s] already exists", model.getConfigurations().getInferenceEntityId()))
        );
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
        assertThat(
            exception.getMessage(),
            is(format("Failed to store inference endpoint [%s]", model.getConfigurations().getInferenceEntityId()))
        );
    }

    public void testIdMatchedDefault() {
        var defaultConfigIds = new ArrayList<InferenceService.DefaultConfigId>();
        defaultConfigIds.add(new InferenceService.DefaultConfigId("foo", TaskType.SPARSE_EMBEDDING, mock(InferenceService.class)));
        defaultConfigIds.add(new InferenceService.DefaultConfigId("bar", TaskType.SPARSE_EMBEDDING, mock(InferenceService.class)));

        var matched = ModelRegistry.idMatchedDefault("bar", defaultConfigIds);
        assertEquals(defaultConfigIds.get(1), matched.get());
        matched = ModelRegistry.idMatchedDefault("baz", defaultConfigIds);
        assertFalse(matched.isPresent());
    }

    public void testTaskTypeMatchedDefaults() {
        var defaultConfigIds = new ArrayList<InferenceService.DefaultConfigId>();
        defaultConfigIds.add(new InferenceService.DefaultConfigId("s1", TaskType.SPARSE_EMBEDDING, mock(InferenceService.class)));
        defaultConfigIds.add(new InferenceService.DefaultConfigId("s2", TaskType.SPARSE_EMBEDDING, mock(InferenceService.class)));
        defaultConfigIds.add(new InferenceService.DefaultConfigId("d1", TaskType.TEXT_EMBEDDING, mock(InferenceService.class)));
        defaultConfigIds.add(new InferenceService.DefaultConfigId("c1", TaskType.COMPLETION, mock(InferenceService.class)));

        var matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.SPARSE_EMBEDDING, defaultConfigIds);
        assertThat(matched, contains(defaultConfigIds.get(0), defaultConfigIds.get(1)));
        matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.TEXT_EMBEDDING, defaultConfigIds);
        assertThat(matched, contains(defaultConfigIds.get(2)));
        matched = ModelRegistry.taskTypeMatchedDefaults(TaskType.RERANK, defaultConfigIds);
        assertThat(matched, empty());
    }

    public void testDuplicateDefaultIds() {
        var client = mockBulkClient();
        var registry = new ModelRegistry(client);

        var id = "my-inference";
        var mockServiceA = mock(InferenceService.class);
        when(mockServiceA.name()).thenReturn("service-a");
        var mockServiceB = mock(InferenceService.class);
        when(mockServiceB.name()).thenReturn("service-b");

        registry.addDefaultIds(new InferenceService.DefaultConfigId(id, randomFrom(TaskType.values()), mockServiceA));
        var ise = expectThrows(
            IllegalStateException.class,
            () -> registry.addDefaultIds(new InferenceService.DefaultConfigId(id, randomFrom(TaskType.values()), mockServiceB))
        );
        assertThat(
            ise.getMessage(),
            containsString(
                "Cannot add default endpoint to the inference endpoint registry with duplicate inference id [my-inference] declared by "
                    + "service [service-b]. The inference Id is already use by [service-a] service."
            )
        );
    }

    private Client mockBulkClient() {
        var client = mockClient();
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client));

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
            ActionListener.respondAndRelease(actionListener, searchResponse);
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
        var searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1);
        try {
            when(searchResponse.getHits()).thenReturn(searchHits.asUnpooled());
        } finally {
            searchHits.decRef();
        }

        return searchResponse;
    }
}
