/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file contains code contributed by a generative AI
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class ModelRegistry {
    public record ModelConfigMap(Map<String, Object> config, Map<String, Object> secrets) {}

    public static UnparsedModel unparsedModelFromMap(ModelConfigMap modelConfigMap) {
        if (modelConfigMap.config() == null) {
            throw new ElasticsearchStatusException("Missing config map", RestStatus.BAD_REQUEST);
        }
        String inferenceEntityId = ServiceUtils.removeStringOrThrowIfNull(
            modelConfigMap.config(),
            ModelConfigurations.INDEX_ONLY_ID_FIELD_NAME
        );
        String service = ServiceUtils.removeStringOrThrowIfNull(modelConfigMap.config(), ModelConfigurations.SERVICE);
        String taskTypeStr = ServiceUtils.removeStringOrThrowIfNull(modelConfigMap.config(), TaskType.NAME);
        TaskType taskType = TaskType.fromString(taskTypeStr);

        return new UnparsedModel(inferenceEntityId, taskType, service, modelConfigMap.config(), modelConfigMap.secrets());
    }

    private static final String TASK_TYPE_FIELD = "task_type";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final Logger logger = LogManager.getLogger(ModelRegistry.class);

    private final OriginSettingClient client;
    private Map<String, UnparsedModel> defaultConfigs;

    private final Set<String> preventDeletionLock = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ModelRegistry(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.defaultConfigs = new HashMap<>();
    }

    public void addDefaultConfiguration(UnparsedModel serviceDefaultConfig) {
        if (defaultConfigs.containsKey(serviceDefaultConfig.inferenceEntityId())) {
            throw new IllegalStateException(
                "Cannot add default endpoint to the inference endpoint registry with duplicate inference id ["
                    + serviceDefaultConfig.inferenceEntityId()
                    + "] declared by service ["
                    + serviceDefaultConfig.service()
                    + "]. The inference Id is already use by ["
                    + defaultConfigs.get(serviceDefaultConfig.inferenceEntityId()).service()
                    + "] service."
            );
        }

        defaultConfigs.put(serviceDefaultConfig.inferenceEntityId(), serviceDefaultConfig);
    }

    /**
     * Get a model with its secret settings
     * @param inferenceEntityId Model to get
     * @param listener Model listener
     */
    public void getModelWithSecrets(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
        if (defaultConfigs.containsKey(inferenceEntityId)) {
            listener.onResponse(deepCopyDefaultConfig(defaultConfigs.get(inferenceEntityId)));
            return;
        }

        ActionListener<SearchResponse> searchListener = listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            // There should be a hit for the configurations and secrets
            if (searchResponse.getHits().getHits().length == 0) {
                delegate.onFailure(inferenceNotFoundException(inferenceEntityId));
                return;
            }

            delegate.onResponse(unparsedModelFromMap(createModelConfigMap(searchResponse.getHits(), inferenceEntityId)));
        });

        QueryBuilder queryBuilder = documentIdQuery(inferenceEntityId);
        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(2)
            .request();

        client.search(modelSearch, searchListener);
    }

    /**
     * Get a model.
     * Secret settings are not included
     * @param inferenceEntityId Model to get
     * @param listener Model listener
     */
    public void getModel(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
        if (defaultConfigs.containsKey(inferenceEntityId)) {
            listener.onResponse(deepCopyDefaultConfig(defaultConfigs.get(inferenceEntityId)));
            return;
        }

        ActionListener<SearchResponse> searchListener = listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            // There should be a hit for the configurations and secrets
            if (searchResponse.getHits().getHits().length == 0) {
                delegate.onFailure(inferenceNotFoundException(inferenceEntityId));
                return;
            }

            var modelConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();
            assert modelConfigs.size() == 1;
            delegate.onResponse(modelConfigs.get(0));
        });

        QueryBuilder queryBuilder = documentIdQuery(inferenceEntityId);
        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(1)
            .setTrackTotalHits(false)
            .request();

        client.search(modelSearch, searchListener);
    }

    private ResourceNotFoundException inferenceNotFoundException(String inferenceEntityId) {
        return new ResourceNotFoundException("Inference endpoint not found [{}]", inferenceEntityId);
    }

    /**
     * Get all models of a particular task type.
     * Secret settings are not included
     * @param taskType The task type
     * @param listener Models listener
     */
    public void getModelsByTaskType(TaskType taskType, ActionListener<List<UnparsedModel>> listener) {
        ActionListener<SearchResponse> searchListener = listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            var defaultConfigsForTaskType = defaultConfigs.values()
                .stream()
                .filter(m -> m.taskType() == taskType)
                .map(ModelRegistry::deepCopyDefaultConfig)
                .toList();

            // Not an error if no models of this task_type
            if (searchResponse.getHits().getHits().length == 0 && defaultConfigsForTaskType.isEmpty()) {
                delegate.onResponse(List.of());
                return;
            }

            var modelConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();

            if (defaultConfigsForTaskType.isEmpty() == false) {
                var allConfigs = new ArrayList<UnparsedModel>();
                allConfigs.addAll(modelConfigs);
                allConfigs.addAll(defaultConfigsForTaskType);
                allConfigs.sort(Comparator.comparing(UnparsedModel::inferenceEntityId));
                delegate.onResponse(allConfigs);
            } else {
                delegate.onResponse(modelConfigs);
            }
        });

        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.termsQuery(TASK_TYPE_FIELD, taskType.toString()));

        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(10_000)
            .setTrackTotalHits(false)
            .addSort(MODEL_ID_FIELD, SortOrder.ASC)
            .request();

        client.search(modelSearch, searchListener);
    }

    /**
     * Get all models.
     * Secret settings are not included
     * @param listener Models listener
     */
    public void getAllModels(ActionListener<List<UnparsedModel>> listener) {
        ActionListener<SearchResponse> searchListener = listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            var defaults = defaultConfigs.values().stream().map(ModelRegistry::deepCopyDefaultConfig).toList();

            if (searchResponse.getHits().getHits().length == 0 && defaults.isEmpty()) {
                delegate.onResponse(List.of());
                return;
            }

            var foundConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();
            var allConfigs = new ArrayList<UnparsedModel>();
            allConfigs.addAll(foundConfigs);
            allConfigs.addAll(defaults);
            allConfigs.sort(Comparator.comparing(UnparsedModel::inferenceEntityId));
            delegate.onResponse(allConfigs);
        });

        // In theory the index should only contain model config documents
        // and a match all query would be sufficient. But just in case the
        // index has been polluted return only docs with a task_type field
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.existsQuery(TASK_TYPE_FIELD));

        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(10_000)
            .setTrackTotalHits(false)
            .addSort(MODEL_ID_FIELD, SortOrder.ASC)
            .request();

        client.search(modelSearch, searchListener);
    }

    private ArrayList<ModelConfigMap> parseHitsAsModels(SearchHits hits) {
        var modelConfigs = new ArrayList<ModelConfigMap>();
        for (var hit : hits) {
            modelConfigs.add(new ModelConfigMap(hit.getSourceAsMap(), Map.of()));
        }
        return modelConfigs;
    }

    private ModelConfigMap createModelConfigMap(SearchHits hits, String inferenceEntityId) {
        Map<String, SearchHit> mappedHits = Arrays.stream(hits.getHits()).collect(Collectors.toMap(hit -> {
            if (hit.getIndex().startsWith(InferenceIndex.INDEX_NAME)) {
                return InferenceIndex.INDEX_NAME;
            }

            if (hit.getIndex().startsWith(InferenceSecretsIndex.INDEX_NAME)) {
                return InferenceSecretsIndex.INDEX_NAME;
            }

            logger.warn(format("Found invalid index for inference endpoint [%s] at index [%s]", inferenceEntityId, hit.getIndex()));
            throw new IllegalArgumentException(
                format(
                    "Invalid result while loading inference endpoint [%s] index: [%s]. Try deleting and reinitializing the service",
                    inferenceEntityId,
                    hit.getIndex()
                )
            );
        }, Function.identity()));

        if (mappedHits.containsKey(InferenceIndex.INDEX_NAME) == false
            || mappedHits.containsKey(InferenceSecretsIndex.INDEX_NAME) == false
            || mappedHits.size() > 2) {
            logger.warn(
                format(
                    "Failed to load inference endpoint [%s], found endpoint parts from index prefixes: [%s]",
                    inferenceEntityId,
                    mappedHits.keySet()
                )
            );
            throw new IllegalStateException(
                format(
                    "Failed to load inference endpoint [%s]. Endpoint is in an invalid state, try deleting and reinitializing the service",
                    inferenceEntityId
                )
            );
        }

        return new ModelConfigMap(
            mappedHits.get(InferenceIndex.INDEX_NAME).getSourceAsMap(),
            mappedHits.get(InferenceSecretsIndex.INDEX_NAME).getSourceAsMap()
        );
    }

    public void updateModelTransaction(Model newModel, Model existingModel, ActionListener<Boolean> finalListener) {

        String inferenceEntityId = newModel.getConfigurations().getInferenceEntityId();
        logger.info("Attempting to store update to inference endpoint [{}]", inferenceEntityId);

        if (preventDeletionLock.contains(inferenceEntityId)) {
            logger.warn(format("Attempted to update endpoint [{}] that is already being updated", inferenceEntityId));
            finalListener.onFailure(
                new ElasticsearchStatusException(
                    "Endpoint [{}] is currently being updated. Try again once the update completes",
                    RestStatus.CONFLICT,
                    inferenceEntityId
                )
            );
            return;
        } else {
            preventDeletionLock.add(inferenceEntityId);
        }

        SubscribableListener.<BulkResponse>newForked((subListener) -> {
            // in this block, we try to update the stored model configurations
            IndexRequest configRequest = createIndexRequest(
                Model.documentId(inferenceEntityId),
                InferenceIndex.INDEX_NAME,
                newModel.getConfigurations(),
                true
            );

            ActionListener<BulkResponse> storeConfigListener = subListener.delegateResponse((l, e) -> {
                // this block will only be called if the bulk unexpectedly throws an exception
                preventDeletionLock.remove(inferenceEntityId);
                l.onFailure(e);
            });

            client.prepareBulk().add(configRequest).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).execute(storeConfigListener);

        }).<BulkResponse>andThen((subListener, configResponse) -> {
            // in this block, we respond to the success or failure of updating the model configurations, then try to store the new secrets
            if (configResponse.hasFailures()) {
                // if storing the model configurations failed, it won't throw an exception, we need to check the BulkResponse and handle the
                // exceptions ourselves.
                logger.error(
                    format("Failed to update inference endpoint [%s] due to [%s]", inferenceEntityId, configResponse.buildFailureMessage())
                );
                // Since none of our updates succeeded at this point, we can simply return.
                finalListener.onFailure(
                    new ElasticsearchStatusException(
                        format("Failed to update inference endpoint [%s] due to [%s]", inferenceEntityId),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        configResponse.buildFailureMessage()
                    )
                );
            } else {
                // Since the model configurations were successfully updated, we can now try to store the new secrets
                IndexRequest secretsRequest = createIndexRequest(
                    Model.documentId(newModel.getConfigurations().getInferenceEntityId()),
                    InferenceSecretsIndex.INDEX_NAME,
                    newModel.getSecrets(),
                    true
                );

                ActionListener<BulkResponse> storeSecretsListener = subListener.delegateResponse((l, e) -> {
                    // this block will only be called if the bulk unexpectedly throws an exception
                    preventDeletionLock.remove(inferenceEntityId);
                    l.onFailure(e);
                });

                client.prepareBulk()
                    .add(secretsRequest)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(storeSecretsListener);
            }
        }).<BulkResponse>andThen((subListener, secretsResponse) -> {
            // in this block, we respond to the success or failure of updating the model secrets
            if (secretsResponse.hasFailures()) {
                // since storing the secrets failed, we will try to restore / roll-back-to the previous model configurations
                IndexRequest configRequest = createIndexRequest(
                    Model.documentId(inferenceEntityId),
                    InferenceIndex.INDEX_NAME,
                    existingModel.getConfigurations(),
                    true
                );
                logger.error(
                    "Failed to update inference endpoint secrets [{}], attempting rolling back to previous state",
                    inferenceEntityId
                );

                ActionListener<BulkResponse> rollbackConfigListener = subListener.delegateResponse((l, e) -> {
                    // this block will only be called if the bulk unexpectedly throws an exception
                    preventDeletionLock.remove(inferenceEntityId);
                    l.onFailure(e);
                });
                client.prepareBulk()
                    .add(configRequest)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(rollbackConfigListener);
            } else {
                // since updating the secrets was successful, we can remove the lock and respond to the final listener
                preventDeletionLock.remove(inferenceEntityId);
                finalListener.onResponse(true);
            }
        }).<BulkResponse>andThen((subListener, configResponse) -> {
            // this block will be called if the secrets response failed, and the rollback didn't throw an exception.
            // The rollback still could have failed though, so we need to check for that.
            preventDeletionLock.remove(inferenceEntityId);
            if (configResponse.hasFailures()) {
                logger.error(
                    format("Failed to update inference endpoint [%s] due to [%s]", inferenceEntityId, configResponse.buildFailureMessage())
                );
                finalListener.onFailure(
                    new ElasticsearchStatusException(
                        format(
                            "Failed to rollback while handling failure to update inference endpoint [%s]. "
                                + "Endpoint may be in an inconsistent state due to [%s]",
                            inferenceEntityId
                        ),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        configResponse.buildFailureMessage()
                    )
                );
            } else {
                logger.warn("Failed to update inference endpoint [{}], successfully rolled back to previous state", inferenceEntityId);
                finalListener.onResponse(false);
            }
        });

    }

    /**
     * Note: storeModel does not overwrite existing models and thus does not need to check the lock
     */
    public void storeModel(Model model, ActionListener<Boolean> listener) {

        ActionListener<BulkResponse> bulkResponseActionListener = getStoreModelListener(model, listener);

        IndexRequest configRequest = createIndexRequest(
            Model.documentId(model.getConfigurations().getInferenceEntityId()),
            InferenceIndex.INDEX_NAME,
            model.getConfigurations(),
            false
        );

        IndexRequest secretsRequest = createIndexRequest(
            Model.documentId(model.getConfigurations().getInferenceEntityId()),
            InferenceSecretsIndex.INDEX_NAME,
            model.getSecrets(),
            false
        );

        client.prepareBulk()
            .add(configRequest)
            .add(secretsRequest)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(bulkResponseActionListener);
    }

    private static ActionListener<BulkResponse> getStoreModelListener(Model model, ActionListener<Boolean> listener) {
        return ActionListener.wrap(bulkItemResponses -> {
            var inferenceEntityId = model.getConfigurations().getInferenceEntityId();

            if (bulkItemResponses.getItems().length == 0) {
                logger.warn(
                    format("Storing inference endpoint [%s] failed, no items were received from the bulk response", inferenceEntityId)
                );

                listener.onFailure(
                    new ElasticsearchStatusException(
                        format(
                            "Failed to store inference endpoint [%s], invalid bulk response received. Try reinitializing the service",
                            inferenceEntityId
                        ),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
                return;
            }

            BulkItemResponse.Failure failure = getFirstBulkFailure(bulkItemResponses);

            if (failure == null) {
                listener.onResponse(true);
                return;
            }

            logBulkFailures(model.getConfigurations().getInferenceEntityId(), bulkItemResponses);

            if (ExceptionsHelper.unwrapCause(failure.getCause()) instanceof VersionConflictEngineException) {
                listener.onFailure(new ResourceAlreadyExistsException("Inference endpoint [{}] already exists", inferenceEntityId));
                return;
            }

            listener.onFailure(
                new ElasticsearchStatusException(
                    format("Failed to store inference endpoint [%s]", inferenceEntityId),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    failure.getCause()
                )
            );
        }, e -> {
            String errorMessage = format("Failed to store inference endpoint [%s]", model.getConfigurations().getInferenceEntityId());
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchStatusException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e));
        });
    }

    private static void logBulkFailures(String inferenceEntityId, BulkResponse bulkResponse) {
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                logger.warn(
                    format(
                        "Failed to store inference endpoint [%s] index: [%s] bulk failure message [%s]",
                        inferenceEntityId,
                        item.getIndex(),
                        item.getFailureMessage()
                    )
                );
            }
        }
    }

    private static BulkItemResponse.Failure getFirstBulkFailure(BulkResponse bulkResponse) {
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                return item.getFailure();
            }
        }

        return null;
    }

    public void deleteModel(String inferenceEntityId, ActionListener<Boolean> listener) {
        if (preventDeletionLock.contains(inferenceEntityId)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Model is currently being updated, you may delete the model once the update completes",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        DeleteByQueryRequest request = new DeleteByQueryRequest().setAbortOnVersionConflict(false);
        request.indices(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN);
        request.setQuery(documentIdQuery(inferenceEntityId));
        request.setRefresh(true);

        client.execute(DeleteByQueryAction.INSTANCE, request, listener.delegateFailureAndWrap((l, r) -> l.onResponse(Boolean.TRUE)));
    }

    private static IndexRequest createIndexRequest(String docId, String indexName, ToXContentObject body, boolean allowOverwriting) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            var request = new IndexRequest(indexName);
            XContentBuilder source = body.toXContent(
                builder,
                new ToXContent.MapParams(Map.of(ModelConfigurations.USE_ID_FOR_INDEX, Boolean.TRUE.toString()))
            );
            var operation = allowOverwriting ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE;

            return request.opType(operation).id(docId).source(source);
        } catch (IOException ex) {
            throw new ElasticsearchException(format("Unexpected serialization exception for index [%s] doc [%s]", indexName, docId), ex);
        }
    }

    private QueryBuilder documentIdQuery(String inferenceEntityId) {
        return QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(Model.documentId(inferenceEntityId)));
    }

    static UnparsedModel deepCopyDefaultConfig(UnparsedModel other) {
        // Because the default config uses immutable maps
        return new UnparsedModel(
            other.inferenceEntityId(),
            other.taskType(),
            other.service(),
            copySettingsMap(other.settings()),
            copySecretsMap(other.secrets())
        );
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> copySettingsMap(Map<String, Object> other) {
        var result = new HashMap<String, Object>();

        var serviceSettings = (Map<String, Object>) other.get(ModelConfigurations.SERVICE_SETTINGS);
        if (serviceSettings != null) {
            var copiedServiceSettings = copyMap1LevelDeep(serviceSettings);
            result.put(ModelConfigurations.SERVICE_SETTINGS, copiedServiceSettings);
        }

        var taskSettings = (Map<String, Object>) other.get(ModelConfigurations.TASK_SETTINGS);
        if (taskSettings != null) {
            var copiedTaskSettings = copyMap1LevelDeep(taskSettings);
            result.put(ModelConfigurations.TASK_SETTINGS, copiedTaskSettings);
        }

        var chunkSettings = (Map<String, Object>) other.get(ModelConfigurations.CHUNKING_SETTINGS);
        if (chunkSettings != null) {
            var copiedChunkSettings = copyMap1LevelDeep(chunkSettings);
            result.put(ModelConfigurations.CHUNKING_SETTINGS, copiedChunkSettings);
        }

        return result;
    }

    static Map<String, Object> copySecretsMap(Map<String, Object> other) {
        return copyMap1LevelDeep(other);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> copyMap1LevelDeep(Map<String, Object> other) {
        var result = new HashMap<String, Object>();
        for (var entry : other.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?>) {
                result.put(entry.getKey(), new HashMap<>((Map<String, Object>) entry.getValue()));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
