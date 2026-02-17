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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
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
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.parser.EndpointMetadataParser;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * A class responsible for persisting and reading inference endpoint configurations.
 * All endpoint modifications (see {@link PutInferenceModelAction}, {@link UpdateInferenceModelAction} and
 * {@link DeleteInferenceEndpointAction}) are executed on the master mode to prevent race conditions when modifying models.
 *
 * <p><strong>Default endpoints:</strong></p>
 * Some inference services provide default configurations, which are registered at startup using
 * {@link #addDefaultIds(InferenceService.DefaultConfigId)}. At this point, only the IDs and service details
 * are registered, as the full configuration definitions may not yet be available.
 * The full configurations are populated lazily upon reading and are then persisted to the index.
 * This process also triggers the creation of the backing index when reading the configurations.
 * To avoid index creation, {@link #getAllModels(boolean, ActionListener)} includes an option to skip writing
 * default configurations to the index during reads.
 *
 * <p><strong>Minimal Service Settings in Cluster State:</strong></p>
 * The cluster state is updated with the {@link MinimalServiceSettings} for all registered models,
 * ensuring these settings are readily accessible to consumers without requiring an asynchronous call
 * to retrieve the full model configurations.
 *
 * <p><strong>Metadata Upgrades:</strong></p>
 * Since cluster state metadata was introduced later, the master node performs an upgrade at startup,
 * if necessary, to load all model settings from the {@link InferenceIndex}.
 */
public class ModelRegistry implements ClusterStateListener {
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
        var endpointMetadata = EndpointMetadataParser.fromMap(modelConfigMap.config());

        return new UnparsedModel(inferenceEntityId, taskType, service, modelConfigMap.config(), modelConfigMap.secrets(), endpointMetadata);
    }

    private static final String TASK_TYPE_FIELD = "task_type";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final Logger logger = LogManager.getLogger(ModelRegistry.class);
    // Currently all EIS preconfigured endpoints are prefixed with a dot. We should remove this restriction by leveraging
    // the information from the authorization response instead.
    private static final String EIS_PRECONFIGURED_ENDPOINT_ID_PREFIX = ".";

    private final OriginSettingClient client;
    private final Map<String, InferenceService.DefaultConfigId> defaultConfigIds;

    private final MasterServiceTaskQueue<ModelRegistryMetadataTask.MetadataTask> metadataTaskQueue;
    private final AtomicBoolean upgradeMetadataInProgress = new AtomicBoolean(false);
    private final Set<String> preventDeletionLock = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ClusterService clusterService;
    private final AtomicReference<Metadata> lastMetadata = new AtomicReference<>();

    public ModelRegistry(ClusterService clusterService, Client client) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.defaultConfigIds = new ConcurrentHashMap<>();
        var executor = new SimpleBatchedAckListenerTaskExecutor<ModelRegistryMetadataTask.MetadataTask>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                ModelRegistryMetadataTask.MetadataTask task,
                ClusterState clusterState
            ) throws Exception {
                var projectMetadata = clusterState.metadata().getProject(task.getProjectId());
                var updated = task.executeTask(ModelRegistryClusterStateMetadata.fromState(projectMetadata));
                var newProjectMetadata = ProjectMetadata.builder(projectMetadata)
                    .putCustom(ModelRegistryClusterStateMetadata.TYPE, updated);
                return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(newProjectMetadata).build(), task);
            }
        };
        this.metadataTaskQueue = clusterService.createTaskQueue("model_registry", Priority.NORMAL, executor);
    }

    /**
     * Returns true if the model registry contains (whether it has persisted it or not) the provided inference entity id.
     * EIS preconfigured endpoints are also considered.
     * @param inferenceEntityId the id to search for
     * @return true if we find a match and false if not
     */
    public boolean containsPreconfiguredInferenceEndpointId(String inferenceEntityId) {
        // This checks an in memory cache local to the node. The cache should be the same across all nodes as it is populated in the
        // inference plugin on boot up (excluding an upgrade scenario where the plugins could be different).
        // This primarily holds endpoints registered by the ElasticsearchInternalService
        if (defaultConfigIds.containsKey(inferenceEntityId)) {
            return true;
        }

        // This checks the cluster state for EIS preconfigured endpoints
        if (lastMetadata.get() != null) {
            var project = lastMetadata.get().getProject(ProjectId.DEFAULT);
            var state = ModelRegistryClusterStateMetadata.fromState(project);
            var eisEndpoints = state.getServiceInferenceIds(ElasticInferenceService.NAME);
            return eisEndpoints.contains(inferenceEntityId) && inferenceEntityId.startsWith(EIS_PRECONFIGURED_ENDPOINT_ID_PREFIX);
        }

        return false;
    }

    /**
     * Adds the default configuration information if it does not already exist internally.
     * @param defaultConfigId the default endpoint information
     */
    public synchronized void putDefaultIdIfAbsent(InferenceService.DefaultConfigId defaultConfigId) {
        defaultConfigIds.putIfAbsent(defaultConfigId.inferenceId(), defaultConfigId);
    }

    /**
     * Set the default inference ids provided by the services
     * @param defaultConfigId The default endpoint information
     * @throws IllegalStateException if the {@link InferenceService.DefaultConfigId#inferenceId()} already exists internally
     */
    public synchronized void addDefaultIds(InferenceService.DefaultConfigId defaultConfigId) throws IllegalStateException {
        var config = defaultConfigIds.get(defaultConfigId.inferenceId());
        if (config != null) {
            throw new IllegalStateException(
                "Cannot add default endpoint to the inference endpoint registry with duplicate inference id ["
                    + defaultConfigId.inferenceId()
                    + "] declared by service ["
                    + defaultConfigId.service().name()
                    + "]. The inference Id is already use by ["
                    + config.service().name()
                    + "] service."
            );
        }
        defaultConfigIds.put(defaultConfigId.inferenceId(), defaultConfigId);
    }

    /**
     * Visible for testing only.
     */
    public void clearDefaultIds() {
        defaultConfigIds.clear();
    }

    /**
     * Retrieves the {@link MinimalServiceSettings} associated with the specified {@code inferenceEntityId}.
     *
     * If the {@code inferenceEntityId} is not found, the method behaves as follows:
     * <ul>
     *   <li>Returns {@code null} if the id might exist but its configuration is not available locally.</li>
     *   <li>Throws a {@link ResourceNotFoundException} if it is certain that the id does not exist in the cluster.</li>
     * </ul>
     *
     * @param inferenceEntityId the unique identifier for the inference entity.
     * @return the {@link MinimalServiceSettings} associated with the provided ID, or {@code null} if unavailable locally.
     * @throws ResourceNotFoundException if the specified id is guaranteed to not exist in the cluster.
     */
    public MinimalServiceSettings getMinimalServiceSettings(String inferenceEntityId) throws ResourceNotFoundException {
        if (lastMetadata.get() == null) {
            throw new IllegalStateException("initial cluster state not set yet");
        }

        var config = defaultConfigIds.get(inferenceEntityId);
        if (config != null) {
            return config.settings();
        }
        var project = lastMetadata.get().getProject(ProjectId.DEFAULT);
        var state = ModelRegistryClusterStateMetadata.fromState(project);
        var existing = state.getMinimalServiceSettings(inferenceEntityId);
        if (state.isUpgraded() && existing == null) {
            throw new ResourceNotFoundException(inferenceEntityId + " does not exist in this cluster.");
        }
        return existing;
    }

    public Set<String> getInferenceIds() {
        Set<String> metadataInferenceIds = Set.of();
        if (lastMetadata.get() != null) {
            var project = lastMetadata.get().getProject(ProjectId.DEFAULT);
            var state = ModelRegistryClusterStateMetadata.fromState(project);
            metadataInferenceIds = state.getInferenceIds();
        }

        var ids = new HashSet<>(metadataInferenceIds);
        ids.addAll(Set.copyOf(defaultConfigIds.keySet()));
        return ids;
    }

    /**
     * Get a model with its secret settings
     * @param inferenceEntityId Model to get
     * @param listener Model listener
     */
    public void getModelWithSecrets(String inferenceEntityId, ActionListener<UnparsedModel> listener) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap((searchResponse) -> {
            // There should be a hit for the configurations
            if (searchResponse.getHits().getHits().length == 0) {
                var maybeDefault = defaultConfigIds.get(inferenceEntityId);
                if (maybeDefault != null) {
                    getDefaultConfig(true, maybeDefault, listener);
                } else {
                    listener.onFailure(inferenceNotFoundException(inferenceEntityId));
                }
                return;
            }

            listener.onResponse(unparsedModelFromMap(createModelConfigMap(searchResponse.getHits(), inferenceEntityId)));
        }, (e) -> {
            logger.warn(format("Failed to load inference endpoint with secrets [%s]", inferenceEntityId), e);
            listener.onFailure(
                new ElasticsearchException(
                    format("Failed to load inference endpoint with secrets [%s], error: [%s]", inferenceEntityId, e.getMessage()),
                    e
                )
            );
        });

        QueryBuilder queryBuilder = documentIdQuery(inferenceEntityId);
        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(2)
            .setAllowPartialSearchResults(false)
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
        ActionListener<SearchResponse> searchListener = ActionListener.wrap((searchResponse) -> {
            // There should be a hit for the configurations
            if (searchResponse.getHits().getHits().length == 0) {
                var maybeDefault = defaultConfigIds.get(inferenceEntityId);
                if (maybeDefault != null) {
                    getDefaultConfig(true, maybeDefault, listener);
                } else {
                    listener.onFailure(inferenceNotFoundException(inferenceEntityId));
                }
                return;
            }

            var modelConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();
            assert modelConfigs.size() == 1;
            listener.onResponse(modelConfigs.get(0));
        }, e -> {
            logger.warn(format("Failed to load inference endpoint [%s]", inferenceEntityId), e);
            listener.onFailure(
                new ElasticsearchException(
                    format("Failed to load inference endpoint [%s], error: [%s]", inferenceEntityId, e.getMessage()),
                    e
                )
            );
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
            var modelConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();
            var defaultConfigsForTaskType = taskTypeMatchedDefaults(taskType, defaultConfigIds.values());
            addAllDefaultConfigsIfMissing(true, modelConfigs, defaultConfigsForTaskType, delegate);
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
     * If the defaults endpoint configurations have not been persisted then only
     * persist them if {@code persistDefaultEndpoints == true}. Persisting the
     * configs has the side effect of creating the index.
     *
     * Secret settings are not included
     * @param persistDefaultEndpoints Persist the defaults endpoint configurations if
     *                                not already persisted. When false this avoids the creation
     *                                of the backing index.
     * @param listener Models listener
     */
    public void getAllModels(boolean persistDefaultEndpoints, ActionListener<List<UnparsedModel>> listener) {
        ActionListener<SearchResponse> searchListener = listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            var foundConfigs = parseHitsAsModels(searchResponse.getHits()).stream().map(ModelRegistry::unparsedModelFromMap).toList();
            addAllDefaultConfigsIfMissing(persistDefaultEndpoints, foundConfigs, defaultConfigIds.values(), delegate);
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

    private void addAllDefaultConfigsIfMissing(
        boolean persistDefaultEndpoints,
        List<UnparsedModel> foundConfigs,
        Collection<InferenceService.DefaultConfigId> matchedDefaults,
        ActionListener<List<UnparsedModel>> listener
    ) {
        var foundIds = foundConfigs.stream().map(UnparsedModel::inferenceEntityId).collect(Collectors.toSet());
        var missing = matchedDefaults.stream().filter(d -> foundIds.contains(d.inferenceId()) == false).toList();

        if (missing.isEmpty()) {
            listener.onResponse(foundConfigs);
        } else {
            var groupedListener = new GroupedActionListener<UnparsedModel>(
                missing.size(),
                listener.delegateFailure((delegate, listOfModels) -> {
                    var allConfigs = new ArrayList<UnparsedModel>();
                    allConfigs.addAll(foundConfigs);
                    allConfigs.addAll(listOfModels);
                    allConfigs.sort(Comparator.comparing(UnparsedModel::inferenceEntityId));
                    delegate.onResponse(allConfigs);
                })
            );

            for (var required : missing) {
                getDefaultConfig(persistDefaultEndpoints, required, groupedListener);
            }
        }
    }

    private void getDefaultConfig(
        boolean persistDefaultEndpoints,
        InferenceService.DefaultConfigId defaultConfig,
        ActionListener<UnparsedModel> listener
    ) {
        defaultConfig.service().defaultConfigs(listener.delegateFailureAndWrap((delegate, models) -> {
            boolean foundModel = false;
            for (var m : models) {
                if (m.getInferenceEntityId().equals(defaultConfig.inferenceId())) {
                    foundModel = true;
                    if (persistDefaultEndpoints) {
                        storeDefaultEndpoint(m, () -> listener.onResponse(modelToUnparsedModel(m)));
                    } else {
                        listener.onResponse(modelToUnparsedModel(m));
                    }
                    break;
                }
            }

            if (foundModel == false) {
                listener.onFailure(
                    new IllegalStateException("Configuration not found for default inference id [" + defaultConfig.inferenceId() + "]")
                );
            }
        }));
    }

    private void storeDefaultEndpoint(Model preconfigured, Runnable runAfter) {
        var responseListener = ActionListener.<Boolean>wrap(success -> {
            logger.debug("Added default inference endpoint [{}]", preconfigured.getInferenceEntityId());
        }, exception -> {
            if (exception instanceof ResourceAlreadyExistsException) {
                logger.debug("Default inference id [{}] already exists", preconfigured.getInferenceEntityId());
            } else {
                logger.error("Failed to store default inference id [" + preconfigured.getInferenceEntityId() + "]", exception);
            }
        });

        // Store the model in the index without adding it to the cluster state,
        // as default models are already managed under defaultConfigIds.
        storeModel(preconfigured, false, ActionListener.runAfter(responseListener, runAfter), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
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
            var configRequestBuilder = createIndexRequestBuilder(
                inferenceEntityId,
                InferenceIndex.INDEX_NAME,
                newModel.getConfigurations(),
                true,
                client
            );

            ActionListener<BulkResponse> storeConfigListener = subListener.delegateResponse((l, e) -> {
                // this block will only be called if the bulk unexpectedly throws an exception
                preventDeletionLock.remove(inferenceEntityId);
                l.onFailure(e);
            });

            client.prepareBulk()
                .add(configRequestBuilder)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .execute(storeConfigListener);

        }).<BulkResponse>andThen((subListener, configResponse) -> {
            // in this block, we respond to the success or failure of updating the model configurations, then try to store the new secrets
            if (configResponse.hasFailures()) {
                // if storing the model configurations failed, it won't throw an exception, we need to check the BulkResponse and handle the
                // exceptions ourselves.
                logger.warn(
                    format("Failed to update inference endpoint [%s] due to [%s]", inferenceEntityId, configResponse.buildFailureMessage())
                );
                preventDeletionLock.remove(inferenceEntityId);
                // Since none of our updates succeeded at this point, we can simply return.
                finalListener.onFailure(
                    new ElasticsearchStatusException(
                        format(
                            "Failed to update inference endpoint [%s] due to [%s]",
                            inferenceEntityId,
                            configResponse.buildFailureMessage()
                        ),
                        RestStatus.INTERNAL_SERVER_ERROR,
                        configResponse.buildFailureMessage()
                    )
                );
            } else {
                // Since the model configurations were successfully updated, we can now try to store the new secrets
                var secretsRequestBuilder = createIndexRequestBuilder(
                    inferenceEntityId,
                    InferenceSecretsIndex.INDEX_NAME,
                    newModel.getSecrets(),
                    true,
                    client
                );

                ActionListener<BulkResponse> storeSecretsListener = subListener.delegateResponse((l, e) -> {
                    // this block will only be called if the bulk unexpectedly throws an exception
                    preventDeletionLock.remove(inferenceEntityId);
                    l.onFailure(e);
                });

                client.prepareBulk()
                    .add(secretsRequestBuilder)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(storeSecretsListener);
            }
        }).<BulkResponse>andThen((subListener, secretsResponse) -> {
            // in this block, we respond to the success or failure of updating the model secrets
            if (secretsResponse.hasFailures()) {
                // since storing the secrets failed, we will try to restore / roll-back-to the previous model configurations
                var configRequestBuilder = createIndexRequestBuilder(
                    inferenceEntityId,
                    InferenceIndex.INDEX_NAME,
                    existingModel.getConfigurations(),
                    true,
                    client
                );

                logger.warn(
                    "Failed to update inference endpoint secrets [{}], attempting rolling back to previous state",
                    inferenceEntityId
                );

                ActionListener<BulkResponse> rollbackConfigListener = subListener.delegateResponse((l, e) -> {
                    // this block will only be called if the bulk unexpectedly throws an exception
                    preventDeletionLock.remove(inferenceEntityId);
                    l.onFailure(e);
                });
                client.prepareBulk()
                    .add(configRequestBuilder)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(rollbackConfigListener);
            } else {
                // since updating the secrets was successful, we can remove the lock and respond to the final listener
                preventDeletionLock.remove(inferenceEntityId);
                refreshInferenceEndpointCache();
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
                            inferenceEntityId,
                            configResponse.buildFailureMessage()
                        ),
                        RestStatus.INTERNAL_SERVER_ERROR
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
     *
     * <p><b>WARNING:</b> This function must always be called on a master node. Failure to do so will result in an error.
     */
    public void storeModel(Model model, ActionListener<Boolean> listener, TimeValue timeout) {
        storeModel(model, true, listener, timeout);
    }

    private void storeModel(Model model, boolean updateClusterState, ActionListener<Boolean> listener, TimeValue timeout) {
        storeModels(List.of(model), updateClusterState, listener.delegateFailureAndWrap((delegate, responses) -> {
            var firstFailureResponse = responses.stream().filter(ModelStoreResponse::failed).findFirst();
            if (firstFailureResponse.isPresent() == false) {
                delegate.onResponse(Boolean.TRUE);
                return;
            }

            var failureItem = firstFailureResponse.get();
            if (ExceptionsHelper.unwrapCause(failureItem.failureCause()) instanceof VersionConflictEngineException) {
                delegate.onFailure(new ResourceAlreadyExistsException("Inference endpoint [{}] already exists", failureItem.inferenceId()));
                return;
            }

            delegate.onFailure(
                new ElasticsearchStatusException(
                    format("Failed to store inference endpoint [%s]", failureItem.inferenceId()),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    failureItem.failureCause()
                )
            );
        }), timeout);
    }

    public void storeModels(List<Model> models, ActionListener<List<ModelStoreResponse>> listener, TimeValue timeout) {
        storeModels(models, true, listener, timeout);
    }

    private void storeModels(
        List<Model> models,
        boolean updateClusterState,
        ActionListener<List<ModelStoreResponse>> listener,
        TimeValue timeout
    ) {
        if (models.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }

        var modelsWithoutDuplicates = models.stream().distinct().toList();

        var bulkRequestBuilder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (var model : modelsWithoutDuplicates) {
            bulkRequestBuilder.add(
                createIndexRequestBuilder(model.getInferenceEntityId(), InferenceIndex.INDEX_NAME, model.getConfigurations(), false, client)
            );

            bulkRequestBuilder.add(
                createIndexRequestBuilder(model.getInferenceEntityId(), InferenceSecretsIndex.INDEX_NAME, model.getSecrets(), false, client)
            );
        }

        bulkRequestBuilder.execute(getStoreMultipleModelsListener(modelsWithoutDuplicates, updateClusterState, listener, timeout));
    }

    private ActionListener<BulkResponse> getStoreMultipleModelsListener(
        List<Model> models,
        boolean updateClusterState,
        ActionListener<List<ModelStoreResponse>> listener,
        TimeValue timeout
    ) {
        var cleanupListener = listener.<List<StoreResponseWithIndexInfo>>delegateFailureAndWrap((delegate, responses) -> {
            var inferenceIdsToBeRemoved = responses.stream()
                .filter(r -> r.modifiedIndex() && r.modelStoreResponse().failed())
                .map(r -> r.modelStoreResponse().inferenceId())
                .collect(Collectors.toSet());

            var storageResponses = responses.stream().map(StoreResponseWithIndexInfo::modelStoreResponse).toList();

            ActionListener<Boolean> deleteListener = ActionListener.wrap(ignored -> delegate.onResponse(storageResponses), e -> {
                logger.atWarn()
                    .withThrowable(e)
                    .log(
                        "Failed to clean up partially stored inference endpoints {}. "
                            + "The service may be in an inconsistent state. Please try deleting and re-adding the endpoints.",
                        inferenceIdsToBeRemoved
                    );
                delegate.onResponse(storageResponses);
            });

            deleteModels(inferenceIdsToBeRemoved, deleteListener);
        });

        return ActionListener.wrap(bulkItemResponses -> {
            var docIdToInferenceId = models.stream()
                .collect(Collectors.toMap(m -> Model.documentId(m.getInferenceEntityId()), Model::getInferenceEntityId, (id1, id2) -> {
                    logger.warn("Encountered duplicate inference ids when storing endpoints: [{}]", id1);
                    return id1;
                }));
            var inferenceIdToModel = models.stream()
                .collect(Collectors.toMap(Model::getInferenceEntityId, Function.identity(), (id1, id2) -> id1));

            if (bulkItemResponses.getItems().length == 0) {
                var inferenceEntityIds = String.join(", ", models.stream().map(Model::getInferenceEntityId).toList());
                logger.warn("Storing inference endpoints [{}] failed, no items were received from the bulk response", inferenceEntityIds);

                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Failed to store inference endpoints [{}], empty bulk response received.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        inferenceEntityIds
                    )
                );
                return;
            }

            var responseInfo = getResponseInfo(bulkItemResponses, docIdToInferenceId, inferenceIdToModel);

            if (updateClusterState) {
                updateClusterState(
                    responseInfo,
                    cleanupListener.delegateFailureIgnoreResponseAndWrap(delegate -> delegate.onResponse(responseInfo.responses())),
                    timeout
                );
            } else {
                cleanupListener.onResponse(responseInfo.responses());
            }
        }, e -> {
            String errorMessage = format(
                "Failed to store inference endpoints [%s]",
                models.stream().map(Model::getInferenceEntityId).collect(Collectors.joining(", "))
            );
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchStatusException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e));
        });
    }

    private record StoreResponseWithIndexInfo(ModelStoreResponse modelStoreResponse, boolean modifiedIndex) {}

    private record ResponseInfo(List<StoreResponseWithIndexInfo> responses, List<Model> successfullyStoredModels) {}

    private static ResponseInfo getResponseInfo(
        BulkResponse bulkResponse,
        Map<String, String> docIdToInferenceId,
        Map<String, Model> inferenceIdToModel
    ) {
        var responses = new ArrayList<StoreResponseWithIndexInfo>();
        var successfullyStoredModels = new ArrayList<Model>();

        var bulkItems = bulkResponse.getItems();
        for (int i = 0; i < bulkItems.length; i += 2) {
            var configurationItem = bulkItems[i];
            var configStoreResponse = createModelStoreResponse(configurationItem, docIdToInferenceId);
            var modelFromBulkItem = getModelFromMap(docIdToInferenceId.get(configurationItem.getId()), inferenceIdToModel);

            if (i + 1 >= bulkResponse.getItems().length) {
                logger.error("Expected an even number of bulk response items, got [{}]", bulkResponse.getItems().length);

                if (configStoreResponse.failed()) {
                    responses.add(new StoreResponseWithIndexInfo(configStoreResponse, false));
                } else {
                    // if we didn't get the last item for some reason, assume it is a failure
                    responses.add(
                        new StoreResponseWithIndexInfo(
                            new ModelStoreResponse(
                                configStoreResponse.inferenceId(),
                                RestStatus.INTERNAL_SERVER_ERROR,
                                new IllegalStateException("Failed to receive part of bulk response")
                            ),
                            true
                        )
                    );
                }
                return new ResponseInfo(responses, successfullyStoredModels);
            }

            var secretsItem = bulkItems[i + 1];
            var secretsStoreResponse = createModelStoreResponse(secretsItem, docIdToInferenceId);

            assert secretsStoreResponse.inferenceId().equals(configStoreResponse.inferenceId())
                : "Mismatched inference ids in bulk response items, configuration id ["
                    + configStoreResponse.inferenceId()
                    + "] secrets id ["
                    + secretsStoreResponse.inferenceId()
                    + "]";

            if (configStoreResponse.failed()) {
                responses.add(new StoreResponseWithIndexInfo(configStoreResponse, secretsStoreResponse.failed() == false));
            } else if (secretsStoreResponse.failed()) {
                // if we got here that means the configuration store bulk item succeeded, so we did modify an index
                responses.add(new StoreResponseWithIndexInfo(secretsStoreResponse, true));
            } else {
                responses.add(new StoreResponseWithIndexInfo(configStoreResponse, true));
                if (modelFromBulkItem != null) {
                    successfullyStoredModels.add(modelFromBulkItem);
                }
            }
        }

        return new ResponseInfo(responses, successfullyStoredModels);
    }

    private static ModelStoreResponse createModelStoreResponse(BulkItemResponse item, Map<String, String> docIdToInferenceId) {
        var failure = item.getFailure();

        String inferenceIdOrUnknown = "unknown";
        var inferenceIdMaybeNull = docIdToInferenceId.get(item.getId());
        if (inferenceIdMaybeNull == null) {
            logger.warn("Failed to find inference id for document id [{}]", item.getId());
        } else {
            inferenceIdOrUnknown = inferenceIdMaybeNull;
        }

        if (item.isFailed() && failure != null) {
            logger.warn(
                format(
                    "Failed to store document id: [%s] inference id: [%s] index: [%s] bulk failure message [%s]",
                    item.getId(),
                    inferenceIdOrUnknown,
                    item.getIndex(),
                    item.getFailureMessage()
                )
            );

            return new ModelStoreResponse(inferenceIdOrUnknown, item.status(), failure.getCause());
        } else {
            return new ModelStoreResponse(inferenceIdOrUnknown, item.status(), null);
        }
    }

    private static Model getModelFromMap(@Nullable String inferenceId, Map<String, Model> inferenceIdToModel) {
        if (inferenceId != null) {
            return inferenceIdToModel.get(inferenceId);
        }

        return null;
    }

    private void updateClusterState(ResponseInfo responseInfo, ActionListener<AcknowledgedResponse> listener, TimeValue timeout) {
        var inferenceIdsSet = responseInfo.successfullyStoredModels().stream().map(Model::getInferenceEntityId).collect(Collectors.toSet());

        SubscribableListener.<Void>newForked(outOfSyncListener -> handleOutOfSyncEndpoints(responseInfo, outOfSyncListener, timeout))
            .<AcknowledgedResponse>andThen(addModelMetadataTaskListener -> {
                var cleanupListener = addModelMetadataTaskListener.delegateResponse((delegate, exc) -> {
                    logger.atWarn()
                        .withThrowable(exc)
                        .log("Failed to add minimal service settings to cluster state for inference endpoints {}", inferenceIdsSet);
                    deleteModels(
                        inferenceIdsSet,
                        ActionListener.running(
                            () -> delegate.onFailure(
                                new ElasticsearchStatusException(
                                    format(
                                        "Failed to add the inference endpoints %s. The service may be in an "
                                            + "inconsistent state. Please try deleting and re-adding the endpoints.",
                                        inferenceIdsSet
                                    ),
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    exc
                                )
                            )
                        )
                    );
                });

                metadataTaskQueue.submitTask(
                    format("add model metadata for %s", inferenceIdsSet),
                    new ModelRegistryMetadataTask.AddModelMetadataTask(
                        ProjectId.DEFAULT,
                        responseInfo.successfullyStoredModels()
                            .stream()
                            .map(
                                model -> new ModelRegistryMetadataTask.ModelAndSettings(
                                    model.getInferenceEntityId(),
                                    new MinimalServiceSettings(model)
                                )
                            )
                            .toList(),
                        cleanupListener
                    ),
                    timeout
                );
            })
            .addListener(listener);
    }

    /**
     * An out of sync endpoint is one that exists in the model store index, but is not present in the cluster state. This doesn't usually
     * happen. It did happen when we transitioned the EIS preconfigured endpoints from being stored in the in memory cache local to
     * each node in {@link #defaultConfigIds} to being stored in cluster state. The issue was that when we transitioned the logic
     * the {@link ElasticInferenceService} no longer registered the preconfigured endpoints on an authorization poll to the in memory
     * cache. The polling logic was moved to a persistent task that only runs on a single node. Since we're only running on one node
     * we need to store the information in a way that all nodes can access it, hence storing in cluster state. The logic to store the
     * information in cluster state was only triggered when a preconfigured endpoint was missing from cluster state (it gets flagged as
     * being new). So the authorization logic would receive the preconfigured endpoints from EIS and attempt to store them. When it
     * stored them a version conflict would occur since the index already had the documents. So this logic identifies that scenario and
     * adds the missing information to cluster state.
     */
    private void handleOutOfSyncEndpoints(ResponseInfo responseInfo, ActionListener<Void> listener, TimeValue timeout) {
        var outOfSyncEndpointsExist = responseInfo.responses.stream()
            .anyMatch(
                response -> response.modelStoreResponse().failed()
                    && response.modelStoreResponse().failureCause() instanceof VersionConflictEngineException
                    // Technically this can only happen for EIS preconfigured endpoints, but checking generally
                    && containsInferenceEndpointId(response.modelStoreResponse().inferenceId()) == false
            );

        if (outOfSyncEndpointsExist == false) {
            listener.onResponse(null);
            return;
        }

        var fixOutOfSyncListener = ActionListener.<GetInferenceModelAction.Response>wrap((response) -> {
            var outOfSyncEndpoints = new ArrayList<ModelRegistryMetadataTask.ModelAndSettings>();

            for (var model : response.getEndpoints()) {
                // If the inference id can't be found in the in memory hash map or the cluster state, then it is out of sync
                if (containsInferenceEndpointId(model.getInferenceEntityId()) == false) {
                    outOfSyncEndpoints.add(
                        new ModelRegistryMetadataTask.ModelAndSettings(
                            model.getInferenceEntityId(),
                            new MinimalServiceSettings(
                                model.getService(),
                                model.getTaskType(),
                                model.getServiceSettings().dimensions(),
                                model.getServiceSettings().similarity(),
                                model.getServiceSettings().elementType()
                            )
                        )
                    );
                }
            }

            if (outOfSyncEndpoints.isEmpty()) {
                listener.onResponse(null);
                return;
            }

            // Add the missing endpoints information from the index to the cluster state
            // This only updates cluster state and not the index
            submitEndpointMetadataToClusterState(outOfSyncEndpoints, listener, timeout);
        }, e -> {
            logger.atWarn().withThrowable(e).log("Failed to retrieve all endpoints to fix out of sync ones");
            listener.onResponse(null);
        });

        client.execute(
            GetInferenceModelAction.INSTANCE,
            new GetInferenceModelAction.Request("*", TaskType.ANY, false),
            fixOutOfSyncListener
        );
    }

    /**
     * Returns true if the model registry contains the provided inference entity id. This includes both preconfigured and user created
     * inference endpoints.
     * @param inferenceEntityId the id to search for
     * @return true if we find a match and false if not
     */
    private boolean containsInferenceEndpointId(String inferenceEntityId) {
        // This checks an in memory cache local to the node. The cache should be the same across all nodes as it is populated in the
        // inference plugin on boot up (excluding an upgrade scenario where the plugins could be different).
        // This primarily holds endpoints registered by the ElasticsearchInternalService
        if (defaultConfigIds.containsKey(inferenceEntityId)) {
            return true;
        }

        // This checks the cluster state for user created endpoints as well as EIS preconfigured endpoints
        if (lastMetadata.get() != null) {
            var project = lastMetadata.get().getProject(ProjectId.DEFAULT);
            var state = ModelRegistryClusterStateMetadata.fromState(project);
            var allInferenceIds = state.getInferenceIds();
            return allInferenceIds.contains(inferenceEntityId);
        }

        return false;
    }

    private void submitEndpointMetadataToClusterState(
        ArrayList<ModelRegistryMetadataTask.ModelAndSettings> endpoints,
        ActionListener<Void> listener,
        TimeValue timeout
    ) {
        metadataTaskQueue.submitTask(
            format(
                "adding out of sync endpoint metadata for %s",
                endpoints.stream().map(ModelRegistryMetadataTask.ModelAndSettings::inferenceEntityId).toList()
            ),
            new ModelRegistryMetadataTask.AddModelMetadataTask(
                ProjectId.DEFAULT,
                endpoints,
                ActionListener.wrap((result) -> listener.onResponse(null), e -> {
                    logger.atWarn().withThrowable(e).log("Failed while submitting task to fix out of sync endpoints");
                    listener.onResponse(null);
                })
            ),
            timeout
        );
    }

    public boolean isReady() {
        if (lastMetadata.get() == null) {
            return false;
        }

        return clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;
    }

    public synchronized void removeDefaultConfigs(Set<String> inferenceEntityIds, ActionListener<Boolean> listener) {
        if (inferenceEntityIds.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        defaultConfigIds.keySet().removeAll(inferenceEntityIds);
        // default models are not stored in the cluster state.
        deleteModels(inferenceEntityIds, false, listener);
    }

    public void deleteModel(String inferenceEntityId, ActionListener<Boolean> listener) {
        deleteModels(Set.of(inferenceEntityId), listener);
    }

    public void deleteModels(Set<String> inferenceEntityIds, ActionListener<Boolean> listener) {
        deleteModels(inferenceEntityIds, true, listener);
    }

    private void deleteModels(Set<String> inferenceEntityIds, boolean updateClusterState, ActionListener<Boolean> listener) {
        if (inferenceEntityIds.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        var lockedInferenceIds = new HashSet<>(inferenceEntityIds);
        lockedInferenceIds.retainAll(preventDeletionLock);

        if (lockedInferenceIds.isEmpty() == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    Strings.format(
                        "The inference endpoint(s) %s are currently being updated, please wait until after they are "
                            + "finished updating to delete.",
                        lockedInferenceIds
                    ),
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        var request = createDeleteRequest(inferenceEntityIds);
        client.execute(
            DeleteByQueryAction.INSTANCE,
            request,
            ActionListener.runAfter(
                getDeleteModelClusterStateListener(inferenceEntityIds, updateClusterState, listener),
                this::refreshInferenceEndpointCache
            )
        );
    }

    private ActionListener<BulkByScrollResponse> getDeleteModelClusterStateListener(
        Set<String> inferenceEntityIds,
        boolean updateClusterState,
        ActionListener<Boolean> listener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                if (updateClusterState == false) {
                    listener.onResponse(Boolean.TRUE);
                    return;
                }
                var clusterStateListener = new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        listener.onResponse(acknowledgedResponse.isAcknowledged());
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                format(
                                    "Failed to delete the inference endpoint [%s]. The service may be in an "
                                        + "inconsistent state. Please try deleting the endpoint again.",
                                    inferenceEntityIds
                                ),
                                RestStatus.INTERNAL_SERVER_ERROR,
                                exc
                            )
                        );
                    }
                };
                try {
                    metadataTaskQueue.submitTask(
                        "delete models [" + inferenceEntityIds + "]",
                        new ModelRegistryMetadataTask.DeleteModelMetadataTask(ProjectId.DEFAULT, inferenceEntityIds, clusterStateListener),
                        null
                    );
                } catch (Exception exc) {
                    clusterStateListener.onFailure(exc);
                }
            }

            @Override
            public void onFailure(Exception exc) {
                listener.onFailure(exc);
            }
        };
    }

    private void refreshInferenceEndpointCache() {
        client.execute(
            ClearInferenceEndpointCacheAction.INSTANCE,
            new ClearInferenceEndpointCacheAction.Request(),
            ActionListener.wrap(
                ignored -> logger.debug("Successfully refreshed inference endpoint cache."),
                e -> logger.atDebug().withThrowable(e).log("Failed to refresh inference endpoint cache.")
            )
        );
    }

    private static DeleteByQueryRequest createDeleteRequest(Set<String> inferenceEntityIds) {
        DeleteByQueryRequest request = new DeleteByQueryRequest().setAbortOnVersionConflict(false);
        request.indices(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN);
        request.setQuery(documentIdsQuery(inferenceEntityIds));
        request.setRefresh(true);
        return request;
    }

    // public for testing
    public static IndexRequestBuilder createIndexRequestBuilder(
        String inferenceId,
        String indexName,
        ToXContentObject body,
        boolean allowOverwriting,
        Client client
    ) {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = body.toXContent(
                xContentBuilder,
                new ToXContent.MapParams(Map.of(ModelConfigurations.USE_ID_FOR_INDEX, Boolean.TRUE.toString()))
            );

            return new IndexRequestBuilder(client).setIndex(indexName)
                .setCreate(allowOverwriting == false)
                .setId(Model.documentId(inferenceId))
                .setSource(source);
        } catch (IOException ex) {
            throw new ElasticsearchException(
                "Unexpected serialization exception for index [{}] inference ID [{}]",
                ex,
                indexName,
                inferenceId
            );
        }
    }

    private static UnparsedModel modelToUnparsedModel(Model model) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            model.getConfigurations()
                .toXContent(builder, new ToXContent.MapParams(Map.of(ModelConfigurations.USE_ID_FOR_INDEX, Boolean.TRUE.toString())));

            var modelConfigMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            return unparsedModelFromMap(new ModelConfigMap(modelConfigMap, new HashMap<>()));

        } catch (IOException ex) {
            throw new ElasticsearchException("[{}] Error serializing inference endpoint configuration", model.getInferenceEntityId(), ex);
        }
    }

    private static QueryBuilder documentIdQuery(String inferenceEntityId) {
        return QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(Model.documentId(inferenceEntityId)));
    }

    private static QueryBuilder documentIdsQuery(Set<String> inferenceEntityIds) {
        var documentIdsArray = inferenceEntityIds.stream().map(Model::documentId).toArray(String[]::new);
        return QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(documentIdsArray));
    }

    static Optional<InferenceService.DefaultConfigId> idMatchedDefault(
        String inferenceId,
        List<InferenceService.DefaultConfigId> defaultConfigIds
    ) {
        return defaultConfigIds.stream().filter(defaultConfigId -> defaultConfigId.inferenceId().equals(inferenceId)).findFirst();
    }

    static List<InferenceService.DefaultConfigId> taskTypeMatchedDefaults(
        TaskType taskType,
        Collection<InferenceService.DefaultConfigId> defaultConfigIds
    ) {
        return defaultConfigIds.stream()
            .filter(defaultConfigId -> defaultConfigId.settings().taskType().equals(taskType))
            .collect(Collectors.toList());
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (lastMetadata.get() == null || event.metadataChanged()) {
            // keep track of the last applied cluster state
            lastMetadata.set(event.state().metadata());
        }

        if (event.localNodeMaster() == false) {
            return;
        }

        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (event.state().metadata().projects().size() > 1) {
            // TODO: Add support to handle multi-projects
            return;
        }

        var state = ModelRegistryClusterStateMetadata.fromState(event.state().projectState().metadata());
        if (state.isUpgraded()) {
            return;
        }

        if (upgradeMetadataInProgress.compareAndSet(false, true) == false) {
            return;
        }

        // GetInferenceModelAction is used because ModelRegistry does not know how to parse the service settings
        client.execute(
            GetInferenceModelAction.INSTANCE,
            new GetInferenceModelAction.Request("*", TaskType.ANY, false),
            new ActionListener<>() {
                @Override
                public void onResponse(GetInferenceModelAction.Response response) {
                    Map<String, MinimalServiceSettings> map = new HashMap<>();
                    for (var model : response.getEndpoints()) {
                        // ignore default models
                        if (defaultConfigIds.containsKey(model.getInferenceEntityId()) == false) {
                            map.put(
                                model.getInferenceEntityId(),
                                new MinimalServiceSettings(
                                    model.getService(),
                                    model.getTaskType(),
                                    model.getServiceSettings().dimensions(),
                                    model.getServiceSettings().similarity(),
                                    model.getServiceSettings().elementType()
                                )
                            );
                        }
                    }
                    metadataTaskQueue.submitTask(
                        "model registry auto upgrade",
                        new ModelRegistryMetadataTask.UpgradeModelsMetadataTask(
                            ProjectId.DEFAULT,
                            map,
                            ActionListener.running(() -> upgradeMetadataInProgress.set(false))
                        ),
                        null
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    upgradeMetadataInProgress.set(false);
                }
            }
        );
    }
}
