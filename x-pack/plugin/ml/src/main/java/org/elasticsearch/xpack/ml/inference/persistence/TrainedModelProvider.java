/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_FAILED_TO_DESERIALIZE;

public class TrainedModelProvider {

    public static final Set<String> MODELS_STORED_AS_RESOURCE = Collections.singleton("lang_ident_model_1");
    private static final String MODEL_RESOURCE_PATH = "/org/elasticsearch/xpack/ml/inference/persistence/";
    private static final String MODEL_RESOURCE_FILE_EXT = ".json";
    private static final int COMPRESSED_STRING_CHUNK_SIZE = 16 * 1024 * 1024;
    private static final int MAX_NUM_DEFINITION_DOCS = 100;
    private static final int MAX_COMPRESSED_STRING_SIZE = COMPRESSED_STRING_CHUNK_SIZE * MAX_NUM_DEFINITION_DOCS;

    private static final Logger logger = LogManager.getLogger(TrainedModelProvider.class);
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

    public TrainedModelProvider(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void storeTrainedModel(TrainedModelConfig trainedModelConfig,
                                  ActionListener<Boolean> listener) {
        if (MODELS_STORED_AS_RESOURCE.contains(trainedModelConfig.getModelId())) {
            listener.onFailure(new ResourceAlreadyExistsException(
                Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
            return;
        }

        try {
            trainedModelConfig.ensureParsedDefinition(xContentRegistry);
        } catch (IOException ex) {
            listener.onFailure(ExceptionsHelper.serverError(
                "Unexpected serialization error when parsing model definition for model [" + trainedModelConfig.getModelId() + "]",
                ex));
            return;
        }

        TrainedModelDefinition definition = trainedModelConfig.getModelDefinition();
        if (definition == null) {
            listener.onFailure(ExceptionsHelper.badRequestException("Unable to store [{}]. [{}] is required",
                trainedModelConfig.getModelId(),
                TrainedModelConfig.DEFINITION.getPreferredName()));
            return;
        }

        storeTrainedModelAndDefinition(trainedModelConfig, listener);
    }

    public void storeTrainedModelConfig(TrainedModelConfig trainedModelConfig, ActionListener<Boolean> listener) {
        if (MODELS_STORED_AS_RESOURCE.contains(trainedModelConfig.getModelId())) {
            listener.onFailure(new ResourceAlreadyExistsException(
                Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
            return;
        }
        assert trainedModelConfig.getModelDefinition() == null;

        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            IndexAction.INSTANCE,
            createRequest(trainedModelConfig.getModelId(), InferenceIndexConstants.LATEST_INDEX_NAME, trainedModelConfig),
            ActionListener.wrap(
                indexResponse -> listener.onResponse(true),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        listener.onFailure(new ResourceAlreadyExistsException(
                            Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
                    } else {
                        listener.onFailure(
                            new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL,
                                RestStatus.INTERNAL_SERVER_ERROR,
                                e,
                                trainedModelConfig.getModelId()));
                    }
                }
            ));
    }

    public void storeTrainedModelDefinitionDoc(TrainedModelDefinitionDoc trainedModelDefinitionDoc, ActionListener<Void> listener) {
        if (MODELS_STORED_AS_RESOURCE.contains(trainedModelDefinitionDoc.getModelId())) {
            listener.onFailure(new ResourceAlreadyExistsException(
                Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelDefinitionDoc.getModelId())));
            return;
        }

        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            IndexAction.INSTANCE,
            createRequest(trainedModelDefinitionDoc.getDocId(), InferenceIndexConstants.LATEST_INDEX_NAME, trainedModelDefinitionDoc),
            ActionListener.wrap(
                indexResponse -> listener.onResponse(null),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        listener.onFailure(new ResourceAlreadyExistsException(
                            Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_DOC_EXISTS,
                                trainedModelDefinitionDoc.getModelId(),
                                trainedModelDefinitionDoc.getDocNum())));
                    } else {
                        listener.onFailure(
                            new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL,
                                RestStatus.INTERNAL_SERVER_ERROR,
                                e,
                                trainedModelDefinitionDoc.getModelId()));
                    }
                }
            ));
    }

    public void storeTrainedModelMetadata(TrainedModelMetadata trainedModelMetadata, ActionListener<Void> listener) {
        if (MODELS_STORED_AS_RESOURCE.contains(trainedModelMetadata.getModelId())) {
            listener.onFailure(new ResourceAlreadyExistsException(
                Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelMetadata.getModelId())));
            return;
        }
        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            IndexAction.INSTANCE,
            createRequest(trainedModelMetadata.getDocId(), InferenceIndexConstants.LATEST_INDEX_NAME, trainedModelMetadata),
            ActionListener.wrap(
                indexResponse -> listener.onResponse(null),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                        listener.onFailure(new ResourceAlreadyExistsException(
                            Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_METADATA_EXISTS,
                                trainedModelMetadata.getModelId())));
                    } else {
                        listener.onFailure(
                            new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL_METADATA,
                                RestStatus.INTERNAL_SERVER_ERROR,
                                e,
                                trainedModelMetadata.getModelId()));
                    }
                }
            ));
    }

    public void getTrainedModelMetadata(Collection<String> modelIds, ActionListener<Map<String, TrainedModelMetadata>> listener) {
        SearchRequest searchRequest = client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders
                .boolQuery()
                .filter(QueryBuilders.termsQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelIds))
                .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(),
                    TrainedModelMetadata.NAME))))
            .setSize(10_000)
            // First find the latest index
            .addSort("_index", SortOrder.DESC)
            .request();
        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
            searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    listener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_METADATA_NOT_FOUND, modelIds)));
                    return;
                }
                HashMap<String, TrainedModelMetadata> map = new HashMap<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    String modelId = TrainedModelMetadata.modelId(Objects.requireNonNull(hit.getId()));
                    map.putIfAbsent(modelId, parseMetadataLenientlyFromSource(hit.getSourceRef(), modelId));
                }
                listener.onResponse(map);
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_METADATA_NOT_FOUND, modelIds)));
                    return;
                }
                listener.onFailure(e);
            }
        ));
    }

    public void refreshInferenceIndex(ActionListener<RefreshResponse> listener) {
        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            RefreshAction.INSTANCE,
            new RefreshRequest(InferenceIndexConstants.INDEX_PATTERN),
            listener);
    }

    private void storeTrainedModelAndDefinition(TrainedModelConfig trainedModelConfig,
                                                ActionListener<Boolean> listener) {

        List<TrainedModelDefinitionDoc> trainedModelDefinitionDocs = new ArrayList<>();
        try {
            String compressedString = trainedModelConfig.getCompressedDefinition();
            if (compressedString.length() > MAX_COMPRESSED_STRING_SIZE) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Unable to store model as compressed definition has length [{}] the limit is [{}]",
                        compressedString.length(),
                        MAX_COMPRESSED_STRING_SIZE));
                return;
            }
            List<String> chunkedStrings = chunkStringWithSize(compressedString, COMPRESSED_STRING_CHUNK_SIZE);
            for(int i = 0; i < chunkedStrings.size(); ++i) {
                trainedModelDefinitionDocs.add(new TrainedModelDefinitionDoc.Builder()
                    .setDocNum(i)
                    .setModelId(trainedModelConfig.getModelId())
                    .setCompressedString(chunkedStrings.get(i))
                    .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                    .setDefinitionLength(chunkedStrings.get(i).length())
                    // If it is the last doc, it is the EOS
                    .setEos(i == chunkedStrings.size() - 1)
                    .build());
            }
        } catch (IOException ex) {
            listener.onFailure(ExceptionsHelper.serverError(
                "Unexpected IOException while serializing definition for storage for model [{}]",
                ex,
                trainedModelConfig.getModelId()));
            return;
        }

        BulkRequestBuilder bulkRequest = client.prepareBulk(InferenceIndexConstants.LATEST_INDEX_NAME, MapperService.SINGLE_MAPPING_NAME)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(createRequest(trainedModelConfig.getModelId(), trainedModelConfig));
        trainedModelDefinitionDocs.forEach(defDoc ->
            bulkRequest.add(createRequest(TrainedModelDefinitionDoc.docId(trainedModelConfig.getModelId(), defDoc.getDocNum()), defDoc)));

        ActionListener<Boolean> wrappedListener = ActionListener.wrap(
            listener::onResponse,
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    listener.onFailure(new ResourceAlreadyExistsException(
                        Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL,
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e,
                            trainedModelConfig.getModelId()));
                }
            }
        );

        ActionListener<BulkResponse> bulkResponseActionListener = ActionListener.wrap(
            r -> {
                assert r.getItems().length == trainedModelDefinitionDocs.size() + 1;
                if (r.getItems()[0].isFailed()) {
                    logger.error(new ParameterizedMessage(
                            "[{}] failed to store trained model config for inference",
                            trainedModelConfig.getModelId()),
                        r.getItems()[0].getFailure().getCause());

                    wrappedListener.onFailure(r.getItems()[0].getFailure().getCause());
                    return;
                }
                if (r.hasFailures()) {
                    Exception firstFailure = Arrays.stream(r.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .map(BulkItemResponse::getFailure)
                        .map(BulkItemResponse.Failure::getCause)
                        .findFirst()
                        .orElse(new Exception("unknown failure"));
                    logger.error(new ParameterizedMessage(
                            "[{}] failed to store trained model definition for inference",
                            trainedModelConfig.getModelId()),
                        firstFailure);
                    wrappedListener.onFailure(firstFailure);
                    return;
                }
                wrappedListener.onResponse(true);
            },
            wrappedListener::onFailure
        );

        executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkRequest.request(), bulkResponseActionListener);
    }

    /**
     * Get the model definition for inference.
     *
     * The caller should ensure the requested model has an InferenceDefinition,
     *
     * @param modelId The model tp get
     * @param unsafe when true, the compressed bytes size is not checked and the circuit breaker is solely responsible for
     *               preventing OOMs
     * @param listener The listener
     */
    public void getTrainedModelForInference(final String modelId, boolean unsafe, final ActionListener<InferenceDefinition> listener) {
        // TODO Change this when we get more than just langIdent stored
        if (MODELS_STORED_AS_RESOURCE.contains(modelId)) {
            try {
                TrainedModelConfig config = loadModelFromResource(modelId, false)
                    .build()
                    .ensureParsedDefinitionUnsafe(xContentRegistry);
                assert config.getModelDefinition().getTrainedModel() instanceof LangIdentNeuralNetwork;
                listener.onResponse(
                    InferenceDefinition.builder()
                        .setPreProcessors(config.getModelDefinition().getPreProcessors())
                        .setTrainedModel((LangIdentNeuralNetwork)config.getModelDefinition().getTrainedModel())
                        .build());
                return;
            } catch (ElasticsearchException|IOException ex) {
                listener.onFailure(ex);
                return;
            }
        }

        SearchRequest searchRequest = client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders
                .boolQuery()
                .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(),
                    TrainedModelDefinitionDoc.NAME))))
            .setSize(MAX_NUM_DEFINITION_DOCS)
            // First find the latest index
            .addSort("_index", SortOrder.DESC)
            // Then, sort by doc_num
            .addSort(SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName())
                .order(SortOrder.ASC)
                .unmappedType("long"))
            .request();
        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
            // TODO how could we stream in the model definition WHILE parsing it?
            // This would reduce the overall memory usage as we won't have to load the whole compressed string
            // XContentParser supports streams.
            searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    listener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                    return;
                }
                List<TrainedModelDefinitionDoc> docs = handleHits(searchResponse.getHits().getHits(),
                    modelId,
                    this::parseModelDefinitionDocLenientlyFromSource);
                try {
                    String compressedString = getDefinitionFromDocs(docs, modelId);
                    InferenceDefinition inferenceDefinition = unsafe ?
                    InferenceToXContentCompressor.inflateUnsafe(compressedString, InferenceDefinition::fromXContent, xContentRegistry) :
                    InferenceToXContentCompressor.inflate(compressedString, InferenceDefinition::fromXContent, xContentRegistry);
                    listener.onResponse(inferenceDefinition);
                } catch (ElasticsearchException elasticsearchException) {
                    listener.onFailure(elasticsearchException);
                }
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                    return;
                }
                listener.onFailure(e);
            }
        ));
    }

    public void getTrainedModel(final String modelId,
                                final GetTrainedModelsAction.Includes includes,
                                final ActionListener<TrainedModelConfig> finalListener) {
        getTrainedModel(modelId, Collections.emptySet(), includes, finalListener);
    }

    public void getTrainedModel(final String modelId,
                                final Set<String> modelAliases,
                                final GetTrainedModelsAction.Includes includes,
                                final ActionListener<TrainedModelConfig> finalListener) {

        if (MODELS_STORED_AS_RESOURCE.contains(modelId)) {
            try {
                finalListener.onResponse(loadModelFromResource(modelId, includes.isIncludeModelDefinition() == false).build());
                return;
            } catch (ElasticsearchException ex) {
                finalListener.onFailure(ex);
                return;
            }
        }

        ActionListener<TrainedModelConfig.Builder> getTrainedModelListener = ActionListener.wrap(
            modelBuilder -> {
                modelBuilder.setModelAliases(modelAliases);
                if ((includes.isIncludeFeatureImportanceBaseline() || includes.isIncludeTotalFeatureImportance()
                  || includes.isIncludeHyperparameters()) == false) {
                    finalListener.onResponse(modelBuilder.build());
                    return;
                }
                this.getTrainedModelMetadata(Collections.singletonList(modelId), ActionListener.wrap(
                    metadata -> {
                        TrainedModelMetadata modelMetadata = metadata.get(modelId);
                        if (modelMetadata != null) {
                            if (includes.isIncludeTotalFeatureImportance()) {
                                modelBuilder.setFeatureImportance(modelMetadata.getTotalFeatureImportances());
                            }
                            if (includes.isIncludeFeatureImportanceBaseline()) {
                                modelBuilder.setBaselineFeatureImportance(modelMetadata.getFeatureImportanceBaselines());
                            }
                            if (includes.isIncludeHyperparameters()) {
                                modelBuilder.setHyperparameters(modelMetadata.getHyperparameters());
                            }
                        }
                        finalListener.onResponse(modelBuilder.build());
                    },
                    failure -> {
                        // total feature importance is not necessary for a model to be valid
                        // we shouldn't fail if it is not found
                        if (ExceptionsHelper.unwrapCause(failure) instanceof ResourceNotFoundException) {
                            finalListener.onResponse(modelBuilder.build());
                            return;
                        }
                        finalListener.onFailure(failure);
                    }
                ));

            },
            finalListener::onFailure
        );

        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders
            .idsQuery()
            .addIds(modelId));
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch()
            .add(client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
                .setQuery(queryBuilder)
                // use sort to get the last
                .addSort("_index", SortOrder.DESC)
                .setSize(1)
                .request());

        if (includes.isIncludeModelDefinition()) {
            multiSearchRequestBuilder.add(client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
                .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders
                    .boolQuery()
                    .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                    .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelDefinitionDoc.NAME))))
                // There should be AT MOST these many docs. There might be more if definitions have been reindex to newer indices
                // If this ends up getting duplicate groups of definition documents, the parsing logic will throw away any doc that
                // is in a different index than the first index seen.
                .setSize(MAX_NUM_DEFINITION_DOCS)
                // First find the latest index
                .addSort("_index", SortOrder.DESC)
                // Then, sort by doc_num
                .addSort(SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName())
                    .order(SortOrder.ASC)
                    // We need this for the search not to fail when there are no mappings yet in the index
                    .unmappedType("long"))
                .request());
        }

        ActionListener<MultiSearchResponse> multiSearchResponseActionListener = ActionListener.wrap(
            multiSearchResponse -> {
                TrainedModelConfig.Builder builder;
                try {
                    builder = handleSearchItem(multiSearchResponse.getResponses()[0], modelId, this::parseInferenceDocLenientlyFromSource);
                } catch (ResourceNotFoundException ex) {
                    getTrainedModelListener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
                    return;
                } catch (Exception ex) {
                    getTrainedModelListener.onFailure(ex);
                    return;
                }

                if (includes.isIncludeModelDefinition()) {
                    try {
                        List<TrainedModelDefinitionDoc> docs = handleSearchItems(multiSearchResponse.getResponses()[1],
                            modelId,
                            this::parseModelDefinitionDocLenientlyFromSource);
                        try {
                            String compressedString = getDefinitionFromDocs(docs, modelId);
                            builder.setDefinitionFromString(compressedString);
                        } catch (ElasticsearchException elasticsearchException) {
                            getTrainedModelListener.onFailure(elasticsearchException);
                            return;
                        }

                    } catch (ResourceNotFoundException ex) {
                        getTrainedModelListener.onFailure(new ResourceNotFoundException(
                            Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                        return;
                    } catch (Exception ex) {
                        getTrainedModelListener.onFailure(ex);
                        return;
                    }
                }
                getTrainedModelListener.onResponse(builder);
            },
            getTrainedModelListener::onFailure
        );

        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            MultiSearchAction.INSTANCE,
            multiSearchRequestBuilder.request(),
            multiSearchResponseActionListener);
    }

    public void getTrainedModels(Set<String> modelIds,
                                 GetTrainedModelsAction.Includes includes,
                                 boolean allowNoResources,
                                 final ActionListener<List<TrainedModelConfig>> finalListener) {
        getTrainedModels(
            modelIds.stream().collect(Collectors.toMap(Function.identity(), _k -> Collections.emptySet())),
            includes,
            allowNoResources,
            finalListener
        );
    }

    /**
     * Gets all the provided trained config model objects
     *
     * NOTE:
     * This does no expansion on the ids.
     * It assumes that there are fewer than 10k.
     */
    public void getTrainedModels(Map<String, Set<String>> modelIds,
                                 GetTrainedModelsAction.Includes includes,
                                 boolean allowNoResources,
                                 final ActionListener<List<TrainedModelConfig>> finalListener) {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(
            QueryBuilders
                .idsQuery()
                .addIds(modelIds.keySet().toArray(new String[0]))
        );

        SearchRequest searchRequest = client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .addSort(TrainedModelConfig.MODEL_ID.getPreferredName(), SortOrder.ASC)
            .addSort("_index", SortOrder.DESC)
            .setQuery(queryBuilder)
            .setSize(modelIds.size())
            .request();
        List<TrainedModelConfig.Builder> configs = new ArrayList<>(modelIds.size());
        Set<String> modelsInIndex = Sets.difference(modelIds.keySet(), MODELS_STORED_AS_RESOURCE);
        Set<String> modelsAsResource = Sets.intersection(MODELS_STORED_AS_RESOURCE, modelIds.keySet());
        for(String modelId : modelsAsResource) {
            try {
                configs.add(loadModelFromResource(modelId, true));
            } catch (ElasticsearchException ex) {
                finalListener.onFailure(ex);
                return;
            }
        }
        if (modelsInIndex.isEmpty()) {
            finalListener.onResponse(configs.stream()
                .map(TrainedModelConfig.Builder::build)
                .sorted(Comparator.comparing(TrainedModelConfig::getModelId))
                .collect(Collectors.toList()));
            return;
        }

        ActionListener<List<TrainedModelConfig.Builder>> getTrainedModelListener = ActionListener.wrap(
            modelBuilders -> {
                if ((includes.isIncludeFeatureImportanceBaseline() || includes.isIncludeTotalFeatureImportance()
                  || includes.isIncludeHyperparameters()) == false) {
                    finalListener.onResponse(modelBuilders.stream()
                        .map(b -> b.setModelAliases(modelIds.get(b.getModelId())).build())
                        .sorted(Comparator.comparing(TrainedModelConfig::getModelId))
                        .collect(Collectors.toList()));
                    return;
                }
                this.getTrainedModelMetadata(modelIds.keySet(), ActionListener.wrap(
                    metadata ->
                        finalListener.onResponse(modelBuilders.stream()
                            .map(builder -> {
                                TrainedModelMetadata modelMetadata = metadata.get(builder.getModelId());
                                if (modelMetadata != null) {
                                    if (includes.isIncludeTotalFeatureImportance()) {
                                        builder.setFeatureImportance(modelMetadata.getTotalFeatureImportances());
                                    }
                                    if (includes.isIncludeFeatureImportanceBaseline()) {
                                        builder.setBaselineFeatureImportance(modelMetadata.getFeatureImportanceBaselines());
                                    }
                                    if (includes.isIncludeHyperparameters()) {
                                        builder.setHyperparameters(modelMetadata.getHyperparameters());
                                    }
                                }
                                return builder.setModelAliases(modelIds.get(builder.getModelId())).build();
                            })
                            .sorted(Comparator.comparing(TrainedModelConfig::getModelId))
                            .collect(Collectors.toList())),
                    failure -> {
                        // total feature importance is not necessary for a model to be valid
                        // we shouldn't fail if it is not found
                        if (ExceptionsHelper.unwrapCause(failure) instanceof ResourceNotFoundException) {
                            finalListener.onResponse(modelBuilders.stream()
                                .map(TrainedModelConfig.Builder::build)
                                .sorted(Comparator.comparing(TrainedModelConfig::getModelId))
                                .collect(Collectors.toList()));
                            return;
                        }
                        finalListener.onFailure(failure);
                    }
                ));
            },
            finalListener::onFailure
        );

        ActionListener<SearchResponse> configSearchHandler = ActionListener.wrap(
            searchResponse -> {
                Set<String> observedIds = new HashSet<>(
                    searchResponse.getHits().getHits().length + modelsAsResource.size(),
                    1.0f);
                observedIds.addAll(modelsAsResource);
                for(SearchHit searchHit : searchResponse.getHits().getHits()) {
                    try {
                        if (observedIds.contains(searchHit.getId()) == false) {
                            configs.add(
                                parseInferenceDocLenientlyFromSource(searchHit.getSourceRef(), searchHit.getId())
                            );
                            observedIds.add(searchHit.getId());
                        }
                    } catch (IOException ex) {
                        getTrainedModelListener.onFailure(
                            ExceptionsHelper.serverError(INFERENCE_FAILED_TO_DESERIALIZE, ex, searchHit.getId()));
                        return;
                    }
                }
                // We previously expanded the IDs.
                // If the config has gone missing between then and now we should throw if allowNoResources is false
                // Otherwise, treat it as if it was never expanded to begin with.
                Set<String> missingConfigs = Sets.difference(modelIds.keySet(), observedIds);
                if (missingConfigs.isEmpty() == false && allowNoResources == false) {
                    getTrainedModelListener.onFailure(new ResourceNotFoundException(Messages.INFERENCE_NOT_FOUND_MULTIPLE, missingConfigs));
                    return;
                }
                // Ensure sorted even with the injection of locally resourced models
                getTrainedModelListener.onResponse(configs);
            },
            getTrainedModelListener::onFailure
        );

        executeAsyncWithOrigin(client, ML_ORIGIN, SearchAction.INSTANCE, searchRequest, configSearchHandler);
    }

    public void deleteTrainedModel(String modelId, ActionListener<Boolean> listener) {
        if (MODELS_STORED_AS_RESOURCE.contains(modelId)) {
            listener.onFailure(ExceptionsHelper.badRequestException(Messages.getMessage(
                Messages.INFERENCE_CANNOT_DELETE_ML_MANAGED_MODEL,
                modelId
            )));
            return;
        }
        DeleteByQueryRequest request = new DeleteByQueryRequest().setAbortOnVersionConflict(false);

        request.indices(InferenceIndexConstants.INDEX_PATTERN, MlStatsIndex.indexPattern());
        QueryBuilder query = QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        request.setQuery(query);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(deleteResponse -> {
            if (deleteResponse.getDeleted() == 0) {
                listener.onFailure(new ResourceNotFoundException(
                    Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                listener.onFailure(new ResourceNotFoundException(
                    Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    public void expandIds(String idExpression,
                          boolean allowNoResources,
                          PageParams pageParams,
                          Set<String> tags,
                          ModelAliasMetadata modelAliasMetadata,
                          ActionListener<Tuple<Long, Map<String, Set<String>>>> idsListener) {
        String[] tokens = Strings.tokenizeToStringArray(idExpression, ",");
        Set<String> expandedIdsFromAliases = new HashSet<>();
        if (Strings.isAllOrWildcard(tokens) == false) {
            for (String token : tokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    for (String modelAlias : modelAliasMetadata.modelAliases().keySet()) {
                        if (Regex.simpleMatch(token, modelAlias)) {
                            expandedIdsFromAliases.add(modelAliasMetadata.getModelId(modelAlias));
                        }
                    }
                } else if (modelAliasMetadata.getModelId(token) != null) {
                    expandedIdsFromAliases.add(modelAliasMetadata.getModelId(token));
                }
            }
        }
        Set<String> matchedResourceIds = matchedResourceIds(tokens);
        Set<String> foundResourceIds;
        if (tags.isEmpty()) {
            foundResourceIds = matchedResourceIds;
        } else {
            foundResourceIds = new HashSet<>();
            for(String resourceId : matchedResourceIds) {
                // Does the model as a resource have all the tags?
                if (Sets.newHashSet(loadModelFromResource(resourceId, true).build().getTags()).containsAll(tags)) {
                    foundResourceIds.add(resourceId);
                }
            }
        }
        expandedIdsFromAliases.addAll(Arrays.asList(tokens));

        // We need to include the translated model alias, and ANY tokens that were not translated
        String[] tokensForQuery = expandedIdsFromAliases.toArray(new String[0]);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .sort(SortBuilders.fieldSort(TrainedModelConfig.MODEL_ID.getPreferredName())
                // If there are no resources, there might be no mapping for the id field.
                // This makes sure we don't get an error if that happens.
                .unmappedType("long"))
            .query(buildExpandIdsQuery(tokensForQuery, tags))
            // We "buffer" the from and size to take into account models stored as resources.
            // This is so we handle the edge cases when the model that is stored as a resource is at the start/end of
            // a page.
            .from(Math.max(0, pageParams.getFrom() - foundResourceIds.size()))
            .size(Math.min(10_000, pageParams.getSize() + foundResourceIds.size()));
        sourceBuilder.trackTotalHits(true)
            // we only care about the item id's
            .fetchSource(TrainedModelConfig.MODEL_ID.getPreferredName(), null);

        IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN)
            .indicesOptions(IndicesOptions.fromOptions(true,
                indicesOptions.allowNoIndices(),
                indicesOptions.expandWildcardsOpen(),
                indicesOptions.expandWildcardsClosed(),
                indicesOptions))
            .source(sourceBuilder);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(
                response -> {
                    long totalHitCount = response.getHits().getTotalHits().value + foundResourceIds.size();
                    Set<String> foundFromDocs = new HashSet<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        Map<String, Object> docSource = hit.getSourceAsMap();
                        if (docSource == null) {
                            continue;
                        }
                        Object idValue = docSource.get(TrainedModelConfig.MODEL_ID.getPreferredName());
                        if (idValue instanceof String) {
                            foundFromDocs.add(idValue.toString());
                        }
                    }
                    Map<String, Set<String>> allFoundIds = collectIds(pageParams, foundResourceIds, foundFromDocs)
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), k -> new HashSet<>()));

                    // We technically have matched on model tokens and any reversed referenced aliases
                    // We may end up with "over matching" on the aliases (matching on an alias that was not provided)
                    // But the expanded ID matcher does not care.
                    Set<String> matchedTokens = new HashSet<>(allFoundIds.keySet());

                    // We should gather ALL model aliases referenced by the given model IDs
                    // This way the callers have access to them
                    modelAliasMetadata.modelAliases().forEach((alias, modelIdEntry) -> {
                        final String modelId = modelIdEntry.getModelId();
                        if (allFoundIds.containsKey(modelId)) {
                            allFoundIds.get(modelId).add(alias);
                            matchedTokens.add(alias);
                        }
                    });

                    // Reverse lookup to see what model aliases were matched by their found trained model IDs
                    ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoResources);
                    requiredMatches.filterMatchedIds(matchedTokens);
                    if (requiredMatches.hasUnmatchedIds()) {
                        idsListener.onFailure(ExceptionsHelper.missingTrainedModel(requiredMatches.unmatchedIdsString()));
                    } else {
                        idsListener.onResponse(Tuple.tuple(totalHitCount, allFoundIds));
                    }
                },
                idsListener::onFailure
            ),
            client::search);
    }

    public void getInferenceStats(String[] modelIds, ActionListener<List<InferenceStats>> listener) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        Arrays.stream(modelIds).map(this::buildStatsSearchRequest).forEach(multiSearchRequest::add);
        if (multiSearchRequest.requests().isEmpty()) {
            listener.onResponse(Collections.emptyList());
            return;
        }
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            multiSearchRequest,
            ActionListener.<MultiSearchResponse>wrap(
                responses -> {
                    List<InferenceStats> allStats = new ArrayList<>(modelIds.length);
                    int modelIndex = 0;
                    assert responses.getResponses().length == modelIds.length :
                        "mismatch between search response size and models requested";
                    for (MultiSearchResponse.Item response : responses.getResponses()) {
                        if (response.isFailure()) {
                            if (ExceptionsHelper.unwrapCause(response.getFailure()) instanceof ResourceNotFoundException) {
                                modelIndex++;
                                continue;
                            }
                            logger.error(new ParameterizedMessage("[{}] search failed for models",
                                    Strings.arrayToCommaDelimitedString(modelIds)),
                                response.getFailure());
                            listener.onFailure(ExceptionsHelper.serverError("Searching for stats for models [{}] failed",
                                response.getFailure(),
                                Strings.arrayToCommaDelimitedString(modelIds)));
                            return;
                        }
                        try {
                            InferenceStats inferenceStats = handleMultiNodeStatsResponse(response.getResponse(), modelIds[modelIndex++]);
                            if (inferenceStats != null) {
                                allStats.add(inferenceStats);
                            }
                        } catch (Exception e) {
                            listener.onFailure(e);
                            return;
                        }
                    }
                    listener.onResponse(allStats);
                },
                e -> {
                    Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                    if (unwrapped instanceof ResourceNotFoundException) {
                        listener.onResponse(Collections.emptyList());
                        return;
                    }
                    listener.onFailure((Exception)unwrapped);
                }
            ),
            client::multiSearch);
    }

    private SearchRequest buildStatsSearchRequest(String modelId) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(InferenceStats.MODEL_ID.getPreferredName(), modelId))
            .filter(QueryBuilders.termQuery(InferenceStats.TYPE.getPreferredName(), InferenceStats.NAME));
        return new SearchRequest(MlStatsIndex.indexPattern())
            .indicesOptions(IndicesOptions.lenientExpandOpen())
            .allowPartialSearchResults(false)
            .source(SearchSourceBuilder.searchSource()
                .size(0)
                .aggregation(AggregationBuilders.sum(InferenceStats.FAILURE_COUNT.getPreferredName())
                    .field(InferenceStats.FAILURE_COUNT.getPreferredName()))
                .aggregation(AggregationBuilders.sum(InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName())
                    .field(InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName()))
                .aggregation(AggregationBuilders.sum(InferenceStats.INFERENCE_COUNT.getPreferredName())
                    .field(InferenceStats.INFERENCE_COUNT.getPreferredName()))
                .aggregation(AggregationBuilders.sum(InferenceStats.CACHE_MISS_COUNT.getPreferredName())
                    .field(InferenceStats.CACHE_MISS_COUNT.getPreferredName()))
                .aggregation(AggregationBuilders.max(InferenceStats.TIMESTAMP.getPreferredName())
                    .field(InferenceStats.TIMESTAMP.getPreferredName()))
                .query(queryBuilder));
    }

    private InferenceStats handleMultiNodeStatsResponse(SearchResponse response, String modelId) {
        if (response.getAggregations() == null) {
            logger.trace(() -> new ParameterizedMessage("[{}] no previously stored stats found", modelId));
            return null;
        }
        Sum failures = response.getAggregations().get(InferenceStats.FAILURE_COUNT.getPreferredName());
        Sum missing = response.getAggregations().get(InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName());
        Sum cacheMiss = response.getAggregations().get(InferenceStats.CACHE_MISS_COUNT.getPreferredName());
        Sum count = response.getAggregations().get(InferenceStats.INFERENCE_COUNT.getPreferredName());
        Max timeStamp = response.getAggregations().get(InferenceStats.TIMESTAMP.getPreferredName());
        return new InferenceStats(
            missing == null ? 0L : Double.valueOf(missing.getValue()).longValue(),
            count == null ? 0L : Double.valueOf(count.getValue()).longValue(),
            failures == null ? 0L : Double.valueOf(failures.getValue()).longValue(),
            cacheMiss == null ? 0L : Double.valueOf(cacheMiss.getValue()).longValue(),
            modelId,
            null,
            timeStamp == null || (Numbers.isValidDouble(timeStamp.getValue()) == false) ?
                Instant.now() :
                Instant.ofEpochMilli(Double.valueOf(timeStamp.getValue()).longValue())
        );
    }

    static Set<String> collectIds(PageParams pageParams, Set<String> foundFromResources, Set<String> foundFromDocs) {
        // If there are no matching resource models, there was no buffering and the models from the docs
        // are paginated correctly.
        if (foundFromResources.isEmpty()) {
            return foundFromDocs;
        }

        TreeSet<String> allFoundIds = new TreeSet<>(foundFromDocs);
        allFoundIds.addAll(foundFromResources);

        if (pageParams.getFrom() > 0) {
            // not the first page so there will be extra results at the front to remove
            int numToTrimFromFront = Math.min(foundFromResources.size(), pageParams.getFrom());
            for (int i = 0; i < numToTrimFromFront; i++) {
                allFoundIds.remove(allFoundIds.first());
            }
        }

        // trim down to size removing from the rear
        while (allFoundIds.size() > pageParams.getSize()) {
            allFoundIds.remove(allFoundIds.last());
        }

        return allFoundIds;
    }

    static QueryBuilder buildExpandIdsQuery(String[] tokens, Collection<String> tags) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
            .filter(buildQueryIdExpressionQuery(tokens, TrainedModelConfig.MODEL_ID.getPreferredName()));
        for(String tag : tags) {
            boolQueryBuilder.filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), tag));
        }
        return QueryBuilders.constantScoreQuery(boolQueryBuilder);
    }

    TrainedModelConfig.Builder loadModelFromResource(String modelId, boolean nullOutDefinition) {
        URL resource = getClass().getResource(MODEL_RESOURCE_PATH + modelId + MODEL_RESOURCE_FILE_EXT);
        if (resource == null) {
            logger.error("[{}] presumed stored as a resource but not found", modelId);
            throw new ResourceNotFoundException(
                Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId));
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                getClass().getResourceAsStream(MODEL_RESOURCE_PATH + modelId + MODEL_RESOURCE_FILE_EXT))) {
            TrainedModelConfig.Builder builder = TrainedModelConfig.fromXContent(parser, true);
            if (nullOutDefinition) {
                builder.clearDefinition();
            }
            return builder;
        } catch (IOException ioEx) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model definition", modelId), ioEx);
            throw ExceptionsHelper.serverError(INFERENCE_FAILED_TO_DESERIALIZE, ioEx, modelId);
        }
    }

    private static QueryBuilder buildQueryIdExpressionQuery(String[] tokens, String resourceIdField) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelConfig.NAME));

        if (Strings.isAllOrWildcard(tokens)) {
            return boolQuery;
        }
        // If the resourceId is not _all or *, we should see if it is a comma delimited string with wild-cards
        // e.g. id1,id2*,id3
        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(QueryBuilders.wildcardQuery(resourceIdField, token));
            } else {
                terms.add(token);
            }
        }
        if (terms.isEmpty() == false) {
            shouldQueries.should(QueryBuilders.termsQuery(resourceIdField, terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQuery.filter(shouldQueries);
        }
        return boolQuery;
    }

    private Set<String> matchedResourceIds(String[] tokens) {
        if (Strings.isAllOrWildcard(tokens)) {
            return MODELS_STORED_AS_RESOURCE;
        }

        Set<String> matchedModels = new HashSet<>();

        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                for (String modelId : MODELS_STORED_AS_RESOURCE) {
                    if(Regex.simpleMatch(token, modelId)) {
                        matchedModels.add(modelId);
                    }
                }
            } else {
                if (MODELS_STORED_AS_RESOURCE.contains(token)) {
                    matchedModels.add(token);
                }
            }
        }
        return Collections.unmodifiableSet(matchedModels);
    }

    private static <T> T handleSearchItem(MultiSearchResponse.Item item,
                                          String resourceId,
                                          CheckedBiFunction<BytesReference, String, T, Exception> parseLeniently) throws Exception {
        return handleSearchItems(item, resourceId, parseLeniently).get(0);
    }

    // NOTE: This ignores any results that are in a different index than the first one seen in the search response.
    private static <T> List<T> handleSearchItems(MultiSearchResponse.Item item,
                                                 String resourceId,
                                                 CheckedBiFunction<BytesReference, String, T, Exception> parseLeniently) throws Exception {
        if (item.isFailure()) {
            throw item.getFailure();
        }
        if (item.getResponse().getHits().getHits().length == 0) {
            throw new ResourceNotFoundException(resourceId);
        }
        return handleHits(item.getResponse().getHits().getHits(), resourceId, parseLeniently);

    }

    private static <T> List<T> handleHits(SearchHit[] hits,
                                          String resourceId,
                                          CheckedBiFunction<BytesReference, String, T, Exception> parseLeniently) throws Exception {
        List<T> results = new ArrayList<>(hits.length);
        String initialIndex = hits[0].getIndex();
        for (SearchHit hit : hits) {
            // We don't want to spread across multiple backing indices
            if (hit.getIndex().equals(initialIndex)) {
                results.add(parseLeniently.apply(hit.getSourceRef(), resourceId));
            }
        }
        return results;
    }

    private static String getDefinitionFromDocs(List<TrainedModelDefinitionDoc> docs, String modelId) throws ElasticsearchException {
        String compressedString = docs.stream()
            .map(TrainedModelDefinitionDoc::getCompressedString)
            .collect(Collectors.joining());
        // BWC for when we tracked the total definition length
        // TODO: remove in 9
        if (docs.get(0).getTotalDefinitionLength() != null) {
            if (compressedString.length() != docs.get(0).getTotalDefinitionLength()) {
                throw ExceptionsHelper.serverError(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId));
            }
        } else {
            TrainedModelDefinitionDoc lastDoc = docs.get(docs.size() - 1);
            // Either we are missing the last doc, or some previous doc
            if(lastDoc.isEos() == false || lastDoc.getDocNum() != docs.size() - 1) {
                throw ExceptionsHelper.serverError(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId));
            }
        }
        return compressedString;
    }

    static List<String> chunkStringWithSize(String str, int chunkSize) {
        List<String> subStrings = new ArrayList<>((int)Math.ceil(str.length()/(double)chunkSize));
        for (int i = 0; i < str.length();i += chunkSize) {
            subStrings.add(str.substring(i, Math.min(i + chunkSize, str.length())));
        }
        return subStrings;
    }

    private TrainedModelConfig.Builder parseInferenceDocLenientlyFromSource(BytesReference source, String modelId) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelConfig.fromXContent(parser, true);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model", modelId), e);
            throw e;
        }
    }

    private TrainedModelDefinitionDoc parseModelDefinitionDocLenientlyFromSource(BytesReference source, String modelId) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelDefinitionDoc.fromXContent(parser, true).build();
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model definition", modelId), e);
            throw e;
        }
    }

    private TrainedModelMetadata parseMetadataLenientlyFromSource(BytesReference source, String modelId) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelMetadata.fromXContent(parser, true);
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model metadata", modelId), e);
            throw e;
        }
    }

    private IndexRequest createRequest(String docId, String index, ToXContentObject body) {
        return createRequest(new IndexRequest(index), docId, body);
    }

    private IndexRequest createRequest(String docId, ToXContentObject body) {
        return createRequest(new IndexRequest(), docId, body);
    }

    private IndexRequest createRequest(IndexRequest request, String docId, ToXContentObject body) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = body.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);
            return request.opType(DocWriteRequest.OpType.CREATE).id(docId).source(source);
        } catch (IOException ex) {
            // This should never happen. If we were able to deserialize the object (from Native or REST) and then fail to serialize it again
            // that is not the users fault. We did something wrong and should throw.
            throw ExceptionsHelper.serverError(
                new ParameterizedMessage("Unexpected serialization exception for [{}]", docId).getFormattedMessage(),
                ex);
        }
    }
}
