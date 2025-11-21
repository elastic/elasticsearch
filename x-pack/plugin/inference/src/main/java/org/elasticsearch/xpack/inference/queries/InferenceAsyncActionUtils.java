/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceAsyncActionUtils {
    private InferenceAsyncActionUtils() {}

    public static InferenceResultGatheringInfo gatherInferenceResults(
        QueryRewriteContext queryRewriteContext,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        @Nullable FullyQualifiedInferenceId inferenceIdOverride,
        @Nullable String query
    ) {
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        Set<FullyQualifiedInferenceId> inferenceIds = getInferenceIdsForFields(
            resolvedIndices.getConcreteLocalIndicesMetadata().values(),
            queryRewriteContext.getLocalClusterAlias(),
            fields,
            resolveWildcards,
            useDefaultFields
        );

        if (inferenceIds.isEmpty()) {
            return new InferenceResultGatheringInfo(InferenceResultGatheringState.NO_INFERENCE_FIELDS, null);
        } else if (inferenceResultsMapSupplier != null) {
            InferenceResultGatheringState state = inferenceResultsMapSupplier.get() != null
                ? InferenceResultGatheringState.COMPLETE
                : InferenceResultGatheringState.PENDING;
            return new InferenceResultGatheringInfo(state, inferenceResultsMapSupplier);
        }

        if (inferenceIdOverride != null) {
            inferenceIds = Set.of(inferenceIdOverride);
        }

        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> newInferenceResultsMapSupplier = getInferenceResults(
            queryRewriteContext,
            inferenceIds,
            inferenceResultsMap,
            query
        );

        InferenceResultGatheringState state = newInferenceResultsMapSupplier != null
            ? InferenceResultGatheringState.PENDING
            : InferenceResultGatheringState.COMPLETE;
        return new InferenceResultGatheringInfo(state, inferenceResultsMapSupplier);
    }

    /**
     * <p>
     * Get inference results for the provided query using the provided fully qualified inference IDs.
     * </p>
     * <p>
     * This method will return an inference results map supplier that will provide a complete map of additional inference results required.
     * If the provided inference results map already contains all required inference results, a null supplier will be returned.
     * </p>
     *
     * @param queryRewriteContext The query rewrite context
     * @param fullyQualifiedInferenceIds The fully qualified inference IDs to use to generate inference results
     * @param inferenceResultsMap The initial inference results map
     * @param query The query to generate inference results for
     * @return An inference results map supplier
     */
    public static SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> getInferenceResults(
        QueryRewriteContext queryRewriteContext,
        Set<FullyQualifiedInferenceId> fullyQualifiedInferenceIds,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable String query
    ) {
        List<String> inferenceIds = new ArrayList<>(fullyQualifiedInferenceIds.size());
        if (query != null) {
            for (FullyQualifiedInferenceId fullyQualifiedInferenceId : fullyQualifiedInferenceIds) {
                if (inferenceResultsMap == null || inferenceResultsMap.containsKey(fullyQualifiedInferenceId) == false) {
                    if (fullyQualifiedInferenceId.clusterAlias().equals(queryRewriteContext.getLocalClusterAlias()) == false) {
                        // Catch if we are missing inference results that should have been generated on another cluster
                        throw new IllegalStateException(
                            "Cannot get inference results for inference endpoint ["
                                + fullyQualifiedInferenceId
                                + "] on cluster ["
                                + queryRewriteContext.getLocalClusterAlias()
                                + "]"
                        );
                    }

                    inferenceIds.add(fullyQualifiedInferenceId.inferenceId());
                }
            }
        }

        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier = null;
        if (inferenceIds.isEmpty() == false) {
            inferenceResultsMapSupplier = new SetOnce<>();
            registerInferenceAsyncActions(queryRewriteContext, inferenceResultsMapSupplier, query, inferenceIds);
        }

        return inferenceResultsMapSupplier;
    }

    public static void registerInferenceAsyncActions(
        QueryRewriteContext queryRewriteContext,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        String query,
        List<String> inferenceIds
    ) {
        List<InferenceAction.Request> inferenceRequests = inferenceIds.stream()
            .map(
                i -> new InferenceAction.Request(
                    TaskType.ANY,
                    i,
                    null,
                    null,
                    null,
                    List.of(query),
                    Map.of(),
                    InputType.INTERNAL_SEARCH,
                    null,
                    false
                )
            )
            .toList();

        queryRewriteContext.registerAsyncAction((client, listener) -> {
            GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> gal = createGroupedActionListener(
                inferenceResultsMapSupplier,
                inferenceRequests.size(),
                listener
            );
            for (InferenceAction.Request inferenceRequest : inferenceRequests) {
                FullyQualifiedInferenceId fullyQualifiedInferenceId = new FullyQualifiedInferenceId(
                    queryRewriteContext.getLocalClusterAlias(),
                    inferenceRequest.getInferenceEntityId()
                );
                executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    InferenceAction.INSTANCE,
                    inferenceRequest,
                    gal.delegateFailureAndWrap((l, inferenceResponse) -> {
                        InferenceResults inferenceResults = validateAndConvertInferenceResults(
                            inferenceResponse.getResults(),
                            fullyQualifiedInferenceId.inferenceId()
                        );
                        l.onResponse(Tuple.tuple(fullyQualifiedInferenceId, inferenceResults));
                    })
                );
            }
        });
    }

    public static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    private static Set<FullyQualifiedInferenceId> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        String clusterAlias,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Set<FullyQualifiedInferenceId> fullyQualifiedInferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            final Map<String, Float> indexQueryFields = (useDefaultFields && fields.isEmpty())
                ? getDefaultFields(indexMetadata.getSettings())
                : fields;

            Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
            for (String indexQueryField : indexQueryFields.keySet()) {
                if (indexInferenceFields.containsKey(indexQueryField)) {
                    // No wildcards in field name
                    InferenceFieldMetadata inferenceFieldMetadata = indexInferenceFields.get(indexQueryField);
                    fullyQualifiedInferenceIds.add(
                        new FullyQualifiedInferenceId(clusterAlias, inferenceFieldMetadata.getSearchInferenceId())
                    );
                    continue;
                }
                if (resolveWildcards) {
                    if (Regex.isMatchAllPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .forEach(
                                ifm -> fullyQualifiedInferenceIds.add(
                                    new FullyQualifiedInferenceId(clusterAlias, ifm.getSearchInferenceId())
                                )
                            );
                    } else if (Regex.isSimpleMatchPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .stream()
                            .filter(ifm -> Regex.simpleMatch(indexQueryField, ifm.getName()))
                            .forEach(
                                ifm -> fullyQualifiedInferenceIds.add(
                                    new FullyQualifiedInferenceId(clusterAlias, ifm.getSearchInferenceId())
                                )
                            );
                    }
                }
            }
        }

        return fullyQualifiedInferenceIds;
    }

    private static GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> createGroupedActionListener(
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        int inferenceRequestCount,
        ActionListener<?> listener
    ) {
        return new GroupedActionListener<>(inferenceRequestCount, listener.delegateFailureAndWrap((l, responses) -> {
            Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap = new HashMap<>(responses.size());
            responses.forEach(r -> inferenceResultsMap.put(r.v1(), r.v2()));
            inferenceResultsMapSupplier.set(inferenceResultsMap);
            l.onResponse(null);
        }));
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String inferenceId
    ) {
        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            return new ErrorInferenceResults(
                new IllegalArgumentException("No query inference results retrieved for inference ID [" + inferenceId + "]")
            );
        } else if (inferenceResultsList.size() > 1) {
            // We don't chunk queries, so there should always be one inference result.
            // Thus, if we receive more than one inference result, it is a server-side error.
            return new ErrorInferenceResults(
                new IllegalStateException(
                    inferenceResultsList.size() + " query inference results retrieved for inference ID [" + inferenceId + "]"
                )
            );
        }

        InferenceResults inferenceResults = inferenceResultsList.getFirst();
        if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlDenseEmbeddingResults == false
            && inferenceResults instanceof ErrorInferenceResults == false
            && inferenceResults instanceof WarningInferenceResults == false) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "Expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlDenseEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint ["
                        + inferenceId
                        + "] configuration changed?"
                )
            );
        }

        return inferenceResults;
    }

    public enum InferenceResultGatheringState {
        NO_INFERENCE_FIELDS,
        PENDING,
        COMPLETE
    }

    public record InferenceResultGatheringInfo(
        InferenceResultGatheringState state,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier
    ) {}
}
