/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
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
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
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
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction.GET_INFERENCE_FIELDS_ACTION_TV;

class InferenceQueryUtils {
    record InferenceInfo(
        int inferenceFieldCount,
        int indexCount,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        TransportVersion minTransportVersion
    ) {}

    private InferenceQueryUtils() {}

    static PlainActionFuture<InferenceInfo> getInferenceInfo(
        QueryRewriteContext queryRewriteContext,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields,
        boolean alwaysSkipRemotes,
        @Nullable String query,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable FullyQualifiedInferenceId inferenceIdOverride
    ) {
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalArgumentException("queryRewriteContext must provide resolved indices");
        }

        SetOnce<InferenceInfo> localInferenceInfoSupplier = new SetOnce<>();
        SetOnce<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoSupplier = new SetOnce<>();

        PlainActionFuture<InferenceInfo> inferenceInfoFuture = new PlainActionFuture<>();
        try (var refs = new RefCountingListener(inferenceInfoFuture.delegateFailureAndWrap((l, v) -> {
            l.onResponse(combineLocalAndRemoteInferenceInfo(localInferenceInfoSupplier.get(), remoteInferenceInfoSupplier.get()));
        }))) {
            ActionListener<InferenceInfo> localInferenceInfoListener = refs.acquire(localInferenceInfoSupplier::set);
            getLocalInferenceInfo(
                queryRewriteContext,
                fields,
                resolveWildcards,
                useDefaultFields,
                localInferenceInfoListener,
                query,
                inferenceResultsMap,
                inferenceIdOverride
            );

            if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false
                && queryRewriteContext.isCcsMinimizeRoundTrips() == false
                && alwaysSkipRemotes == false) {

                ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoListener = refs
                    .acquire(remoteInferenceInfoSupplier::set);
                getRemoteInferenceInfo(
                    queryRewriteContext,
                    fields,
                    resolveWildcards,
                    useDefaultFields,
                    remoteInferenceInfoListener,
                    query,
                    inferenceIdOverride
                );
            }
        }

        return inferenceInfoFuture;
    }

    private static void getLocalInferenceInfo(
        QueryRewriteContext queryRewriteContext,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields,
        ActionListener<InferenceInfo> localInferenceInfoListener,
        @Nullable String query,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable FullyQualifiedInferenceId inferenceIdOverride
    ) {
        Map<String, Set<InferenceFieldMetadata>> localInferenceFields = getLocalInferenceFields(
            queryRewriteContext.getResolvedIndices(),
            fields,
            resolveWildcards,
            useDefaultFields
        );

        int indexCount = localInferenceFields.size();
        int inferenceFieldCount = 0;
        for (var inferenceFieldMetadataSet : localInferenceFields.values()) {
            inferenceFieldCount += inferenceFieldMetadataSet.size();
        }

        if (inferenceFieldCount == 0 || query == null) {
            // Either no inference fields were queried, or no query was provided. Either way, there are no inference results to generate.
            localInferenceInfoListener.onResponse(
                new InferenceInfo(
                    inferenceFieldCount,
                    indexCount,
                    inferenceResultsMap != null ? inferenceResultsMap : Map.of(),
                    queryRewriteContext.getMinTransportVersion()
                )
            );
            return;
        }

        Set<FullyQualifiedInferenceId> localInferenceIds = inferenceIdOverride != null
            ? Set.of(inferenceIdOverride)
            : getLocalInferenceIds(localInferenceFields, queryRewriteContext.getLocalClusterAlias());

        final int finalInferenceFieldCount = inferenceFieldCount;
        getLocalInferenceResults(
            queryRewriteContext,
            query,
            localInferenceIds,
            inferenceResultsMap,
            localInferenceInfoListener.delegateFailureAndWrap((l, m) -> {
                InferenceInfo inferenceInfo = new InferenceInfo(
                    finalInferenceFieldCount,
                    indexCount,
                    m,
                    queryRewriteContext.getMinTransportVersion()
                );
                l.onResponse(inferenceInfo);
            })
        );
    }

    private static void getRemoteInferenceInfo(
        QueryRewriteContext queryRewriteContext,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields,
        ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoListener,
        @Nullable String query,
        @Nullable FullyQualifiedInferenceId inferenceIdOverride
    ) {
        var remoteIndices = queryRewriteContext.getResolvedIndices().getRemoteClusterIndices();
        GroupedActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> gal = new GroupedActionListener<>(
            remoteIndices.size(),
            remoteInferenceInfoListener.delegateFailureAndWrap((l, c) -> {
                Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>> remoteInferenceInfoMap = new HashMap<>(c.size());
                c.forEach(remoteInferenceInfoMap::putAll);
                l.onResponse(remoteInferenceInfoMap);
            })
        );

        // When an inference ID override is set, inference is only performed on the local cluster. Set the query to null in this case
        // to disable remote inference result generation.
        String effectiveQuery = inferenceIdOverride == null ? query : null;
        for (var entry : remoteIndices.entrySet()) {
            String clusterAlias = entry.getKey();
            OriginalIndices originalIndices = entry.getValue();

            GetInferenceFieldsAction.Request request = new GetInferenceFieldsAction.Request(
                Set.of(originalIndices.indices()),
                fields,
                resolveWildcards,
                useDefaultFields,
                effectiveQuery,
                originalIndices.indicesOptions()
            );

            queryRewriteContext.registerRemoteAsyncAction(clusterAlias, (client, threadContext, listener) -> {
                ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> wrappedListener = listener
                    .delegateFailureAndWrap((l, m) -> {
                        gal.onResponse(m);
                        l.onResponse(null);
                    });

                executeAsyncWithOrigin(threadContext, ML_ORIGIN, request, wrappedListener, (req, l1) -> {
                    client.getConnection(req, l1.delegateFailureAndWrap((l2, c) -> {
                        TransportVersion transportVersion = c.getTransportVersion();
                        if (transportVersion.supports(GET_INFERENCE_FIELDS_ACTION_TV) == false) {
                            // Assume that no remote inference fields are queried. We must do this because we cannot throw an error here
                            // without breaking BwC for interception-eligible queries (ex: match/knn/sparse_vector) that don't need to be
                            // intercepted. We track the transport version in the response so that more thorough error checking can be
                            // performed with the complete output of getInferenceInfo (i.e. complete local and remote inference info).
                            l2.onResponse(
                                Map.of(
                                    clusterAlias,
                                    Tuple.tuple(new GetInferenceFieldsAction.Response(Map.of(), Map.of()), transportVersion)
                                )
                            );
                        } else {
                            client.execute(GetInferenceFieldsAction.REMOTE_TYPE, req, l2.delegateFailureAndWrap((l3, resp) -> {
                                l3.onResponse(Map.of(clusterAlias, Tuple.tuple(resp, transportVersion)));
                            }));
                        }
                    }));
                });
            });
        }
    }

    private static InferenceInfo combineLocalAndRemoteInferenceInfo(
        InferenceInfo localInferenceInfo,
        Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>> remoteInferenceInfo
    ) {
        int totalInferenceFieldCount = localInferenceInfo.inferenceFieldCount;
        int totalIndexCount = localInferenceInfo.indexCount;
        Map<FullyQualifiedInferenceId, InferenceResults> completeInferenceResultsMap = new HashMap<>(
            localInferenceInfo.inferenceResultsMap
        );  // TODO: Use a copy-on-write map implementation here?
        TransportVersion minTransportVersion = localInferenceInfo.minTransportVersion;

        if (remoteInferenceInfo != null) {
            for (var entry : remoteInferenceInfo.entrySet()) {
                String clusterAlias = entry.getKey();
                TransportVersion transportVersion = entry.getValue().v2();
                var response = entry.getValue().v1();
                var inferenceFieldsMap = response.getInferenceFieldsMap();
                var inferenceResultsMap = response.getInferenceResultsMap()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> new FullyQualifiedInferenceId(clusterAlias, e.getKey()), Map.Entry::getValue));

                totalIndexCount += inferenceFieldsMap.size();
                for (var value : inferenceFieldsMap.values()) {
                    totalInferenceFieldCount += value.size();
                }

                completeInferenceResultsMap.putAll(inferenceResultsMap);
                minTransportVersion = TransportVersion.min(minTransportVersion, transportVersion);
            }
        }

        return new InferenceInfo(totalInferenceFieldCount, totalIndexCount, completeInferenceResultsMap, minTransportVersion);
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
    static SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> getInferenceResults(
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

    static void registerInferenceAsyncActions(
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

    static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    private static Map<String, Set<InferenceFieldMetadata>> getLocalInferenceFields(
        ResolvedIndices resolvedIndices,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Map<String, Set<InferenceFieldMetadata>> inferenceFieldMap = new HashMap<>();

        Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            final String indexName = indexMetadata.getIndex().getName();
            final Map<InferenceFieldMetadata, Float> matchingInferenceFieldMap = indexMetadata.getMatchingInferenceFields(
                fields,
                resolveWildcards,
                useDefaultFields
            );

            inferenceFieldMap.put(indexName, matchingInferenceFieldMap.keySet());
        }

        return inferenceFieldMap;
    }

    private static Set<FullyQualifiedInferenceId> getLocalInferenceIds(
        Map<String, Set<InferenceFieldMetadata>> localInferenceFields,
        String clusterAlias
    ) {
        return localInferenceFields.values()
            .stream()
            .flatMap(Collection::stream)
            .map(ifm -> new FullyQualifiedInferenceId(clusterAlias, ifm.getSearchInferenceId()))
            .collect(Collectors.toSet());
    }

    private static void getLocalInferenceResults(
        QueryRewriteContext queryRewriteContext,
        String query,
        Set<FullyQualifiedInferenceId> fullyQualifiedInferenceIds,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapListener

    ) {
        List<String> inferenceIds = new ArrayList<>(fullyQualifiedInferenceIds.size());
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

        if (inferenceIds.isEmpty() == false) {
            registerInferenceAsyncActions(queryRewriteContext, query, inferenceIds, inferenceResultsMapListener);
        } else {
            // Inference results map already contains all necessary inference results
            inferenceResultsMapListener.onResponse(inferenceResultsMap != null ? inferenceResultsMap : Map.of());
        }
    }

    private static void registerInferenceAsyncActions(
        QueryRewriteContext queryRewriteContext,
        String query,
        List<String> inferenceIds,
        ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapListener
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
            ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> wrappedListener = listener.delegateFailureAndWrap((l, m) -> {
                inferenceResultsMapListener.onResponse(m);
                l.onResponse(null);
            });

            GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> gal = createGroupedActionListener(
                wrappedListener,
                inferenceRequests.size()
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

    private static GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> createGroupedActionListener(
        ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapListener,
        int inferenceRequestCount
    ) {
        return new GroupedActionListener<>(inferenceRequestCount, inferenceResultsMapListener.delegateFailureAndWrap((l, responses) -> {
            Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap = new HashMap<>(responses.size());
            responses.forEach(r -> inferenceResultsMap.put(r.v1(), r.v2()));
            l.onResponse(inferenceResultsMap);
        }));
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
}
