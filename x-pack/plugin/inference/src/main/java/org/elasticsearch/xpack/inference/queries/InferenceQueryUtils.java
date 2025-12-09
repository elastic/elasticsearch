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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.InferenceException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction.GET_INFERENCE_FIELDS_ACTION_TV;
import static org.elasticsearch.xpack.core.ml.action.InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API;

public final class InferenceQueryUtils {
    /**
     * Inference info aggregated across queried local and remote indices. All info provided is guaranteed to be
     * complete when {@link InferenceInfoRequest#alwaysSkipRemotes()} is {@code false}.
     * When {@link InferenceInfoRequest#alwaysSkipRemotes()} is {@code true}, then only {@code minTransportVersion} is
     * guaranteed to be complete.
     *
     * @param inferenceFieldCount The number of inference fields queried across all concrete indices
     * @param indexCount The number of concrete indices queried
     * @param inferenceResultsMap The inference results map
     * @param minTransportVersion The global min transport version
     */
    public record InferenceInfo(
        int inferenceFieldCount,
        int indexCount,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        TransportVersion minTransportVersion
    ) {}

    /**
     * <p>
     * Inference info request args.
     * </p>
     * <p>
     * If {@code query} is {@code null}, then no additional inference results will be generated.
     * </p>
     * <p>
     * If {@code useDefaultFields} is true and {@code fields} is empty, then the field pattern map will be derived
     * from the value of {@link IndexSettings#DEFAULT_FIELD_SETTING} for the index.
     * </p>
     *
     * @param fields The field pattern map, where the key is the field pattern and the value is the pattern weight.
     * @param query The query string
     * @param inferenceResultsMap The current inference results map
     * @param inferenceIdOverride The inference ID override
     * @param resolveWildcards If {@code true}, wildcards in field patterns will be resolved. Otherwise, only explicit
     *                         field name matches will be used.
     * @param useDefaultFields If {@code true}, default fields will be used if {@code fields} is empty.
     * @param alwaysSkipRemotes If {@code true}, roundtrips to remote clusters will always be skipped
     */
    public record InferenceInfoRequest(
        Map<String, Float> fields,
        @Nullable String query,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable FullyQualifiedInferenceId inferenceIdOverride,
        boolean resolveWildcards,
        boolean useDefaultFields,
        boolean alwaysSkipRemotes
    ) {}

    private InferenceQueryUtils() {}

    /**
     * <p>
     * Get inference info for the queried local and remote indices.
     * </p>
     * <p>
     * Gets the inference info for the indices resolved in {@link QueryRewriteContext#getResolvedIndices()}. If
     * {@link QueryRewriteContext#isCcsMinimizeRoundTrips()} is {@code false}, a roundtrip to remote cluster(s) is
     * performed to get inference info for them. Otherwise, inference info is gathered only for local indices.
     * </p>
     * <p>
     * Inference info is returned in the form of an {@link InferenceInfo} listener. The listener is called by this
     * method once all requested inference info has been gathered and aggregated into a single {@link InferenceInfo}
     * instance.
     * </p>
     * <p>
     * If {@link InferenceInfoRequest#alwaysSkipRemotes()} is {@code true}, then no roundtrips to remote cluster(s)
     * will be performed, regardless of the value of {@link QueryRewriteContext#isCcsMinimizeRoundTrips()}. In this
     * case, the returned {@link InferenceInfo} may be incomplete, as it will only contain info for local indices.
     * This can be useful when calling the method with a-priori knowledge that remote cluster inference info is not
     * necessary.
     * </p>
     * <p>
     * NOTE: {@link InferenceInfo#minTransportVersion()} is an exception to the above statement. Min transport versions
     * for remote clusters will always be gathered when {@link QueryRewriteContext#isCcsMinimizeRoundTrips()} is
     * {@code false}. This can be determined using only the connection(s) to the remote cluster(s), so no roundtrip is
     * necessary.
     * </p>
     *
     * @param queryRewriteContext The query rewrite context
     * @param inferenceInfoRequest The inference info request args
     * @param inferenceInfoListener The inference info listener
     */
    public static void getInferenceInfo(
        QueryRewriteContext queryRewriteContext,
        InferenceInfoRequest inferenceInfoRequest,
        ActionListener<InferenceInfo> inferenceInfoListener
    ) {
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalArgumentException("queryRewriteContext must provide resolved indices");
        }

        SetOnce<InferenceInfo> localInferenceInfoSupplier = new SetOnce<>();
        SetOnce<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoSupplier = new SetOnce<>();

        try (var refs = new RefCountingListener(inferenceInfoListener.delegateFailureAndWrap((l, v) -> {
            l.onResponse(combineLocalAndRemoteInferenceInfo(localInferenceInfoSupplier.get(), remoteInferenceInfoSupplier.get()));
        }))) {
            ActionListener<InferenceInfo> localInferenceInfoListener = refs.acquire(localInferenceInfoSupplier::set);
            getLocalInferenceInfo(
                queryRewriteContext,
                inferenceInfoRequest.fields(),
                inferenceInfoRequest.resolveWildcards(),
                inferenceInfoRequest.useDefaultFields(),
                localInferenceInfoListener,
                inferenceInfoRequest.query(),
                inferenceInfoRequest.inferenceResultsMap(),
                inferenceInfoRequest.inferenceIdOverride()
            );

            if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false && queryRewriteContext.isCcsMinimizeRoundTrips() == false) {
                ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoListener = refs
                    .acquire(remoteInferenceInfoSupplier::set);

                if (inferenceInfoRequest.alwaysSkipRemotes() == false) {
                    getRemoteInferenceInfo(
                        queryRewriteContext,
                        inferenceInfoRequest.fields(),
                        inferenceInfoRequest.resolveWildcards(),
                        inferenceInfoRequest.useDefaultFields(),
                        remoteInferenceInfoListener,
                        inferenceInfoRequest.query(),
                        inferenceInfoRequest.inferenceIdOverride()
                    );
                } else {
                    // Even if we are skipping remotes, we still need to collect the remote cluster transport versions
                    // for downstream validation. This only requires opening a connection to the remote cluster,
                    // which in most cases should already exist.
                    getRemoteTransportVersion(queryRewriteContext, remoteInferenceInfoListener);
                }
            }
        }
    }

    public static InferenceInfo getResultFromFuture(PlainActionFuture<InferenceInfo> future) {
        if (future.isDone() == false) {
            return null;
        }

        try {
            return future.result();
        } catch (ExecutionException e) {
            throw new InferenceException("Unable to get inference information", e.getCause());
        }
    }

    public static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    public static void ccsMinimizeRoundTripsFalseSupportCheck(
        QueryRewriteContext queryRewriteContext,
        InferenceInfo inferenceInfo,
        String queryName
    ) {
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (inferenceInfo.minTransportVersion().supports(GET_INFERENCE_FIELDS_ACTION_TV) == false
            && inferenceInfo.inferenceFieldCount() > 0
            && resolvedIndices.getRemoteClusterIndices().isEmpty() == false
            && queryRewriteContext.isCcsMinimizeRoundTrips() == false) {

            throw new IllegalArgumentException(
                "One or more remote clusters do not support "
                    + queryName
                    + " query cross-cluster search when"
                    + " [ccs_minimize_roundtrips] is false. Please update all clusters to at least "
                    + GET_INFERENCE_FIELDS_ACTION_TV.toReleaseVersion()
            );
        }
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

        if ((inferenceFieldCount == 0 && inferenceIdOverride == null) || query == null) {
            // Skip local inference result generation if:
            // - No inference fields were queried and no inference ID override was specified
            // - The query is null
            // We perform local inference result generation if an inference ID override is specified and the query is non-null because
            // remote cluster fields (either inference or non-inference) may need this inference result to handle the query.
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

        Set<FullyQualifiedInferenceId> localInferenceIds;
        boolean useCoordinatedInferenceAction;
        if (inferenceIdOverride != null) {
            // Use CoordinatedInferenceAction when an override is set because the override could refer to trained model
            localInferenceIds = Set.of(inferenceIdOverride);
            useCoordinatedInferenceAction = true;
        } else {
            localInferenceIds = getLocalInferenceIds(localInferenceFields, queryRewriteContext.getLocalClusterAlias());
            useCoordinatedInferenceAction = false;
        }

        final int finalInferenceFieldCount = inferenceFieldCount;
        getLocalInferenceResults(
            queryRewriteContext,
            query,
            localInferenceIds,
            inferenceResultsMap,
            useCoordinatedInferenceAction,
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
        GroupedActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> gal =
            createRemoteInferenceInfoGroupedActionListener(remoteIndices.size(), remoteInferenceInfoListener);

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

    private static void getRemoteTransportVersion(
        QueryRewriteContext queryRewriteContext,
        ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoListener
    ) {
        var remoteIndices = queryRewriteContext.getResolvedIndices().getRemoteClusterIndices();
        GroupedActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> gal =
            createRemoteInferenceInfoGroupedActionListener(remoteIndices.size(), remoteInferenceInfoListener);

        for (var entry : remoteIndices.entrySet()) {
            String clusterAlias = entry.getKey();

            queryRewriteContext.registerRemoteAsyncAction(clusterAlias, (client, threadContext, listener) -> {
                ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> wrappedListener = listener
                    .delegateFailureAndWrap((l, m) -> {
                        gal.onResponse(m);
                        l.onResponse(null);
                    });

                client.getConnection(null, wrappedListener.delegateFailureAndWrap((l, c) -> {
                    l.onResponse(
                        Map.of(
                            clusterAlias,
                            Tuple.tuple(new GetInferenceFieldsAction.Response(Map.of(), Map.of()), c.getTransportVersion())
                        )
                    );
                }));
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
        );
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
        boolean useCoordinatedInferenceAction,
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
            if (useCoordinatedInferenceAction) {
                registerInferenceAsyncActionsWithCoordinatedAction(queryRewriteContext, query, inferenceIds, inferenceResultsMapListener);
            } else {
                registerInferenceAsyncActionsWithInferenceAction(queryRewriteContext, query, inferenceIds, inferenceResultsMapListener);
            }
        } else {
            // Inference results map already contains all necessary inference results
            inferenceResultsMapListener.onResponse(inferenceResultsMap != null ? inferenceResultsMap : Map.of());
        }
    }

    private static void registerInferenceAsyncActionsWithInferenceAction(
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

            GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> gal = createLocalInferenceGroupedActionListener(
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

    private static void registerInferenceAsyncActionsWithCoordinatedAction(
        QueryRewriteContext queryRewriteContext,
        String query,
        List<String> inferenceIds,
        ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapListener
    ) {
        // TODO: Get timeout from INFERENCE_QUERY_TIMEOUT
        List<CoordinatedInferenceAction.Request> inferenceRequests = inferenceIds.stream().map(inferenceId -> {
            var request = CoordinatedInferenceAction.Request.forTextInput(
                inferenceId,
                List.of(query),
                null,
                false,
                DEFAULT_TIMEOUT_FOR_API
            );
            request.setHighPriority(true);
            request.setPrefixType(TrainedModelPrefixStrings.PrefixType.SEARCH);
            return request;
        }).toList();

        queryRewriteContext.registerAsyncAction((client, listener) -> {
            ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> wrappedListener = listener.delegateFailureAndWrap((l, m) -> {
                inferenceResultsMapListener.onResponse(m);
                l.onResponse(null);
            });

            GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> gal = createLocalInferenceGroupedActionListener(
                wrappedListener,
                inferenceRequests.size()
            );
            for (CoordinatedInferenceAction.Request inferenceRequest : inferenceRequests) {
                FullyQualifiedInferenceId fullyQualifiedInferenceId = new FullyQualifiedInferenceId(
                    queryRewriteContext.getLocalClusterAlias(),
                    inferenceRequest.getModelId()
                );
                executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    CoordinatedInferenceAction.INSTANCE,
                    inferenceRequest,
                    gal.delegateFailureAndWrap((l, inferenceResponse) -> {
                        InferenceResults inferenceResults = validateAndConvertInferenceResults(
                            inferenceResponse.getInferenceResults(),
                            fullyQualifiedInferenceId.inferenceId()
                        );
                        l.onResponse(Tuple.tuple(fullyQualifiedInferenceId, inferenceResults));
                    })
                );
            }
        });
    }

    private static GroupedActionListener<Tuple<FullyQualifiedInferenceId, InferenceResults>> createLocalInferenceGroupedActionListener(
        ActionListener<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapListener,
        int inferenceRequestCount
    ) {
        return new GroupedActionListener<>(inferenceRequestCount, inferenceResultsMapListener.delegateFailureAndWrap((l, responses) -> {
            Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap = new HashMap<>(responses.size());
            responses.forEach(r -> inferenceResultsMap.put(r.v1(), r.v2()));
            l.onResponse(inferenceResultsMap);
        }));
    }

    private static
        GroupedActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>>
        createRemoteInferenceInfoGroupedActionListener(
            int remoteClusterCount,
            ActionListener<Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>>> remoteInferenceInfoListener
        ) {

        return new GroupedActionListener<>(remoteClusterCount, remoteInferenceInfoListener.delegateFailureAndWrap((l, c) -> {
            Map<String, Tuple<GetInferenceFieldsAction.Response, TransportVersion>> remoteInferenceInfoMap = new HashMap<>(c.size());
            c.forEach(remoteInferenceInfoMap::putAll);
            l.onResponse(remoteInferenceInfoMap);
        }));
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String inferenceId
    ) {
        return validateAndConvertInferenceResults(inferenceServiceResults.transformToCoordinationFormat(), inferenceId);
    }

    private static InferenceResults validateAndConvertInferenceResults(
        List<? extends InferenceResults> inferenceResultsList,
        String inferenceId
    ) {
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
