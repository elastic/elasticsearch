/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ToXContentParams;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.inference.action.PutRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.RegionPolicyResponse;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.common.InferencePreferences;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.utils.InferenceProcessorInfoExtractor.pipelineIdsForResource;
import static org.elasticsearch.xpack.inference.common.SemanticTextInfoExtractor.extractIndexesReferencingInferenceEndpoints;

public class TransportPutRegionPolicyAction extends HandledTransportAction<PutRegionPolicyAction.Request, RegionPolicyResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutRegionPolicyAction.class);

    // package-private for testing
    static final String DENIED_ENDPOINT_IDS_METADATA_KEY = "es.denied_endpoint_ids";
    static final String REFERENCING_PIPELINES_METADATA_KEY = "es.referencing_pipelines";
    static final String REFERENCING_INDEXES_METADATA_KEY = "es.referencing_indexes";

    private final OriginSettingClient client;
    private final Optional<SecurityContext> securityContext;
    private final ClusterService clusterService;
    private final FeatureService featureService;
    private final InferencePreferencesCache inferencePreferencesCache;
    private final ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler;
    private final Sender sender;

    @Inject
    public TransportPutRegionPolicyAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        FeatureService featureService,
        InferencePreferencesCache inferencePreferencesCache,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler,
        Sender sender
    ) {
        super(
            PutRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            PutRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? Optional.of(new SecurityContext(settings, threadPool.getThreadContext()))
            : Optional.empty();
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.inferencePreferencesCache = inferencePreferencesCache;
        this.authorizationHandler = authorizationHandler;
        this.sender = sender;
    }

    @Override
    protected void doExecute(Task task, PutRegionPolicyAction.Request request, ActionListener<RegionPolicyResponse> finalListener) {
        SubscribableListener.newForked(this::getRegionPolicyOrNullWhenMissing)
            .<RegionPolicyDocWithSeqNo>andThen(
                (l, existingRegionPolicy) -> checkForDeniedInUseEndpoints(
                    request,
                    l.delegateFailureAndWrap((delegate, ignored) -> delegate.onResponse(existingRegionPolicy))
                )
            )
            .<RegionPolicyResponse>andThen((l, existingRegionPolicy) -> putRegionPolicy(existingRegionPolicy, request.regionPolicy(), l))
            .addListener(finalListener);
    }

    private void checkForDeniedInUseEndpoints(PutRegionPolicyAction.Request request, ActionListener<Void> listener) {
        if (request.force()) {
            listener.onResponse(null);
            return;
        }

        var preferences = new InferencePreferences(request.regionPolicy());
        authorizationHandler.getAuthorizationWithPreferences(listener.delegateFailureAndWrap((l, authModel) -> {
            var deniedInUseEndpoints = findDeniedInUseEndpoints(authModel);
            if (deniedInUseEndpoints.isEmpty()) {
                l.onResponse(null);
            } else {
                l.onFailure(buildDeniedInUseEndpointsException(deniedInUseEndpoints));
            }
        }), sender, preferences);
    }

    private Map<String, InUseReferences> findDeniedInUseEndpoints(ElasticInferenceServiceAuthorizationModel authModel) {
        var deniedEndpointIds = authModel.getEndpoints(authModel.getEndpointIds())
            .stream()
            .filter(model -> model.getConfigurations().getEndpointMetadataOrEmpty().deniedByRegionPolicy())
            .map(Model::getInferenceEntityId)
            .collect(Collectors.toSet());

        if (deniedEndpointIds.isEmpty()) {
            return Map.of();
        }

        ClusterState state = clusterService.state();
        Map<String, InUseReferences> deniedInUseEndpoints = new TreeMap<>();
        for (String endpointId : deniedEndpointIds) {
            var pipelines = pipelineIdsForResource(state.metadata(), Set.of(endpointId));
            var indexes = extractIndexesReferencingInferenceEndpoints(state.getMetadata(), Set.of(endpointId));
            if (pipelines.isEmpty() == false || indexes.isEmpty() == false) {
                deniedInUseEndpoints.put(endpointId, new InUseReferences(pipelines, indexes));
            }
        }
        return deniedInUseEndpoints;
    }

    private static ElasticsearchStatusException buildDeniedInUseEndpointsException(Map<String, InUseReferences> deniedInUseEndpoints) {
        var exception = new ElasticsearchStatusException(
            Strings.format(
                "Cannot put the region policy because it would deny access to the following in-use "
                    + "inference endpoints: %s. Ensure that these inference endpoints are not in use, "
                    + "or use force to ignore this warning and proceed anyway.",
                deniedInUseEndpoints.keySet()
            ),
            RestStatus.CONFLICT
        );
        exception.addMetadata(DENIED_ENDPOINT_IDS_METADATA_KEY, List.copyOf(deniedInUseEndpoints.keySet()));
        exception.addMetadata(REFERENCING_PIPELINES_METADATA_KEY, flattenReferences(deniedInUseEndpoints, InUseReferences::pipelines));
        exception.addMetadata(REFERENCING_INDEXES_METADATA_KEY, flattenReferences(deniedInUseEndpoints, InUseReferences::indexes));
        return exception;
    }

    private static List<String> flattenReferences(
        Map<String, InUseReferences> deniedInUseEndpoints,
        Function<InUseReferences, Set<String>> accessor
    ) {
        return deniedInUseEndpoints.entrySet()
            .stream()
            .flatMap(e -> accessor.apply(e.getValue()).stream().map(ref -> e.getKey() + ":" + ref))
            .sorted()
            .toList();
    }

    private record InUseReferences(Set<String> pipelines, Set<String> indexes) {}

    private void getRegionPolicyOrNullWhenMissing(ActionListener<RegionPolicyDocWithSeqNo> listener) {
        TransportGetRegionPolicyAction.doSearchRegionPolicy(client, true, listener.delegateResponse((l, e) -> {
            if (e instanceof IndexNotFoundException) {
                l.onResponse(null);
            } else {
                l.onFailure(e);
            }
        }).<SearchResponse>delegateFailureAndWrap((l, searchResponse) -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assert hits.length <= 1 : "multiple region policies found when only one is expected";
            if (hits.length == 0) {
                l.onResponse(null);
            } else {
                RegionPolicyDoc regionPolicyDoc = TransportGetRegionPolicyAction.parseRegionPolicy(hits[0]);
                l.onResponse(new RegionPolicyDocWithSeqNo(regionPolicyDoc, hits[0].getSeqNo(), hits[0].getPrimaryTerm()));
            }
        }));
    }

    private void putRegionPolicy(
        @Nullable RegionPolicyDocWithSeqNo existingRegionPolicyDoc,
        RegionPolicy newRegionPolicy,
        ActionListener<RegionPolicyResponse> listener
    ) {
        RegionPolicyDoc doc = createNewRegionPolicyDoc(
            existingRegionPolicyDoc == null ? null : existingRegionPolicyDoc.regionPolicyDoc(),
            newRegionPolicy
        );

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(InferenceIndex.INDEX_NAME)
            .setId(RegionPolicyDoc.DOCUMENT_ID)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        boolean includeDocType = InferenceIndex.inferenceIndexHasV4Mappings(clusterService.state(), featureService);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            ToXContent.Params params = includeDocType
                ? new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"))
                : ToXContent.EMPTY_PARAMS;
            indexRequestBuilder.setSource(doc.toXContent(builder, params));
        } catch (IOException e) {
            listener.onFailure(new IllegalStateException("Failed to serialise region policy", e));
            return;
        }

        if (existingRegionPolicyDoc == null) {
            indexRequestBuilder.setOpType(IndexRequest.OpType.CREATE);
        } else {
            indexRequestBuilder.setIfSeqNo(existingRegionPolicyDoc.seqNo());
            indexRequestBuilder.setIfPrimaryTerm(existingRegionPolicyDoc.primaryTerm());
        }

        indexRequestBuilder.execute(new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse docWriteResponse) {
                logger.info(
                    "Region policy [{}] by [{}]: {}",
                    existingRegionPolicyDoc == null ? "created" : "updated",
                    existingRegionPolicyDoc == null ? doc.createdBy() : doc.updatedBy(),
                    Strings.toString(doc.regionPolicy())
                );
                inferencePreferencesCache.invalidate(
                    ActionListener.runAfter(
                        ActionListener.wrap(
                            ignored -> {},
                            e -> logger.warn("Failed to invalidate inference preferences cache after updating region policy", e)
                        ),
                        () -> refreshAuthorizedEndpointsAndRespond(doc, listener)
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Failed to put region policy due to a concurrent update conflict. Please retry.",
                            RestStatus.CONFLICT,
                            e
                        )
                    );
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    private void refreshAuthorizedEndpointsAndRespond(RegionPolicyDoc doc, ActionListener<RegionPolicyResponse> listener) {
        client.execute(
            RefreshAuthorizedEndpointsAction.INSTANCE,
            new RefreshAuthorizedEndpointsAction.Request(),
            ActionListener.runAfter(
                ActionListener.<ActionResponse.Empty>wrap(
                    ignored -> {},
                    e -> logger.warn("Failed to refresh authorized endpoints after updating region policy", e)
                ),
                () -> listener.onResponse(new RegionPolicyResponse(doc))
            )
        );
    }

    private RegionPolicyDoc createNewRegionPolicyDoc(@Nullable RegionPolicyDoc existingRegionPolicy, RegionPolicy newRegionPolicy) {
        String username = securityContext.map(ctx -> ctx.getUser()).map(user -> user.principal()).orElse(null);
        if (existingRegionPolicy == null) {
            return new RegionPolicyDoc(newRegionPolicy, Instant.now(), username, null, null);
        }
        return new RegionPolicyDoc(
            newRegionPolicy,
            existingRegionPolicy.createdAt(),
            existingRegionPolicy.createdBy(),
            Instant.now(),
            username
        );
    }

    private record RegionPolicyDocWithSeqNo(RegionPolicyDoc regionPolicyDoc, long seqNo, long primaryTerm) {}
}
