/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;
import org.elasticsearch.xpack.rank.vectors.query.ExactRankVectorsQueryBuilder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsCandidateQueryBuilder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsQueryBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Runs exact, raw-candidate and normal rescored passes against one PIT. */
public final class TransportRankVectorsEvalAction extends HandledTransportAction<RankVectorsEvalRequest, RankVectorsEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRankVectorsEvalAction.class);
    private static final TimeValue PIT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    static final long MAX_EXACT_DOT_PRODUCTS = 100_000_000L;
    static final int MAX_PRIMARY_SHARDS = 128;
    static final int MAX_TOTAL_CANDIDATES = 10_000;
    private static final int ASSUMED_DOCUMENT_VECTORS = RankVectorsFieldMapper.MAX_MUVERA_VECTORS;

    private final Client client;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportRankVectorsEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            RankVectorsPlugin.RANK_VECTORS_EVAL_ACTION.name(),
            transportService,
            actionFilters,
            RankVectorsEvalRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.licenseState = licenseState;
    }

    @Override
    protected void doExecute(Task task, RankVectorsEvalRequest request, ActionListener<RankVectorsEvalResponse> listener) {
        if (cancelled(task, listener)) {
            return;
        }
        if (RankVectorsPlugin.RANK_VECTORS_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException("Rank Vectors"));
            return;
        }
        if (clusterService.getClusterSettings().get(RankVectorsPlugin.RANK_VECTORS_EVAL_ENABLED) == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "["
                        + RestRankVectorsEvalAction.ENDPOINT
                        + "] is disabled by ["
                        + RankVectorsPlugin.RANK_VECTORS_EVAL_ENABLED.getKey()
                        + "]"
                )
            );
            return;
        }
        try {
            ProjectMetadata project = projectResolver.getProjectMetadata(clusterService.state());
            Set<String> indices = Set.of(
                indexNameExpressionResolver.concreteIndexNames(project, request.indicesOptions(), true, request.indices())
            );
            if (indices.isEmpty()) {
                throw new IllegalArgumentException("no matching indices to evaluate");
            }
            IndexValidation indexValidation = validateIndices(project, indices, request.spec());
            resolveMapping(task, request, indices, indexValidation, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private IndexValidation validateIndices(ProjectMetadata project, Set<String> indices, RankVectorsEvalSpec spec) {
        int primaryShards = 0;
        String encodingProfile = null;
        int encodingVersion = -1;
        for (String index : indices) {
            IndexMetadata metadata = project.index(index);
            if (metadata == null) {
                continue;
            }
            if (RankVectorsPlugin.INDEX_RANK_VECTORS_EVAL_ENABLED.get(metadata.getSettings()) == false) {
                throw new IllegalArgumentException(
                    "["
                        + RestRankVectorsEvalAction.ENDPOINT
                        + "] is disabled on index ["
                        + index
                        + "] by ["
                        + RankVectorsPlugin.INDEX_RANK_VECTORS_EVAL_ENABLED.getKey()
                        + "]"
                );
            }
            String indexEncodingProfile = RankVectorsFieldMapper.muveraEncodingProfile(metadata.getCreationVersion());
            int indexEncodingVersion = RankVectorsFieldMapper.muveraEncodingVersion(metadata.getCreationVersion());
            if (encodingProfile == null) {
                encodingProfile = indexEncodingProfile;
                encodingVersion = indexEncodingVersion;
            } else if (encodingProfile.equals(indexEncodingProfile) == false || encodingVersion != indexEncodingVersion) {
                throw new IllegalArgumentException("all target indices must use the same rank_vectors encoding profile");
            }
            primaryShards = Math.addExact(primaryShards, metadata.getNumberOfShards());
        }
        if (primaryShards > MAX_PRIMARY_SHARDS) {
            throw new IllegalArgumentException("primary shard count [" + primaryShards + "] exceeds [" + MAX_PRIMARY_SHARDS + "]");
        }
        long totalCandidates = saturatingMultiply(spec.numCandidates(), primaryShards);
        if (totalCandidates > MAX_TOTAL_CANDIDATES) {
            throw new IllegalArgumentException(
                "[num_candidates] times primary shard count [" + totalCandidates + "] exceeds [" + MAX_TOTAL_CANDIDATES + "]"
            );
        }
        return new IndexValidation(primaryShards, encodingProfile, encodingVersion);
    }

    private void resolveMapping(
        Task task,
        RankVectorsEvalRequest request,
        Set<String> indices,
        IndexValidation indexValidation,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        GetFieldMappingsRequest mappingRequest = new GetFieldMappingsRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(request.spec().field());
        setParent(task, mappingRequest);
        client.execute(GetFieldMappingsAction.INSTANCE, mappingRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            MappingInfo mappingInfo = validateMappings(
                indices,
                request.spec(),
                response,
                indexValidation.encodingProfile,
                indexValidation.encodingVersion
            );
            rejectNested(task, request, indexValidation.primaryShards, mappingInfo, delegate);
        }));
    }

    private void rejectNested(
        Task task,
        RankVectorsEvalRequest request,
        int primaryShards,
        MappingInfo mappingInfo,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        FieldCapabilitiesRequest fieldCaps = new FieldCapabilitiesRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields("*");
        setParent(task, fieldCaps);
        client.execute(TransportFieldCapabilitiesAction.TYPE, fieldCaps, listener.delegateFailureAndWrap((delegate, response) -> {
            String field = request.spec().field();
            for (int dot = field.lastIndexOf('.'); dot > 0; dot = field.lastIndexOf('.', dot - 1)) {
                String ancestor = field.substring(0, dot);
                Map<String, FieldCapabilities> types = response.get().get(ancestor);
                if (types != null && types.containsKey("nested")) {
                    throw new IllegalArgumentException("nested rank_vectors fields are not supported");
                }
            }
            openPit(task, request, primaryShards, mappingInfo, delegate);
        }));
    }

    private void openPit(
        Task task,
        RankVectorsEvalRequest request,
        int primaryShards,
        MappingInfo mappingInfo,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        OpenPointInTimeRequest open = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(PIT_KEEP_ALIVE);
        setParent(task, open);
        client.execute(TransportOpenPointInTimeAction.TYPE, open, listener.delegateFailureAndWrap((delegate, response) -> {
            BytesReference pit = response.getPointInTimeId();
            ActionListener<RankVectorsEvalResponse> closing = ActionListener.runAfter(delegate, () -> closePit(pit));
            ActionListener.run(closing, l -> countDocuments(task, request.spec(), primaryShards, mappingInfo, pit, l));
        }));
    }

    private void countDocuments(
        Task task,
        RankVectorsEvalSpec spec,
        int primaryShards,
        MappingInfo mappingInfo,
        BytesReference pit,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        SearchSourceBuilder source = commonSource(pit).query(QueryBuilders.matchAllQuery()).size(0).trackTotalHits(true);
        SearchRequest count = searchRequest(source);
        setParent(task, count);
        childClient(task).search(count, listener.delegateFailureAndWrap((delegate, response) -> {
            long documentCount = response.getHits().getTotalHits().value();
            long exactWork = saturatingMultiply(saturatingMultiply(documentCount, ASSUMED_DOCUMENT_VECTORS), spec.queryVector().length);
            if (exactWork > MAX_EXACT_DOT_PRODUCTS) {
                throw new IllegalArgumentException(
                    "exact baseline work [" + exactWork + " dot products] exceeds [" + MAX_EXACT_DOT_PRODUCTS + "]"
                );
            }
            runExact(task, spec, primaryShards, mappingInfo, documentCount, exactWork, pit, delegate);
        }));
    }

    private void runExact(
        Task task,
        RankVectorsEvalSpec spec,
        int primaryShards,
        MappingInfo mappingInfo,
        long documentCount,
        long exactWork,
        BytesReference pit,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        SearchSourceBuilder source = commonSource(pit).query(new ExactRankVectorsQueryBuilder(spec.field(), spec.queryVector()))
            .size(spec.k() + 1);
        SearchRequest exact = searchRequest(source);
        setParent(task, exact);
        childClient(task).search(exact, listener.delegateFailureAndWrap((delegate, response) -> {
            SearchHit[] allExactHits = response.getHits().getHits();
            if (allExactHits.length == 0) {
                throw new IllegalArgumentException("exact baseline returned no hits");
            }
            boolean cutoffTie = allExactHits.length > spec.k()
                && Float.compare(allExactHits[spec.k() - 1].getScore(), allExactHits[spec.k()].getScore()) == 0;
            List<RankVectorsEvalResponse.Hit> exactHits = copyHits(allExactHits, Math.min(spec.k(), allExactHits.length));
            runCandidates(
                task,
                spec,
                primaryShards,
                mappingInfo,
                documentCount,
                exactWork,
                pit,
                exactHits,
                cutoffTie,
                response.getTook().millis(),
                delegate
            );
        }));
    }

    private void runCandidates(
        Task task,
        RankVectorsEvalSpec spec,
        int primaryShards,
        MappingInfo mappingInfo,
        long documentCount,
        long exactWork,
        BytesReference pit,
        List<RankVectorsEvalResponse.Hit> exactHits,
        boolean cutoffTie,
        long exactTook,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        int candidateBudget = Math.multiplyExact(spec.numCandidates(), primaryShards);
        SearchSourceBuilder source = commonSource(pit).query(
            new RankVectorsCandidateQueryBuilder(
                spec.field(),
                spec.queryVector(),
                spec.k(),
                spec.numCandidates(),
                spec.rescoreVectorBuilder() == null ? null : spec.rescoreVectorBuilder().oversample()
            )
        ).size(candidateBudget);
        SearchRequest candidates = searchRequest(source);
        setParent(task, candidates);
        childClient(task).search(candidates, listener.delegateFailureAndWrap((delegate, response) -> {
            Set<Identity> candidateIds = identities(response.getHits().getHits());
            runFinal(
                task,
                spec,
                primaryShards,
                mappingInfo,
                documentCount,
                exactWork,
                pit,
                exactHits,
                cutoffTie,
                exactTook,
                candidateIds,
                response.getTook().millis(),
                delegate
            );
        }));
    }

    private void runFinal(
        Task task,
        RankVectorsEvalSpec spec,
        int primaryShards,
        MappingInfo mappingInfo,
        long documentCount,
        long exactWork,
        BytesReference pit,
        List<RankVectorsEvalResponse.Hit> exactHits,
        boolean cutoffTie,
        long exactTook,
        Set<Identity> candidateIds,
        long candidateTook,
        ActionListener<RankVectorsEvalResponse> listener
    ) {
        SearchSourceBuilder source = commonSource(pit).query(
            new RankVectorsQueryBuilder(spec.field(), spec.queryVector(), spec.k(), spec.numCandidates(), spec.rescoreVectorBuilder())
        ).size(spec.k());
        SearchRequest request = searchRequest(source);
        setParent(task, request);
        childClient(task).search(request, listener.delegateFailureAndWrap((delegate, response) -> {
            List<RankVectorsEvalResponse.Hit> finalHits = copyHits(response.getHits().getHits(), response.getHits().getHits().length);
            Set<Identity> finalIds = identities(response.getHits().getHits());
            if (candidateIds.containsAll(finalIds) == false) {
                throw new IllegalStateException("final rank_vectors hits were not a subset of raw MUVERA candidates");
            }
            Set<Identity> exactIds = identities(exactHits);
            int candidateIntersection = intersection(exactIds, candidateIds);
            int finalIntersection = intersection(exactIds, finalIds);
            if (finalIntersection > candidateIntersection) {
                throw new IllegalStateException("final recall exceeded raw candidate recall");
            }
            int denominator = exactHits.size();
            delegate.onResponse(
                new RankVectorsEvalResponse(
                    (double) candidateIntersection / denominator,
                    (double) finalIntersection / denominator,
                    candidateIntersection,
                    finalIntersection,
                    denominator,
                    candidateIds.size(),
                    finalIds.size(),
                    Math.max(0, spec.k() - denominator),
                    cutoffTie,
                    exactTook,
                    candidateTook,
                    response.getTook().millis(),
                    documentCount,
                    exactWork,
                    saturatingMultiply(spec.numCandidates(), primaryShards),
                    saturatingMultiply(saturatingMultiply(candidateIds.size(), spec.queryVector().length), ASSUMED_DOCUMENT_VECTORS),
                    primaryShards,
                    "muvera",
                    mappingInfo.encodingProfile,
                    mappingInfo.encodingVersion,
                    exactHits,
                    finalHits
                )
            );
        }));
    }

    private static MappingInfo validateMappings(
        Set<String> indices,
        RankVectorsEvalSpec spec,
        GetFieldMappingsResponse response,
        String encodingProfile,
        int encodingVersion
    ) {
        if (response.mappings().keySet().containsAll(indices) == false) {
            throw new IllegalArgumentException("could not read the field mapping across all target indices");
        }
        MappingInfo first = null;
        for (String index : indices) {
            GetFieldMappingsResponse.FieldMappingMetadata metadata = response.mappings().get(index).get(spec.field());
            if (metadata == null || metadata.sourceAsMap().size() != 1) {
                throw new IllegalArgumentException("field [" + spec.field() + "] is not mapped in index [" + index + "]");
            }
            Object mappingObject = metadata.sourceAsMap().values().iterator().next();
            if (mappingObject instanceof Map<?, ?> == false) {
                throw new IllegalArgumentException("invalid mapping for field [" + spec.field() + "]");
            }
            Map<?, ?> mapping = (Map<?, ?>) mappingObject;
            MappingInfo current = MappingInfo.from(mapping, encodingProfile, encodingVersion);
            if (first != null && first.equals(current) == false) {
                throw new IllegalArgumentException("field [" + spec.field() + "] resolves differently across target indices");
            }
            first = current;
        }
        Objects.requireNonNull(first);
        int dimensions = first.dimensions;
        for (float[] vector : spec.queryVector()) {
            if (vector.length != dimensions) {
                throw new IllegalArgumentException(
                    "query vector dimensions [" + vector.length + "] differ from field dimensions [" + dimensions + "]"
                );
            }
        }
        return first;
    }

    private static SearchSourceBuilder commonSource(BytesReference pit) {
        // Keep hit metadata such as _id while avoiding source retrieval.
        return new SearchSourceBuilder().fetchSource(false).pointInTimeBuilder(new PointInTimeBuilder(pit));
    }

    private static SearchRequest searchRequest(SearchSourceBuilder source) {
        return new SearchRequest().source(source).allowPartialSearchResults(false);
    }

    private Client childClient(Task task) {
        return task == null ? client : new ParentTaskAssigningClient(client, clusterService.localNode(), task);
    }

    private void closePit(BytesReference pit) {
        client.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit), ActionListener.wrap(ignored -> {}, e -> {
            logger.warn("failed to close the point in time opened for rank-vectors evaluation", e);
        }));
    }

    private void setParent(Task task, ActionRequest request) {
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }
    }

    private static boolean cancelled(Task task, ActionListener<RankVectorsEvalResponse> listener) {
        return task instanceof CancellableTask cancellable && cancellable.notifyIfCancelled(listener);
    }

    private static List<RankVectorsEvalResponse.Hit> copyHits(SearchHit[] hits, int count) {
        List<RankVectorsEvalResponse.Hit> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(new RankVectorsEvalResponse.Hit(hits[i].getIndex(), hits[i].getId(), hits[i].getScore()));
        }
        return result;
    }

    private static Set<Identity> identities(SearchHit[] hits) {
        Set<Identity> result = new HashSet<>();
        for (SearchHit hit : hits) {
            result.add(new Identity(hit.getIndex(), hit.getId()));
        }
        return result;
    }

    private static Set<Identity> identities(List<RankVectorsEvalResponse.Hit> hits) {
        Set<Identity> result = new HashSet<>();
        for (RankVectorsEvalResponse.Hit hit : hits) {
            result.add(new Identity(hit.index(), hit.id()));
        }
        return result;
    }

    private static int intersection(Set<Identity> left, Set<Identity> right) {
        int count = 0;
        for (Identity identity : left) {
            if (right.contains(identity)) {
                count++;
            }
        }
        return count;
    }

    static long saturatingMultiply(long left, long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private record Identity(String index, String id) {}

    private record IndexValidation(int primaryShards, String encodingProfile, int encodingVersion) {}

    private record MappingInfo(int dimensions, String elementType, String encodingProfile, int encodingVersion) {
        private static MappingInfo from(Map<?, ?> mapping, String encodingProfile, int encodingVersion) {
            if ("rank_vectors".equals(mapping.get("type")) == false || Boolean.TRUE.equals(mapping.get("index")) == false) {
                throw new IllegalArgumentException("field must be an indexed [rank_vectors] field");
            }
            String elementType = Objects.toString(mapping.get("element_type"), "float");
            if ("float".equals(elementType) == false) {
                throw new IllegalArgumentException("MUVERA evaluation only supports float [rank_vectors] fields");
            }
            if (mapping.get("dims") instanceof Number == false) {
                throw new IllegalArgumentException("rank_vectors field must declare [dims]");
            }
            Number dimensions = (Number) mapping.get("dims");
            return new MappingInfo(dimensions.intValue(), elementType, encodingProfile, encodingVersion);
        }
    }
}
