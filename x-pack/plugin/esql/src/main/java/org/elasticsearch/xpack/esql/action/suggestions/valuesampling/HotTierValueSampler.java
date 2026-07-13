/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions.valuesampling;

import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.esql.action.FieldSuggestion;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Direct, node-grouped {@code TermsEnum} value sampling for the {@code STRING_LITERAL_EQUALITY}
 * completion context (see the suggestions API spec). Modeled on
 * {@code TransportTermsEnumAction}/{@code NodeTermsEnumRequest}/{@code MultiShardTermsEnum} — raw
 * term-dictionary reads off each shard's {@link Engine.Searcher}, not a terms aggregation — but not a
 * dependency on it: that action's wire shape has no doc-frequency counts, which this feature needs.
 *
 * <p><b>Ranking choice:</b> the first {@code size} terms encountered per shard (term order, which
 * Lucene's term dictionary already guarantees is sorted, but not ranked by global frequency) are kept,
 * not the {@code size} most-frequent terms cluster-wide — true frequency ranking would require scanning
 * the entire term dictionary per shard, defeating the point of using a raw {@code TermsEnum} instead of
 * an aggregation. A common value that sorts late in a shard whose term count exceeds {@code size} may
 * not surface. This is a real, deliberate trade-off.
 *
 * <p><b>{@code docFreq} denominator:</b> a value's {@code docFreq} is {@code sum(TermsEnum#docFreq())}
 * across the shards that contributed a sample of it, divided by {@code sum(IndexReader#numDocs())}
 * across those same shards — not a search hit count, since no search ever runs on this path.
 *
 * <p><b>Security:</b> reading a raw {@code TermsEnum} bypasses normal per-document query-time security
 * filtering entirely (there is no query for a search-authorization interceptor to attach a filter to),
 * so this class applies its own explicit gates, modeled on {@code TransportTermsEnumAction#canAccess} —
 * FLS: skip a shard outright if the field is not permitted; DLS: refuse the raw read for a shard unless
 * the requesting user's DLS role query(ies) all rewrite to {@code match_all}. This is stricter than a
 * real search's per-document DLS filtering: it is "no raw term-dictionary access at all" rather than
 * "filtered results," and is the reason {@code dls_active} and "no values for this field" go together on
 * this path.
 */
public class HotTierValueSampler {

    private static final Logger logger = LogManager.getLogger(HotTierValueSampler.class);

    /** The node-request transport action name, matching {@code TransportTermsEnumAction}'s {@code [s]}/{@code [n]} suffix convention. */
    public static final String NODE_ACTION_NAME = "indices:data/read/esql/suggestions/sample_values[n]";

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final ScriptService scriptService;
    private final Settings settings;
    private final Executor autoCompleteExecutor;

    public HotTierValueSampler(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ScriptService scriptService,
        Settings settings,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.settings = settings;
        this.autoCompleteExecutor = threadPool.executor(ThreadPool.Names.AUTO_COMPLETE);

        transportService.registerRequestHandler(
            NODE_ACTION_NAME,
            autoCompleteExecutor,
            NodeSuggestValuesRequest::new,
            (request, channel, task) -> ActionListener.completeWith(
                new ChannelActionListener<>(channel),
                () -> dataNodeOperation(request, transportService.getThreadPool().getThreadContext())
            )
        );
    }

    /** The outcome of sampling values across the hot tier for one field. */
    public record SampleResult(List<FieldSuggestion.ValueSuggestion> values, boolean shardsSkipped, boolean dlsActive) {
        public static final SampleResult NO_HOT_NODES = new SampleResult(List.of(), false, false);
    }

    /**
     * {@code true} if the concrete mapping type of {@code field} in {@code index} is plain
     * {@code keyword} — not {@code constant_keyword}, {@code wildcard}, or anything else that ESQL's
     * {@code DataType.KEYWORD} also collapses onto.
     */
    public static boolean isPlainKeywordMapping(ProjectMetadata metadata, String index, String field) {
        return "keyword".equals(concreteMappingType(metadata, index, field));
    }

    private static String concreteMappingType(ProjectMetadata metadata, String index, String field) {
        var indexMetadata = metadata.index(index);
        if (indexMetadata == null || indexMetadata.mapping() == null) {
            return null;
        }
        Map<String, Object> root = indexMetadata.mapping().sourceAsMap();
        Object cursor = root.get("properties");
        String[] segments = field.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            if (!(cursor instanceof Map<?, ?> map)) {
                return null;
            }
            Object node = map.get(segments[i]);
            if (!(node instanceof Map<?, ?> nodeMap)) {
                return null;
            }
            if (i == segments.length - 1) {
                Object type = nodeMap.get("type");
                return type == null ? "object" : type.toString();
            }
            cursor = nodeMap.get("properties");
        }
        return null;
    }

    /**
     * Nodes carrying a hot-tier copy of any shard of {@code concreteIndices}, grouped by node id.
     * Empty when no hot-tier node holds a copy of any target shard at all, in which case the caller
     * skips the fan-out entirely.
     */
    public Map<String, Set<ShardId>> hotTierNodeBundles(ProjectMetadata metadata, Set<String> concreteIndices) {
        return hotTierNodeBundles(clusterService.state().routingTable(metadata.id()), clusterService.state().nodes(), concreteIndices);
    }

    /**
     * Pure, cluster-state-driven variant of {@link #hotTierNodeBundles(ProjectMetadata, Set)}, extracted
     * so it can be unit-tested without a live {@link ClusterService}.
     */
    public static Map<String, Set<ShardId>> hotTierNodeBundles(
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        Set<String> concreteIndices
    ) {
        Map<String, Set<ShardId>> bundles = new HashMap<>();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = routingTable.index(index);
            if (indexRoutingTable == null) {
                continue;
            }
            for (int shard = 0; shard < indexRoutingTable.size(); shard++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shard);
                for (ShardRouting copy : shardRoutingTable.assignedShards()) {
                    if (copy.active() == false) {
                        continue;
                    }
                    DiscoveryNode node = nodes.get(copy.currentNodeId());
                    if (node != null && DataTier.isHotNode(node)) {
                        bundles.computeIfAbsent(copy.currentNodeId(), n -> new HashSet<>()).add(copy.shardId());
                        break; // one copy per shard is enough
                    }
                }
            }
        }
        return bundles;
    }

    /** Fan out to the hot-tier node bundles and merge into a capped, ranked set of value suggestions. */
    public void sampleValues(
        String field,
        Map<String, Set<ShardId>> nodeBundles,
        int size,
        long timeoutMillis,
        ActionListener<SampleResult> listener
    ) {
        if (nodeBundles.isEmpty()) {
            listener.onResponse(SampleResult.NO_HOT_NODES);
            return;
        }

        long startMillis = System.currentTimeMillis();
        Map<Object, long[]> merged = new ConcurrentHashMap<>(); // value -> [numerator, denominator]
        AtomicBoolean shardsSkipped = new AtomicBoolean(false);
        AtomicBoolean dlsActive = new AtomicBoolean(false);
        AtomicInteger remaining = new AtomicInteger(nodeBundles.size());
        var nodes = clusterService.state().nodes();

        for (Map.Entry<String, Set<ShardId>> entry : nodeBundles.entrySet()) {
            DiscoveryNode node = nodes.get(entry.getKey());
            if (node == null) {
                shardsSkipped.set(true);
                finishOne(remaining, merged, shardsSkipped, dlsActive, size, listener);
                continue;
            }
            NodeSuggestValuesRequest nodeRequest = new NodeSuggestValuesRequest(field, entry.getValue(), size, timeoutMillis, startMillis);
            transportService.sendRequest(node, NODE_ACTION_NAME, nodeRequest, new TransportResponseHandler<NodeSuggestValuesResponse>() {
                @Override
                public NodeSuggestValuesResponse read(StreamInput in) throws IOException {
                    return new NodeSuggestValuesResponse(in);
                }

                @Override
                public void handleResponse(NodeSuggestValuesResponse response) {
                    if (response.partialOrErrored()) {
                        shardsSkipped.set(true);
                    }
                    if (response.dlsActive()) {
                        dlsActive.set(true);
                    }
                    mergeInto(merged, response);
                    finishOne(remaining, merged, shardsSkipped, dlsActive, size, listener);
                }

                @Override
                public void handleException(TransportException exc) {
                    shardsSkipped.set(true);
                    finishOne(remaining, merged, shardsSkipped, dlsActive, size, listener);
                }

                @Override
                public Executor executor() {
                    return autoCompleteExecutor;
                }
            });
        }
    }

    private static void mergeInto(Map<Object, long[]> merged, NodeSuggestValuesResponse response) {
        for (Map.Entry<Object, Long> entry : response.docFreqByTerm().entrySet()) {
            long[] slot = merged.computeIfAbsent(entry.getKey(), k -> new long[2]);
            synchronized (slot) {
                slot[0] += entry.getValue();
                slot[1] += response.liveDocs();
            }
        }
    }

    private static void finishOne(
        AtomicInteger remaining,
        Map<Object, long[]> merged,
        AtomicBoolean shardsSkipped,
        AtomicBoolean dlsActive,
        int size,
        ActionListener<SampleResult> listener
    ) {
        if (remaining.decrementAndGet() == 0) {
            List<FieldSuggestion.ValueSuggestion> values = merged.entrySet()
                .stream()
                .map(
                    e -> new FieldSuggestion.ValueSuggestion(
                        e.getKey(),
                        e.getValue()[1] == 0 ? 0.0 : (double) e.getValue()[0] / e.getValue()[1]
                    )
                )
                .sorted(Comparator.comparingDouble(FieldSuggestion.ValueSuggestion::docFreq).reversed())
                .limit(size)
                .toList();
            listener.onResponse(new SampleResult(values, shardsSkipped.get(), dlsActive.get()));
        }
    }

    /**
     * The per-node shard read (the {@code dataNodeOperation}-equivalent). Applies the FLS/DLS gates
     * below before reading, then iterates each permitted shard's term dictionary directly, honoring
     * the wall-clock timeout budget.
     */
    private NodeSuggestValuesResponse dataNodeOperation(NodeSuggestValuesRequest request, ThreadContext threadContext) {
        request.startTimerOnDataNode();
        String nodeId = clusterService.localNode().getId();
        Map<Object, Long> docFreqByTerm = new HashMap<>();
        long liveDocs = 0;
        boolean dlsActive = false;
        ArrayList<Closeable> opened = new ArrayList<>();
        try {
            for (ShardId shardId : request.shardIds()) {
                if (System.currentTimeMillis() > request.nodeDeadlineMillis()) {
                    return new NodeSuggestValuesResponse(nodeId, docFreqByTerm, liveDocs, false, dlsActive, null);
                }
                if (fieldPermitted(shardId, request.field(), threadContext) == false) {
                    // FLS: field not permitted for this shard's index — skip silently, no error, no leak.
                    continue;
                }
                DlsGateResult dlsGate = checkDls(shardId, threadContext);
                if (dlsGate.dlsActive()) {
                    dlsActive = true;
                }
                if (dlsGate.refuseRead()) {
                    // DLS: the requesting user's role query is not match_all — refuse the raw read entirely
                    // for this shard rather than serving DLS-inconsistent docFreq numbers.
                    continue;
                }

                IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                IndexShard indexShard = indexService.getShard(shardId.getId());
                Engine.Searcher searcher = indexShard.acquireSearcher(Engine.SEARCH_SOURCE);
                opened.add(searcher);
                MappedFieldType fieldType = indexShard.mapperService().fieldType(request.field());
                if (fieldType == null) {
                    continue;
                }
                TermsEnum terms = fieldType.getTerms(searcher.getIndexReader(), "", false, null);
                if (terms == null) {
                    continue;
                }
                liveDocs += searcher.getIndexReader().numDocs();
                int collected = 0;
                int sinceClockCheck = 0;
                while (terms.next() != null && collected < request.size()) {
                    sinceClockCheck++;
                    if (sinceClockCheck >= 100) {
                        sinceClockCheck = 0;
                        if (System.currentTimeMillis() > request.nodeDeadlineMillis()) {
                            return new NodeSuggestValuesResponse(nodeId, docFreqByTerm, liveDocs, false, dlsActive, null);
                        }
                    }
                    // valueForDisplay converts the term's raw bytes into a display String immediately, so
                    // the BytesRef TermsEnum#term() reuses across iterations doesn't need to be copied here.
                    Object value = fieldType.valueForDisplay(terms.term());
                    docFreqByTerm.merge(value, (long) terms.docFreq(), Long::sum);
                    collected++;
                }
            }
            return new NodeSuggestValuesResponse(nodeId, docFreqByTerm, liveDocs, true, dlsActive, null);
        } catch (Exception e) {
            logger.warn(() -> Strings.format("failed to sample values for field [%s] on node [%s]", request.field(), nodeId), e);
            return NodeSuggestValuesResponse.error(nodeId, e);
        } finally {
            IOUtils.closeWhileHandlingException(opened);
        }
    }

    private boolean fieldPermitted(ShardId shardId, String field, ThreadContext threadContext) {
        if (XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return true;
        }
        IndicesAccessControl indicesAccessControl = AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.get(threadContext);
        if (indicesAccessControl == null) {
            return true;
        }
        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
        if (indexAccessControl == null) {
            return true;
        }
        return indexAccessControl.getFieldPermissions().grantsAccessTo(field);
    }

    private record DlsGateResult(boolean dlsActive, boolean refuseRead) {
        static final DlsGateResult NO_DLS = new DlsGateResult(false, false);
    }

    /**
     * Mirrors {@code TransportTermsEnumAction#canAccess}'s DLS check: refuses the raw read unless every
     * one of the user's DLS role queries rewrites to {@code match_all}.
     */
    private DlsGateResult checkDls(ShardId shardId, ThreadContext threadContext) {
        if (XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return DlsGateResult.NO_DLS;
        }
        IndicesAccessControl indicesAccessControl = AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.get(threadContext);
        if (indicesAccessControl == null) {
            return DlsGateResult.NO_DLS;
        }
        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(shardId.getIndexName());
        if (indexAccessControl == null || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions() == false) {
            return DlsGateResult.NO_DLS;
        }

        SecurityContext securityContext = new SecurityContext(clusterService.getSettings(), threadContext);
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            shardId.id(),
            0,
            null,
            System::currentTimeMillis,
            null,
            Collections.emptyMap(),
            null,
            null
        );

        boolean allMatchAll = indexAccessControl.getDocumentPermissions()
            .getListOfQueries()
            .stream()
            .allMatch(queries -> hasMatchAllEquivalent(queries, securityContext, searchExecutionContext));
        // dls_active fires exactly when the raw read is refused: a non-match-all DLS role query, unlike
        // a real search, doesn't narrow this path's results — it disables them outright.
        return new DlsGateResult(allMatchAll == false, allMatchAll == false);
    }

    private boolean hasMatchAllEquivalent(
        Set<BytesReference> queries,
        SecurityContext securityContext,
        SearchExecutionContext searchExecutionContext
    ) {
        if (queries == null) {
            return true;
        }
        for (BytesReference querySource : queries) {
            QueryBuilder queryBuilder = DLSRoleQueryValidator.evaluateAndVerifyRoleQuery(
                querySource,
                scriptService,
                searchExecutionContext.getParserConfig().registry(),
                securityContext.getUser()
            );
            QueryBuilder rewritten;
            try {
                rewritten = Rewriteable.rewrite(queryBuilder, searchExecutionContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (rewritten instanceof MatchAllQueryBuilder) {
                return true;
            }
        }
        return false;
    }
}
