/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;

public class TransportTermsEnumAction extends HandledTransportAction<TermsEnumRequest, TermsEnumResponse> {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final RemoteClusterService remoteClusterService;
    private final SearchService searchService;
    private final IndicesService indicesService;
    private final ScriptService scriptService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportShardAction;
    private final String shardExecutor;
    private final XPackLicenseState licenseState;
    private final Settings settings;
    private final boolean ccsCheckCompatibility;

    @Inject
    public TransportTermsEnumAction(
        ClusterService clusterService,
        SearchService searchService,
        SearchTransportService searchTransportService,
        TransportService transportService,
        IndicesService indicesService,
        ScriptService scriptService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(TermsEnumAction.NAME, transportService, actionFilters, TermsEnumRequest::new);

        this.clusterService = clusterService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportShardAction = actionName + "[s]";
        this.shardExecutor = ThreadPool.Names.AUTO_COMPLETE;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.licenseState = licenseState;
        this.settings = settings;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());

        transportService.registerRequestHandler(
            transportShardAction,
            ThreadPool.Names.SAME,
            NodeTermsEnumRequest::new,
            new NodeTransportHandler()
        );

    }

    @Override
    protected void doExecute(Task task, TermsEnumRequest request, ActionListener<TermsEnumResponse> listener) {
        if (ccsCheckCompatibility) {
            checkCCSVersionCompatibility(request);
        }
        new AsyncBroadcastAction(task, request, listener).start();
    }

    protected NodeTermsEnumRequest newNodeRequest(
        final OriginalIndices originalIndices,
        final String nodeId,
        final Set<ShardId> shardIds,
        TermsEnumRequest request,
        long taskStartMillis
    ) {
        // Given we look terms up in the terms dictionary alias filters is another aspect of search (like DLS) that we
        // currently do not support.
        // final ClusterState clusterState = clusterService.state();
        // final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        // final AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, shard.getIndexName(), indicesAndAliases);
        return new NodeTermsEnumRequest(originalIndices, nodeId, shardIds, request, taskStartMillis);
    }

    protected NodeTermsEnumResponse readShardResponse(StreamInput in) throws IOException {
        return new NodeTermsEnumResponse(in);
    }

    protected Map<String, Set<ShardId>> getNodeBundles(ClusterState clusterState, String[] concreteIndices) {
        // Group targeted shards by nodeId
        Map<String, Set<ShardId>> fastNodeBundles = new HashMap<>();
        for (String indexName : concreteIndices) {

            String[] singleIndex = { indexName };

            GroupShardsIterator<ShardIterator> shards = clusterService.operationRouting()
                .searchShards(clusterState, singleIndex, null, null);

            for (ShardIterator copiesOfShard : shards) {
                ShardRouting selectedCopyOfShard = null;
                for (ShardRouting copy : copiesOfShard) {
                    // Pick the first active node with a copy of the shard
                    if (copy.active() && copy.assignedToNode()) {
                        selectedCopyOfShard = copy;
                        break;
                    }
                }
                if (selectedCopyOfShard == null) {
                    break;
                }
                String nodeId = selectedCopyOfShard.currentNodeId();
                final Set<ShardId> bundle;
                if (fastNodeBundles.containsKey(nodeId)) {
                    bundle = fastNodeBundles.get(nodeId);
                } else {
                    bundle = new HashSet<>();
                    fastNodeBundles.put(nodeId, bundle);
                }
                if (bundle != null) {
                    bundle.add(selectedCopyOfShard.shardId());
                }
            }
        }
        return fastNodeBundles;
    }

    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermsEnumRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, TermsEnumRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    protected TermsEnumResponse mergeResponses(
        TermsEnumRequest request,
        AtomicReferenceArray<?> atomicResponses,
        boolean complete,
        Map<String, Set<ShardId>> nodeBundles
    ) {
        int successfulShards = 0;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        List<List<String>> termsList = new ArrayList<>();
        for (int i = 0; i < atomicResponses.length(); i++) {
            Object atomicResponse = atomicResponses.get(i);
            if (atomicResponse == null) {
                // simply ignore non active operations
            } else if (atomicResponse instanceof NodeTermsEnumResponse str) {
                // Only one node response has to be incomplete for the entire result to be labelled incomplete.
                if (str.isComplete() == false) {
                    complete = false;
                }

                Set<ShardId> shards = nodeBundles.get(str.getNodeId());
                if (str.getError() != null) {
                    complete = false;
                    // A single reported error is assumed to be for all shards queried on that node.
                    // When reading we read from multiple Lucene indices in one unified view so any error is
                    // assumed to be all shards on that node.
                    failedShards += shards.size();
                    if (shardFailures == null) {
                        shardFailures = new ArrayList<>();
                    }
                    for (ShardId failedShard : shards) {
                        shardFailures.add(
                            new DefaultShardOperationFailedException(
                                new BroadcastShardOperationFailedException(failedShard, str.getError())
                            )
                        );
                    }
                } else {
                    successfulShards += shards.size();
                }
                termsList.add(str.terms());
            } else if (atomicResponse instanceof RemoteClusterTermsEnumResponse rc) {
                // Only one node response has to be incomplete for the entire result to be labelled incomplete.
                if (rc.resp.isComplete() == false || rc.resp.getFailedShards() > 0) {
                    complete = false;
                }
                successfulShards += rc.resp.getSuccessfulShards();
                failedShards += rc.resp.getFailedShards();
                for (DefaultShardOperationFailedException exc : rc.resp.getShardFailures()) {
                    shardFailures.add(
                        new DefaultShardOperationFailedException(rc.clusterAlias + ":" + exc.index(), exc.shardId(), exc.getCause())
                    );
                }
                termsList.add(rc.resp.getTerms());
            } else {
                throw new AssertionError("Unknown atomic response type: " + atomicResponse.getClass().getName());
            }
        }

        List<String> ans = termsList.size() == 1 ? termsList.get(0) : mergeResponses(termsList, request.size());
        return new TermsEnumResponse(ans, (failedShards + successfulShards), successfulShards, failedShards, shardFailures, complete);
    }

    private List<String> mergeResponses(List<List<String>> termsList, int size) {
        final PriorityQueue<TermIterator> pq = new PriorityQueue<>(termsList.size()) {
            @Override
            protected boolean lessThan(TermIterator a, TermIterator b) {
                return a.compareTo(b) < 0;
            }
        };

        for (List<String> terms : termsList) {
            Iterator<String> it = terms.iterator();
            if (it.hasNext()) {
                pq.add(new TermIterator(it));
            }
        }

        String lastTerm = null;
        final List<String> ans = new ArrayList<>();
        while (pq.size() != 0) {
            TermIterator it = pq.top();
            String term = it.term();
            if (lastTerm != null && lastTerm.compareTo(term) != 0) {
                ans.add(lastTerm);
                if (ans.size() == size) {
                    break;
                }
                lastTerm = null;
            }
            if (lastTerm == null) {
                lastTerm = term;
            }
            if (it.hasNext()) {
                String itTerm = it.term();
                it.next();
                assert itTerm.compareTo(it.term()) <= 0;
                pq.updateTop();
            } else {
                pq.pop();
            }
        }
        if (lastTerm != null && ans.size() < size) {
            ans.add(lastTerm);
        }
        return ans;
    }

    protected NodeTermsEnumResponse dataNodeOperation(NodeTermsEnumRequest request, Task task) throws IOException {
        List<String> termsList = new ArrayList<>();
        String error = null;

        long timeout_millis = request.timeout();
        long scheduledEnd = request.nodeStartedTimeMillis() + timeout_millis;

        ArrayList<TermsEnum> shardTermsEnums = new ArrayList<>();
        ArrayList<Closeable> openedResources = new ArrayList<>();
        try {
            for (ShardId shardId : request.shardIds()) {
                // Check we haven't just arrived on a node and time is up already.
                if (System.currentTimeMillis() > scheduledEnd) {
                    return new NodeTermsEnumResponse(request.nodeId(), termsList, error, false);
                }
                final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                final IndexShard indexShard = indexService.getShard(shardId.getId());

                Engine.Searcher searcher = indexShard.acquireSearcher(Engine.SEARCH_SOURCE);
                openedResources.add(searcher);
                final SearchExecutionContext queryShardContext = indexService.newSearchExecutionContext(
                    shardId.id(),
                    0,
                    searcher,
                    request::nodeStartedTimeMillis,
                    null,
                    Collections.emptyMap()
                );
                final MappedFieldType mappedFieldType = indexShard.mapperService().fieldType(request.field());
                if (mappedFieldType != null) {
                    TermsEnum terms = mappedFieldType.getTerms(
                        request.caseInsensitive(),
                        request.string() == null ? "" : request.string(),
                        queryShardContext,
                        request.searchAfter()
                    );
                    if (terms != null) {
                        shardTermsEnums.add(terms);
                    }
                }
            }
            if (shardTermsEnums.size() == 0) {
                // No term enums available
                return new NodeTermsEnumResponse(request.nodeId(), termsList, error, true);
            }
            MultiShardTermsEnum te = new MultiShardTermsEnum(shardTermsEnums.toArray(new TermsEnum[0]));

            int shard_size = request.size();
            // All the above prep might take a while - do a timer check now before we continue further.
            if (System.currentTimeMillis() > scheduledEnd) {
                return new NodeTermsEnumResponse(request.nodeId(), termsList, error, false);
            }

            int numTermsBetweenClockChecks = 100;
            int termCount = 0;
            // Collect in alphabetical order
            while (te.next() != null) {
                termCount++;
                if (termCount > numTermsBetweenClockChecks) {
                    if (System.currentTimeMillis() > scheduledEnd) {
                        boolean complete = te.next() == null;
                        return new NodeTermsEnumResponse(request.nodeId(), termsList, error, complete);
                    }
                    termCount = 0;
                }
                BytesRef bytes = te.term();
                termsList.add(bytes.utf8ToString());
                if (termsList.size() >= shard_size) {
                    break;
                }
            }

        } catch (Exception e) {
            error = ExceptionsHelper.stackTrace(e);
        } finally {
            IOUtils.close(openedResources);
        }
        return new NodeTermsEnumResponse(request.nodeId(), termsList, error, true);
    }

    // TODO remove this so we can shift code to server module - write a separate Interceptor class to
    // rewrite requests according to security rules
    private boolean canAccess(
        ShardId shardId,
        NodeTermsEnumRequest request,
        XPackLicenseState frozenLicenseState,
        ThreadContext threadContext
    ) throws IOException {
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(shardId.getIndexName());

            if (indexAccessControl != null) {
                final boolean dls = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                if (dls && DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(frozenLicenseState)) {
                    // Check to see if any of the roles defined for the current user rewrite to match_all

                    SecurityContext securityContext = new SecurityContext(clusterService.getSettings(), threadContext);
                    final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                    final SearchExecutionContext queryShardContext = indexService.newSearchExecutionContext(
                        shardId.id(),
                        0,
                        null,
                        request::nodeStartedTimeMillis,
                        null,
                        Collections.emptyMap()
                    );

                    // Current user has potentially many roles and therefore potentially many queries
                    // defining sets of docs accessible
                    Set<BytesReference> queries = indexAccessControl.getDocumentPermissions().getQueries();
                    for (BytesReference querySource : queries) {
                        QueryBuilder queryBuilder = DLSRoleQueryValidator.evaluateAndVerifyRoleQuery(
                            querySource,
                            scriptService,
                            queryShardContext.getParserConfig().registry(),
                            securityContext.getUser()
                        );
                        QueryBuilder rewrittenQueryBuilder = Rewriteable.rewrite(queryBuilder, queryShardContext);
                        if (rewrittenQueryBuilder instanceof MatchAllQueryBuilder) {
                            // One of the roles assigned has "all" permissions - allow unfettered access to termsDict
                            return true;
                        }
                    }
                    return false;
                }
            }
        }
        return true;
    }

    private boolean canMatchShard(ShardId shardId, NodeTermsEnumRequest req) throws IOException {
        if (req.indexFilter() == null || req.indexFilter() instanceof MatchAllQueryBuilder) {
            return true;
        }
        ShardSearchRequest searchRequest = new ShardSearchRequest(shardId, req.nodeStartedTimeMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(req.indexFilter()));
        return searchService.canMatch(searchRequest).canMatch();
    }

    protected class AsyncBroadcastAction {

        private final Task task;
        private final TermsEnumRequest request;
        private ActionListener<TermsEnumResponse> listener;
        private final DiscoveryNodes nodes;
        private final int expectedOps;
        private final AtomicInteger counterOps = new AtomicInteger();
        private final AtomicReferenceArray<Object> atomicResponses;
        private final Map<String, Set<ShardId>> nodeBundles;
        private final OriginalIndices localIndices;
        private final Map<String, OriginalIndices> remoteClusterIndices;

        protected AsyncBroadcastAction(Task task, TermsEnumRequest request, ActionListener<TermsEnumResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }

            this.remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(), request.indices());
            this.localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

            // update to concrete indices
            String[] concreteIndices = localIndices == null
                ? new String[0]
                : indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            logger.trace("resolving shards based on cluster state version [{}]", clusterState.version());
            nodeBundles = getNodeBundles(clusterState, concreteIndices);
            expectedOps = nodeBundles.size() + remoteClusterIndices.size();

            atomicResponses = new AtomicReferenceArray<>(expectedOps);
        }

        public void start() {
            if (expectedOps == 0) {
                // no shards
                try {
                    listener.onResponse(mergeResponses(request, new AtomicReferenceArray<>(0), true, nodeBundles));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
                // TODO or remove above try and instead just call finishHim() here? Helps keep return logic consistent
                return;
            }
            // count the local operations, and perform the non local ones
            int numOps = 0;
            for (final String nodeId : nodeBundles.keySet()) {
                if (checkForEarlyFinish()) {
                    return;
                }
                Set<ShardId> shardIds = nodeBundles.get(nodeId);
                if (shardIds.size() > 0) {
                    performOperation(nodeId, shardIds, numOps);
                } else {
                    // really, no shards active in this group
                    onNodeFailure(nodeId, numOps, null);
                }
                ++numOps;
            }
            // handle remote clusters
            for (String clusterAlias : remoteClusterIndices.keySet()) {
                performRemoteClusterOperation(clusterAlias, remoteClusterIndices.get(clusterAlias), numOps);
                ++numOps;
            }
        }

        // Returns true if we exited with a response to the caller.
        boolean checkForEarlyFinish() {
            long now = System.currentTimeMillis();
            if ((now - task.getStartTime()) > request.timeout().getMillis()) {
                finishHim(false);
                return true;
            }
            return false;
        }

        protected void performOperation(final String nodeId, final Set<ShardId> shardIds, final int opsIndex) {
            if (shardIds.size() == 0) {
                // no more active shards... (we should not really get here, just safety)
                onNodeFailure(nodeId, opsIndex, null);
            } else {
                try {
                    final NodeTermsEnumRequest nodeRequest = newNodeRequest(localIndices, nodeId, shardIds, request, task.getStartTime());
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    DiscoveryNode node = nodes.get(nodeId);
                    if (node == null) {
                        // no node connected, act as failure
                        onNodeFailure(nodeId, opsIndex, null);
                    } else if (checkForEarlyFinish() == false) {
                        transportService.sendRequest(
                            node,
                            transportShardAction,
                            nodeRequest,
                            new TransportResponseHandler<NodeTermsEnumResponse>() {
                                @Override
                                public NodeTermsEnumResponse read(StreamInput in) throws IOException {
                                    return readShardResponse(in);
                                }

                                @Override
                                public void handleResponse(NodeTermsEnumResponse response) {
                                    onNodeResponse(nodeId, opsIndex, response);
                                }

                                @Override
                                public void handleException(TransportException exc) {
                                    onNodeFailure(nodeId, opsIndex, exc);
                                }
                            }
                        );
                    }
                } catch (Exception exc) {
                    onNodeFailure(nodeId, opsIndex, exc);
                }
            }
        }

        void performRemoteClusterOperation(final String clusterAlias, final OriginalIndices remoteIndices, final int opsIndex) {
            try {
                TermsEnumRequest req = new TermsEnumRequest(request).indices(remoteIndices.indices());

                Client remoteClient = remoteClusterService.getRemoteClusterClient(transportService.getThreadPool(), clusterAlias);
                remoteClient.execute(TermsEnumAction.INSTANCE, req, new ActionListener<>() {
                    @Override
                    public void onResponse(TermsEnumResponse termsEnumResponse) {
                        onRemoteClusterResponse(
                            clusterAlias,
                            opsIndex,
                            new RemoteClusterTermsEnumResponse(clusterAlias, termsEnumResponse)
                        );
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        onRemoteClusterFailure(clusterAlias, opsIndex, exc);
                    }
                });
            } catch (Exception exc) {
                onRemoteClusterFailure(clusterAlias, opsIndex, null);
            }
        }

        private void onNodeResponse(String nodeId, int opsIndex, NodeTermsEnumResponse response) {
            logger.trace("received response for node {}", nodeId);
            atomicResponses.set(opsIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            } else {
                checkForEarlyFinish();
            }
        }

        private void onRemoteClusterResponse(String clusterAlias, int opsIndex, RemoteClusterTermsEnumResponse response) {
            logger.trace("received response for cluster {}", clusterAlias);
            atomicResponses.set(opsIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            } else {
                checkForEarlyFinish();
            }
        }

        private void onNodeFailure(String nodeId, int opsIndex, Exception exc) {
            logger.trace("received failure {} for node {}", exc, nodeId);
            // TODO: Handle exceptions in the atomic response array
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            }
        }

        private void onRemoteClusterFailure(String clusterAlias, int opsIndex, Exception exc) {
            logger.trace("received failure {} for cluster {}", exc, clusterAlias);
            // TODO: Handle exceptions in the atomic response array
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            }
        }

        // Can be called multiple times - either for early time-outs or for fully-completed collections.
        protected synchronized void finishHim(boolean complete) {
            if (listener == null) {
                return;
            }
            try {
                listener.onResponse(mergeResponses(request, atomicResponses, complete, nodeBundles));
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                listener = null;
            }
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeTermsEnumRequest> {

        @Override
        public void messageReceived(NodeTermsEnumRequest request, TransportChannel channel, Task task) throws Exception {
            asyncNodeOperation(request, task, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (Exception e1) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Failed to send error response for action [{}] and request [{}]",
                            actionName,
                            request
                        ),
                        e1
                    );
                }
            }));
        }
    }

    private void asyncNodeOperation(NodeTermsEnumRequest request, Task task, ActionListener<NodeTermsEnumResponse> listener)
        throws IOException {
        // Start the clock ticking on the data node using the data node's local current time.
        request.startTimerOnDataNode();

        // DLS/FLS check copied from ResizeRequestInterceptor - check permissions and
        // any index_filter canMatch checks on network thread before allocating work
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        final XPackLicenseState frozenLicenseState = licenseState.copyCurrentLicenseState();
        for (ShardId shardId : request.shardIds().toArray(new ShardId[0])) {
            if (canAccess(shardId, request, frozenLicenseState, threadContext) == false || canMatchShard(shardId, request) == false) {
                // Permission denied or can't match, remove shardID from request
                request.remove(shardId);
            }
        }
        if (request.shardIds().size() == 0) {
            listener.onResponse(new NodeTermsEnumResponse(request.nodeId(), Collections.emptyList(), null, true));
        } else {
            // Use the search threadpool if its queue is empty
            assert transportService.getThreadPool().executor(ThreadPool.Names.SEARCH) instanceof EsThreadPoolExecutor
                : "SEARCH threadpool must be an instance of ThreadPoolExecutor";
            EsThreadPoolExecutor ex = (EsThreadPoolExecutor) transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
            final String executorName = ex.getQueue().size() == 0 ? ThreadPool.Names.SEARCH : shardExecutor;
            transportService.getThreadPool()
                .executor(executorName)
                .execute(ActionRunnable.supply(listener, () -> dataNodeOperation(request, task)));
        }
    }

    private static class RemoteClusterTermsEnumResponse {
        final String clusterAlias;
        final TermsEnumResponse resp;

        private RemoteClusterTermsEnumResponse(String clusterAlias, TermsEnumResponse resp) {
            this.clusterAlias = clusterAlias;
            this.resp = resp;
        }
    }

    private static class TermIterator implements Iterator<String>, Comparable<TermIterator> {
        private final Iterator<String> iterator;
        private String current;

        private TermIterator(Iterator<String> iterator) {
            this.iterator = iterator;
            this.current = iterator.next();
        }

        public String term() {
            return current;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public String next() {
            return current = iterator.next();
        }

        @Override
        public int compareTo(TermIterator o) {
            return current.compareTo(o.term());
        }
    }
}
