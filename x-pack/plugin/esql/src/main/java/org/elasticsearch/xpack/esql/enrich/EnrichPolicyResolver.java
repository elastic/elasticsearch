/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves enrich policies across clusters in several steps:
 * 1. Calculates the policies that need to be resolved for each cluster, see {@link #lookupPolicies}.
 * 2. Sends out {@link LookupRequest} to each cluster to resolve policies. Internally, a remote cluster handles the lookup in two steps:
 *    - 2.1 Ensures the caller has permission to access the enrich policies.
 *    - 2.2 For each found enrich policy, uses {@link IndexResolver} to resolve the mappings of the concrete enrich index.
 * 3. For each unresolved policy, combines the lookup results to compute the actual enrich policy and mappings depending on the enrich mode.
 * This approach requires at most one cross-cluster call for each cluster.
 */
public class EnrichPolicyResolver {
    private static final String RESOLVE_ACTION_NAME = "cluster:monitor/xpack/enrich/esql/resolve_policy";

    private final ClusterService clusterService;
    private final IndexResolver indexResolver;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    public EnrichPolicyResolver(ClusterService clusterService, TransportService transportService, IndexResolver indexResolver) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexResolver = indexResolver;
        this.threadPool = transportService.getThreadPool();
        transportService.registerRequestHandler(
            RESOLVE_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.SEARCH),
            LookupRequest::new,
            new RequestHandler()
        );
    }

    public record UnresolvedPolicy(String name, Enrich.Mode mode) {

    }

    /**
     * Resolves a set of enrich policies
     *
     * @param targetClusters     the target clusters
     * @param unresolvedPolicies the unresolved policies
     * @param listener           notified with the enrich resolution
     */
    public void resolvePolicies(
        Collection<String> targetClusters,
        Collection<UnresolvedPolicy> unresolvedPolicies,
        ActionListener<EnrichResolution> listener
    ) {
        if (unresolvedPolicies.isEmpty() || targetClusters.isEmpty()) {
            listener.onResponse(new EnrichResolution());
            return;
        }
        final Set<String> remoteClusters = new HashSet<>(targetClusters);
        final boolean includeLocal = remoteClusters.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        lookupPolicies(remoteClusters, includeLocal, unresolvedPolicies, listener.map(lookupResponses -> {
            final EnrichResolution enrichResolution = new EnrichResolution();
            for (UnresolvedPolicy unresolved : unresolvedPolicies) {
                Tuple<ResolvedEnrichPolicy, String> resolved = mergeLookupResults(
                    unresolved,
                    calculateTargetClusters(unresolved.mode, includeLocal, remoteClusters),
                    lookupResponses
                );
                if (resolved.v1() != null) {
                    enrichResolution.addResolvedPolicy(unresolved.name, unresolved.mode, resolved.v1());
                } else {
                    assert resolved.v2() != null;
                    enrichResolution.addError(unresolved.name, unresolved.mode, resolved.v2());
                }
            }
            return enrichResolution;
        }));
    }

    private Collection<String> calculateTargetClusters(Enrich.Mode mode, boolean includeLocal, Set<String> remoteClusters) {
        return switch (mode) {
            case ANY -> CollectionUtils.appendToCopy(remoteClusters, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            case COORDINATOR -> List.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            case REMOTE -> includeLocal
                ? CollectionUtils.appendToCopy(remoteClusters, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)
                : remoteClusters;
        };
    }

    /**
     * Resolve an enrich policy by merging the lookup responses from the target clusters.
     * @return a resolved enrich policy or an error
     */
    private Tuple<ResolvedEnrichPolicy, String> mergeLookupResults(
        UnresolvedPolicy unresolved,
        Collection<String> targetClusters,
        Map<String, LookupResponse> lookupResults
    ) {
        assert targetClusters.isEmpty() == false;
        String policyName = unresolved.name;
        final Map<String, ResolvedEnrichPolicy> policies = new HashMap<>();
        final List<String> failures = new ArrayList<>();
        for (String cluster : targetClusters) {
            LookupResponse lookupResult = lookupResults.get(cluster);
            if (lookupResult != null) {
                ResolvedEnrichPolicy policy = lookupResult.policies.get(policyName);
                if (policy != null) {
                    policies.put(cluster, policy);
                } else {
                    final String failure = lookupResult.failures.get(policyName);
                    if (failure != null) {
                        failures.add(failure);
                    }
                }
            }
        }
        if (targetClusters.size() != policies.size()) {
            final String reason;
            if (failures.isEmpty()) {
                List<String> missingClusters = targetClusters.stream().filter(c -> policies.containsKey(c) == false).sorted().toList();
                reason = missingPolicyError(policyName, targetClusters, missingClusters);
            } else {
                reason = "failed to resolve enrich policy [" + policyName + "]; reason " + failures;
            }
            return Tuple.tuple(null, reason);
        }
        Map<String, EsField> mappings = new HashMap<>();
        Map<String, String> concreteIndices = new HashMap<>();
        ResolvedEnrichPolicy last = null;
        for (Map.Entry<String, ResolvedEnrichPolicy> e : policies.entrySet()) {
            ResolvedEnrichPolicy curr = e.getValue();
            if (last != null && last.matchField().equals(curr.matchField()) == false) {
                String error = "enrich policy [" + policyName + "] has different match fields ";
                error += "[" + last.matchField() + ", " + curr.matchField() + "] across clusters";
                return Tuple.tuple(null, error);
            }
            if (last != null && last.matchType().equals(curr.matchType()) == false) {
                String error = "enrich policy [" + policyName + "] has different match types ";
                error += "[" + last.matchType() + ", " + curr.matchType() + "] across clusters";
                return Tuple.tuple(null, error);
            }
            // merge mappings
            for (Map.Entry<String, EsField> m : curr.mapping().entrySet()) {
                EsField field = m.getValue();
                field = new EsField(
                    field.getName(),
                    DataType.fromTypeName(field.getDataType().typeName()),
                    field.getProperties(),
                    field.isAggregatable(),
                    field.isAlias()
                );
                EsField old = mappings.putIfAbsent(m.getKey(), field);
                if (old != null && old.getDataType().equals(field.getDataType()) == false) {
                    String error = "field [" + m.getKey() + "] of enrich policy [" + policyName + "] has different data types ";
                    error += "[" + old.getDataType() + ", " + field.getDataType() + "] across clusters";
                    return Tuple.tuple(null, error);
                }
            }
            if (last != null) {
                Map<String, Integer> counts = Maps.newMapWithExpectedSize(last.enrichFields().size());
                last.enrichFields().forEach(f -> counts.put(f, 1));
                curr.enrichFields().forEach(f -> counts.compute(f, (k, v) -> v == null ? 1 : v + 1));
                // should be sorted-then-limit, but this sorted is for testing only
                var diff = counts.entrySet().stream().filter(f -> f.getValue() < 2).map(Map.Entry::getKey).limit(20).sorted().toList();
                if (diff.isEmpty() == false) {
                    String detailed = "these fields are missing in some policies: " + diff;
                    return Tuple.tuple(null, "enrich policy [" + policyName + "] has different enrich fields across clusters; " + detailed);
                }
            }
            // merge concrete indices
            concreteIndices.putAll(curr.concreteIndices());
            last = curr;
        }
        assert last != null;
        var resolved = new ResolvedEnrichPolicy(last.matchField(), last.matchType(), last.enrichFields(), concreteIndices, mappings);
        return Tuple.tuple(resolved, null);
    }

    private String missingPolicyError(String policyName, Collection<String> targetClusters, List<String> missingClusters) {
        // local cluster only
        String reason = "cannot find enrich policy [" + policyName + "]";
        if (targetClusters.size() == 1 && Iterables.get(missingClusters, 0).isEmpty()) {
            // accessing the policy names directly after we have checked the permission.
            List<String> potentialMatches = StringUtils.findSimilar(policyName, availablePolicies().keySet());
            if (potentialMatches.isEmpty() == false) {
                var suggestion = potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches;
                reason += ", did you mean " + suggestion + "?";
            }
            return reason;
        }
        String detailed = missingClusters.stream().sorted().map(c -> c.isEmpty() ? "_local" : c).collect(Collectors.joining(", "));
        return reason + " on clusters [" + detailed + "]";
    }

    private void lookupPolicies(
        Collection<String> remoteClusters,
        boolean includeLocal,
        Collection<UnresolvedPolicy> unresolvedPolicies,
        ActionListener<Map<String, LookupResponse>> listener
    ) {
        final Map<String, LookupResponse> lookupResponses = ConcurrentCollections.newConcurrentMap();
        try (RefCountingListener refs = new RefCountingListener(listener.map(unused -> lookupResponses))) {
            Set<String> remotePolicies = unresolvedPolicies.stream()
                .filter(u -> u.mode != Enrich.Mode.COORDINATOR)
                .map(u -> u.name)
                .collect(Collectors.toSet());
            // remote clusters
            if (remotePolicies.isEmpty() == false) {
                for (String cluster : remoteClusters) {
                    final Transport.Connection connection;
                    try {
                        connection = getRemoteConnection(cluster);
                    } catch (Exception e) {
                        refs.acquire().onFailure(e);
                        return;
                    }
                    transportService.sendRequest(
                        connection,
                        RESOLVE_ACTION_NAME,
                        new LookupRequest(cluster, remotePolicies),
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            refs.acquire(resp -> lookupResponses.put(cluster, resp)),
                            LookupResponse::new,
                            threadPool.executor(ThreadPool.Names.SEARCH)
                        )
                    );
                }
            }
            // local cluster
            Set<String> localPolicies = unresolvedPolicies.stream()
                .filter(u -> includeLocal || u.mode != Enrich.Mode.REMOTE)
                .map(u -> u.name)
                .collect(Collectors.toSet());
            if (localPolicies.isEmpty() == false) {
                transportService.sendRequest(
                    transportService.getLocalNode(),
                    RESOLVE_ACTION_NAME,
                    new LookupRequest(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, localPolicies),
                    new ActionListenerResponseHandler<>(
                        refs.acquire(resp -> lookupResponses.put(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, resp)),
                        LookupResponse::new,
                        threadPool.executor(ThreadPool.Names.SEARCH)
                    )
                );
            }
        }
    }

    private static class LookupRequest extends TransportRequest {
        private final String clusterAlias;
        private final Collection<String> policyNames;

        LookupRequest(String clusterAlias, Collection<String> policyNames) {
            this.clusterAlias = clusterAlias;
            this.policyNames = policyNames;
        }

        LookupRequest(StreamInput in) throws IOException {
            this.clusterAlias = in.readString();
            this.policyNames = in.readStringCollectionAsList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterAlias);
            out.writeStringCollection(policyNames);
        }
    }

    private static class LookupResponse extends TransportResponse {
        final Map<String, ResolvedEnrichPolicy> policies;
        final Map<String, String> failures;

        LookupResponse(Map<String, ResolvedEnrichPolicy> policies, Map<String, String> failures) {
            this.policies = policies;
            this.failures = failures;
        }

        LookupResponse(StreamInput in) throws IOException {
            this.policies = in.readMap(StreamInput::readString, ResolvedEnrichPolicy::new);
            this.failures = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(policies, (o, v) -> v.writeTo(o));
            out.writeMap(failures, StreamOutput::writeString);
        }
    }

    private class RequestHandler implements TransportRequestHandler<LookupRequest> {
        @Override
        public void messageReceived(LookupRequest request, TransportChannel channel, Task task) {
            final Map<String, EnrichPolicy> availablePolicies = availablePolicies();
            final Map<String, String> failures = ConcurrentCollections.newConcurrentMap();
            final Map<String, ResolvedEnrichPolicy> resolvedPolices = ConcurrentCollections.newConcurrentMap();
            ThreadContext threadContext = threadPool.getThreadContext();
            ActionListener<LookupResponse> listener = ContextPreservingActionListener.wrapPreservingContext(
                new ChannelActionListener<>(channel),
                threadContext
            );
            try (
                RefCountingListener refs = new RefCountingListener(listener.map(unused -> new LookupResponse(resolvedPolices, failures)))
            ) {
                for (String policyName : request.policyNames) {
                    EnrichPolicy p = availablePolicies.get(policyName);
                    if (p == null) {
                        continue;
                    }
                    try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                        String indexName = EnrichPolicy.getBaseName(policyName);
                        indexResolver.resolveAsMergedMapping(indexName, IndexResolver.ALL_FIELDS, refs.acquire(indexResult -> {
                            if (indexResult.isValid() && indexResult.get().concreteIndices().size() == 1) {
                                EsIndex esIndex = indexResult.get();
                                var concreteIndices = Map.of(request.clusterAlias, Iterables.get(esIndex.concreteIndices(), 0));
                                var resolved = new ResolvedEnrichPolicy(
                                    p.getMatchField(),
                                    p.getType(),
                                    p.getEnrichFields(),
                                    concreteIndices,
                                    esIndex.mapping()
                                );
                                resolvedPolices.put(policyName, resolved);
                            } else {
                                failures.put(policyName, indexResult.toString());
                            }
                        }));
                    }
                }
            }
        }
    }

    protected Map<String, EnrichPolicy> availablePolicies() {
        final EnrichMetadata metadata = clusterService.state().metadata().custom(EnrichMetadata.TYPE);
        return metadata == null ? Map.of() : metadata.getPolicies();
    }

    protected Transport.Connection getRemoteConnection(String cluster) {
        return transportService.getRemoteClusterService().getConnection(cluster);
    }

    public Map<String, List<String>> groupIndicesPerCluster(String[] indices) {
        return transportService.getRemoteClusterService()
            .groupIndices(SearchRequest.DEFAULT_INDICES_OPTIONS, indices)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Arrays.asList(e.getValue().indices())));
    }
}
