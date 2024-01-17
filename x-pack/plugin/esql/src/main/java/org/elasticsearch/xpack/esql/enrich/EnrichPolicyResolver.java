/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            threadPool.executor(EsqlPlugin.ESQL_THREAD_POOL_NAME),
            LookupRequest::new,
            new RequestHandler()
        );
    }

    public void resolvePolicy(Collection<String> policyNames, ActionListener<EnrichResolution> listener) {
        if (policyNames.isEmpty()) {
            listener.onResponse(new EnrichResolution());
            return;
        }
        transportService.sendRequest(
            clusterService.localNode(),
            RESOLVE_ACTION_NAME,
            new LookupRequest(policyNames),
            new ActionListenerResponseHandler<>(listener.delegateFailureAndWrap((l, lookup) -> {
                final EnrichResolution resolution = new EnrichResolution();
                resolution.addExistingPolicies(lookup.allPolicies);
                try (RefCountingListener refs = new RefCountingListener(l.map(unused -> resolution))) {
                    for (Map.Entry<String, EnrichPolicy> e : lookup.policies.entrySet()) {
                        resolveOnePolicy(e.getKey(), e.getValue(), resolution, refs.acquire());
                    }
                }
            }), LookupResponse::new, threadPool.executor(EsqlPlugin.ESQL_THREAD_POOL_NAME))
        );
    }

    private void resolveOnePolicy(String policyName, EnrichPolicy policy, EnrichResolution resolution, ActionListener<Void> listener) {
        ThreadContext threadContext = threadPool.getThreadContext();
        listener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
            indexResolver.resolveAsMergedMapping(
                EnrichPolicy.getBaseName(policyName),
                IndexResolver.ALL_FIELDS,
                false,
                Map.of(),
                listener.map(indexResult -> {
                    if (indexResult.isValid()) {
                        EsIndex esIndex = indexResult.get();
                        Set<String> indices = esIndex.concreteIndices();
                        var concreteIndices = Map.of(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, Iterables.get(indices, 0));
                        resolution.addResolvedPolicy(policyName, policy, concreteIndices, esIndex.mapping());
                    } else {
                        resolution.addError(policyName, indexResult.toString());
                    }
                    return null;
                }),
                EsqlSession::specificValidity
            );
        }
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("local node transport action");
    }

    private static class LookupRequest extends TransportRequest {
        private final Collection<String> policyNames;

        LookupRequest(Collection<String> policyNames) {
            this.policyNames = policyNames;
        }

        LookupRequest(StreamInput in) {
            throw unsupported();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw unsupported();
        }
    }

    private static class LookupResponse extends TransportResponse {
        final Map<String, EnrichPolicy> policies;
        final Set<String> allPolicies;

        LookupResponse(Map<String, EnrichPolicy> policies, Set<String> allPolicies) {
            this.policies = policies;
            this.allPolicies = allPolicies;
        }

        LookupResponse(StreamInput in) {
            throw unsupported();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw unsupported();
        }
    }

    private class RequestHandler implements TransportRequestHandler<LookupRequest> {
        @Override
        public void messageReceived(LookupRequest request, TransportChannel channel, Task task) throws Exception {
            final EnrichMetadata metadata = clusterService.state().metadata().custom(EnrichMetadata.TYPE);
            final Map<String, EnrichPolicy> policies = metadata == null ? Map.of() : metadata.getPolicies();
            final Map<String, EnrichPolicy> results = Maps.newMapWithExpectedSize(request.policyNames.size());
            for (String policyName : request.policyNames) {
                EnrichPolicy p = policies.get(policyName);
                if (p != null) {
                    results.put(policyName, new EnrichPolicy(p.getType(), null, List.of(), p.getMatchField(), p.getEnrichFields()));
                }
            }
            new ChannelActionListener<>(channel).onResponse(new LookupResponse(results, policies.keySet()));
        }
    }
}
