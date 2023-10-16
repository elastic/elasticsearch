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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.index.IndexResolver;

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
            ResolveRequest::new,
            new RequestHandler()
        );
    }

    public void resolvePolicy(String policyName, ActionListener<EnrichPolicyResolution> listener) {
        transportService.sendRequest(
            clusterService.localNode(),
            RESOLVE_ACTION_NAME,
            new ResolveRequest(policyName),
            new ActionListenerResponseHandler<>(
                listener.map(r -> r.resolution),
                ResolveResponse::new,
                threadPool.executor(EsqlPlugin.ESQL_THREAD_POOL_NAME)
            )
        );
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("local node transport action");
    }

    private static class ResolveRequest extends TransportRequest {
        private final String policyName;

        ResolveRequest(String policyName) {
            this.policyName = policyName;
        }

        ResolveRequest(StreamInput in) {
            throw unsupported();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw unsupported();
        }
    }

    private static class ResolveResponse extends TransportResponse {
        private final EnrichPolicyResolution resolution;

        ResolveResponse(EnrichPolicyResolution resolution) {
            this.resolution = resolution;
        }

        ResolveResponse(StreamInput in) {
            throw unsupported();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw unsupported();
        }
    }

    private class RequestHandler implements TransportRequestHandler<ResolveRequest> {
        @Override
        public void messageReceived(ResolveRequest request, TransportChannel channel, Task task) throws Exception {
            String policyName = request.policyName;
            EnrichPolicy policy = policies().get(policyName);
            ThreadContext threadContext = threadPool.getThreadContext();
            ActionListener<ResolveResponse> listener = new ChannelActionListener<>(channel);
            listener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
            try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                indexResolver.resolveAsMergedMapping(
                    EnrichPolicy.getBaseName(policyName),
                    IndexResolver.ALL_FIELDS,
                    false,
                    Map.of(),
                    listener.map(indexResult -> new ResolveResponse(new EnrichPolicyResolution(policyName, policy, indexResult))),
                    EsqlSession::specificValidity
                );
            }
        }
    }

    public Set<String> allPolicyNames() {
        // TODO: remove this suggestion as it exposes policy names without the right permission
        return policies().keySet();
    }

    private Map<String, EnrichPolicy> policies() {
        if (clusterService == null || clusterService.state() == null) {
            return Map.of();
        }
        EnrichMetadata metadata = clusterService.state().metadata().custom(EnrichMetadata.TYPE);
        return metadata == null ? Map.of() : metadata.getPolicies();
    }

}
