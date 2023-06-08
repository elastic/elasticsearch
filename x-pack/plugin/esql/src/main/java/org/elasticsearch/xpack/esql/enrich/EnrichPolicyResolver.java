/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import java.util.Map;
import java.util.Set;

public class EnrichPolicyResolver {

    private final ClusterService clusterService;
    private final IndexResolver indexResolver;
    private final ThreadPool threadPool;

    public EnrichPolicyResolver(ClusterService clusterService, IndexResolver indexResolver, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indexResolver = indexResolver;
        this.threadPool = threadPool;
    }

    public void resolvePolicy(String policyName, ActionListener<EnrichPolicyResolution> listener) {
        EnrichPolicy policy = policies().get(policyName);
        ThreadContext threadContext = threadPool.getThreadContext();
        ActionListener<EnrichPolicyResolution> wrappedListener = new ContextPreservingActionListener<>(
            threadContext.newRestorableContext(false),
            listener
        );
        try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
            indexResolver.resolveAsMergedMapping(
                EnrichPolicy.getBaseName(policyName),
                false,
                Map.of(),
                wrappedListener.map(indexResult -> new EnrichPolicyResolution(policyName, policy, indexResult))
            );
        }
    }

    public Set<String> allPolicyNames() {
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
