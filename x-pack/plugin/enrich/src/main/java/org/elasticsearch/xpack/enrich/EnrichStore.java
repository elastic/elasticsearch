/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A components that provides access and stores an enrich policy.
 */
public final class EnrichStore {

    private final ClusterService clusterService;

    EnrichStore(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Adds a new enrich policy or overwrites an existing policy if there is already a policy with the same name.
     * This method can only be invoked on the elected master node.
     *
     * @param name      The unique name of the policy
     * @param policy    The policy to store
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public void putPolicy(String name, EnrichPolicy policy, Consumer<Exception> handler) {
        assert clusterService.localNode().isMasterNode();

        // TODO: add validation

        final Map<String, EnrichPolicy> policies;
        final EnrichMetadata enrichMetadata = clusterService.state().metaData().custom(EnrichMetadata.TYPE);
        if (enrichMetadata != null) {
            // Make a copy, because policies map inside custom metadata is read only:
            policies = new HashMap<>(enrichMetadata.getPolicies());
        } else {
            policies = new HashMap<>();
        }
        policies.put(name, policy);
        clusterService.submitStateUpdateTask("update-enrich-policy", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = MetaData.builder(currentState.metaData())
                    .putCustom(EnrichMetadata.TYPE, new EnrichMetadata(policies))
                    .build();
                return ClusterState.builder(currentState)
                    .metaData(metaData)
                    .build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                handler.accept(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                handler.accept(e);
            }
        });
    }

    /**
     * Gets an enrich policy for the provided name if exists or otherwise returns <code>null</code>.
     *
     * @param name  The name of the policy to fetch
     * @return enrich policy if exists or <code>null</code> otherwise
     */
    public EnrichPolicy getPolicy(String name) {
        EnrichMetadata enrichMetadata = clusterService.state().metaData().custom(EnrichMetadata.TYPE);
        if (enrichMetadata == null) {
            return null;
        }
        return enrichMetadata.getPolicies().get(name);
    }

}
