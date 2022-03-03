/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Helper methods for access and storage of an enrich policy.
 */
public final class EnrichStore {

    private EnrichStore() {}

    /**
     * Adds a new enrich policy. If a policy already exists with the same name then
     * this method throws an {@link IllegalArgumentException}.
     * This method can only be invoked on the elected master node.
     *
     * @param name      The unique name of the policy
     * @param policy    The policy to store
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void putPolicy(
        final String name,
        final EnrichPolicy policy,
        final ClusterService clusterService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Consumer<Exception> handler
    ) {
        assert clusterService.localNode().isMasterNode();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is missing");
        }
        // The policy name is used to create the enrich index name and
        // therefor a policy name has the same restrictions as an index name
        MetadataCreateIndexService.validateIndexOrAliasName(
            name,
            (policyName, error) -> new IllegalArgumentException("Invalid policy name [" + policyName + "], " + error)
        );
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            throw new IllegalArgumentException("Invalid policy name [" + name + "], must be lowercase");
        }
        Set<String> supportedPolicyTypes = Set.of(EnrichPolicy.SUPPORTED_POLICY_TYPES);
        if (supportedPolicyTypes.contains(policy.getType()) == false) {
            throw new IllegalArgumentException(
                "unsupported policy type ["
                    + policy.getType()
                    + "], supported types are "
                    + Arrays.toString(EnrichPolicy.SUPPORTED_POLICY_TYPES)
            );
        }

        final EnrichPolicy finalPolicy;
        if (policy.getElasticsearchVersion() == null) {
            finalPolicy = new EnrichPolicy(
                policy.getType(),
                policy.getQuery(),
                policy.getIndices(),
                policy.getMatchField(),
                policy.getEnrichFields(),
                Version.CURRENT
            );
        } else {
            finalPolicy = policy;
        }
        updateClusterState(clusterService, handler, current -> {
            for (String indexExpression : finalPolicy.getIndices()) {
                // indices field in policy can contain wildcards, aliases etc.
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                    current,
                    IndicesOptions.strictExpandOpen(),
                    indexExpression
                );
                for (String concreteIndex : concreteIndices) {
                    IndexMetadata imd = current.getMetadata().index(concreteIndex);
                    assert imd != null;
                    MappingMetadata mapping = imd.mapping();
                    if (mapping == null) {
                        throw new IllegalArgumentException("source index [" + concreteIndex + "] has no mapping");
                    }
                    Map<String, Object> mappingSource = mapping.getSourceAsMap();
                    EnrichPolicyRunner.validateMappings(name, finalPolicy, concreteIndex, mappingSource);
                }
            }

            final Map<String, EnrichPolicy> policies = getPolicies(current);
            EnrichPolicy existing = policies.putIfAbsent(name, finalPolicy);
            if (existing != null) {
                throw new ResourceAlreadyExistsException("policy [{}] already exists", name);
            }
            return policies;
        });
    }

    /**
     * Removes an enrich policy from the policies in the cluster state. This method can only be invoked on the
     * elected master node.
     *
     * @param name      The unique name of the policy
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void deletePolicy(String name, ClusterService clusterService, Consumer<Exception> handler) {
        assert clusterService.localNode().isMasterNode();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        updateClusterState(clusterService, handler, current -> {
            final Map<String, EnrichPolicy> policies = getPolicies(current);
            if (policies.containsKey(name) == false) {
                throw new ResourceNotFoundException("policy [{}] not found", name);
            }

            policies.remove(name);
            return policies;
        });
    }

    /**
     * Gets an enrich policy for the provided name if exists or otherwise returns <code>null</code>.
     *
     * @param name  The name of the policy to fetch
     * @return enrich policy if exists or <code>null</code> otherwise
     */
    public static EnrichPolicy getPolicy(String name, ClusterState state) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        return getPolicies(state).get(name);
    }

    /**
     * Gets all policies in the cluster.
     *
     * @param state the cluster state
     * @return a Map of <code>policyName, EnrichPolicy</code> of the policies
     */
    public static Map<String, EnrichPolicy> getPolicies(ClusterState state) {
        final Map<String, EnrichPolicy> policies;
        final EnrichMetadata enrichMetadata = state.metadata().custom(EnrichMetadata.TYPE);
        if (enrichMetadata != null) {
            // Make a copy, because policies map inside custom metadata is read only:
            policies = new HashMap<>(enrichMetadata.getPolicies());
        } else {
            policies = new HashMap<>();
        }
        return policies;
    }

    private static void updateClusterState(
        ClusterService clusterService,
        Consumer<Exception> handler,
        Function<ClusterState, Map<String, EnrichPolicy>> function
    ) {
        clusterService.submitStateUpdateTask("update-enrich-metadata", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Map<String, EnrichPolicy> policies = function.apply(currentState);
                Metadata metadata = Metadata.builder(currentState.metadata())
                    .putCustom(EnrichMetadata.TYPE, new EnrichMetadata(policies))
                    .build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                handler.accept(null);
            }

            @Override
            public void onFailure(Exception e) {
                handler.accept(e);
            }
        }, newExecutor());
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
