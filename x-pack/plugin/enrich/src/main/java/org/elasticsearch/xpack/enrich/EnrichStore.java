/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
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
     * @param projectId The project ID
     * @param name      The unique name of the policy
     * @param policy    The policy to store
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void putPolicy(
        final ProjectId projectId,
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

        updateClusterState(clusterService, projectId, handler, project -> {
            final Map<String, EnrichPolicy> originalPolicies = getPolicies(project);
            if (originalPolicies.containsKey(name)) {
                throw new ResourceAlreadyExistsException("policy [{}] already exists", name);
            }
            for (String indexExpression : policy.getIndices()) {
                // indices field in policy can contain wildcards, aliases etc.
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                    project,
                    IndicesOptions.strictExpandOpen(),
                    true,
                    indexExpression
                );
                for (String concreteIndex : concreteIndices) {
                    IndexMetadata imd = project.index(concreteIndex);
                    assert imd != null;
                    MappingMetadata mapping = imd.mapping();
                    if (mapping == null) {
                        throw new IllegalArgumentException("source index [" + concreteIndex + "] has no mapping");
                    }
                    Map<String, Object> mappingSource = mapping.getSourceAsMap();
                    EnrichPolicyRunner.validateMappings(name, policy, concreteIndex, mappingSource);
                }
            }

            final Map<String, EnrichPolicy> updatedPolicies = new HashMap<>(originalPolicies);
            updatedPolicies.put(name, policy);
            return updatedPolicies;
        });
    }

    /**
     * Removes an enrich policy from the policies in the cluster state. This method can only be invoked on the
     * elected master node.
     *
     * @param projectId The project ID
     * @param name      The unique name of the policy
     * @param handler   The handler that gets invoked if policy has been stored or a failure has occurred.
     */
    public static void deletePolicy(ProjectId projectId, String name, ClusterService clusterService, Consumer<Exception> handler) {
        assert clusterService.localNode().isMasterNode();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        updateClusterState(clusterService, projectId, handler, project -> {
            final Map<String, EnrichPolicy> originalPolicies = getPolicies(project);
            if (originalPolicies.containsKey(name) == false) {
                throw new ResourceNotFoundException("policy [{}] not found", name);
            }

            final Map<String, EnrichPolicy> updatedPolicies = new HashMap<>(originalPolicies);
            updatedPolicies.remove(name);
            return updatedPolicies;
        });
    }

    /**
     * Gets an enrich policy for the provided name if exists or otherwise returns <code>null</code>.
     *
     * @param name  The name of the policy to fetch
     * @return enrich policy if exists or <code>null</code> otherwise
     */
    public static EnrichPolicy getPolicy(String name, ProjectMetadata project) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        return getPolicies(project).get(name);
    }

    /**
     * Gets all policies in the cluster.
     *
     * @param project the project metadata
     * @return a read-only Map of <code>policyName, EnrichPolicy</code> of the policies
     */
    public static Map<String, EnrichPolicy> getPolicies(ProjectMetadata project) {
        final EnrichMetadata metadata = project.custom(EnrichMetadata.TYPE, EnrichMetadata.EMPTY);
        return metadata.getPolicies();
    }

    private static void updateClusterState(
        ClusterService clusterService,
        ProjectId projectId,
        Consumer<Exception> handler,
        Function<ProjectMetadata, Map<String, EnrichPolicy>> function
    ) {
        submitUnbatchedTask(clusterService, "update-enrich-metadata", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                final var project = currentState.metadata().getProject(projectId);
                Map<String, EnrichPolicy> policies = function.apply(project);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(ProjectMetadata.builder(project).putCustom(EnrichMetadata.TYPE, new EnrichMetadata(policies)))
                    .build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                handler.accept(null);
            }

            @Override
            public void onFailure(Exception e) {
                handler.accept(e);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
