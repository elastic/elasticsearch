/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.function.BiConsumer;

public final class GlobalRoutingTableTestHelper {

    /**
     * Construct a new {@link GlobalRoutingTable} based on all the projects and indices in {@code metadata}.
     * Each index is passed to the {@code indexConsumer} along with a builder for that project's routing table
     */
    public static GlobalRoutingTable buildRoutingTable(Metadata metadata, BiConsumer<RoutingTable.Builder, IndexMetadata> indexConsumer) {
        ImmutableOpenMap.Builder<ProjectId, RoutingTable> projectRouting = ImmutableOpenMap.builder(metadata.projects().size());
        metadata.projects().forEach((projectId, projectMetadata) -> {
            final RoutingTable.Builder rtBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            projectMetadata.indices().values().forEach(indexMetadata -> indexConsumer.accept(rtBuilder, indexMetadata));
            projectRouting.put(projectId, rtBuilder.build());
        });
        return new GlobalRoutingTable(projectRouting.build());
    }

    /**
     * Update the existing {@link GlobalRoutingTable}
     * @param newIndicesConsumer Called for indices that do not exist in the routing table
     */
    public static GlobalRoutingTable updateRoutingTable(
        ClusterState clusterState,
        BiConsumer<RoutingTable.Builder, IndexMetadata> newIndicesConsumer
    ) {
        return updateRoutingTable(clusterState, newIndicesConsumer, (ignoreBuilder, ignoreIndex) -> {
            // no-op
        });
    }

    /**
     * Update the existing {@link GlobalRoutingTable}
     * @param newIndicesConsumer Called for indices that do not exist in the routing table
     * @param updateIndicesConsumer Called for indices that already exist in the routing table
     */
    public static GlobalRoutingTable updateRoutingTable(
        ClusterState clusterState,
        BiConsumer<RoutingTable.Builder, IndexMetadata> newIndicesConsumer,
        BiConsumer<RoutingTable.Builder, IndexMetadata> updateIndicesConsumer
    ) {
        final GlobalRoutingTable.Builder globalBuilder = GlobalRoutingTable.builder(clusterState.globalRoutingTable());
        clusterState.metadata().projects().forEach((projectId, projectMetadata) -> {
            final RoutingTable existingRoutingTable = clusterState.routingTable(projectId);
            final RoutingTable.Builder rtBuilder = existingRoutingTable == null
                ? RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                : RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, existingRoutingTable);
            projectMetadata.indices().values().forEach(indexMetadata -> {
                if (existingRoutingTable != null && existingRoutingTable.hasIndex(indexMetadata.getIndex())) {
                    updateIndicesConsumer.accept(rtBuilder, indexMetadata);
                } else {
                    newIndicesConsumer.accept(rtBuilder, indexMetadata);
                }
            });
            globalBuilder.put(projectId, rtBuilder.build());
        });
        return globalBuilder.build();
    }

    private GlobalRoutingTableTestHelper() {
        // Utility class
    }

    public static GlobalRoutingTable routingTable(ProjectId projectId, RoutingTable.Builder projectRouting) {
        return routingTable(projectId, projectRouting.build());
    }

    public static GlobalRoutingTable routingTable(ProjectId projectId, RoutingTable projectRouting) {
        return GlobalRoutingTable.builder().put(projectId, projectRouting).build();
    }

    public static GlobalRoutingTable routingTable(ProjectId projectId, IndexRoutingTable... indexRouting) {
        final RoutingTable.Builder rt = RoutingTable.builder();
        for (IndexRoutingTable irt : indexRouting) {
            rt.add(irt);
        }
        return routingTable(projectId, rt);
    }
}
