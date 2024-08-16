/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

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
        return new GlobalRoutingTable(0L, projectRouting.build());
    }

    private GlobalRoutingTableTestHelper() {
        // Utility class
    }

}
