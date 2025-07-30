/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ProjectDeletedListenerTests extends ESTestCase {

    public void testInvocation() {
        final List<ProjectId> existingProjects = randomList(5, 15, ESTestCase::randomUniqueProjectId);

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool())) {
            final ClusterState.Builder csBuilder = ClusterState.builder(ClusterName.DEFAULT);
            existingProjects.forEach(p -> csBuilder.putProjectMetadata(ProjectMetadata.builder(p).build()));
            final ClusterState cs0 = csBuilder.build();

            ClusterServiceUtils.setState(clusterService, cs0);

            final Set<ProjectId> notifiedProjects = new HashSet<>();
            var pdl = new ProjectDeletedListener(notifiedProjects::add);
            pdl.attach(clusterService);

            final Set<ProjectId> projectsToDelete = Set.copyOf(
                randomSubsetOf(randomIntBetween(1, existingProjects.size() / 2), existingProjects)
            );
            final List<ProjectId> projectsToCreate = randomList(0, 3, ESTestCase::randomUniqueProjectId);

            final var mdBuilder = Metadata.builder(cs0.metadata());
            projectsToDelete.forEach(mdBuilder::removeProject);
            projectsToCreate.forEach(p -> mdBuilder.put(ProjectMetadata.builder(p).build()));
            var md = mdBuilder.build();
            var cs1 = ClusterState.builder(cs0)
                .metadata(md)
                .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(md, RoutingTable.Builder::addAsNew))
                .build();
            ClusterServiceUtils.setState(clusterService, cs1);

            assertThat(notifiedProjects, equalTo(projectsToDelete));
        }
    }

}
