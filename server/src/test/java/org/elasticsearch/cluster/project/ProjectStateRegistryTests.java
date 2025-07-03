/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.elasticsearch.cluster.project.ProjectStateRegistrySerializationTests.randomSettings;

public class ProjectStateRegistryTests extends ESTestCase {

    public void testBuilder() {
        final var projects = randomSet(1, 5, ESTestCase::randomUniqueProjectId);
        final var projectsUnderDeletion = randomSet(0, 5, ESTestCase::randomUniqueProjectId);
        var builder = ProjectStateRegistry.builder();
        projects.forEach(projectId -> builder.putProjectSettings(projectId, randomSettings()));
        projectsUnderDeletion.forEach(
            projectId -> builder.putProjectSettings(projectId, randomSettings()).markProjectForDeletion(projectId)
        );
        var projectStateRegistry = builder.build();
        var gen1 = projectStateRegistry.getProjectsMarkedForDeletionGeneration();
        assertThat(gen1, Matchers.equalTo(projectsUnderDeletion.isEmpty() ? 0L : 1L));

        projectStateRegistry = ProjectStateRegistry.builder(projectStateRegistry).markProjectForDeletion(randomFrom(projects)).build();
        var gen2 = projectStateRegistry.getProjectsMarkedForDeletionGeneration();
        assertThat(gen2, Matchers.equalTo(gen1 + 1));

        if (projectsUnderDeletion.isEmpty() == false) {
            // re-adding the same projectId should not change the generation
            projectStateRegistry = ProjectStateRegistry.builder(projectStateRegistry)
                .markProjectForDeletion(randomFrom(projectsUnderDeletion))
                .build();
            assertThat(projectStateRegistry.getProjectsMarkedForDeletionGeneration(), Matchers.equalTo(gen2));
        }

        var unknownProjectId = randomUniqueProjectId();
        var throwingBuilder = ProjectStateRegistry.builder(projectStateRegistry).markProjectForDeletion(unknownProjectId);
        assertThrows(IllegalArgumentException.class, throwingBuilder::build);
    }
}
