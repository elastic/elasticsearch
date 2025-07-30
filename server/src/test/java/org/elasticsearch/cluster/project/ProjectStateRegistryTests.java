/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.project.ProjectStateRegistrySerializationTests.randomSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

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
        assertThat(gen1, equalTo(projectsUnderDeletion.isEmpty() ? 0L : 1L));

        projectStateRegistry = ProjectStateRegistry.builder(projectStateRegistry).markProjectForDeletion(randomFrom(projects)).build();
        var gen2 = projectStateRegistry.getProjectsMarkedForDeletionGeneration();
        assertThat(gen2, equalTo(gen1 + 1));

        if (projectsUnderDeletion.isEmpty() == false) {
            // re-adding the same projectId should not change the generation
            projectStateRegistry = ProjectStateRegistry.builder(projectStateRegistry)
                .markProjectForDeletion(randomFrom(projectsUnderDeletion))
                .build();
            assertThat(projectStateRegistry.getProjectsMarkedForDeletionGeneration(), equalTo(gen2));
        }

        var unknownProjectId = randomUniqueProjectId();
        var throwingBuilder = ProjectStateRegistry.builder(projectStateRegistry).markProjectForDeletion(unknownProjectId);
        assertThrows(IllegalArgumentException.class, throwingBuilder::build);

        var projectToRemove = randomFrom(projectStateRegistry.knownProjects());
        projectStateRegistry = ProjectStateRegistry.builder(projectStateRegistry).removeProject(projectToRemove).build();
        assertFalse(projectStateRegistry.hasProject(projectToRemove));
        assertFalse(projectStateRegistry.isProjectMarkedForDeletion(projectToRemove));
    }

    public void testDiff() {
        ProjectStateRegistry originalRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(randomUniqueProjectId(), randomSettings())
            .putProjectSettings(randomUniqueProjectId(), randomSettings())
            .putProjectSettings(randomUniqueProjectId(), randomSettings())
            .build();

        ProjectId newProjectId = randomUniqueProjectId();
        Settings newSettings = randomSettings();
        ProjectId projectToMarkForDeletion = randomFrom(originalRegistry.knownProjects());
        ProjectId projectToModifyId = randomFrom(originalRegistry.knownProjects());
        Settings modifiedSettings = randomSettings();

        ProjectStateRegistry modifiedRegistry = ProjectStateRegistry.builder(originalRegistry)
            .putProjectSettings(newProjectId, newSettings)
            .markProjectForDeletion(projectToMarkForDeletion)
            .putProjectSettings(projectToModifyId, modifiedSettings)
            .build();

        var diff = modifiedRegistry.diff(originalRegistry);
        var appliedRegistry = (ProjectStateRegistry) diff.apply(originalRegistry);

        assertThat(appliedRegistry, equalTo(modifiedRegistry));
        assertThat(appliedRegistry.size(), equalTo(originalRegistry.size() + 1));
        assertTrue(appliedRegistry.knownProjects().contains(newProjectId));
        assertTrue(appliedRegistry.isProjectMarkedForDeletion(projectToMarkForDeletion));
        assertThat(appliedRegistry.getProjectSettings(newProjectId), equalTo(newSettings));
        assertThat(appliedRegistry.getProjectSettings(projectToModifyId), equalTo(modifiedSettings));
    }

    public void testDiffNoChanges() {
        ProjectStateRegistry originalRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(randomUniqueProjectId(), randomSettings())
            .build();

        var diff = originalRegistry.diff(originalRegistry);
        var appliedRegistry = (ProjectStateRegistry) diff.apply(originalRegistry);

        assertThat(appliedRegistry, sameInstance(originalRegistry));
    }
}
