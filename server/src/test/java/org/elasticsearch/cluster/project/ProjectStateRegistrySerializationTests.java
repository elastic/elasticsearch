/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;

import java.io.IOException;
import java.util.stream.IntStream;

public class ProjectStateRegistrySerializationTests extends SimpleDiffableWireSerializationTestCase<ClusterState.Custom> {

    @Override
    protected ClusterState.Custom makeTestChanges(ClusterState.Custom testInstance) {
        return mutate((ProjectStateRegistry) testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<ClusterState.Custom>> diffReader() {
        return ProjectStateRegistry::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<ClusterState.Custom> instanceReader() {
        return ProjectStateRegistry::new;
    }

    @Override
    protected ClusterState.Custom createTestInstance() {
        return randomProjectStateRegistry();
    }

    @Override
    protected ClusterState.Custom mutateInstance(ClusterState.Custom instance) throws IOException {
        return mutate((ProjectStateRegistry) instance);
    }

    private ProjectStateRegistry mutate(ProjectStateRegistry instance) {
        if (randomBoolean() && instance.size() > 0) {
            // Remove or mutate a project's settings or deletion flag
            var projectId = randomFrom(instance.getProjectsSettings().keySet());
            var builder = ProjectStateRegistry.builder(instance);
            builder.putProjectSettings(projectId, randomSettings());
            if (randomBoolean()) {
                // mark for deletion
                builder.markProjectForDeletion(projectId);
            }
            return builder.build();
        } else {
            // add a new project
            return ProjectStateRegistry.builder(instance).putProjectSettings(randomUniqueProjectId(), randomSettings()).build();
        }
    }

    private static ProjectStateRegistry randomProjectStateRegistry() {
        final var projects = randomSet(1, 5, ESTestCase::randomUniqueProjectId);
        final var projectsUnderDeletion = randomSet(0, 5, ESTestCase::randomUniqueProjectId);
        var builder = ProjectStateRegistry.builder();
        projects.forEach(projectId -> builder.putProjectSettings(projectId, randomSettings()));
        projectsUnderDeletion.forEach(
            projectId -> builder.putProjectSettings(projectId, randomSettings()).markProjectForDeletion(projectId)
        );
        return builder.build();
    }

    public static Settings randomSettings() {
        var builder = Settings.builder();
        IntStream.range(0, randomIntBetween(1, 5)).forEach(i -> builder.put(randomIdentifier(), randomIdentifier()));
        return builder.build();
    }
}
