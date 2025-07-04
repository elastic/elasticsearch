/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a registry for managing and retrieving project-specific state in the cluster state.
 */
public class ProjectStateRegistry extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "projects_registry";
    public static final ProjectStateRegistry EMPTY = new ProjectStateRegistry(Collections.emptyMap(), Collections.emptySet(), 0);

    private final Map<ProjectId, Settings> projectsSettings;
    // Projects that have been marked for deletion based on their file-based setting
    private final Set<ProjectId> projectsMarkedForDeletion;
    // A counter that is incremented each time one or more projects are marked for deletion.
    private final long projectsMarkedForDeletionGeneration;

    public ProjectStateRegistry(StreamInput in) throws IOException {
        projectsSettings = in.readMap(ProjectId::readFrom, Settings::readSettingsFromStream);
        if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_RECORDS_DELETIONS)) {
            projectsMarkedForDeletion = in.readCollectionAsImmutableSet(ProjectId::readFrom);
            projectsMarkedForDeletionGeneration = in.readVLong();
        } else {
            projectsMarkedForDeletion = Collections.emptySet();
            projectsMarkedForDeletionGeneration = 0;
        }
    }

    private ProjectStateRegistry(
        Map<ProjectId, Settings> projectsSettings,
        Set<ProjectId> projectsMarkedForDeletion,
        long projectsMarkedForDeletionGeneration
    ) {
        this.projectsSettings = projectsSettings;
        this.projectsMarkedForDeletion = projectsMarkedForDeletion;
        this.projectsMarkedForDeletionGeneration = projectsMarkedForDeletionGeneration;
    }

    /**
     * Retrieves the settings for a specific project based on its project ID from the specified cluster state without creating a new object.
     * If you need a full state of the project rather than just its setting, please use {@link ClusterState#projectState(ProjectId)}
     *
     * @param projectId id of the project
     * @param clusterState cluster state
     * @return the settings for the specified project, or an empty settings object if no settings are found
     */
    public static Settings getProjectSettings(ProjectId projectId, ClusterState clusterState) {
        ProjectStateRegistry registry = clusterState.custom(TYPE, EMPTY);
        return registry.projectsSettings.getOrDefault(projectId, Settings.EMPTY);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        boolean multiProject = params.paramAsBoolean("multi-project", false);
        if (multiProject == false) {
            return Collections.emptyIterator();
        }

        return Iterators.concat(
            Iterators.single((builder, p) -> builder.startArray("projects")),
            Iterators.map(projectsSettings.entrySet().iterator(), entry -> (builder, p) -> {
                builder.startObject();
                builder.field("id", entry.getKey());
                builder.startObject("settings");
                entry.getValue().toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
                builder.field("marked_for_deletion", projectsMarkedForDeletion.contains(entry.getKey()));
                return builder.endObject();
            }),
            Iterators.single((builder, p) -> builder.endArray()),
            Iterators.single((builder, p) -> builder.field("projects_marked_for_deletion_generation", projectsMarkedForDeletionGeneration))
        );
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.CLUSTER_STATE_PROJECTS_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(projectsSettings);
        if (out.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_RECORDS_DELETIONS)) {
            out.writeCollection(projectsMarkedForDeletion);
            out.writeVLong(projectsMarkedForDeletionGeneration);
        } else {
            // There should be no deletion unless all MP nodes are at or after PROJECT_STATE_REGISTRY_RECORDS_DELETIONS
            assert projectsMarkedForDeletion.isEmpty();
            assert projectsMarkedForDeletionGeneration == 0;
        }
    }

    public int size() {
        return projectsSettings.size();
    }

    public long getProjectsMarkedForDeletionGeneration() {
        return projectsMarkedForDeletionGeneration;
    }

    // visible for testing
    Map<ProjectId, Settings> getProjectsSettings() {
        return Collections.unmodifiableMap(projectsSettings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ProjectStateRegistry == false) return false;
        ProjectStateRegistry that = (ProjectStateRegistry) o;
        return projectsMarkedForDeletionGeneration == that.projectsMarkedForDeletionGeneration
            && Objects.equals(projectsSettings, that.projectsSettings)
            && Objects.equals(projectsMarkedForDeletion, that.projectsMarkedForDeletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectsSettings, projectsMarkedForDeletion, projectsMarkedForDeletionGeneration);
    }

    public static Builder builder(ClusterState original) {
        ProjectStateRegistry projectRegistry = original.custom(TYPE, EMPTY);
        return builder(projectRegistry);
    }

    public static Builder builder(ProjectStateRegistry projectRegistry) {
        return new Builder(projectRegistry);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ImmutableOpenMap.Builder<ProjectId, Settings> projectsSettings;
        private final Set<ProjectId> projectsMarkedForDeletion;
        private final long projectsMarkedForDeletionGeneration;
        private boolean newProjectMarkedForDeletion = false;

        private Builder() {
            this.projectsSettings = ImmutableOpenMap.builder();
            projectsMarkedForDeletion = new HashSet<>();
            projectsMarkedForDeletionGeneration = 0;
        }

        private Builder(ProjectStateRegistry original) {
            this.projectsSettings = ImmutableOpenMap.builder(original.projectsSettings);
            this.projectsMarkedForDeletion = new HashSet<>(original.projectsMarkedForDeletion);
            this.projectsMarkedForDeletionGeneration = original.projectsMarkedForDeletionGeneration;
        }

        public Builder putProjectSettings(ProjectId projectId, Settings settings) {
            projectsSettings.put(projectId, settings);
            return this;
        }

        public Builder markProjectForDeletion(ProjectId projectId) {
            if (projectsMarkedForDeletion.add(projectId)) {
                newProjectMarkedForDeletion = true;
            }
            return this;
        }

        public ProjectStateRegistry build() {
            final var unknownButUnderDeletion = Sets.difference(projectsMarkedForDeletion, projectsSettings.keys());
            if (unknownButUnderDeletion.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Cannot mark projects for deletion that are not in the registry: " + unknownButUnderDeletion
                );
            }
            return new ProjectStateRegistry(
                projectsSettings.build(),
                projectsMarkedForDeletion,
                newProjectMarkedForDeletion ? projectsMarkedForDeletionGeneration + 1 : projectsMarkedForDeletionGeneration
            );
        }
    }
}
