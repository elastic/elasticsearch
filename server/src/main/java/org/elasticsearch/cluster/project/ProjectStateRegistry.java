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
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Represents a registry for managing and retrieving project-specific state in the cluster state.
 */
public class ProjectStateRegistry extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "projects_registry";
    public static final ProjectStateRegistry EMPTY = new ProjectStateRegistry(Collections.emptyMap());

    private final Map<ProjectId, Settings> projectsSettings;

    public ProjectStateRegistry(StreamInput in) throws IOException {
        projectsSettings = in.readMap(ProjectId::readFrom, Settings::readSettingsFromStream);
    }

    private ProjectStateRegistry(Map<ProjectId, Settings> projectsSettings) {
        this.projectsSettings = projectsSettings;
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
                return builder.endObject();
            }),
            Iterators.single((builder, p) -> builder.endArray())
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
    }

    public int size() {
        return projectsSettings.size();
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

        private Builder() {
            this.projectsSettings = ImmutableOpenMap.builder();
        }

        private Builder(ProjectStateRegistry original) {
            this.projectsSettings = ImmutableOpenMap.builder(original.projectsSettings);
        }

        public Builder putProjectSettings(ProjectId projectId, Settings settings) {
            projectsSettings.put(projectId, settings);
            return this;
        }

        public ProjectStateRegistry build() {
            return new ProjectStateRegistry(projectsSettings.build());
        }
    }
}
