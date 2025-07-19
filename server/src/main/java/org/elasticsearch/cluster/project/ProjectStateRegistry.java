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
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a registry for managing and retrieving project-specific state in the cluster state.
 */
public class ProjectStateRegistry extends AbstractNamedDiffable<Custom> implements Custom, NamedDiffable<Custom> {
    public static final String TYPE = "projects_registry";
    public static final ProjectStateRegistry EMPTY = new ProjectStateRegistry(Collections.emptyMap(), Collections.emptySet(), 0);
    private static final Entry EMPTY_ENTRY = new Entry(Settings.EMPTY);

    private final Map<ProjectId, Entry> projectsEntries;
    // Projects that have been marked for deletion based on their file-based setting
    private final Set<ProjectId> projectsMarkedForDeletion;
    // A counter that is incremented each time one or more projects are marked for deletion.
    private final long projectsMarkedForDeletionGeneration;

    public ProjectStateRegistry(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_ENTRY)) {
            projectsEntries = in.readMap(ProjectId::readFrom, Entry::readFrom);
        } else {
            Map<ProjectId, Settings> settingsMap = in.readMap(ProjectId::readFrom, Settings::readSettingsFromStream);
            projectsEntries = settingsMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Entry(e.getValue())));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_RECORDS_DELETIONS)) {
            projectsMarkedForDeletion = in.readCollectionAsImmutableSet(ProjectId::readFrom);
            projectsMarkedForDeletionGeneration = in.readVLong();
        } else {
            projectsMarkedForDeletion = Collections.emptySet();
            projectsMarkedForDeletionGeneration = 0;
        }
    }

    private ProjectStateRegistry(
        Map<ProjectId, Entry> projectEntries,
        Set<ProjectId> projectsMarkedForDeletion,
        long projectsMarkedForDeletionGeneration
    ) {
        this.projectsEntries = projectEntries;
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
        return registry.getProjectSettings(projectId);
    }

    public Settings getProjectSettings(ProjectId projectId) {
        return projectsEntries.getOrDefault(projectId, EMPTY_ENTRY).settings;
    }

    public boolean isProjectMarkedForDeletion(ProjectId projectId) {
        return projectsMarkedForDeletion.contains(projectId);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        boolean multiProject = params.paramAsBoolean("multi-project", false);
        if (multiProject == false) {
            return Collections.emptyIterator();
        }

        return Iterators.concat(
            Iterators.single((builder, p) -> builder.startArray("projects")),
            Iterators.map(projectsEntries.entrySet().iterator(), entry -> (builder, p) -> {
                builder.startObject();
                builder.field("id", entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.field("marked_for_deletion", projectsMarkedForDeletion.contains(entry.getKey()));
                return builder.endObject();
            }),
            Iterators.single((builder, p) -> builder.endArray()),
            Iterators.single((builder, p) -> builder.field("projects_marked_for_deletion_generation", projectsMarkedForDeletionGeneration))
        );
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_ENTRY)) {
            return new ProjectStateRegistryDiff(in);
        }
        return readDiffFrom(Custom.class, TYPE, in);
    }

    @Override
    public Diff<Custom> diff(Custom previousState) {
        if (this.equals(previousState)) {
            return SimpleDiffable.empty();
        }
        return new ProjectStateRegistryDiff((ProjectStateRegistry) previousState, this);
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.PROJECT_STATE_REGISTRY_ENTRY)) {
            out.writeMap(projectsEntries);
        } else {
            Map<ProjectId, Settings> settingsMap = projectsEntries.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().settings()));
            out.writeMap(settingsMap);
        }
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
        return projectsEntries.size();
    }

    public long getProjectsMarkedForDeletionGeneration() {
        return projectsMarkedForDeletionGeneration;
    }

    // visible for testing
    Set<ProjectId> knownProjects() {
        return projectsEntries.keySet();
    }

    @Override
    public String toString() {
        return "ProjectStateRegistry["
            + "entities="
            + projectsEntries
            + ", projectsMarkedForDeletion="
            + projectsMarkedForDeletion
            + ", projectsMarkedForDeletionGeneration="
            + projectsMarkedForDeletionGeneration
            + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ProjectStateRegistry == false) return false;
        ProjectStateRegistry that = (ProjectStateRegistry) o;
        return projectsMarkedForDeletionGeneration == that.projectsMarkedForDeletionGeneration
            && Objects.equals(projectsEntries, that.projectsEntries)
            && Objects.equals(projectsMarkedForDeletion, that.projectsMarkedForDeletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectsEntries, projectsMarkedForDeletion, projectsMarkedForDeletionGeneration);
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

    static class ProjectStateRegistryDiff implements NamedDiff<Custom> {
        private static final DiffableUtils.DiffableValueReader<ProjectId, Entry> VALUE_READER = new DiffableUtils.DiffableValueReader<>(
            Entry::readFrom,
            Entry.EntryDiff::readFrom
        );

        private final DiffableUtils.MapDiff<ProjectId, Entry, Map<ProjectId, Entry>> projectsEntriesDiff;
        private final Set<ProjectId> projectsMarkedForDeletion;
        private final long projectsMarkedForDeletionGeneration;

        ProjectStateRegistryDiff(StreamInput in) throws IOException {
            projectsEntriesDiff = DiffableUtils.readJdkMapDiff(in, ProjectId.PROJECT_ID_SERIALIZER, VALUE_READER);
            projectsMarkedForDeletion = in.readCollectionAsImmutableSet(ProjectId.READER);
            projectsMarkedForDeletionGeneration = in.readVLong();
        }

        ProjectStateRegistryDiff(ProjectStateRegistry previousState, ProjectStateRegistry currentState) {
            projectsEntriesDiff = DiffableUtils.diff(
                previousState.projectsEntries,
                currentState.projectsEntries,
                ProjectId.PROJECT_ID_SERIALIZER,
                VALUE_READER
            );
            projectsMarkedForDeletion = currentState.projectsMarkedForDeletion;
            projectsMarkedForDeletionGeneration = currentState.projectsMarkedForDeletionGeneration;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.PROJECT_STATE_REGISTRY_ENTRY;
        }

        @Override
        public Custom apply(Custom part) {
            return new ProjectStateRegistry(
                projectsEntriesDiff.apply(((ProjectStateRegistry) part).projectsEntries),
                projectsMarkedForDeletion,
                projectsMarkedForDeletionGeneration
            );
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            projectsEntriesDiff.writeTo(out);
            out.writeCollection(projectsMarkedForDeletion);
            out.writeVLong(projectsMarkedForDeletionGeneration);
        }
    }

    public static class Builder {
        private final ImmutableOpenMap.Builder<ProjectId, Entry> projectsEntries;
        private final Set<ProjectId> projectsMarkedForDeletion;
        private final long projectsMarkedForDeletionGeneration;
        private boolean newProjectMarkedForDeletion = false;

        private Builder() {
            this.projectsEntries = ImmutableOpenMap.builder();
            projectsMarkedForDeletion = new HashSet<>();
            projectsMarkedForDeletionGeneration = 0;
        }

        private Builder(ProjectStateRegistry original) {
            this.projectsEntries = ImmutableOpenMap.builder(original.projectsEntries);
            this.projectsMarkedForDeletion = new HashSet<>(original.projectsMarkedForDeletion);
            this.projectsMarkedForDeletionGeneration = original.projectsMarkedForDeletionGeneration;
        }

        public Builder putProjectSettings(ProjectId projectId, Settings settings) {
            Entry entry = projectsEntries.get(projectId);
            if (entry == null) {
                entry = new Entry(settings);
            } else {
                entry = entry.withSettings(settings);
            }
            projectsEntries.put(projectId, entry);
            return this;
        }

        public Builder markProjectForDeletion(ProjectId projectId) {
            if (projectsMarkedForDeletion.add(projectId)) {
                newProjectMarkedForDeletion = true;
            }
            return this;
        }

        public ProjectStateRegistry build() {
            final var unknownButUnderDeletion = Sets.difference(projectsMarkedForDeletion, projectsEntries.keys());
            if (unknownButUnderDeletion.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Cannot mark projects for deletion that are not in the registry: " + unknownButUnderDeletion
                );
            }
            return new ProjectStateRegistry(
                projectsEntries.build(),
                projectsMarkedForDeletion,
                newProjectMarkedForDeletion ? projectsMarkedForDeletionGeneration + 1 : projectsMarkedForDeletionGeneration
            );
        }
    }

    private record Entry(Settings settings) implements Writeable, Diffable<Entry> {

        public static Entry readFrom(StreamInput in) throws IOException {
            return new Entry(Settings.readSettingsFromStream(in));
        }

        public Entry withSettings(Settings settings) {
            return new Entry(settings);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeWriteable(settings);
        }

        public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject("settings");
            settings.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
            builder.endObject();
        }

        @Override
        public Diff<Entry> diff(Entry previousState) {
            if (this == previousState) {
                return SimpleDiffable.empty();
            }
            return new EntryDiff(settings.diff(previousState.settings));
        }

        private record EntryDiff(Diff<Settings> settingsDiff) implements Diff<Entry> {
            public static EntryDiff readFrom(StreamInput in) throws IOException {
                return new EntryDiff(Settings.readSettingsDiffFromStream(in));
            }

            @Override
            public Entry apply(Entry part) {
                return part.withSettings(settingsDiff.apply(part.settings));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeWriteable(settingsDiff);
            }
        }
    }
}
