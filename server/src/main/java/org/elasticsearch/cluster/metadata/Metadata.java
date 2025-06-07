/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.DiffableUtils.MapDiff;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * {@link Metadata} is the part of the {@link ClusterState} which persists across restarts. This persistence is XContent-based, so a
 * round-trip through XContent must be faithful in {@link XContentContext#GATEWAY} context.
 * <p>
 * The details of how this is persisted are covered in {@link org.elasticsearch.gateway.PersistedClusterStateService}.
 * </p>
 */
public class Metadata implements Diffable<Metadata>, ChunkedToXContent {

    private static final Logger logger = LogManager.getLogger(Metadata.class);

    public static final Runnable ON_NEXT_INDEX_FIND_MAPPINGS_NOOP = () -> {};
    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = "_na_";
    // TODO multi-project: verify that usages are really expected to work on the default project only,
    // and that they are not a stop-gap solution to make the tests pass
    public static final ProjectId DEFAULT_PROJECT_ID = ProjectId.DEFAULT;

    public enum XContentContext {
        /* Custom metadata should be returned as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT;

        public static XContentContext from(ToXContent.Params params) {
            return valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));
        }
    }

    /**
     * Indicates that this custom metadata will be returned as part of an API call but will not be persisted
     */
    public static final EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    public static final EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and stored as a part of
     * a snapshot global state, but will not be persisted between node restarts
     */
    public static final EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    /**
     * Indicates that this custom metadata will be returned as part of an API call, stored as a part of
     * a snapshot global state, and will be persisted between node restarts
     */
    public static final EnumSet<XContentContext> ALL_CONTEXTS = EnumSet.allOf(XContentContext.class);

    public interface MetadataCustom<T> extends NamedDiffable<T>, ChunkedToXContent {

        EnumSet<XContentContext> context();

        /**
         * @return true if this custom could be restored from snapshot
         */
        default boolean isRestorable() {
            return context().contains(XContentContext.SNAPSHOT);
        }
    }

    /**
     * Cluster-level custom metadata that persists (via XContent) across restarts.
     * The deserialization method for each implementation must be registered with the {@link NamedXContentRegistry}.
     */
    public interface ClusterCustom extends MetadataCustom<ClusterCustom> {}

    /**
     * Project-level custom metadata that persists (via XContent) across restarts.
     * The deserialization method for each implementation must be registered with the {@link NamedXContentRegistry}.
     */
    public interface ProjectCustom extends MetadataCustom<ProjectCustom> {}

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(
        6,
        "cluster read-only (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only_allow_delete",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(
        13,
        "cluster read-only / allow delete (api)",
        false,
        false,
        true,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String CONTEXT_MODE_API = XContentContext.API.toString();

    public static final String DEDUPLICATED_MAPPINGS_PARAM = "deduplicated_mappings";
    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    private static final NamedDiffableValueSerializer<ClusterCustom> CLUSTER_CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(
        ClusterCustom.class
    );
    private static final NamedDiffableValueSerializer<Metadata.ProjectCustom> PROJECT_CUSTOM_VALUE_SERIALIZER =
        new NamedDiffableValueSerializer<>(Metadata.ProjectCustom.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final NamedDiffableValueSerializer BWC_CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer(MetadataCustom.class) {
        @Override
        public MetadataCustom read(StreamInput in, String key) throws IOException {
            final Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet();
            final Set<String> projectScopedNames = in.namedWriteableRegistry().getReaders(ProjectCustom.class).keySet();
            if (clusterScopedNames.contains(key)) {
                return in.readNamedWriteable(ClusterCustom.class, key);
            } else if (projectScopedNames.contains(key)) {
                return in.readNamedWriteable(ProjectCustom.class, key);
            } else {
                throw new IllegalArgumentException("Unknown custom name [" + key + "]");
            }
        }
    };

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetadata coordinationMetadata;
    private final Map<ProjectId, ProjectMetadata> projectMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    private final ImmutableOpenMap<String, Metadata.ClusterCustom> customs;
    private final ImmutableOpenMap<String, ReservedStateMetadata> reservedStateMetadata;

    private Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        Map<ProjectId, ProjectMetadata> projectMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        Settings settings,
        DiffableStringMap hashesOfConsistentSettings,
        ImmutableOpenMap<String, ClusterCustom> customs,
        ImmutableOpenMap<String, ReservedStateMetadata> reservedStateMetadata
    ) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.projectMetadata = projectMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = settings;
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.customs = customs;
        this.reservedStateMetadata = reservedStateMetadata;
    }

    /**
     * Temporary exception indicating a call to {@link #getSingleProject} when there are multiple projects.
     * This can be used to filter out exceptions due to code not converted over yet.
     * To be removed when {@link #getSingleProject} is removed.
     */
    @FixForMultiProject
    public static class MultiProjectPendingException extends UnsupportedOperationException {
        public MultiProjectPendingException(String message) {
            super(message);
        }
    }

    private boolean isSingleProject() {
        return projectMetadata.size() == 1 && projectMetadata.containsKey(DEFAULT_PROJECT_ID);
    }

    /**
     * TODO: Revisit as part of multi-project (do we need it for BWC APIs / transport versions, or can we remove it)
     * If we keep it, then we need to audit every case in which it is called to ensure they don't need to work in
     * multi-project environments.
     */
    @FixForMultiProject
    private ProjectMetadata getSingleProject() {
        if (projectMetadata.isEmpty()) {
            throw new UnsupportedOperationException("There are no projects");
        } else if (projectMetadata.size() != 1) {
            throw new MultiProjectPendingException("There are multiple projects " + projectMetadata.keySet());
        }
        final ProjectMetadata defaultProject = projectMetadata.get(DEFAULT_PROJECT_ID);
        if (defaultProject == null) {
            throw new UnsupportedOperationException(
                "There is 1 project, but it has id " + projectMetadata.keySet() + " rather than " + DEFAULT_PROJECT_ID
            );
        }
        return defaultProject;
    }

    public Metadata withIncrementedVersion() {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version + 1,
            coordinationMetadata,
            projectMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
    }

    /**
     * Given an index and lifecycle state, returns a metadata where the lifecycle state will be
     * associated with the given index.
     *
     * The passed-in index must already be present in the cluster state, this method cannot
     * be used to add an index.
     *
     * @param index A non-null index
     * @param lifecycleState A non-null lifecycle execution state
     * @return a <code>Metadata</code> instance where the index has the provided lifecycle state
     */
    public Metadata withLifecycleState(Index index, LifecycleExecutionState lifecycleState) {
        return updateSingleProject(project -> project.withLifecycleState(index, lifecycleState));
    }

    public Metadata withCoordinationMetadata(CoordinationMetadata coordinationMetadata) {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            projectMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
    }

    public Metadata withLastCommittedValues(
        boolean clusterUUIDCommitted,
        CoordinationMetadata.VotingConfiguration lastCommittedConfiguration
    ) {
        if (clusterUUIDCommitted == this.clusterUUIDCommitted
            && lastCommittedConfiguration.equals(this.coordinationMetadata.getLastCommittedConfiguration())) {
            return this;
        }
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            CoordinationMetadata.builder(coordinationMetadata).lastCommittedConfiguration(lastCommittedConfiguration).build(),
            projectMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
    }

    /**
     * Creates a copy of this instance with the given {@code index} added.
     * @param index index to add
     * @return copy with added index
     */
    public Metadata withAddedIndex(IndexMetadata index) {
        return updateSingleProject(project -> project.withAddedIndex(index));
    }

    private Metadata updateSingleProject(Function<ProjectMetadata, ProjectMetadata> update) {
        if (projectMetadata.size() == 1) {
            var entry = this.projectMetadata.entrySet().iterator().next();
            var newProject = update.apply(entry.getValue());
            return newProject == entry.getValue()
                ? this
                : new Metadata(
                    clusterUUID,
                    clusterUUIDCommitted,
                    version,
                    coordinationMetadata,
                    Map.of(entry.getKey(), newProject),
                    transientSettings,
                    persistentSettings,
                    settings,
                    hashesOfConsistentSettings,
                    customs,
                    reservedStateMetadata
                );
        } else {
            throw new MultiProjectPendingException("There are multiple projects " + projectMetadata.keySet());
        }
    }

    public long version() {
        return this.version;
    }

    /**
     * @return A UUID which identifies this cluster. Nodes record the UUID of the cluster they first join on disk, and will then refuse to
     * join clusters with different UUIDs. Note that when the cluster is forming for the first time this value may not yet be committed,
     * and therefore it may change. Check {@link #clusterUUIDCommitted()} to verify that the value is committed if needed.
     */
    public String clusterUUID() {
        return this.clusterUUID;
    }

    /**
     * Whether the current node with the given cluster state is locked into the cluster with the UUID returned by {@link #clusterUUID()},
     * meaning that it will not accept any cluster state with a different clusterUUID.
     */
    public boolean clusterUUIDCommitted() {
        return this.clusterUUIDCommitted;
    }

    /**
     * Returns the merged transient and persistent settings.
     */
    public Settings settings() {
        return this.settings;
    }

    public Settings transientSettings() {
        return this.transientSettings;
    }

    public Settings persistentSettings() {
        return this.persistentSettings;
    }

    public Map<String, String> hashesOfConsistentSettings() {
        return this.hashesOfConsistentSettings;
    }

    public CoordinationMetadata coordinationMetadata() {
        return this.coordinationMetadata;
    }

    public Map<ProjectId, ProjectMetadata> projects() {
        return this.projectMetadata;
    }

    public Iterable<IndexMetadata> indicesAllProjects() {
        return () -> Iterators.flatMap(projectMetadata.values().iterator(), ProjectMetadata::iterator);
    }

    /**
     * TODO: Remove as part of multi-project
     */
    @FixForMultiProject
    @Deprecated(forRemoval = true)
    public ProjectMetadata getProject() {
        return getSingleProject();
    }

    @FixForMultiProject(description = "temporarily allow non-multi-project aware code to work with just the default project")
    @Deprecated(forRemoval = true)
    public ProjectMetadata getDefaultProject() {
        return getProject(DEFAULT_PROJECT_ID);
    }

    public boolean hasProject(ProjectId projectId) {
        return projectMetadata.containsKey(projectId);
    }

    public ProjectMetadata getProject(ProjectId projectId) {
        ProjectMetadata metadata = projectMetadata.get(projectId);
        if (metadata == null) {
            throw new IllegalArgumentException("project [" + projectId + "] not found in metadata version [" + this.version() + "]");
        }
        return metadata;
    }

    /**
     * Gets the total number of open shards of all indices across all projects. Includes
     * replicas, but does not include shards that are part of closed indices.
     */
    public int getTotalOpenIndexShards() {
        int shards = 0;
        for (ProjectMetadata project : projects().values()) {
            shards += project.getTotalOpenIndexShards();
        }
        return shards;
    }

    /**
     * Utility method that allows retrieving a {@link ProjectCustom} from a project.
     * Throws an exception when multiple projects have that {@link ProjectCustom}.
     * @return the {@link ProjectCustom} if and only if it's present in a single project. If it's not present in any project, returns null
     */
    @FixForMultiProject
    @Nullable
    public <T extends ProjectCustom> T getSingleProjectCustom(String type) {
        var project = getSingleProjectWithCustom(type);
        return project == null ? null : project.custom(type);
    }

    /**
     * Utility method that allows retrieving a project that has a certain {@link ProjectCustom}.
     * Throws an exception when multiple projects have that {@link ProjectCustom}.
     * @return the project that has the {@link ProjectCustom} if and only if it's present in a single project.
     *         If it's not present in any project, returns null
     */
    @FixForMultiProject
    @Nullable
    public ProjectMetadata getSingleProjectWithCustom(String type) {
        ProjectMetadata resultingProject = null;
        for (ProjectMetadata project : projects().values()) {
            ProjectCustom projectCustom = project.custom(type);
            if (projectCustom == null) {
                continue;
            }
            if (resultingProject != null) {
                throw new UnsupportedOperationException("Multiple custom projects found for type [" + type + "]");
            }
            resultingProject = project;
        }
        return resultingProject;
    }

    /**
     * @return The total number of shards across all projects in this cluster
     */
    public int getTotalNumberOfShards() {
        int shards = 0;
        for (ProjectMetadata project : projects().values()) {
            shards += project.getTotalNumberOfShards();
        }
        return shards;
    }

    /**
     * @return The total number of indices across all projects in this cluster
     */
    public int getTotalNumberOfIndices() {
        int indexCount = 0;
        for (ProjectMetadata project : projects().values()) {
            indexCount += project.indices().size();
        }
        return indexCount;
    }

    /**
     * @return {code true} if there are any indices in any project in this cluster
     */
    public boolean hasAnyIndices() {
        for (ProjectMetadata project : projects().values()) {
            if (project.indices().isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return The oldest {@link IndexVersion} of indices across all projects
     */
    public IndexVersion oldestIndexVersionAllProjects() {
        IndexVersion oldest = IndexVersion.current();
        for (var projectMetadata : projectMetadata.values()) {
            if (oldest.compareTo(projectMetadata.oldestIndexVersion()) > 0) {
                oldest = projectMetadata.oldestIndexVersion();
            }
        }
        return oldest;
    }

    public NodesShutdownMetadata nodeShutdowns() {
        return custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
    }

    public ImmutableOpenMap<String, ClusterCustom> customs() {
        return this.customs;
    }

    /**
     * Returns the full {@link ReservedStateMetadata} Map for all
     * reserved state namespaces.
     * @return a map of namespace to {@link ReservedStateMetadata}
     */
    public Map<String, ReservedStateMetadata> reservedStateMetadata() {
        return this.reservedStateMetadata;
    }

    @SuppressWarnings("unchecked")
    public <T extends ClusterCustom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends ClusterCustom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    public static boolean isGlobalStateEquals(Metadata metadata1, Metadata metadata2) {
        if (metadata1.coordinationMetadata.equals(metadata2.coordinationMetadata) == false) {
            return false;
        }
        if (metadata1.persistentSettings.equals(metadata2.persistentSettings) == false) {
            return false;
        }
        if (metadata1.hashesOfConsistentSettings.equals(metadata2.hashesOfConsistentSettings) == false) {
            return false;
        }
        if (metadata1.clusterUUID.equals(metadata2.clusterUUID) == false) {
            return false;
        }
        if (metadata1.clusterUUIDCommitted != metadata2.clusterUUIDCommitted) {
            return false;
        }
        if (customsEqual(metadata1.customs, metadata2.customs) == false) {
            return false;
        }
        if (Objects.equals(metadata1.reservedStateMetadata, metadata2.reservedStateMetadata) == false) {
            return false;
        }
        if (projectMetadataEqual(metadata1.projectMetadata, metadata2.projectMetadata) == false) {
            return false;
        }
        return true;
    }

    static <C extends MetadataCustom<C>> boolean customsEqual(Map<String, C> customs1, Map<String, C> customs2) {
        // Check if any persistent metadata needs to be saved
        int customCount1 = 0;
        for (Map.Entry<String, C> cursor : customs1.entrySet()) {
            if (cursor.getValue().context().contains(XContentContext.GATEWAY)) {
                if (cursor.getValue().equals(customs2.get(cursor.getKey())) == false) {
                    return false;
                }
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (C custom : customs2.values()) {
            if (custom.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        return customCount1 == customCount2;
    }

    private static boolean projectMetadataEqual(
        Map<ProjectId, ProjectMetadata> projectMetadata1,
        Map<ProjectId, ProjectMetadata> projectMetadata2
    ) {
        if (projectMetadata1.size() != projectMetadata2.size()) {
            return false;
        }
        for (ProjectMetadata project1 : projectMetadata1.values()) {
            var project2 = projectMetadata2.get(project1.id());
            if (project2 == null) {
                return false;
            }
            if (ProjectMetadata.isStateEquals(project1, project2) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? SimpleDiffable.empty() : new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params p) {
        final XContentContext context = XContentContext.from(p);
        final Iterator<? extends ToXContent> start = context == XContentContext.API
            ? ChunkedToXContentHelper.startObject("metadata")
            : Iterators.single((builder, params) -> builder.startObject("meta-data").field("version", version()));

        final Iterator<? extends ToXContent> clusterCoordination = Iterators.single((builder, params) -> {
            builder.field("cluster_uuid", clusterUUID);
            builder.field("cluster_uuid_committed", clusterUUIDCommitted);
            builder.startObject("cluster_coordination");
            coordinationMetadata().toXContent(builder, params);
            return builder.endObject();
        });

        final Iterator<? extends ToXContent> persistentSettings = context != XContentContext.API && persistentSettings().isEmpty() == false
            ? Iterators.single((builder, params) -> {
                builder.startObject("settings");
                persistentSettings().toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                return builder.endObject();
            })
            : Collections.emptyIterator();

        @FixForMultiProject
        // Need to revisit whether this should be a param or something else.
        final boolean multiProject = p.paramAsBoolean("multi-project", false);
        if (multiProject) {
            return toXContentChunkedWithMultiProjectFormat(p, context, start, clusterCoordination, persistentSettings);
        } else {
            return toXContentChunkedWithSingleProjectFormat(p, context, start, clusterCoordination, persistentSettings);
        }
    }

    private Iterator<? extends ToXContent> toXContentChunkedWithMultiProjectFormat(
        ToXContent.Params p,
        XContentContext context,
        Iterator<? extends ToXContent> start,
        Iterator<? extends ToXContent> clusterCoordination,
        Iterator<? extends ToXContent> persistentSettings
    ) {
        return Iterators.concat(
            start,
            clusterCoordination,
            persistentSettings,
            ChunkedToXContentHelper.array(
                "projects",
                Iterators.flatMap(
                    projectMetadata.entrySet().iterator(),
                    e -> Iterators.concat(
                        ChunkedToXContentHelper.startObject(),
                        Iterators.single((builder, params) -> builder.field("id", e.getKey())),
                        e.getValue().toXContentChunked(p),
                        ChunkedToXContentHelper.endObject()
                    )
                )
            ),
            Iterators.flatMap(
                customs.entrySet().iterator(),
                entry -> entry.getValue().context().contains(context)
                    ? ChunkedToXContentHelper.object(entry.getKey(), entry.getValue().toXContentChunked(p))
                    : Collections.emptyIterator()
            ),
            ChunkedToXContentHelper.object("reserved_state", reservedStateMetadata().values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    private Iterator<? extends ToXContent> toXContentChunkedWithSingleProjectFormat(
        ToXContent.Params p,
        XContentContext context,
        Iterator<? extends ToXContent> start,
        Iterator<? extends ToXContent> clusterCoordination,
        Iterator<? extends ToXContent> persistentSettings
    ) {
        if (projectMetadata.size() != 1) {
            throw new MultiProjectPendingException("There are multiple projects " + projectMetadata.keySet());
        }
        // Need to rethink what to do here. This might be right, but maybe not...
        @FixForMultiProject
        final ProjectMetadata project = projectMetadata.values().iterator().next();

        // need to combine reserved state together into a single block so we don't get duplicate keys
        // and not include it in the project xcontent output (through the lack of multi-project params)
        // use a tree map so the order is deterministic
        final Map<String, ReservedStateMetadata> clusterReservedState = new TreeMap<>(reservedStateMetadata);
        clusterReservedState.putAll(project.reservedStateMetadata());

        // Similarly, combine cluster and project persistent tasks and report them under a single key
        Iterator<ToXContent> customs = Iterators.flatMap(customs().entrySet().iterator(), entry -> {
            if (entry.getValue().context().contains(context) && ClusterPersistentTasksCustomMetadata.TYPE.equals(entry.getKey()) == false) {
                return ChunkedToXContentHelper.object(entry.getKey(), entry.getValue().toXContentChunked(p));
            } else {
                return Collections.emptyIterator();
            }
        });
        final var combinedTasks = PersistentTasksCustomMetadata.combine(
            ClusterPersistentTasksCustomMetadata.get(this),
            PersistentTasksCustomMetadata.get(project)
        );
        if (combinedTasks != null) {
            customs = Iterators.concat(
                customs,
                ChunkedToXContentHelper.object(PersistentTasksCustomMetadata.TYPE, combinedTasks.toXContentChunked(p))
            );
        }

        return Iterators.concat(
            start,
            clusterCoordination,
            persistentSettings,
            project.toXContentChunked(p),
            customs,
            ChunkedToXContentHelper.object("reserved_state", clusterReservedState.values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    private static class MetadataDiff implements Diff<Metadata> {

        private final long version;
        private final String clusterUUID;
        private final boolean clusterUUIDCommitted;
        private final CoordinationMetadata coordinationMetadata;
        private final Settings transientSettings;
        private final Settings persistentSettings;
        private final Diff<DiffableStringMap> hashesOfConsistentSettings;
        private final ProjectMetadata.ProjectMetadataDiff singleProject;
        private final MapDiff<ProjectId, ProjectMetadata, Map<ProjectId, ProjectMetadata>> multiProject;
        private final MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>> clusterCustoms;
        private final MapDiff<String, ReservedStateMetadata, ImmutableOpenMap<String, ReservedStateMetadata>> reservedStateMetadata;

        /**
         * true if this diff is a noop because before and after were the same instance
         */
        private final boolean empty;
        /**
         * true if this diff is read from an old node that does not know about multi-project
         */
        private final boolean fromNodeBeforeMultiProjectsSupport;
        // A combined diff for both cluster and project scoped persistent tasks and packaged as project scoped ones.
        // This is used only when the node has a single project and needs to send the diff to an old node (wire BWC).
        private final MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> combinedTasksDiff;

        MetadataDiff(Metadata before, Metadata after) {
            this.empty = before == after;
            this.fromNodeBeforeMultiProjectsSupport = false; // diff on this node, always after multi-projects, even when disabled
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            if (before.isSingleProject() && after.isSingleProject()) {
                // single-project, just handle the project metadata diff itself
                singleProject = after.getSingleProject().diff(before.getSingleProject());
                multiProject = null;
            } else {
                singleProject = null;
                multiProject = DiffableUtils.diff(before.projectMetadata, after.projectMetadata, ProjectId.PROJECT_ID_SERIALIZER);
            }

            if (empty) {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
                clusterCustoms = DiffableUtils.emptyDiff();
                reservedStateMetadata = DiffableUtils.emptyDiff();
                combinedTasksDiff = null; // identical metadata, no need for combined tasks diff.
            } else {
                // If only has a single project, we need to prepare a combined diff for both cluster and project
                // scoped persistent tasks so that it is compatible with the old node in case wire BWC is needed.
                if (singleProject != null) {
                    final var beforeTasks = PersistentTasksCustomMetadata.combine(
                        before.custom(ClusterPersistentTasksCustomMetadata.TYPE),
                        before.getSingleProject().custom(PersistentTasksCustomMetadata.TYPE)
                    );
                    final var afterTasks = PersistentTasksCustomMetadata.combine(
                        after.custom(ClusterPersistentTasksCustomMetadata.TYPE),
                        after.getSingleProject().custom(PersistentTasksCustomMetadata.TYPE)
                    );

                    if (beforeTasks == null && afterTasks == null) {
                        combinedTasksDiff = null;
                    } else if (beforeTasks == null) {
                        combinedTasksDiff = DiffableUtils.singleUpsertDiff(
                            PersistentTasksCustomMetadata.TYPE,
                            afterTasks,
                            DiffableUtils.getStringKeySerializer()
                        );
                    } else if (afterTasks == null) {
                        combinedTasksDiff = DiffableUtils.singleDeleteDiff(
                            PersistentTasksCustomMetadata.TYPE,
                            DiffableUtils.getStringKeySerializer()
                        );
                    } else {
                        combinedTasksDiff = DiffableUtils.singleEntryDiff(
                            PersistentTasksCustomMetadata.TYPE,
                            afterTasks.diff(beforeTasks),
                            DiffableUtils.getStringKeySerializer()
                        );
                    }
                } else {
                    combinedTasksDiff = null; // Metadata with multi-projects can never be sent to old node, no need for special handling
                }
                hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
                clusterCustoms = DiffableUtils.diff(
                    before.customs,
                    after.customs,
                    DiffableUtils.getStringKeySerializer(),
                    CLUSTER_CUSTOM_VALUE_SERIALIZER
                );
                reservedStateMetadata = DiffableUtils.diff(
                    before.reservedStateMetadata,
                    after.reservedStateMetadata,
                    DiffableUtils.getStringKeySerializer()
                );
            }
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, ReservedStateMetadata> RESERVED_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(ReservedStateMetadata::readFrom, ReservedStateMetadata::readDiffFrom);

        private MetadataDiff(StreamInput in) throws IOException {
            empty = false;
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            // We don't need combined diff for persistent tasks when the whole diff is read from wire because:
            // (1) If the diff is read from an old node, it is already combined.
            // (2) If the diff is read from a new node, multiProject != null, which prevents it from being sent to old nodes.
            combinedTasksDiff = null;
            if (in.getTransportVersion().before(TransportVersions.MULTI_PROJECT)) {
                fromNodeBeforeMultiProjectsSupport = true;
                var indices = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    INDEX_METADATA_DIFF_VALUE_READER
                );
                var templates = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    TEMPLATES_DIFF_VALUE_READER
                );

                // Read customs and split them into cluster and project ones. Note that the persistent tasks need further handling
                // since they are returned as part of project customs at this point.
                var bwcCustoms = readBwcCustoms(in);
                clusterCustoms = bwcCustoms.v1();
                var projectCustoms = bwcCustoms.v2();

                // on a single-project install, all reserved state metadata is stored in Metadata rather than ProjectMetadata
                reservedStateMetadata = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    RESERVED_DIFF_VALUE_READER
                );

                singleProject = new ProjectMetadata.ProjectMetadataDiff(indices, templates, projectCustoms, DiffableUtils.emptyDiff());
                multiProject = null;
            } else {
                fromNodeBeforeMultiProjectsSupport = false;
                // Repositories metadata is sent as Metadata#customs diff from old node. We need to
                // 1. Split it from the Metadata#customs diff
                // 2. Merge it into the default project's ProjectMetadataDiff
                final var bwcCustoms = maybeReadBwcCustoms(in);
                clusterCustoms = bwcCustoms.v1();
                final var defaultProjectCustoms = bwcCustoms.v2();

                reservedStateMetadata = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    RESERVED_DIFF_VALUE_READER
                );

                singleProject = null;
                multiProject = readMultiProjectDiffs(in, defaultProjectCustoms);
            }
        }

        private static
            Tuple<
                MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>>,
                MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>>>
            maybeReadBwcCustoms(StreamInput in) throws IOException {
            if (in.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)) {
                return readBwcCustoms(in);
            } else {
                return new Tuple<>(
                    DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CLUSTER_CUSTOM_VALUE_SERIALIZER),
                    null
                );
            }
        }

        @SuppressWarnings("unchecked")
        private static MapDiff<ProjectId, ProjectMetadata, Map<ProjectId, ProjectMetadata>> readMultiProjectDiffs(
            StreamInput in,
            MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> defaultProjectCustoms
        ) throws IOException {
            final var multiProject = DiffableUtils.readJdkMapDiff(
                in,
                ProjectId.PROJECT_ID_SERIALIZER,
                ProjectMetadata::readFrom,
                ProjectMetadata.ProjectMetadataDiff::new
            );

            // If the defaultProjectCustoms has content, the diff is read from an old node. We need to merge it into the
            // default project's ProjectMetadataDiff
            if (defaultProjectCustoms != null && defaultProjectCustoms.isEmpty() == false) {
                return DiffableUtils.updateDiffsAndUpserts(multiProject, ProjectId.DEFAULT::equals, (k, v) -> {
                    assert ProjectId.DEFAULT.equals(k) : k;
                    assert v instanceof ProjectMetadata.ProjectMetadataDiff : v;
                    final var projectMetadataDiff = (ProjectMetadata.ProjectMetadataDiff) v;
                    return projectMetadataDiff.withCustoms(
                        DiffableUtils.merge(
                            projectMetadataDiff.customs(),
                            defaultProjectCustoms,
                            DiffableUtils.getStringKeySerializer(),
                            BWC_CUSTOM_VALUE_SERIALIZER
                        )
                    );
                }, (k, v) -> {
                    assert ProjectId.DEFAULT.equals(k) : k;
                    return ProjectMetadata.builder(v).clearCustoms().customs(defaultProjectCustoms.apply(v.customs())).build();
                });
            } else {
                return multiProject;
            }
        }

        @SuppressWarnings("unchecked")
        private static
            Tuple<
                MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>>,
                MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>>>
            readBwcCustoms(StreamInput in) throws IOException {
            MapDiff<String, MetadataCustom<?>, ImmutableOpenMap<String, MetadataCustom<?>>> customs = DiffableUtils
                .readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), BWC_CUSTOM_VALUE_SERIALIZER);
            return DiffableUtils.split(
                customs,
                in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet()::contains,
                CLUSTER_CUSTOM_VALUE_SERIALIZER,
                in.namedWriteableRegistry().getReaders(ProjectCustom.class).keySet()::contains,
                PROJECT_CUSTOM_VALUE_SERIALIZER
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(empty);
            if (empty) {
                // noop diff
                return;
            }
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            transientSettings.writeTo(out);
            persistentSettings.writeTo(out);
            hashesOfConsistentSettings.writeTo(out);
            if (out.getTransportVersion().before(TransportVersions.MULTI_PROJECT)) {
                // there's only ever a single project with pre-multi-project
                if (multiProject != null) {
                    throw new UnsupportedOperationException(
                        "Trying to serialize a multi-project diff with a single-project serialization version"
                    );
                }
                singleProject.indices().writeTo(out);
                singleProject.templates().writeTo(out);
                buildUnifiedCustomDiff().writeTo(out);
                buildUnifiedReservedStateMetadataDiff().writeTo(out);
            } else {
                final var multiProjectToWrite = multiProject != null
                    ? multiProject
                    : DiffableUtils.singleEntryDiff(DEFAULT_PROJECT_ID, singleProject, ProjectId.PROJECT_ID_SERIALIZER);

                if (out.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)) {
                    writeDiffWithRepositoriesMetadataAsClusterCustom(out, clusterCustoms, multiProjectToWrite, reservedStateMetadata);
                } else {
                    clusterCustoms.writeTo(out);
                    reservedStateMetadata.writeTo(out);
                    multiProjectToWrite.writeTo(out);
                }
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private static void writeDiffWithRepositoriesMetadataAsClusterCustom(
            StreamOutput out,
            MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>> clusterCustoms,
            MapDiff<ProjectId, ProjectMetadata, Map<ProjectId, ProjectMetadata>> multiProject,
            MapDiff<String, ReservedStateMetadata, ImmutableOpenMap<String, ReservedStateMetadata>> reservedStateMetadata
        ) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)
                && out.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM) : out.getTransportVersion();

            // For old nodes, RepositoriesMetadata needs to be sent as a cluster custom. This is possible when (a) the repositories
            // are defined only for the default project or (b) no repositories at all. What we need to do are:
            // 1. Iterate through the multi-project's MapDiff to extract the RepositoriesMetadata of the default project
            // 2. Throws if any repositories are found for non-default projects
            // 3. Merge default project's RepositoriesMetadata into Metadata#customs
            final var combineClustersCustoms = new SetOnce<MapDiff<String, MetadataCustom, Map<String, MetadataCustom>>>();
            final var updatedMultiProject = DiffableUtils.updateDiffsAndUpserts(multiProject, ignore -> true, (k, v) -> {
                assert v instanceof ProjectMetadata.ProjectMetadataDiff : v;
                final var projectMetadataDiff = (ProjectMetadata.ProjectMetadataDiff) v;
                final var bwcCustoms = DiffableUtils.split(
                    projectMetadataDiff.customs(),
                    RepositoriesMetadata.TYPE::equals,
                    PROJECT_CUSTOM_VALUE_SERIALIZER,
                    type -> RepositoriesMetadata.TYPE.equals(type) == false,
                    PROJECT_CUSTOM_VALUE_SERIALIZER
                );
                // Simply return if RepositoriesMetadata is not found
                if (bwcCustoms.v1().isEmpty()) {
                    return projectMetadataDiff;
                }
                // RepositoriesMetadata can only be defined for the default project. Otherwise throw exception.
                if (ProjectId.DEFAULT.equals(k) == false) {
                    throwForVersionBeforeRepositoriesMetadataMigration(out);
                }
                // RepositoriesMetadata is found for the default project as a diff, merge it into the Metadata#customs
                combineClustersCustoms.set(
                    DiffableUtils.<String, MetadataCustom, ClusterCustom, ProjectCustom, Map<String, MetadataCustom>>merge(
                        clusterCustoms,
                        bwcCustoms.v1(),
                        DiffableUtils.getStringKeySerializer()
                    )
                );
                return projectMetadataDiff.withCustoms(bwcCustoms.v2());
            }, (k, v) -> {
                final ProjectCustom projectCustom = v.customs().get(RepositoriesMetadata.TYPE);
                // Simply return if RepositoriesMetadata is not found
                if (projectCustom == null) {
                    return v;
                }
                // RepositoriesMetadata can only be defined for the default project. Otherwise throw exception.
                if (ProjectId.DEFAULT.equals(k) == false) {
                    throwForVersionBeforeRepositoriesMetadataMigration(out);
                }
                // RepositoriesMetadata found for the default project as an upsert, package it as MapDiff and merge into Metadata#customs
                combineClustersCustoms.set(
                    DiffableUtils.<String, MetadataCustom, ClusterCustom, ProjectCustom, Map<String, MetadataCustom>>merge(
                        clusterCustoms,
                        DiffableUtils.singleUpsertDiff(RepositoriesMetadata.TYPE, projectCustom, DiffableUtils.getStringKeySerializer()),
                        DiffableUtils.getStringKeySerializer()
                    )
                );
                return ProjectMetadata.builder(v).removeCustom(RepositoriesMetadata.TYPE).build();
            });

            if (combineClustersCustoms.get() != null) {
                combineClustersCustoms.get().writeTo(out);
            } else {
                clusterCustoms.writeTo(out);
            }

            reservedStateMetadata.writeTo(out);
            updatedMultiProject.writeTo(out);
        }

        private static void throwForVersionBeforeRepositoriesMetadataMigration(StreamOutput out) {
            assert out.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM) : out.getTransportVersion();
            throw new UnsupportedOperationException(
                "Serialize a diff with repositories defined for multiple projects requires version on or after ["
                    + TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM
                    + "], but got ["
                    + out.getTransportVersion()
                    + "]"
            );
        }

        @SuppressWarnings("unchecked")
        private Diff<ImmutableOpenMap<String, ?>> buildUnifiedCustomDiff() {
            assert multiProject == null : "should only be used for single project metadata";
            // First merge the cluster customs and project customs
            final var mergedClusterAndProjectCustomDiff = DiffableUtils.merge(
                clusterCustoms,
                singleProject.customs(),
                DiffableUtils.getStringKeySerializer(),
                BWC_CUSTOM_VALUE_SERIALIZER
            );
            if (combinedTasksDiff == null) {
                // No combined diff means either (1) no tasks are involved or (2) the diff is from an old node
                // In both cases, we can proceed without further changes.
                // For both cases, no cluster persistent tasks should exist in the merged diff
                assert DiffableUtils.hasKey(mergedClusterAndProjectCustomDiff, ClusterPersistentTasksCustomMetadata.TYPE) == false;
                // Unless it is (2), the merge diff should not contain project persistent tasks
                assert fromNodeBeforeMultiProjectsSupport
                    || DiffableUtils.hasKey(mergedClusterAndProjectCustomDiff, PersistentTasksCustomMetadata.TYPE) == false;
                return mergedClusterAndProjectCustomDiff;
            } else {
                // We need first delete the persistent tasks entries from the diffs by cluster and project customs themselves.
                // Then add the combined tasks diff to the result.
                return DiffableUtils.merge(
                    DiffableUtils.removeKeys(
                        mergedClusterAndProjectCustomDiff,
                        Set.of(PersistentTasksCustomMetadata.TYPE, ClusterPersistentTasksCustomMetadata.TYPE)
                    ),
                    combinedTasksDiff,
                    DiffableUtils.getStringKeySerializer(),
                    BWC_CUSTOM_VALUE_SERIALIZER
                );
            }
        }

        private Diff<Map<String, ReservedStateMetadata>> buildUnifiedReservedStateMetadataDiff() {
            return DiffableUtils.merge(
                reservedStateMetadata,
                singleProject.reservedStateMetadata(),
                DiffableUtils.getStringKeySerializer(),
                RESERVED_DIFF_VALUE_READER
            );
        }

        @Override
        public Metadata apply(Metadata part) {
            if (empty) {
                return part;
            }
            // create builder from existing mappings hashes so we don't change existing index metadata instances when deduplicating
            // mappings in the builder
            Builder builder = new Builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            if (singleProject != null) {
                // should only be applied to single projects
                if (part.isSingleProject() == false) {
                    throw new UnsupportedOperationException("Trying to apply a single-project diff to a multi-project metadata");
                }
                // This diff is either (1) read from an old node or (2) because this node only has a single project
                if (fromNodeBeforeMultiProjectsSupport) {
                    // If this diff is read from an old node, it has combination between cluster and project tasks.
                    // So we need to combine the tasks first for the diff to apply.
                    final var combinedTasksBefore = PersistentTasksCustomMetadata.combine(
                        part.custom(ClusterPersistentTasksCustomMetadata.TYPE),
                        part.getSingleProject().custom(PersistentTasksCustomMetadata.TYPE)
                    );
                    // Apply the diff to get the new project metadata with combined tasks
                    final ProjectMetadata projectWithCombinedTasks;
                    if (combinedTasksBefore == null) {
                        projectWithCombinedTasks = singleProject.apply(part.getSingleProject());
                    } else {
                        projectWithCombinedTasks = singleProject.apply(
                            ProjectMetadata.builder(part.getSingleProject())
                                .putCustom(PersistentTasksCustomMetadata.TYPE, combinedTasksBefore)
                                .build()
                        );
                    }
                    final var combinedTasksAfter = (PersistentTasksCustomMetadata) projectWithCombinedTasks.custom(
                        PersistentTasksCustomMetadata.TYPE
                    );
                    if (combinedTasksAfter == null) {
                        builder.projectMetadata(Map.of(DEFAULT_PROJECT_ID, projectWithCombinedTasks));
                        builder.customs(clusterCustoms.apply(part.customs));
                    } else {
                        // Now split the combined tasks back to cluster and project scoped so that they can be stored separately
                        final var tuple = combinedTasksAfter.split();
                        builder.projectMetadata(
                            Map.of(
                                DEFAULT_PROJECT_ID,
                                ProjectMetadata.builder(projectWithCombinedTasks)
                                    .putCustom(PersistentTasksCustomMetadata.TYPE, tuple.v2())
                                    .build()
                            )
                        );
                        // For cluster customs, we need to apply the diff (contains no info for tasks), then put the persistent tasks
                        // from the split cluster scoped one
                        final var clusterCustomsBuilder = ImmutableOpenMap.builder(clusterCustoms.apply(part.customs));
                        clusterCustomsBuilder.put(ClusterPersistentTasksCustomMetadata.TYPE, tuple.v1());
                        builder.customs(clusterCustomsBuilder.build());
                    }
                } else {
                    builder.customs(clusterCustoms.apply(part.customs));
                    builder.projectMetadata(Map.of(DEFAULT_PROJECT_ID, singleProject.apply(part.getSingleProject())));
                }
            } else {
                assert fromNodeBeforeMultiProjectsSupport == false;
                builder.customs(clusterCustoms.apply(part.customs));
                builder.projectMetadata(multiProject.apply(part.projectMetadata));
            }
            builder.put(reservedStateMetadata.apply(part.reservedStateMetadata));
            return builder.build(true);
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version(in.readLong());
        builder.clusterUUID(in.readString());
        builder.clusterUUIDCommitted(in.readBoolean());
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        if (in.getTransportVersion().before(TransportVersions.MULTI_PROJECT)) {
            final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(ProjectId.DEFAULT);
            builder.put(projectBuilder);
            final Function<String, MappingMetadata> mappingLookup;
            final Map<String, MappingMetadata> mappingMetadataMap = in.readMapValues(MappingMetadata::new, MappingMetadata::getSha256);
            if (mappingMetadataMap.isEmpty() == false) {
                mappingLookup = mappingMetadataMap::get;
            } else {
                mappingLookup = null;
            }
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                projectBuilder.put(IndexMetadata.readFrom(in, mappingLookup), false);
            }
            size = in.readVInt();
            for (int i = 0; i < size; i++) {
                projectBuilder.put(IndexTemplateMetadata.readFrom(in));
            }
            readBwcCustoms(in, builder);

            int reservedStateSize = in.readVInt();
            for (int i = 0; i < reservedStateSize; i++) {
                builder.put(ReservedStateMetadata.readFrom(in));
            }
        } else {
            List<ProjectCustom> defaultProjectCustoms = List.of();
            if (in.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)) {
                // Extract the default project's repositories metadata from the Metadata#customs from an old node
                defaultProjectCustoms = new ArrayList<>();
                readBwcCustoms(in, builder, defaultProjectCustoms::add);
                assert defaultProjectCustoms.size() <= 1
                    : "expect only a single default project custom for repository metadata, but got "
                        + defaultProjectCustoms.stream().map(ProjectCustom::getWriteableName).toList();
            } else {
                readClusterCustoms(in, builder);
            }

            int reservedStateSize = in.readVInt();
            for (int i = 0; i < reservedStateSize; i++) {
                builder.put(ReservedStateMetadata.readFrom(in));
            }

            builder.projectMetadata(in.readMap(ProjectId::readFrom, ProjectMetadata::readFrom));
            defaultProjectCustoms.forEach(c -> builder.getProject(ProjectId.DEFAULT).putCustom(c.getWriteableName(), c));
        }
        return builder.build();
    }

    private static void readBwcCustoms(StreamInput in, Builder builder) throws IOException {
        readBwcCustoms(in, builder, projectCustom -> builder.putProjectCustom(projectCustom.getWriteableName(), projectCustom));
    }

    private static void readBwcCustoms(StreamInput in, Builder builder, Consumer<ProjectCustom> projectCustomConsumer) throws IOException {
        final Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet();
        final Set<String> projectScopedNames = in.namedWriteableRegistry().getReaders(ProjectCustom.class).keySet();
        final int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            final String name = in.readString();
            if (clusterScopedNames.contains(name)) {
                final ClusterCustom custom = in.readNamedWriteable(ClusterCustom.class, name);
                builder.putCustom(custom.getWriteableName(), custom);
            } else if (projectScopedNames.contains(name)) {
                final ProjectCustom custom = in.readNamedWriteable(ProjectCustom.class, name);
                // Persistent tasks from an old node are serialized with the project scoped class. We split them into cluster and project
                // scoped ones and store them separately.
                if (custom instanceof PersistentTasksCustomMetadata persistentTasksCustomMetadata) {
                    final var tuple = persistentTasksCustomMetadata.split();
                    builder.putCustom(tuple.v1().getWriteableName(), tuple.v1());
                    projectCustomConsumer.accept(tuple.v2());
                } else {
                    projectCustomConsumer.accept(custom);
                }
            } else {
                throw new IllegalArgumentException("Unknown custom name [" + name + "]");
            }
        }
    }

    private static void readClusterCustoms(StreamInput in, Builder builder) throws IOException {
        Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet();
        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            final String name = in.readString();
            if (clusterScopedNames.contains(name)) {
                ClusterCustom custom = in.readNamedWriteable(ClusterCustom.class, name);
                builder.putCustom(custom.getWriteableName(), custom);
            } else {
                throw new IllegalArgumentException("Unknown cluster custom name [" + name + "]");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        transientSettings.writeTo(out);
        persistentSettings.writeTo(out);
        hashesOfConsistentSettings.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.MULTI_PROJECT)) {
            ProjectMetadata singleProject = getSingleProject();
            out.writeMapValues(singleProject.getMappingsByHash());
            out.writeVInt(singleProject.size());
            for (IndexMetadata indexMetadata : singleProject) {
                indexMetadata.writeTo(out, true);
            }
            out.writeCollection(singleProject.templates().values());

            // It would be nice to do this as flattening iterable (rather than allocation a whole new list), but flattening
            // Iterable<? extends VersionNamedWriteable> into Iterable<VersionNamedWriteable> is messy, so we can fix that later
            List<VersionedNamedWriteable> combinedCustoms = new ArrayList<>(customs.size() + singleProject.customs().size());
            // Old node expects persistent tasks to be a single custom. So we merge cluster and project scoped tasks into one single
            // custom for serialization so that old node understands it.
            final var persistentTasksCustomMetadata = PersistentTasksCustomMetadata.combine(
                ClusterPersistentTasksCustomMetadata.get(this),
                PersistentTasksCustomMetadata.get(singleProject)
            );
            if (persistentTasksCustomMetadata != null) {
                combinedCustoms.add(persistentTasksCustomMetadata);
            }
            combinedCustoms.addAll(
                customs.values().stream().filter(c -> c instanceof ClusterPersistentTasksCustomMetadata == false).toList()
            );
            combinedCustoms.addAll(
                singleProject.customs().values().stream().filter(c -> c instanceof PersistentTasksCustomMetadata == false).toList()
            );
            VersionedNamedWriteable.writeVersionedWriteables(out, combinedCustoms);

            List<ReservedStateMetadata> combinedMetadata = new ArrayList<>(
                reservedStateMetadata.size() + singleProject.reservedStateMetadata().size()
            );
            combinedMetadata.addAll(reservedStateMetadata.values());
            combinedMetadata.addAll(singleProject.reservedStateMetadata().values());
            out.writeCollection(combinedMetadata);
        } else {
            if (out.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)) {
                if (isSingleProject() || hasNoNonDefaultProjectRepositories(projects().values())) {
                    // Repositories metadata must be sent as Metadata#customs for old nodes
                    final List<VersionedNamedWriteable> combinedCustoms = new ArrayList<>(customs.size() + 1);
                    combinedCustoms.addAll(customs.values());
                    final ProjectCustom custom = getProject(ProjectId.DEFAULT).custom(RepositoriesMetadata.TYPE);
                    if (custom != null) {
                        combinedCustoms.add(custom);
                    }
                    VersionedNamedWriteable.writeVersionedWriteables(out, combinedCustoms);
                } else {
                    throw new UnsupportedOperationException(
                        "Serialize metadata with repositories defined for multiple projects requires version on or after ["
                            + TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM
                            + "], but got ["
                            + out.getTransportVersion()
                            + "]"
                    );
                }
            } else {
                VersionedNamedWriteable.writeVersionedWriteables(out, customs.values());
            }

            out.writeCollection(reservedStateMetadata.values());
            out.writeMap(projectMetadata, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
        }
    }

    /**
     * @return {@code true} iff no repositories are defined for non-default-projects.
     */
    private static boolean hasNoNonDefaultProjectRepositories(Collection<ProjectMetadata> projects) {
        return projects.stream()
            .allMatch(project -> ProjectId.DEFAULT.equals(project.id()) || project.custom(RepositoriesMetadata.TYPE) == null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    public Metadata copyAndUpdate(Consumer<Builder> updater) {
        var builder = builder(this);
        updater.accept(builder);
        return builder.build();
    }

    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.EMPTY;
        private Settings persistentSettings = Settings.EMPTY;
        private DiffableStringMap hashesOfConsistentSettings = DiffableStringMap.EMPTY;

        private final ImmutableOpenMap.Builder<String, ClusterCustom> customs;

        /**
         * TODO: This should map to {@link ProjectMetadata} (not Builder), but that's tricky to do due to
         * legacy delegation methods such as {@link #indices(Map)} which expect to have a mutable project
         */
        private final Map<ProjectId, ProjectMetadata.Builder> projectMetadata;

        private final ImmutableOpenMap.Builder<String, ReservedStateMetadata> reservedStateMetadata;

        @SuppressWarnings("this-escape")
        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            customs = ImmutableOpenMap.builder();
            projectMetadata = new HashMap<>();
            reservedStateMetadata = ImmutableOpenMap.builder();
        }

        Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.hashesOfConsistentSettings = metadata.hashesOfConsistentSettings;
            this.version = metadata.version;
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.projectMetadata = Maps.transformValues(metadata.projectMetadata, ProjectMetadata::builder);
            this.reservedStateMetadata = ImmutableOpenMap.builder(metadata.reservedStateMetadata);
        }

        private ProjectMetadata.Builder getSingleProject() {
            if (projectMetadata.isEmpty()) {
                createDefaultProject();
            } else if (projectMetadata.size() != 1) {
                throw new MultiProjectPendingException("There are multiple projects " + projectMetadata.keySet());
            }
            return projectMetadata.values().iterator().next();
        }

        public Builder projectMetadata(Map<ProjectId, ProjectMetadata> projectMetadata) {
            assert projectMetadata.entrySet().stream().allMatch(e -> e.getValue().id().equals(e.getKey()))
                : "Project metadata map is inconsistent";
            this.projectMetadata.clear();
            projectMetadata.forEach((k, v) -> this.projectMetadata.put(k, ProjectMetadata.builder(v)));
            return this;
        }

        public Builder put(ProjectMetadata projectMetadata) {
            return put(ProjectMetadata.builder(projectMetadata));
        }

        public Builder put(ProjectMetadata.Builder projectMetadata) {
            this.projectMetadata.put(projectMetadata.getId(), projectMetadata);
            return this;
        }

        public Builder removeProject(ProjectId projectId) {
            this.projectMetadata.remove(projectId);
            return this;
        }

        public ProjectMetadata.Builder getProject(ProjectId projectId) {
            return projectMetadata.get(projectId);
        }

        public Builder forEachProject(UnaryOperator<ProjectMetadata.Builder> modifier) {
            projectMetadata.replaceAll((p, b) -> modifier.apply(b));
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            getSingleProject().put(indexMetadataBuilder);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            getSingleProject().put(indexMetadata, incrementVersion);
            return this;
        }

        @Deprecated(forRemoval = true)
        public IndexMetadata get(String index) {
            return getSingleProject().get(index);
        }

        @Deprecated(forRemoval = true)
        public IndexMetadata getSafe(Index index) {
            return getSingleProject().getSafe(index);
        }

        @Deprecated(forRemoval = true)
        public Builder remove(String index) {
            getSingleProject().remove(index);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeAllIndices() {
            getSingleProject().removeAllIndices();
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder indices(Map<String, IndexMetadata> indices) {
            getSingleProject().indices(indices);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(IndexTemplateMetadata.Builder template) {
            getSingleProject().put(template);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(IndexTemplateMetadata template) {
            getSingleProject().put(template);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeTemplate(String templateName) {
            getSingleProject().removeTemplate(templateName);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            getSingleProject().templates(templates);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(String name, ComponentTemplate componentTemplate) {
            getSingleProject().put(name, componentTemplate);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeComponentTemplate(String name) {
            getSingleProject().removeComponentTemplate(name);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            getSingleProject().componentTemplates(componentTemplates);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            getSingleProject().indexTemplates(indexTemplates);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            getSingleProject().put(name, indexTemplate);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeIndexTemplate(String name) {
            getSingleProject().removeIndexTemplate(name);
            return this;
        }

        @Deprecated(forRemoval = true)
        public DataStream dataStream(String dataStreamName) {
            return getSingleProject().dataStream(dataStreamName);
        }

        @Deprecated(forRemoval = true)
        public Builder dataStreams(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            getSingleProject().dataStreams(dataStreams, dataStreamAliases);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder put(DataStream dataStream) {
            getSingleProject().put(dataStream);
            return this;
        }

        @Deprecated(forRemoval = true)
        public DataStreamMetadata dataStreamMetadata() {
            return getSingleProject().dataStreamMetadata();
        }

        @Deprecated(forRemoval = true)
        public boolean put(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
            return getSingleProject().put(aliasName, dataStream, isWriteDataStream, filter);
        }

        @Deprecated(forRemoval = true)
        public Builder removeDataStream(String name) {
            getSingleProject().removeDataStream(name);
            return this;
        }

        @Deprecated(forRemoval = true)
        public boolean removeDataStreamAlias(String aliasName, String dataStreamName, boolean mustExist) {
            return getSingleProject().removeDataStreamAlias(aliasName, dataStreamName, mustExist);
        }

        public Builder putCustom(String type, ClusterCustom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder putCustom(String type, ProjectCustom custom) {
            return putProjectCustom(type, custom);
        }

        public ClusterCustom getCustom(String type) {
            return customs.get(type);
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder removeCustomIf(BiPredicate<String, ? super ClusterCustom> p) {
            customs.removeAll(p);
            return this;
        }

        public Builder customs(Map<String, ClusterCustom> clusterCustoms) {
            clusterCustoms.forEach((key, value) -> Objects.requireNonNull(value, key));
            customs.putAllFromMap(clusterCustoms);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder putProjectCustom(String type, ProjectCustom custom) {
            getSingleProject().putCustom(type, Objects.requireNonNull(custom, type));
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeProjectCustom(String type) {
            getSingleProject().removeCustom(type);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeProjectCustomIf(BiPredicate<String, ? super ProjectCustom> p) {
            getSingleProject().removeCustomIf(p);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder projectCustoms(Map<String, ProjectCustom> projectCustoms) {
            projectCustoms.forEach((key, value) -> Objects.requireNonNull(value, key));
            getSingleProject().customs(projectCustoms);
            return this;
        }

        /**
         * Adds a map of namespace to {@link ReservedStateMetadata} into the metadata builder
         * @param reservedStateMetadata a map of namespace to {@link ReservedStateMetadata}
         * @return {@link Builder}
         */
        public Builder put(Map<String, ReservedStateMetadata> reservedStateMetadata) {
            this.reservedStateMetadata.putAllFromMap(reservedStateMetadata);
            return this;
        }

        /**
         * Adds a {@link ReservedStateMetadata} for a given namespace to the metadata builder
         * @param metadata a {@link ReservedStateMetadata}
         * @return {@link Builder}
         */
        public Builder put(ReservedStateMetadata metadata) {
            reservedStateMetadata.put(metadata.namespace(), metadata);
            return this;
        }

        /**
         * Removes a {@link ReservedStateMetadata} for a given namespace
         * @param metadata a {@link ReservedStateMetadata}
         * @return {@link Builder}
         */
        public Builder removeReservedState(ReservedStateMetadata metadata) {
            reservedStateMetadata.remove(metadata.namespace());
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            getSingleProject().indexGraveyard(indexGraveyard);
            return this;
        }

        @Deprecated(forRemoval = true)
        public IndexGraveyard indexGraveyard() {
            return getSingleProject().indexGraveyard();
        }

        @Deprecated(forRemoval = true)
        public Builder updateSettings(Settings settings, String... indices) {
            getSingleProject().updateSettings(settings, indices);
            return this;
        }

        /**
         * Update the number of replicas for the specified indices.
         *
         * @param numberOfReplicas the number of replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        @Deprecated(forRemoval = true)
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indices) {
            getSingleProject().updateNumberOfReplicas(numberOfReplicas, indices);
            return this;
        }

        public Builder coordinationMetadata(CoordinationMetadata coordinationMetadata) {
            this.coordinationMetadata = coordinationMetadata;
            return this;
        }

        public Settings transientSettings() {
            return this.transientSettings;
        }

        public Builder transientSettings(Settings settings) {
            this.transientSettings = settings;
            return this;
        }

        public Settings persistentSettings() {
            return this.persistentSettings;
        }

        public Builder persistentSettings(Settings settings) {
            this.persistentSettings = settings;
            return this;
        }

        public Builder hashesOfConsistentSettings(DiffableStringMap hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = hashesOfConsistentSettings;
            return this;
        }

        public Builder hashesOfConsistentSettings(Map<String, String> hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = new DiffableStringMap(hashesOfConsistentSettings);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }

        public Builder generateClusterUuidIfNeeded() {
            if (clusterUUID.equals(UNKNOWN_CLUSTER_UUID)) {
                clusterUUID(UUIDs.randomBase64UUID());
            }
            return this;
        }

        /**
         * @return a new <code>Metadata</code> instance
         */
        public Metadata build() {
            return build(false);
        }

        public Metadata build(boolean skipNameCollisionChecks) {
            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                buildProjectMetadata(skipNameCollisionChecks),
                transientSettings,
                persistentSettings,
                Settings.builder().put(persistentSettings).put(transientSettings).build(),
                hashesOfConsistentSettings,
                customs.build(),
                reservedStateMetadata.build()
            );
        }

        private Map<ProjectId, ProjectMetadata> buildProjectMetadata(boolean skipNameCollisionChecks) {
            if (projectMetadata.isEmpty()) {
                createDefaultProject();
            }
            assert assertProjectIdAndProjectMetadataConsistency();
            if (projectMetadata.size() == 1) {
                final var entry = projectMetadata.entrySet().iterator().next();
                // Map.of() with a single entry is highly optimized
                // so we want take advantage of that performance boost for this common case of a single project
                return Map.of(entry.getKey(), entry.getValue().build(skipNameCollisionChecks));
            } else {
                return Collections.unmodifiableMap(Maps.transformValues(projectMetadata, m -> m.build(skipNameCollisionChecks)));
            }
        }

        private ProjectMetadata.Builder createDefaultProject() {
            return projectMetadata.put(DEFAULT_PROJECT_ID, new ProjectMetadata.Builder(Map.of(), 0).id(DEFAULT_PROJECT_ID));
        }

        private boolean assertProjectIdAndProjectMetadataConsistency() {
            projectMetadata.forEach((id, project) -> {
                assert project.getId().equals(id) : "project id mismatch key=[" + id + "] builder=[" + project.getId() + "]";
            });
            return true;
        }

        /**
         * There are a set of specific custom sections that have moved from top-level sections to project-level sections
         * as part of the multi-project refactor. Enumerate them here so we can move them to the right place
         * if they are read as a top-level section from a previous metadata version.
         */
        private static final Set<String> MOVED_PROJECT_CUSTOMS = Set.of(
            IndexGraveyard.TYPE,
            DataStreamMetadata.TYPE,
            ComposableIndexTemplateMetadata.TYPE,
            ComponentTemplateMetadata.TYPE
        );

        public static Metadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            // we might get here after the meta-data element, or on a fresh parser
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if ("meta-data".equals(currentFieldName) == false) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name (meta-data)
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                    // move to the next object
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
            }

            if ("meta-data".equals(currentFieldName) == false) {
                throw new IllegalArgumentException("Expected [meta-data] as a field name but got " + currentFieldName);
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            /**
             * Used when reading BWC fields from when indices etc used to be directly on metadata
             */
            final Supplier<ProjectMetadata.Builder> projectBuilderForBwc = () -> {
                // Due to the way we handle repository metadata (we changed it from cluster scoped to project scoped)
                // we may have cases where we have both project scoped XContent (with its own indices, customs etc)
                // and also cluster scoped XContent that needs to be applied to the default project
                // And, in this case there may be multiple projects even while we're applying BWC logic to the default project
                ProjectMetadata.Builder pmb = builder.getProject(ProjectId.DEFAULT);
                if (pmb == null) {
                    pmb = ProjectMetadata.builder(ProjectId.DEFAULT);
                    builder.put(pmb);
                }
                return pmb;
            };
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    switch (currentFieldName) {
                        case "projects" -> {
                            assert builder.projectMetadata.isEmpty() : "expect empty projectMetadata, but got " + builder.projectMetadata;
                            readProjects(parser, builder);
                        }
                        default -> throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case "cluster_coordination" -> builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                        case "settings" -> builder.persistentSettings(Settings.fromXContent(parser));
                        case "hashes_of_consistent_settings" -> builder.hashesOfConsistentSettings(parser.mapStrings());
                        /* Cluster reserved state */
                        case "reserved_state" -> {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                builder.put(ReservedStateMetadata.fromXContent(parser));
                            }
                        }
                        /* BwC Top-level project things */
                        case "indices" -> {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                projectBuilderForBwc.get().put(IndexMetadata.Builder.fromXContent(parser), false);
                            }
                        }
                        case "templates" -> {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                projectBuilderForBwc.get().put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                            }
                        }
                        /* Cluster customs (and project customs in older formats) */
                        default -> {
                            // Older clusters didn't separate cluster-scoped and project-scope customs so a top-level custom object might
                            // actually be a project-scoped custom
                            final NamedXContentRegistry registry = parser.getXContentRegistry();
                            if (registry.hasParser(ClusterCustom.class, currentFieldName, parser.getRestApiVersion())
                                && MOVED_PROJECT_CUSTOMS.contains(currentFieldName) == false) {
                                parseCustomObject(parser, currentFieldName, ClusterCustom.class, builder::putCustom);
                            } else if (registry.hasParser(ProjectCustom.class, currentFieldName, parser.getRestApiVersion())) {
                                parseCustomObject(parser, currentFieldName, ProjectCustom.class, (name, projectCustom) -> {
                                    if (projectCustom instanceof PersistentTasksCustomMetadata persistentTasksCustomMetadata) {
                                        assert PersistentTasksCustomMetadata.TYPE.equals(name)
                                            : name + " != " + PersistentTasksCustomMetadata.TYPE;
                                        final var tuple = persistentTasksCustomMetadata.split();
                                        projectBuilderForBwc.get().putCustom(PersistentTasksCustomMetadata.TYPE, tuple.v2());
                                        builder.putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tuple.v1());
                                    } else {
                                        projectBuilderForBwc.get().putCustom(name, projectCustom);
                                    }
                                });
                            } else {
                                logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                                parser.skipChildren();
                            }
                        }
                    }
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case "version" -> builder.version(parser.longValue());
                        case "cluster_uuid", "uuid" -> builder.clusterUUID(parser.text());
                        case "cluster_uuid_committed" -> builder.clusterUUIDCommitted(parser.booleanValue());
                        default -> throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            return builder.build();
        }

        private static void readProjects(XContentParser parser, Builder builder) throws IOException {
            XContentParser.Token token;

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                builder.put(ProjectMetadata.Builder.fromXContent(parser));
            }
        }

        static <C extends MetadataCustom<C>> void parseCustomObject(
            XContentParser parser,
            String name,
            Class<C> categoryClass,
            BiConsumer<String, C> consumer
        ) throws IOException {
            try {
                C custom = parser.namedObject(categoryClass, name, null);
                consumer.accept(custom.getWriteableName(), custom);
            } catch (NamedObjectNotFoundException _ex) {
                logger.warn("Skipping unknown custom [{}] object with type {}", categoryClass.getSimpleName(), name);
                parser.skipChildren();
            }
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = Maps.newMapWithExpectedSize(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    /**
     * State format for {@link Metadata} to write to and load from disk
     */
    public static final MetadataStateFormat<Metadata> FORMAT = new MetadataStateFormat<>(GLOBAL_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, Metadata state) throws IOException {
            ChunkedToXContent.wrapAsToXContent(state).toXContent(builder, FORMAT_PARAMS);
        }

        @Override
        public Metadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

    private volatile Metadata.ProjectLookup projectLookup = null;

    /**
     * Attempt to find a project for the supplied {@link Index}.
     */
    public Optional<ProjectMetadata> lookupProject(Index index) {
        return getProjectLookup().project(index);
    }

    /**
     * Attempt to find a project for the supplied {@link Index}.
     * @throws org.elasticsearch.index.IndexNotFoundException if the index does not exist in any project
     */
    public ProjectMetadata projectFor(Index index) {
        return lookupProject(index).orElseThrow(
            () -> new IndexNotFoundException("index [" + index + "] does not exist in any project", index)
        );
    }

    /**
     * Attempt to find the IndexMetadata for the supplied {@link Index}.
     * @throws org.elasticsearch.index.IndexNotFoundException if the index does not exist in any project
     */
    public IndexMetadata indexMetadata(Index index) {
        return projectFor(index).getIndexSafe(index);
    }

    /**
     * This method is similar to {@link #indexMetadata}. But it returns an {@link Optional} instead of
     * throwing when either the project or the index is not found.
     */
    public Optional<IndexMetadata> findIndex(Index index) {
        return lookupProject(index).map(projectMetadata -> projectMetadata.index(index));
    }

    ProjectLookup getProjectLookup() {
        /*
         * projectLookup is volatile, but this assignment is not synchronized
         * That means it is possible that we will generate multiple lookup objects if there are multiple concurrent callers
         * Those lookup objects will be identical, and the double assignment will be safe, but there is the cost of building the lookup
         * more than once.
         * In the single project case building the lookup is cheap, and synchronization would be costly.
         * In the multiple project case, it might be cheaper to synchronize, but the long term solution is to maintain the lookup table
         *  as projects/indices are added/removed rather than rebuild it each time the cluster-state/metadata object changes.
         */
        if (this.projectLookup == null) {
            if (this.isSingleProject()) {
                projectLookup = new SingleProjectLookup(getSingleProject());
            } else {
                projectLookup = new MultiProjectLookup();
            }
        }
        return projectLookup;
    }

    /**
     * A lookup table from {@link Index} to {@link ProjectId}
     */
    interface ProjectLookup {
        /**
         * Return the {@link ProjectId} for the provided {@link Index}, if it exists
         */
        Optional<ProjectMetadata> project(Index index);
    }

    /**
     * An implementation of {@link ProjectLookup} that is optimized for the case where there is a single project.
     *
     */
    static class SingleProjectLookup implements ProjectLookup {

        private final ProjectMetadata project;

        SingleProjectLookup(ProjectMetadata project) {
            this.project = project;
        }

        @Override
        public Optional<ProjectMetadata> project(Index index) {
            if (project.hasIndex(index)) {
                return Optional.of(project);
            } else {
                return Optional.empty();
            }
        }
    }

    class MultiProjectLookup implements ProjectLookup {
        private final Map<String, ProjectMetadata> lookup;

        private MultiProjectLookup() {
            this.lookup = Maps.newMapWithExpectedSize(Metadata.this.getTotalNumberOfIndices());
            for (var project : projectMetadata.values()) {
                for (var indexMetadata : project) {
                    final String uuid = indexMetadata.getIndex().getUUID();
                    final ProjectMetadata previousProject = lookup.put(uuid, project);
                    if (previousProject != null && previousProject != project) {
                        throw new IllegalStateException(
                            "Index UUID [" + uuid + "] exists in project [" + project.id() + "] and [" + previousProject.id() + "]"
                        );
                    }
                }
            }
        }

        @Override
        public Optional<ProjectMetadata> project(Index index) {
            final ProjectMetadata project = lookup.get(index.getUUID());
            if (project != null && project.hasIndex(index)) {
                return Optional.of(project);
            } else {
                return Optional.empty();
            }
        }
    }

}
