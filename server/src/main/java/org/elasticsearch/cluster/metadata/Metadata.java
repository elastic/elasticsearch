/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteable;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * {@link Metadata} is the part of the {@link ClusterState} which persists across restarts. This persistence is XContent-based, so a
 * round-trip through XContent must be faithful in {@link XContentContext#GATEWAY} context.
 * <p>
 * The details of how this is persisted are covered in {@link org.elasticsearch.gateway.PersistedClusterStateService}.
 * </p>
 */
public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata>, ChunkedToXContent {

    private static final Logger logger = LogManager.getLogger(Metadata.class);

    public static final Runnable ON_NEXT_INDEX_FIND_MAPPINGS_NOOP = () -> {};
    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = "_na_";

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
    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and stored as a part of
     * a snapshot global state, but will not be persisted between node restarts
     */
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    /**
     * Indicates that this custom metadata will be returned as part of an API call, stored as a part of
     * a snapshot global state, and will be persisted between node restarts
     */
    public static EnumSet<XContentContext> ALL_CONTEXTS = EnumSet.allOf(XContentContext.class);

    public interface MetadataCustom<SELF> extends NamedDiffable<SELF>, ChunkedToXContent {

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final NamedDiffableValueSerializer BWC_CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer(MetadataCustom.class) {
        @Override
        public MetadataCustom read(StreamInput in, String key) throws IOException {
            final Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet();
            final Set<String> projectScopedNames = in.namedWriteableRegistry().getReaders(ProjectMetadata.ProjectCustom.class).keySet();
            if (clusterScopedNames.contains(key)) {
                return in.readNamedWriteable(ClusterCustom.class, key);
            } else if (projectScopedNames.contains(key)) {
                return in.readNamedWriteable(ProjectMetadata.ProjectCustom.class, key);
            } else {
                throw new IllegalArgumentException("Unknown custom name [" + key + "]");
            }
        }
    };

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;

    private final ImmutableOpenMap<String, ClusterCustom> clusterCustoms;
    private final Map<String, ReservedStateMetadata> reservedStateMetadata;

    private final ProjectMetadata project;

    private Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        Settings settings,
        DiffableStringMap hashesOfConsistentSettings,
        int totalNumberOfShards,
        int totalOpenIndexShards,
        ImmutableOpenMap<String, IndexMetadata> indices,
        ImmutableOpenMap<String, Set<Index>> aliasedIndices,
        ImmutableOpenMap<String, IndexTemplateMetadata> templates,
        ImmutableOpenMap<String, ClusterCustom> clusterCustoms,
        ImmutableOpenMap<String, ProjectMetadata.ProjectCustom> projectCustoms,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, MappingMetadata> mappingsByHash,
        IndexVersion oldestIndexVersion,
        Map<String, ReservedStateMetadata> reservedStateMetadata
    ) {
        this(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            new ProjectMetadata(
                totalNumberOfShards,
                totalOpenIndexShards,
                indices,
                aliasedIndices,
                templates,
                projectCustoms,
                allIndices,
                visibleIndices,
                allOpenIndices,
                visibleOpenIndices,
                allClosedIndices,
                visibleClosedIndices,
                indicesLookup,
                mappingsByHash,
                oldestIndexVersion
            )
        );
    }

    private Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        Settings settings,
        DiffableStringMap hashesOfConsistentSettings,
        ImmutableOpenMap<String, ClusterCustom> clusterCustoms,
        Map<String, ReservedStateMetadata> reservedStateMetadata,
        ProjectMetadata project
    ) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = settings;
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.clusterCustoms = clusterCustoms;
        this.reservedStateMetadata = reservedStateMetadata;
        this.project = project;
        assert assertConsistent();
    }

    private boolean assertConsistent() {
        return project.assertConsistent();
    }

    public Metadata withIncrementedVersion() {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version + 1,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project
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
    public Metadata withLifecycleState(final Index index, final LifecycleExecutionState lifecycleState) {
        var newProject = project.withLifecycleState(index, lifecycleState);
        if (newProject == this.project) {
            return this;
        }
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            newProject
        );
    }

    public Metadata withIndexSettingsUpdates(final Map<Index, Settings> updates) {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project.withIndexSettingsUpdates(updates)
        );
    }

    public Metadata withCoordinationMetadata(CoordinationMetadata coordinationMetadata) {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project
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
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project
        );
    }

    /**
     * Creates a copy of this instance updated with the given {@link IndexMetadata} that must only contain changes to primary terms
     * and in-sync allocation ids relative to the existing entries. This method is only used by
     * {@link org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater#applyChanges(Metadata, RoutingTable, TransportVersion)}.
     * @param updates map of index name to {@link IndexMetadata}.
     * @return updated metadata instance
     */
    public Metadata withAllocationAndTermUpdatesOnly(Map<String, IndexMetadata> updates) {
        if (updates.isEmpty()) {
            return this;
        }
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project.withAllocationAndTermUpdatesOnly(updates)
        );
    }

    /**
     * Creates a copy of this instance with the given {@code index} added.
     * @param index index to add
     * @return copy with added index
     */
    public Metadata withAddedIndex(IndexMetadata index) {
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            clusterCustoms,
            reservedStateMetadata,
            project.withAddedIndex(index)
        );
    }

    public ProjectMetadata project() {
        return project;
    }

    public long version() {
        return this.version;
    }

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

    public IndexVersion oldestIndexVersion() {
        return project.oldestIndexVersion();
    }

    public boolean equalsAliases(Metadata other) {
        return this.project.equalsAliases(other.project);
    }

    public boolean indicesLookupInitialized() {
        return project.indicesLookupInitialized();
    }

    public SortedMap<String, IndexAbstraction> getIndicesLookup() {
        return project.getIndicesLookup();
    }

    public boolean sameIndicesLookup(Metadata other) {
        return this.project.sameIndicesLookup(other.project);
    }

    /**
     * Finds the specific index aliases that point to the requested concrete indices directly
     * or that match with the indices via wildcards.
     *
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAllAliases(final String[] concreteIndices) {
        return project.findAllAliases(concreteIndices);
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards, and
     * that point to the specified concrete indices (directly or matching indices via wildcards).
     *
     * @param aliases The aliases to look for. Might contain include or exclude wildcards.
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAliases(final String[] aliases, final String[] concreteIndices) {
        return project.findAliases(aliases, concreteIndices);
    }

    /**
     * Finds the specific data stream aliases that match with the specified aliases directly or partially via wildcards, and
     * that point to the specified data streams (directly or matching data streams via wildcards).
     *
     * @param aliases The aliases to look for. Might contain include or exclude wildcards.
     * @param dataStreams The data streams that the aliases must point to in order to be returned
     * @return A map of data stream name to the list of DataStreamAlias objects that match. If a data stream does not have matching
     * aliases then the result will <b>not</b> include the data stream's key.
     */
    public Map<String, List<DataStreamAlias>> findDataStreamAliases(final String[] aliases, final String[] dataStreams) {
        return project.findDataStreamAliases(aliases, dataStreams);
    }

    /**
     * Finds all mappings for concrete indices. Only fields that match the provided field
     * filter will be returned (default is a predicate that always returns true, which can be
     * overridden via plugins)
     *
     * @see MapperPlugin#getFieldFilter()
     *
     * @param onNextIndex a hook that gets notified for each index that's processed
     */
    public Map<String, MappingMetadata> findMappings(
        String[] concreteIndices,
        Function<String, ? extends Predicate<String>> fieldFilter,
        Runnable onNextIndex
    ) {
        return project.findMappings(concreteIndices, fieldFilter, onNextIndex);
    }

    /**
     * Finds the parent data streams, if any, for the specified concrete indices.
     */
    public Map<String, DataStream> findDataStreams(String... concreteIndices) {
        return project.findDataStreams(concreteIndices);
    }

    /**
     * Checks whether the provided index is a data stream.
     */
    public boolean indexIsADataStream(String indexName) {
        return project.indexIsADataStream(indexName);
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] getConcreteAllIndices() {
        return project.getConcreteAllIndices();
    }

    /**
     * Returns all the concrete indices that are not hidden.
     */
    public String[] getConcreteVisibleIndices() {
        return project.getConcreteVisibleIndices();
    }

    /**
     * Returns all of the concrete indices that are open.
     */
    public String[] getConcreteAllOpenIndices() {
        return project.getConcreteAllOpenIndices();
    }

    /**
     * Returns all of the concrete indices that are open and not hidden.
     */
    public String[] getConcreteVisibleOpenIndices() {
        return project.getConcreteVisibleOpenIndices();
    }

    /**
     * Returns all of the concrete indices that are closed.
     */
    public String[] getConcreteAllClosedIndices() {
        return project.getConcreteAllClosedIndices();
    }

    /**
     * Returns all of the concrete indices that are closed and not hidden.
     */
    public String[] getConcreteVisibleClosedIndices() {
        return project.getConcreteVisibleClosedIndices();
    }

    /**
     * Returns indexing routing for the given <code>aliasOrIndex</code>. Resolves routing from the alias metadata used
     * in the write index.
     */
    public String resolveWriteIndexRouting(@Nullable String routing, String aliasOrIndex) {
        return project.resolveWriteIndexRouting(routing, aliasOrIndex);
    }

    /**
     * Returns indexing routing for the given index.
     */
    public String resolveIndexRouting(@Nullable String routing, String aliasOrIndex) {
        return project.resolveIndexRouting(routing, aliasOrIndex);
    }

    /**
     * Checks whether an index exists (as of this {@link Metadata} with the given name. Does not check aliases or data streams.
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if a concrete index with that name exists, {@code false} otherwise.
     */
    public boolean hasIndex(String index) {
        return project.hasIndex(index);
    }

    /**
     * Checks whether an index exists. Similar to {@link Metadata#hasIndex(String)}, but ensures that the index has the same UUID as
     * the given {@link Index}.
     * @param index An {@link Index} object that may or may not exist in the cluster.
     * @return {@code true} if an index exists with the same name and UUID as the given index object, {@code false} otherwise.
     */
    public boolean hasIndex(Index index) {
        return project.hasIndex(index);
    }

    /**
     * Checks whether an index abstraction (that is, index, alias, or data stream) exists (as of this {@link Metadata} with the given name.
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if an index abstraction with that name exists, {@code false} otherwise.
     */
    public boolean hasIndexAbstraction(String index) {
        return project.hasIndexAbstraction(index);
    }

    public IndexMetadata index(String index) {
        return project.index(index);
    }

    public IndexMetadata index(Index index) {
        return project.index(index);
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return project.hasIndexMetadata(indexMetadata);
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetadata getIndexSafe(Index index) {
        return project.getIndexSafe(index);
    }

    public Map<String, IndexMetadata> indices() {
        return project.indices();
    }

    public Map<String, IndexMetadata> getIndices() {
        return project.getIndices();
    }

    /**
     * Returns whether an alias exists with provided alias name.
     *
     * @param aliasName The provided alias name
     * @return whether an alias exists with provided alias name
     */
    public boolean hasAlias(String aliasName) {
        return project.hasAlias(aliasName);
    }

    /**
     * Returns all the indices that the alias with the provided alias name refers to.
     * These are aliased indices. Not that, this only return indices that have been aliased
     * and not indices that are behind a data stream or data stream alias.
     *
     * @param aliasName The provided alias name
     * @return all aliased indices by the alias with the provided alias name
     */
    public Set<Index> aliasedIndices(String aliasName) {
        return project.aliasedIndices(aliasName);
    }

    /**
     * @return the names of all indices aliases.
     */
    public Set<String> aliasedIndices() {
        return project.aliasedIndices();
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return project.templates();
    }

    public Map<String, IndexTemplateMetadata> getTemplates() {
        return templates();
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return project.componentTemplates();
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return project.templatesV2();
    }

    public boolean isTimeSeriesTemplate(ComposableIndexTemplate indexTemplate) {
        return project.isTimeSeriesTemplate(indexTemplate);
    }

    public Map<String, DataStream> dataStreams() {
        return project.dataStreams();
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return project.dataStreamAliases();
    }

    /**
     * Return a map of DataStreamAlias objects by DataStream name
     * @return a map of DataStreamAlias objects by DataStream name
     */
    public Map<String, List<DataStreamAlias>> dataStreamAliasesByDataStream() {
        return project.dataStreamAliasesByDataStream();
    }

    public NodesShutdownMetadata nodeShutdowns() {
        return clusterCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
    }

    /**
     * Indicates if the provided index is managed by ILM. This takes into account if the index is part of
     * data stream that's potentially managed by data stream lifecycle and the value of the
     * {@link org.elasticsearch.index.IndexSettings#PREFER_ILM_SETTING}
     */
    public boolean isIndexManagedByILM(IndexMetadata indexMetadata) {
        return project.isIndexManagedByILM(indexMetadata);
    }

    public Map<String, ClusterCustom> clusterCustoms() {
        return this.clusterCustoms;
    }

    public Map<String, ProjectMetadata.ProjectCustom> projectCustoms() {
        return this.project.customs();
    }

    /**
     * Returns the full {@link ReservedStateMetadata} Map for all
     * reserved state namespaces.
     * @return a map of namespace to {@link ReservedStateMetadata}
     */
    public Map<String, ReservedStateMetadata> reservedStateMetadata() {
        return this.reservedStateMetadata;
    }

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return project.indexGraveyard();
    }

    @SuppressWarnings("unchecked")
    public <T extends ClusterCustom> T clusterCustom(String type) {
        return (T) clusterCustoms.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends ClusterCustom> T clusterCustom(String type, T defaultValue) {
        return (T) clusterCustoms.getOrDefault(type, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public <T extends ProjectMetadata.ProjectCustom> T projectCustom(String type) {
        return project.custom(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends ProjectMetadata.ProjectCustom> T projectCustom(String type, T defaultValue) {
        return project.custom(type, defaultValue);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return project.getTotalNumberOfShards();
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return project.getTotalOpenIndexShards();
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return project.iterator();
    }

    public Stream<IndexMetadata> stream() {
        return project.stream();
    }

    public int size() {
        return project.size();
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
        if (customsEqual(metadata1.clusterCustoms, metadata2.clusterCustoms) == false) {
            return false;
        }
        if (Objects.equals(metadata1.reservedStateMetadata, metadata2.reservedStateMetadata) == false) {
            return false;
        }
        return ProjectMetadata.isGlobalStateEquals(metadata1.project, metadata2.project);
    }

    static <C extends MetadataCustom<C>> boolean customsEqual(ImmutableOpenMap<String, C> customs1, ImmutableOpenMap<String, C> customs2) {
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

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(MetadataDiff.NOOP_METADATA_DIFF_VERSION) && in.readBoolean()) {
            return SimpleDiffable.empty();
        }
        return new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params p) {
        XContentContext context = XContentContext.from(p);
        final Iterator<? extends ToXContent> start = context == XContentContext.API
            ? ChunkedToXContentHelper.startObject("metadata")
            : Iterators.single((builder, params) -> builder.startObject("meta-data").field("version", version()));

        final Iterator<? extends ToXContent> persistentSettings = context != XContentContext.API && persistentSettings().isEmpty() == false
            ? Iterators.single((builder, params) -> {
                builder.startObject("settings");
                persistentSettings().toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                return builder.endObject();
            })
            : Collections.emptyIterator();

        return Iterators.concat(start, Iterators.single((builder, params) -> {
            builder.field("cluster_uuid", clusterUUID);
            builder.field("cluster_uuid_committed", clusterUUIDCommitted);
            builder.startObject("cluster_coordination");
            coordinationMetadata().toXContent(builder, params);
            return builder.endObject();
        }),
            persistentSettings,
            Iterators.flatMap(
                clusterCustoms.entrySet().iterator(),
                entry -> entry.getValue().context().contains(context)
                    ? ChunkedToXContentHelper.wrapWithObject(entry.getKey(), entry.getValue().toXContentChunked(p))
                    : Collections.emptyIterator()
            ),
            // For BWC, the API does not have an embedded "project:", but other contexts do
            context == XContentContext.API
                ? project.toXContentChunked(p)
                : ChunkedToXContentHelper.wrapWithObject("project", project.toXContentChunked(p)),
            ChunkedToXContentHelper.wrapWithObject("reserved_state", reservedStateMetadata().values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    public Map<String, MappingMetadata> getMappingsByHash() {
        return project.getMappingsByHash();
    }

    private static class MetadataDiff implements Diff<Metadata> {

        private static final TransportVersion NOOP_METADATA_DIFF_VERSION = TransportVersions.V_8_5_0;
        private static final TransportVersion NOOP_METADATA_DIFF_SAFE_VERSION =
            PublicationTransportHandler.INCLUDES_LAST_COMMITTED_DATA_VERSION;

        private final long version;
        private final String clusterUUID;
        private final boolean clusterUUIDCommitted;
        private final CoordinationMetadata coordinationMetadata;
        private final Settings transientSettings;
        private final Settings persistentSettings;
        private final Diff<DiffableStringMap> hashesOfConsistentSettings;
        private final MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>> clusterCustoms;
        private final Diff<Map<String, ReservedStateMetadata>> reservedStateMetadata;
        private final Diff<ProjectMetadata> project;

        /**
         * true if this diff is a noop because before and after were the same instance
         */
        private final boolean empty;

        MetadataDiff(Metadata before, Metadata after) {
            this.empty = before == after;
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            project = after.project.diff(before.project);
            if (empty) {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
                clusterCustoms = DiffableUtils.emptyDiff();
                reservedStateMetadata = DiffableUtils.emptyDiff();
            } else {
                hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
                clusterCustoms = DiffableUtils.diff(
                    before.clusterCustoms,
                    after.clusterCustoms,
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
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
                hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            } else {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                clusterCustoms = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    CLUSTER_CUSTOM_VALUE_SERIALIZER
                );
                project = ProjectMetadata.readDiffFrom(in);
            } else {
                var indices = ProjectMetadata.ProjectMetadataDiff.readIndices(in);
                var templates = ProjectMetadata.ProjectMetadataDiff.readTemplates(in);
                var bwcCustoms = readBwcCustoms(in);
                clusterCustoms = bwcCustoms.v1();
                project = new ProjectMetadata.ProjectMetadataDiff(indices, templates, bwcCustoms.v2());
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                reservedStateMetadata = DiffableUtils.readJdkMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    RESERVED_DIFF_VALUE_READER
                );
            } else {
                reservedStateMetadata = DiffableUtils.emptyDiff();
            }
        }

        @SuppressWarnings("unchecked")
        private static
            Tuple<
                MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>>,
                MapDiff<String, ProjectMetadata.ProjectCustom, ImmutableOpenMap<String, ProjectMetadata.ProjectCustom>>>
            readBwcCustoms(StreamInput in) throws IOException {
            MapDiff<String, MetadataCustom<?>, ImmutableOpenMap<String, MetadataCustom<?>>> customs = DiffableUtils
                .readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), BWC_CUSTOM_VALUE_SERIALIZER);
            return DiffableUtils.split(
                customs,
                in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet(),
                CLUSTER_CUSTOM_VALUE_SERIALIZER,
                in.namedWriteableRegistry().getReaders(ProjectMetadata.ProjectCustom.class).keySet(),
                ProjectMetadata.PROJECT_CUSTOM_VALUE_SERIALIZER
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(NOOP_METADATA_DIFF_SAFE_VERSION)) {
                out.writeBoolean(empty);
                if (empty) {
                    // noop diff
                    return;
                }
            } else if (out.getTransportVersion().onOrAfter(NOOP_METADATA_DIFF_VERSION)) {
                // noops are not safe with these versions, see #92259
                out.writeBoolean(false);
            }
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            transientSettings.writeTo(out);
            persistentSettings.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
                hashesOfConsistentSettings.writeTo(out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                clusterCustoms.writeTo(out);
                project.writeTo(out);
            } else {
                ProjectMetadata.ProjectMetadataDiff.indices(project).writeTo(out);
                ProjectMetadata.ProjectMetadataDiff.templates(project).writeTo(out);
                buildUnifiedCustomDiff().writeTo(out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                reservedStateMetadata.writeTo(out);
            }
        }

        @SuppressWarnings("unchecked")
        private Diff<ImmutableOpenMap<String, ?>> buildUnifiedCustomDiff() {
            return DiffableUtils.merge(
                clusterCustoms,
                ProjectMetadata.ProjectMetadataDiff.customs(project),
                DiffableUtils.getStringKeySerializer(),
                BWC_CUSTOM_VALUE_SERIALIZER
            );
        }

        @Override
        public Metadata apply(Metadata part) {
            if (empty) {
                return part;
            }
            Builder builder = new Builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.clusterCustoms(clusterCustoms.apply(part.clusterCustoms));
            builder.put(reservedStateMetadata.apply(part.reservedStateMetadata));
            return builder.build(project.apply(part.project));
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            Builder builder = new Builder();
            builder.version = in.readLong();
            builder.clusterUUID = in.readString();
            builder.clusterUUIDCommitted = in.readBoolean();
            builder.coordinationMetadata(new CoordinationMetadata(in));
            builder.transientSettings(readSettingsFromStream(in));
            builder.persistentSettings(readSettingsFromStream(in));
            builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
            readNamedWriteables(in, ClusterCustom.class, builder::putClusterCustom);
            in.readCollection(ignore -> null, (stream, ignore) -> builder.put(ReservedStateMetadata.readFrom(stream)));
            return builder.build(ProjectMetadata.readFrom(in));
        } else {
            return readWithEmbeddedProject(in);
        }
    }

    /**
     * Reads the "old style" of serialization from before ProjectMetadata existed
     */
    private static Metadata readWithEmbeddedProject(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
            builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        }
        final Function<String, MappingMetadata> mappingLookup;
        if (in.getTransportVersion().onOrAfter(ProjectMetadata.MAPPINGS_AS_HASH_VERSION)) {
            final Map<String, MappingMetadata> mappingMetadataMap = in.readMapValues(MappingMetadata::new, MappingMetadata::getSha256);
            if (mappingMetadataMap.size() > 0) {
                mappingLookup = mappingMetadataMap::get;
            } else {
                mappingLookup = null;
            }
        } else {
            mappingLookup = null;
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in, mappingLookup), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetadata.readFrom(in));
        }
        readBwcCustoms(in, builder);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            int reservedStateSize = in.readVInt();
            for (int i = 0; i < reservedStateSize; i++) {
                builder.put(ReservedStateMetadata.readFrom(in));
            }
        }
        return builder.build();
    }

    private static <T extends NamedWriteable> void readNamedWriteables(StreamInput in, Class<T> type, BiConsumer<String, T> consumer)
        throws IOException {
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            T customIndexMetadata = in.readNamedWriteable(type);
            consumer.accept(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
    }

    private static <T extends NamedWriteable> void readBwcCustoms(StreamInput in, Builder builder) throws IOException {
        final Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet();
        final Set<String> projectScopedNames = in.namedWriteableRegistry().getReaders(ProjectMetadata.ProjectCustom.class).keySet();
        final int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            final String name = in.readString();
            if (clusterScopedNames.contains(name)) {
                final ClusterCustom custom = in.readNamedWriteable(ClusterCustom.class, name);
                builder.putClusterCustom(custom.getWriteableName(), custom);
            } else if (projectScopedNames.contains(name)) {
                final ProjectMetadata.ProjectCustom custom = in.readNamedWriteable(ProjectMetadata.ProjectCustom.class, name);
                builder.putProjectCustom(custom.getWriteableName(), custom);
            } else {
                throw new IllegalArgumentException("Unknown custom name [" + name + "]");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            out.writeLong(version);
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            coordinationMetadata.writeTo(out);
            transientSettings.writeTo(out);
            persistentSettings.writeTo(out);
            hashesOfConsistentSettings.writeTo(out);
            VersionedNamedWriteable.writeVersionedWritables(out, clusterCustoms);
            out.writeCollection(reservedStateMetadata.values());
            project.writeTo(out);
        } else {
            bwcWriteWithProject(out);
        }
    }

    /**
     * Serializes in the format before we had a separate {@link ProjectMetadata} object
     */
    private void bwcWriteWithProject(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        transientSettings.writeTo(out);
        persistentSettings.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
            hashesOfConsistentSettings.writeTo(out);
        }
        // Starting in #MAPPINGS_AS_HASH_VERSION we write the mapping metadata first and then write the indices without metadata so that
        // we avoid writing duplicate mappings twice
        if (out.getTransportVersion().onOrAfter(ProjectMetadata.MAPPINGS_AS_HASH_VERSION)) {
            out.writeMapValues(project.getMappingsByHash());
        }
        out.writeVInt(project.indices().size());
        final boolean writeMappingsHash = out.getTransportVersion().onOrAfter(ProjectMetadata.MAPPINGS_AS_HASH_VERSION);
        for (IndexMetadata indexMetadata : project) {
            indexMetadata.writeTo(out, writeMappingsHash);
        }
        out.writeCollection(project.templates().values());
        // It would be nice to do this as flattening iterable (rather than allocation a whole new list), but flattening
        // Iterable<? extends VersionNamedWriteable> into Iterable<VersionNamedWriteable> is messy, so we can fix that later
        List<VersionedNamedWriteable> merge = new ArrayList<>(clusterCustoms.size() + project.customs().size());
        merge.addAll(clusterCustoms.values());
        merge.addAll(project.customs().values());
        VersionedNamedWriteable.writeVersionedWriteables(out, merge);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeCollection(reservedStateMetadata.values());
        }
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
        private final ImmutableOpenMap.Builder<String, ClusterCustom> clusterCustoms;

        private final Map<String, ReservedStateMetadata> reservedStateMetadata;

        private ProjectMetadata.Builder project;

        @SuppressWarnings("this-escape")
        public Builder() {
            this(Map.of(), 0);
        }

        Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.hashesOfConsistentSettings = metadata.hashesOfConsistentSettings;
            this.version = metadata.version;
            this.clusterCustoms = ImmutableOpenMap.builder(metadata.clusterCustoms);
            this.reservedStateMetadata = new HashMap<>(metadata.reservedStateMetadata);
            this.project = new ProjectMetadata.Builder(metadata.project);
        }

        @SuppressWarnings("this-escape")
        private Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            clusterCustoms = ImmutableOpenMap.builder();
            reservedStateMetadata = new HashMap<>();
            project = new ProjectMetadata.Builder(mappingsByHash, indexCountHint);
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            project.put(indexMetadataBuilder);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            project.put(indexMetadata, incrementVersion);
            return this;
        }

        public IndexMetadata get(String index) {
            return project.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            return project.getSafe(index);
        }

        public Builder remove(String index) {
            project.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            project.removeAllIndices();
            return this;
        }

        public Builder indices(Map<String, IndexMetadata> indices) {
            project.indices(indices);
            return this;
        }

        public Builder put(IndexTemplateMetadata.Builder template) {
            project.put(template);
            return this;
        }

        public Builder put(IndexTemplateMetadata template) {
            project.put(template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            project.removeTemplate(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            project.templates(templates);
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            project.put(name, componentTemplate);
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            project.removeComponentTemplate(name);
            return this;
        }

        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            project.componentTemplates(componentTemplates);
            return this;
        }

        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            project.indexTemplates(indexTemplates);
            return this;
        }

        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            project.put(name, indexTemplate);
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            project.removeIndexTemplate(name);
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            return project.dataStream(dataStreamName);
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            project.dataStreams(dataStreams, dataStreamAliases);
            return this;
        }

        public Builder put(DataStream dataStream) {
            project.put(dataStream);
            return this;
        }

        public DataStreamMetadata dataStreamMetadata() {
            return project.dataStreamMetadata();
        }

        public boolean put(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
            return project.put(aliasName, dataStream, isWriteDataStream, filter);
        }

        public Builder removeDataStream(String name) {
            project.removeDataStream(name);
            return this;
        }

        public boolean removeDataStreamAlias(String aliasName, String dataStreamName, boolean mustExist) {
            return project.removeDataStreamAlias(aliasName, dataStreamName, mustExist);
        }

        public Builder putCustom(String type, ClusterCustom custom) {
            return putClusterCustom(type, custom);
        }

        public Builder putCustom(String type, ProjectMetadata.ProjectCustom custom) {
            return putProjectCustom(type, custom);
        }

        public ClusterCustom getClusterCustom(String type) {
            return clusterCustoms.get(type);
        }

        public Builder putClusterCustom(String type, ClusterCustom custom) {
            // TODO[MultiProject] Should we disallow SNAPSHOT context here?
            clusterCustoms.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeClusterCustom(String type) {
            clusterCustoms.remove(type);
            return this;
        }

        public Builder removeClusterCustomIf(BiPredicate<String, ? super ClusterCustom> p) {
            clusterCustoms.removeAll(p);
            return this;
        }

        public Builder clusterCustoms(Map<String, ClusterCustom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.clusterCustoms.putAllFromMap(customs);
            return this;
        }

        public ProjectMetadata.ProjectCustom getProjectCustom(String type) {
            return project.getCustom(type);
        }

        public Builder putProjectCustom(String type, ProjectMetadata.ProjectCustom custom) {
            project.putCustom(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeProjectCustom(String type) {
            project.removeCustom(type);
            return this;
        }

        public Builder removeProjectCustomIf(BiPredicate<String, ? super ProjectMetadata.ProjectCustom> p) {
            project.removeCustomIf(p);
            return this;
        }

        public Builder projectCustoms(Map<String, ProjectMetadata.ProjectCustom> customs) {
            project.customs(customs);
            return this;
        }

        /**
         * Adds a map of namespace to {@link ReservedStateMetadata} into the metadata builder
         * @param reservedStateMetadata a map of namespace to {@link ReservedStateMetadata}
         * @return {@link Builder}
         */
        public Builder put(Map<String, ReservedStateMetadata> reservedStateMetadata) {
            this.reservedStateMetadata.putAll(reservedStateMetadata);
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

        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            project.indexGraveyard(indexGraveyard);
            return this;
        }

        public IndexGraveyard indexGraveyard() {
            return project.indexGraveyard();
        }

        public Builder updateSettings(Settings settings, String... indices) {
            project.updateSettings(settings, indices);
            return this;
        }

        /**
         * Update the number of replicas for the specified indices.
         *
         * @param numberOfReplicas the number of replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indices) {
            project.updateNumberOfReplicas(numberOfReplicas, indices);
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
                clusterUUID = UUIDs.randomBase64UUID();
            }
            return this;
        }

        /**
         * @return a new <code>Metadata</code> instance
         */
        public Metadata build() {
            return build(project.build());
        }

        private Metadata build(ProjectMetadata projectMetadata) {
            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                Settings.builder().put(persistentSettings).put(transientSettings).build(),
                hashesOfConsistentSettings,
                clusterCustoms.build(),
                Collections.unmodifiableMap(reservedStateMetadata),
                projectMetadata
            );
        }

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

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("cluster_coordination".equals(currentFieldName)) {
                        builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                    } else if ("settings".equals(currentFieldName)) {
                        builder.persistentSettings(Settings.fromXContent(parser));
                    } else if ("indices".equals(currentFieldName)) {
                        // For BWC (before multi-project)
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                        }
                    } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                        builder.hashesOfConsistentSettings(parser.mapStrings());
                    } else if ("templates".equals(currentFieldName)) {
                        // For BWC (before multi-project)
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else if ("reserved_state".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(ReservedStateMetadata.fromXContent(parser));
                        }
                    } else if ("project".equals(currentFieldName)) {
                        builder.project = ProjectMetadata.Builder.fromXContent(parser);
                    } else {
                        // Older clusters didn't separate cluster-scoped and project-scope customs so a top-level custom object might
                        // actually be a project-scoped custom
                        final NamedXContentRegistry registry = parser.getXContentRegistry();
                        if (registry.hasParser(ClusterCustom.class, currentFieldName, parser.getRestApiVersion())) {
                            parseCustomObject(parser, currentFieldName, ClusterCustom.class, builder::putClusterCustom);
                        } else if (registry.hasParser(ProjectMetadata.ProjectCustom.class, currentFieldName, parser.getRestApiVersion())) {
                            parseCustomObject(parser, currentFieldName, ProjectMetadata.ProjectCustom.class, builder::putProjectCustom);
                        } else {
                            logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                            parser.skipChildren();
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                        builder.clusterUUID = parser.text();
                    } else if ("cluster_uuid_committed".equals(currentFieldName)) {
                        builder.clusterUUIDCommitted = parser.booleanValue();
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            return builder.build();
        }

        private static <C extends MetadataCustom<C>> void parseCustomObject(
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
}
