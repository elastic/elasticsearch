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
import org.apache.lucene.util.CollectionUtil;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

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

    public enum XContentContext {
        /* Custom metadata should be returned as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT
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
    private static final NamedDiffableValueSerializer<ProjectCustom> PROJECT_CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(
        ProjectCustom.class
    );

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
    public final ProjectMetadata projectMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    final ImmutableOpenMap<String, Metadata.ClusterCustom> customs;
    private final Map<String, ReservedStateMetadata> reservedStateMetadata;

    // Used in the findAliases and findDataStreamAliases functions
    private interface AliasInfoGetter {
        List<? extends AliasInfo> get(String entityName);
    }

    // Used in the findAliases and findDataStreamAliases functions
    private interface AliasInfoSetter {
        void put(String entityName, List<AliasInfo> aliases);
    }

    private Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        ProjectMetadata projectMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        Settings settings,
        DiffableStringMap hashesOfConsistentSettings,
        ImmutableOpenMap<String, ClusterCustom> customs,
        Map<String, ReservedStateMetadata> reservedStateMetadata
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
    public Metadata withLifecycleState(final Index index, final LifecycleExecutionState lifecycleState) {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(lifecycleState, "lifecycleState must not be null");

        IndexMetadata indexMetadata = projectMetadata.getIndexSafe(index);
        if (lifecycleState.equals(indexMetadata.getLifecycleExecutionState())) {
            return this;
        }

        // build a new index metadata with the version incremented and the new lifecycle state
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
        indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
        indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap());

        // drop it into the indices
        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(projectMetadata.indices);
        builder.put(index.getName(), indexMetadataBuilder.build());

        // construct a new Metadata object directly rather than using Metadata.builder(this).[...].build().
        // the Metadata.Builder validation needs to handle the general case where anything at all could
        // have changed, and hence it is expensive -- since we are changing so little about the metadata
        // (and at a leaf in the object tree), we can bypass that validation for efficiency's sake
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            new ProjectMetadata(
                builder.build(),
                projectMetadata.aliasedIndices,
                projectMetadata.templates,
                projectMetadata.customs,
                projectMetadata.totalNumberOfShards,
                projectMetadata.totalOpenIndexShards,
                projectMetadata.allIndices,
                projectMetadata.visibleIndices,
                projectMetadata.allOpenIndices,
                projectMetadata.visibleOpenIndices,
                projectMetadata.allClosedIndices,
                projectMetadata.visibleClosedIndices,
                projectMetadata.indicesLookup,
                projectMetadata.mappingsByHash,
                projectMetadata.oldestIndexVersion
            ),
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
    }

    public Metadata withIndexSettingsUpdates(final Map<Index, Settings> updates) {
        Objects.requireNonNull(updates, "no indices to update settings for");

        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(projectMetadata.indices);
        updates.forEach((index, settings) -> {
            IndexMetadata previous = builder.remove(index.getName());
            assert previous != null : index;
            builder.put(
                index.getName(),
                IndexMetadata.builder(previous).settingsVersion(previous.getSettingsVersion() + 1L).settings(settings).build()
            );
        });
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            new ProjectMetadata(
                builder.build(),
                projectMetadata.aliasedIndices,
                projectMetadata.templates,
                projectMetadata.customs,
                projectMetadata.totalNumberOfShards,
                projectMetadata.totalOpenIndexShards,
                projectMetadata.allIndices,
                projectMetadata.visibleIndices,
                projectMetadata.allOpenIndices,
                projectMetadata.visibleOpenIndices,
                projectMetadata.allClosedIndices,
                projectMetadata.visibleClosedIndices,
                projectMetadata.indicesLookup,
                projectMetadata.mappingsByHash,
                projectMetadata.oldestIndexVersion
            ),
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
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
        final var updatedIndicesBuilder = ImmutableOpenMap.builder(projectMetadata.indices);
        updatedIndicesBuilder.putAllFromMap(updates);
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            new ProjectMetadata(
                updatedIndicesBuilder.build(),
                projectMetadata.aliasedIndices,
                projectMetadata.templates,
                projectMetadata.customs,
                projectMetadata.totalNumberOfShards,
                projectMetadata.totalOpenIndexShards,
                projectMetadata.allIndices,
                projectMetadata.visibleIndices,
                projectMetadata.allOpenIndices,
                projectMetadata.visibleOpenIndices,
                projectMetadata.allClosedIndices,
                projectMetadata.visibleClosedIndices,
                projectMetadata.indicesLookup,
                projectMetadata.mappingsByHash,
                projectMetadata.oldestIndexVersion
            ),
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
        final String indexName = index.getIndex().getName();
        ensureNoNameCollision(indexName);
        final Map<String, AliasMetadata> aliases = index.getAliases();
        final ImmutableOpenMap<String, Set<Index>> updatedAliases = aliasesAfterAddingIndex(index, aliases);
        final String[] updatedVisibleIndices;
        if (index.isHidden()) {
            updatedVisibleIndices = projectMetadata.visibleIndices;
        } else {
            updatedVisibleIndices = ArrayUtils.append(projectMetadata.visibleIndices, indexName);
        }

        final String[] updatedAllIndices = ArrayUtils.append(projectMetadata.allIndices, indexName);
        final String[] updatedOpenIndices;
        final String[] updatedClosedIndices;
        final String[] updatedVisibleOpenIndices;
        final String[] updatedVisibleClosedIndices;
        switch (index.getState()) {
            case OPEN -> {
                updatedOpenIndices = ArrayUtils.append(projectMetadata.allOpenIndices, indexName);
                if (index.isHidden() == false) {
                    updatedVisibleOpenIndices = ArrayUtils.append(projectMetadata.visibleOpenIndices, indexName);
                } else {
                    updatedVisibleOpenIndices = projectMetadata.visibleOpenIndices;
                }
                updatedVisibleClosedIndices = projectMetadata.visibleClosedIndices;
                updatedClosedIndices = projectMetadata.allClosedIndices;
            }
            case CLOSE -> {
                updatedOpenIndices = projectMetadata.allOpenIndices;
                updatedClosedIndices = ArrayUtils.append(projectMetadata.allClosedIndices, indexName);
                updatedVisibleOpenIndices = projectMetadata.visibleOpenIndices;
                if (index.isHidden() == false) {
                    updatedVisibleClosedIndices = ArrayUtils.append(projectMetadata.visibleClosedIndices, indexName);
                } else {
                    updatedVisibleClosedIndices = projectMetadata.visibleClosedIndices;
                }
            }
            default -> throw new AssertionError("impossible, index is either open or closed");
        }

        final MappingMetadata mappingMetadata = index.mapping();
        final Map<String, MappingMetadata> updatedMappingsByHash;
        if (mappingMetadata == null) {
            updatedMappingsByHash = projectMetadata.mappingsByHash;
        } else {
            final MappingMetadata existingMapping = projectMetadata.mappingsByHash.get(mappingMetadata.getSha256());
            if (existingMapping != null) {
                index = index.withMappingMetadata(existingMapping);
                updatedMappingsByHash = projectMetadata.mappingsByHash;
            } else {
                updatedMappingsByHash = Maps.copyMapWithAddedEntry(
                    projectMetadata.mappingsByHash,
                    mappingMetadata.getSha256(),
                    mappingMetadata
                );
            }
        }

        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(projectMetadata.indices);
        builder.put(indexName, index);
        final ImmutableOpenMap<String, IndexMetadata> indicesMap = builder.build();
        for (var entry : updatedAliases.entrySet()) {
            List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
            ProjectMetadata.Builder.validateAlias(entry.getKey(), aliasIndices);
        }
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            new ProjectMetadata(
                indicesMap,
                updatedAliases,
                projectMetadata.templates,
                projectMetadata.customs,
                projectMetadata.totalNumberOfShards + index.getTotalNumberOfShards(),
                projectMetadata.totalOpenIndexShards + (index.getState() == IndexMetadata.State.OPEN ? index.getTotalNumberOfShards() : 0),
                updatedAllIndices,
                updatedVisibleIndices,
                updatedOpenIndices,
                updatedVisibleOpenIndices,
                updatedClosedIndices,
                updatedVisibleClosedIndices,
                null,
                updatedMappingsByHash,
                IndexVersion.min(index.getCompatibilityVersion(), projectMetadata.oldestIndexVersion)
            ),
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            customs,
            reservedStateMetadata
        );
    }

    private ImmutableOpenMap<String, Set<Index>> aliasesAfterAddingIndex(IndexMetadata index, Map<String, AliasMetadata> aliases) {
        if (aliases.isEmpty()) {
            return projectMetadata.aliasedIndices;
        }
        final String indexName = index.getIndex().getName();
        final ImmutableOpenMap.Builder<String, Set<Index>> aliasesBuilder = ImmutableOpenMap.builder(projectMetadata.aliasedIndices);
        for (String alias : aliases.keySet()) {
            ensureNoNameCollision(alias);
            if (projectMetadata.aliasedIndices.containsKey(indexName)) {
                throw new IllegalArgumentException("alias with name [" + indexName + "] already exists");
            }
            final Set<Index> found = aliasesBuilder.get(alias);
            final Set<Index> updated;
            if (found == null) {
                updated = Set.of(index.getIndex());
            } else {
                final Set<Index> tmp = new HashSet<>(found);
                tmp.add(index.getIndex());
                updated = Set.copyOf(tmp);
            }
            aliasesBuilder.put(alias, updated);
        }
        return aliasesBuilder.build();
    }

    private void ensureNoNameCollision(String indexName) {
        if (projectMetadata.indices.containsKey(indexName)) {
            throw new IllegalArgumentException("index with name [" + indexName + "] already exists");
        }
        if (dataStreams().containsKey(indexName)) {
            throw new IllegalArgumentException("data stream with name [" + indexName + "] already exists");
        }
        if (dataStreamAliases().containsKey(indexName)) {
            throw new IllegalStateException("data stream alias and indices alias have the same name (" + indexName + ")");
        }
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

    public ProjectMetadata getProject() {
        return this.projectMetadata;
    }

    public boolean equalsAliases(Metadata other) {
        for (IndexMetadata otherIndex : other.projectMetadata.indices().values()) {
            IndexMetadata thisIndex = projectMetadata.index(otherIndex.getIndex());
            if (thisIndex == null) {
                return false;
            }
            if (otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return false;
            }
        }

        if (other.dataStreamAliases().size() != dataStreamAliases().size()) {
            return false;
        }
        for (DataStreamAlias otherAlias : other.dataStreamAliases().values()) {
            DataStreamAlias thisAlias = dataStreamAliases().get(otherAlias.getName());
            if (thisAlias == null) {
                return false;
            }
            if (thisAlias.equals(otherAlias) == false) {
                return false;
            }
        }

        return true;
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
        return findAliases(Strings.EMPTY_ARRAY, concreteIndices);
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
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();

        AliasInfoGetter getter = index -> projectMetadata.indices.get(index).getAliases().values().stream().toList();

        AliasInfoSetter setter = (index, foundAliases) -> {
            List<AliasMetadata> d = new ArrayList<>();
            foundAliases.forEach(i -> d.add((AliasMetadata) i));
            mapBuilder.put(index, d);
        };

        findAliasInfo(aliases, concreteIndices, getter, setter);

        return mapBuilder.build();
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
        ImmutableOpenMap.Builder<String, List<DataStreamAlias>> mapBuilder = ImmutableOpenMap.builder();
        Map<String, List<DataStreamAlias>> dataStreamAliases = dataStreamAliasesByDataStream();

        AliasInfoGetter getter = dataStream -> dataStreamAliases.getOrDefault(dataStream, Collections.emptyList());

        AliasInfoSetter setter = (dataStream, foundAliases) -> {
            List<DataStreamAlias> dsAliases = new ArrayList<>();
            foundAliases.forEach(alias -> dsAliases.add((DataStreamAlias) alias));
            mapBuilder.put(dataStream, dsAliases);
        };

        findAliasInfo(aliases, dataStreams, getter, setter);

        return mapBuilder.build();
    }

    /**
     * Find the aliases that point to the specified data streams or indices. Called from findAliases or findDataStreamAliases.
     *
     * @param aliases The aliases to look for. Might contain include or exclude wildcards.
     * @param possibleMatches The data streams or indices that the aliases must point to in order to be returned
     * @param getter A function that is used to get the alises for a given data stream or index
     * @param setter A function that is used to keep track of the found aliases
     */
    private void findAliasInfo(final String[] aliases, final String[] possibleMatches, AliasInfoGetter getter, AliasInfoSetter setter) {
        assert aliases != null;
        assert possibleMatches != null;
        if (possibleMatches.length == 0) {
            return;
        }

        // create patterns to use to search for targets
        String[] patterns = new String[aliases.length];
        boolean[] include = new boolean[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            String alias = aliases[i];
            if (alias.charAt(0) == '-') {
                patterns[i] = alias.substring(1);
                include[i] = false;
            } else {
                patterns[i] = alias;
                include[i] = true;
            }
        }

        boolean matchAllAliases = patterns.length == 0;

        for (String index : possibleMatches) {
            List<AliasInfo> filteredValues = new ArrayList<>();

            List<? extends AliasInfo> entities = getter.get(index);
            for (AliasInfo aliasInfo : entities) {
                boolean matched = matchAllAliases;
                String alias = aliasInfo.getAlias();
                for (int i = 0; i < patterns.length; i++) {
                    if (include[i]) {
                        if (matched == false) {
                            String pattern = patterns[i];
                            matched = ALL.equals(pattern) || Regex.simpleMatch(pattern, alias);
                        }
                    } else if (matched) {
                        matched = Regex.simpleMatch(patterns[i], alias) == false;
                    }
                }
                if (matched) {
                    filteredValues.add(aliasInfo);
                }
            }
            if (filteredValues.isEmpty() == false) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasInfo::getAlias));
                setter.put(index, Collections.unmodifiableList(filteredValues));
            }
        }
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
        assert Transports.assertNotTransportThread("decompressing mappings is too expensive for a transport thread");

        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, MappingMetadata> indexMapBuilder = ImmutableOpenMap.builder();
        Set<String> indicesKeys = projectMetadata.indices.keySet();
        Stream.of(concreteIndices).filter(indicesKeys::contains).forEach(index -> {
            onNextIndex.run();
            IndexMetadata indexMetadata = projectMetadata.indices.get(index);
            Predicate<String> fieldPredicate = fieldFilter.apply(index);
            indexMapBuilder.put(index, filterFields(indexMetadata.mapping(), fieldPredicate));
        });
        return indexMapBuilder.build();
    }

    /**
     * Finds the parent data streams, if any, for the specified concrete indices.
     */
    public Map<String, DataStream> findDataStreams(String... concreteIndices) {
        assert concreteIndices != null;
        final ImmutableOpenMap.Builder<String, DataStream> builder = ImmutableOpenMap.builder();
        final SortedMap<String, IndexAbstraction> lookup = projectMetadata.getIndicesLookup();
        for (String indexName : concreteIndices) {
            IndexAbstraction index = lookup.get(indexName);
            assert index != null;
            assert index.getType() == IndexAbstraction.Type.CONCRETE_INDEX;
            if (index.getParentDataStream() != null) {
                builder.put(indexName, index.getParentDataStream());
            }
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static MappingMetadata filterFields(MappingMetadata mappingMetadata, Predicate<String> fieldPredicate) {
        if (mappingMetadata == null) {
            return MappingMetadata.EMPTY_MAPPINGS;
        }
        if (fieldPredicate == FieldPredicate.ACCEPT_ALL) {
            return mappingMetadata;
        }
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(mappingMetadata.source().compressedReference(), true).v2();
        Map<String, Object> mapping;
        if (sourceAsMap.size() == 1 && sourceAsMap.containsKey(mappingMetadata.type())) {
            mapping = (Map<String, Object>) sourceAsMap.get(mappingMetadata.type());
        } else {
            mapping = sourceAsMap;
        }

        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        if (properties == null || properties.isEmpty()) {
            return mappingMetadata;
        }

        filterFields("", properties, fieldPredicate);

        return new MappingMetadata(mappingMetadata.type(), sourceAsMap);
    }

    @SuppressWarnings("unchecked")
    private static boolean filterFields(String currentPath, Map<String, Object> fields, Predicate<String> fieldPredicate) {
        assert fieldPredicate != FieldPredicate.ACCEPT_ALL;
        Iterator<Map.Entry<String, Object>> entryIterator = fields.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, Object> entry = entryIterator.next();
            String newPath = mergePaths(currentPath, entry.getKey());
            Object value = entry.getValue();
            boolean mayRemove = true;
            boolean isMultiField = false;
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                Map<String, Object> properties = (Map<String, Object>) map.get("properties");
                if (properties != null) {
                    mayRemove = filterFields(newPath, properties, fieldPredicate);
                } else {
                    Map<String, Object> subFields = (Map<String, Object>) map.get("fields");
                    if (subFields != null) {
                        isMultiField = true;
                        if (mayRemove = filterFields(newPath, subFields, fieldPredicate)) {
                            map.remove("fields");
                        }
                    }
                }
            } else {
                throw new IllegalStateException("cannot filter mappings, found unknown element of type [" + value.getClass() + "]");
            }

            // only remove a field if it has no sub-fields left and it has to be excluded
            if (fieldPredicate.test(newPath) == false) {
                if (mayRemove) {
                    entryIterator.remove();
                } else if (isMultiField) {
                    // multi fields that should be excluded but hold subfields that don't have to be excluded are converted to objects
                    Map<String, Object> map = (Map<String, Object>) value;
                    Map<String, Object> subFields = (Map<String, Object>) map.get("fields");
                    assert subFields.size() > 0;
                    map.put("properties", subFields);
                    map.remove("fields");
                    map.remove("type");
                }
            }
        }
        // return true if the ancestor may be removed, as it has no sub-fields left
        return fields.size() == 0;
    }

    private static String mergePaths(String path, String field) {
        if (path.length() == 0) {
            return field;
        }
        return path + "." + field;
    }

    /**
     * Returns whether an alias exists with provided alias name.
     *
     * @param aliasName The provided alias name
     * @return whether an alias exists with provided alias name
     */
    public boolean hasAlias(String aliasName) {
        return projectMetadata.aliasedIndices.containsKey(aliasName) || dataStreamAliases().containsKey(aliasName);
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return Optional.ofNullable((ComponentTemplateMetadata) this.projectMetadata.custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) projectMetadata.custom(ComposableIndexTemplateMetadata.TYPE))
            .map(ComposableIndexTemplateMetadata::indexTemplates)
            .orElse(Collections.emptyMap());
    }

    public boolean isTimeSeriesTemplate(ComposableIndexTemplate indexTemplate) {
        if (indexTemplate.getDataStreamTemplate() == null) {
            return false;
        }

        var settings = MetadataIndexTemplateService.resolveSettings(indexTemplate, componentTemplates());
        // Not using IndexSettings.MODE.get() to avoid validation that may fail at this point.
        var rawIndexMode = settings.get(IndexSettings.MODE.getKey());
        var indexMode = rawIndexMode != null ? Enum.valueOf(IndexMode.class, rawIndexMode.toUpperCase(Locale.ROOT)) : null;
        if (indexMode == IndexMode.TIME_SERIES) {
            // No need to check for the existence of index.routing_path here, because index.mode=time_series can't be specified without it.
            // Setting validation takes care of this.
            // Also no need to validate that the fields defined in index.routing_path are keyword fields with time_series_dimension
            // attribute enabled. This is validated elsewhere (DocumentMapper).
            return true;
        }

        // in a followup change: check the existence of keyword fields of type keyword and time_series_dimension attribute enabled in
        // the template. In this case the index.routing_path setting can be generated from the mapping.

        return false;
    }

    public Map<String, DataStream> dataStreams() {
        return projectMetadata.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).dataStreams();
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return projectMetadata.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).getDataStreamAliases();
    }

    /**
     * Return a map of DataStreamAlias objects by DataStream name
     * @return a map of DataStreamAlias objects by DataStream name
     */
    public Map<String, List<DataStreamAlias>> dataStreamAliasesByDataStream() {
        Map<String, List<DataStreamAlias>> dataStreamAliases = new HashMap<>();

        for (DataStreamAlias dsAlias : dataStreamAliases().values()) {
            for (String dataStream : dsAlias.getDataStreams()) {
                if (dataStreamAliases.containsKey(dataStream) == false) {
                    dataStreamAliases.put(dataStream, new ArrayList<>());
                }
                dataStreamAliases.get(dataStream).add(dsAlias);
            }
        }

        return dataStreamAliases;
    }

    public NodesShutdownMetadata nodeShutdowns() {
        return custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
    }

    /**
     * Indicates if the provided index is managed by ILM. This takes into account if the index is part of
     * data stream that's potentially managed by data stream lifecycle and the value of the
     * {@link org.elasticsearch.index.IndexSettings#PREFER_ILM_SETTING}
     */
    public boolean isIndexManagedByILM(IndexMetadata indexMetadata) {
        if (Strings.hasText(indexMetadata.getLifecyclePolicyName()) == false) {
            // no ILM policy configured so short circuit this to *not* managed by ILM
            return false;
        }

        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(indexMetadata.getIndex().getName());
        if (indexAbstraction == null) {
            // index doesn't exist anymore
            return false;
        }

        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        if (parentDataStream != null && parentDataStream.getLifecycle() != null && parentDataStream.getLifecycle().isEnabled()) {
            // index has both ILM and data stream lifecycle configured so let's check which is preferred
            return PREFER_ILM_SETTING.get(indexMetadata.getSettings());
        }

        return true;
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

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return projectMetadata.custom(IndexGraveyard.TYPE);
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
        if (metadata1.projectMetadata.templates.equals(metadata2.projectMetadata.templates()) == false) {
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
        if (customsEqual(metadata1.projectMetadata.customs, metadata2.projectMetadata.customs) == false) {
            return false;
        }
        if (Objects.equals(metadata1.reservedStateMetadata, metadata2.reservedStateMetadata) == false) {
            return false;
        }
        return true;
    }

    private static <C extends MetadataCustom<C>> boolean customsEqual(
        ImmutableOpenMap<String, C> customs1,
        ImmutableOpenMap<String, C> customs2
    ) {
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
        XContentContext context = XContentContext.valueOf(p.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));
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

        final Iterator<? extends ToXContent> indices = context == XContentContext.API
            ? ChunkedToXContentHelper.wrapWithObject("indices", projectMetadata.indices().values().iterator())
            : Collections.emptyIterator();

        final Iterator<ToXContent> projectCustoms = Iterators.flatMap(
            this.projectMetadata.customs.entrySet().iterator(),
            entry -> entry.getValue().context().contains(context)
                ? ChunkedToXContentHelper.wrapWithObject(entry.getKey(), entry.getValue().toXContentChunked(p))
                : Collections.emptyIterator()
        );
        return Iterators.concat(start, Iterators.single((builder, params) -> {
            builder.field("cluster_uuid", clusterUUID);
            builder.field("cluster_uuid_committed", clusterUUIDCommitted);
            builder.startObject("cluster_coordination");
            coordinationMetadata().toXContent(builder, params);
            return builder.endObject();
        }),
            persistentSettings,
            ChunkedToXContentHelper.wrapWithObject(
                "templates",
                Iterators.map(
                    projectMetadata.templates().values().iterator(),
                    template -> (builder, params) -> IndexTemplateMetadata.Builder.toXContentWithTypes(template, builder, params)
                )
            ),
            indices,
            Iterators.flatMap(
                customs.entrySet().iterator(),
                entry -> entry.getValue().context().contains(context)
                    ? ChunkedToXContentHelper.wrapWithObject(entry.getKey(), entry.getValue().toXContentChunked(p))
                    : Collections.emptyIterator()
            ),
            // For BWC, the API does not have an embedded "project:", but other contexts do
            context == XContentContext.API ? projectCustoms : ChunkedToXContentHelper.wrapWithObject("project", projectCustoms),
            ChunkedToXContentHelper.wrapWithObject("reserved_state", reservedStateMetadata().values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    public Map<String, MappingMetadata> getMappingsByHash() {
        return projectMetadata.mappingsByHash;
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
        private final Diff<ImmutableOpenMap<String, IndexMetadata>> indices;
        private final Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates;
        private final MapDiff<String, ClusterCustom, ImmutableOpenMap<String, ClusterCustom>> clusterCustoms;
        private final MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> projectCustoms;
        private final Diff<Map<String, ReservedStateMetadata>> reservedStateMetadata;

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
            if (empty) {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
                indices = DiffableUtils.emptyDiff();
                templates = DiffableUtils.emptyDiff();
                clusterCustoms = DiffableUtils.emptyDiff();
                projectCustoms = DiffableUtils.emptyDiff();
                reservedStateMetadata = DiffableUtils.emptyDiff();
            } else {
                hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
                indices = DiffableUtils.diff(
                    before.projectMetadata.indices,
                    after.projectMetadata.indices,
                    DiffableUtils.getStringKeySerializer()
                );
                templates = DiffableUtils.diff(
                    before.projectMetadata.templates,
                    after.projectMetadata.templates,
                    DiffableUtils.getStringKeySerializer()
                );
                clusterCustoms = DiffableUtils.diff(
                    before.customs,
                    after.customs,
                    DiffableUtils.getStringKeySerializer(),
                    CLUSTER_CUSTOM_VALUE_SERIALIZER
                );
                projectCustoms = DiffableUtils.diff(
                    before.projectMetadata.customs,
                    after.projectMetadata.customs,
                    DiffableUtils.getStringKeySerializer(),
                    PROJECT_CUSTOM_VALUE_SERIALIZER
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
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
                hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            } else {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
            }
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            var bwcCustoms = readBwcCustoms(in);
            clusterCustoms = bwcCustoms.v1();
            projectCustoms = bwcCustoms.v2();
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
                MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>>>
            readBwcCustoms(StreamInput in) throws IOException {
            MapDiff<String, MetadataCustom<?>, ImmutableOpenMap<String, MetadataCustom<?>>> customs = DiffableUtils
                .readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), BWC_CUSTOM_VALUE_SERIALIZER);
            return DiffableUtils.split(
                customs,
                in.namedWriteableRegistry().getReaders(ClusterCustom.class).keySet(),
                CLUSTER_CUSTOM_VALUE_SERIALIZER,
                in.namedWriteableRegistry().getReaders(ProjectCustom.class).keySet(),
                PROJECT_CUSTOM_VALUE_SERIALIZER
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
            indices.writeTo(out);
            templates.writeTo(out);
            buildUnifiedCustomDiff().writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                reservedStateMetadata.writeTo(out);
            }
        }

        @SuppressWarnings("unchecked")
        private Diff<ImmutableOpenMap<String, ?>> buildUnifiedCustomDiff() {
            return DiffableUtils.merge(clusterCustoms, projectCustoms, DiffableUtils.getStringKeySerializer(), BWC_CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public Metadata apply(Metadata part) {
            if (empty) {
                return part;
            }
            // create builder from existing mappings hashes so we don't change existing index metadata instances when deduplicating
            // mappings in the builder
            final var updatedIndices = indices.apply(part.projectMetadata.indices);
            Builder builder = new Builder(part.projectMetadata.mappingsByHash, updatedIndices.size());
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(updatedIndices);
            builder.templates(templates.apply(part.projectMetadata.templates));
            builder.customs(clusterCustoms.apply(part.customs));
            builder.projectCustoms(projectCustoms.apply(part.projectMetadata.customs));
            builder.put(reservedStateMetadata.apply(part.reservedStateMetadata));
            if (part.projectMetadata.indices == updatedIndices
                && builder.dataStreamMetadata() == part.projectMetadata.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY)) {
                builder.projectMetadata.previousIndicesLookup = part.projectMetadata.indicesLookup;
            }
            return builder.build(true);
        }
    }

    public static final TransportVersion MAPPINGS_AS_HASH_VERSION = TransportVersions.V_8_1_0;

    public static Metadata readFrom(StreamInput in) throws IOException {
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
        if (in.getTransportVersion().onOrAfter(MAPPINGS_AS_HASH_VERSION)) {
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

    private static <T extends NamedWriteable> void readBwcCustoms(StreamInput in, Builder builder) throws IOException {
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
                builder.putProjectCustom(custom.getWriteableName(), custom);
            } else {
                throw new IllegalArgumentException("Unknown custom name [" + name + "]");
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_3_0)) {
            hashesOfConsistentSettings.writeTo(out);
        }
        // Starting in #MAPPINGS_AS_HASH_VERSION we write the mapping metadata first and then write the indices without metadata so that
        // we avoid writing duplicate mappings twice
        if (out.getTransportVersion().onOrAfter(MAPPINGS_AS_HASH_VERSION)) {
            out.writeMapValues(projectMetadata.mappingsByHash);
        }
        out.writeVInt(projectMetadata.indices.size());
        final boolean writeMappingsHash = out.getTransportVersion().onOrAfter(MAPPINGS_AS_HASH_VERSION);
        for (IndexMetadata indexMetadata : projectMetadata) {
            indexMetadata.writeTo(out, writeMappingsHash);
        }
        out.writeCollection(projectMetadata.templates.values());
        // It would be nice to do this as flattening iterable (rather than allocation a whole new list), but flattening
        // Iterable<? extends VersionNamedWriteable> into Iterable<VersionNamedWriteable> is messy, so we can fix that later
        List<VersionedNamedWriteable> merge = new ArrayList<>(customs.size() + projectMetadata.customs.size());
        merge.addAll(customs.values());
        merge.addAll(projectMetadata.customs.values());
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

        private final ImmutableOpenMap.Builder<String, ClusterCustom> customs;

        private final ProjectMetadata.Builder projectMetadata;

        private final Map<String, ReservedStateMetadata> reservedStateMetadata;

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
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.projectMetadata = ProjectMetadata.builder(metadata.projectMetadata);
            this.reservedStateMetadata = new HashMap<>(metadata.reservedStateMetadata);
        }

        @SuppressWarnings("this-escape")
        private Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            customs = ImmutableOpenMap.builder();
            projectMetadata = new ProjectMetadata.Builder(mappingsByHash, indexCountHint);
            reservedStateMetadata = new HashMap<>();
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            projectMetadata.put(indexMetadataBuilder);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            projectMetadata.put(indexMetadata, incrementVersion);
            return this;
        }

        public IndexMetadata get(String index) {
            return projectMetadata.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            return projectMetadata.getSafe(index);
        }

        public Builder remove(String index) {
            projectMetadata.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            projectMetadata.removeAllIndices();
            return this;
        }

        public Builder indices(Map<String, IndexMetadata> indices) {
            projectMetadata.indices(indices);
            return this;
        }

        public Builder put(IndexTemplateMetadata.Builder template) {
            projectMetadata.put(template);
            return this;
        }

        public Builder put(IndexTemplateMetadata template) {
            projectMetadata.put(template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            projectMetadata.removeTemplate(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            projectMetadata.templates(templates);
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            projectMetadata.put(name, componentTemplate);
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            projectMetadata.removeComponentTemplate(name);
            return this;
        }

        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            projectMetadata.componentTemplates(componentTemplates);
            return this;
        }

        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            projectMetadata.indexTemplates(indexTemplates);
            return this;
        }

        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            projectMetadata.put(name, indexTemplate);
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            projectMetadata.removeIndexTemplate(name);
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            return projectMetadata.dataStream(dataStreamName);
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            projectMetadata.dataStreams(dataStreams, dataStreamAliases);
            return this;
        }

        public Builder put(DataStream dataStream) {
            projectMetadata.put(dataStream);
            return this;
        }

        public DataStreamMetadata dataStreamMetadata() {
            return projectMetadata.dataStreamMetadata();
        }

        public boolean put(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
            return projectMetadata.put(aliasName, dataStream, isWriteDataStream, filter);
        }

        public Builder removeDataStream(String name) {
            projectMetadata.removeDataStream(name);
            return this;
        }

        public boolean removeDataStreamAlias(String aliasName, String dataStreamName, boolean mustExist) {
            return projectMetadata.removeDataStreamAlias(aliasName, dataStreamName, mustExist);
        }

        public Builder putCustom(String type, ClusterCustom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

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

        public ProjectCustom getProjectCustom(String type) {
            return projectMetadata.getCustom(type);
        }

        public Builder putProjectCustom(String type, ProjectCustom custom) {
            projectMetadata.putCustom(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeProjectCustom(String type) {
            projectMetadata.removeCustom(type);
            return this;
        }

        public Builder removeProjectCustomIf(BiPredicate<String, ? super ProjectCustom> p) {
            projectMetadata.removeCustomIf(p);
            return this;
        }

        public Builder projectCustoms(Map<String, ProjectCustom> projectCustoms) {
            projectCustoms.forEach((key, value) -> Objects.requireNonNull(value, key));
            projectMetadata.customs(projectCustoms);
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
            putCustom(IndexGraveyard.TYPE, indexGraveyard);
            return this;
        }

        public IndexGraveyard indexGraveyard() {
            return (IndexGraveyard) getProjectCustom(IndexGraveyard.TYPE);
        }

        public Builder updateSettings(Settings settings, String... indices) {
            projectMetadata.updateSettings(settings, indices);
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
            projectMetadata.updateNumberOfReplicas(numberOfReplicas, indices);
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
            return build(false);
        }

        public Metadata build(boolean skipNameCollisionChecks) {
            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                projectMetadata.build(skipNameCollisionChecks),
                transientSettings,
                persistentSettings,
                Settings.builder().put(persistentSettings).put(transientSettings).build(),
                hashesOfConsistentSettings,
                customs.build(),
                Collections.unmodifiableMap(reservedStateMetadata)
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
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                        }
                    } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                        builder.hashesOfConsistentSettings(parser.mapStrings());
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else if ("reserved_state".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(ReservedStateMetadata.fromXContent(parser));
                        }
                    } else if ("project".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                                    parseCustomObject(parser, currentFieldName, ProjectCustom.class, builder::putProjectCustom);
                                }
                            }
                        }
                    } else {
                        // Older clusters didn't separate cluster-scoped and project-scope customs so a top-level custom object might
                        // actually be a project-scoped custom
                        final NamedXContentRegistry registry = parser.getXContentRegistry();
                        if (registry.hasParser(ClusterCustom.class, currentFieldName, parser.getRestApiVersion())) {
                            parseCustomObject(parser, currentFieldName, ClusterCustom.class, builder::putCustom);
                        } else if (registry.hasParser(ProjectCustom.class, currentFieldName, parser.getRestApiVersion())) {
                            parseCustomObject(parser, currentFieldName, ProjectCustom.class, builder::putProjectCustom);
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
