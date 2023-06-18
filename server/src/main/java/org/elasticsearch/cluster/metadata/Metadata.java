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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexAbstraction.ConcreteIndex;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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
public class Metadata extends AbstractCollection<IndexMetadata> implements Diffable<Metadata>, ChunkedToXContent {

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

    /**
     * Custom metadata that persists (via XContent) across restarts. The deserialization method for each implementation must be registered
     * with the {@link NamedXContentRegistry}.
     */
    public interface Custom extends NamedDiffable<Custom>, ChunkedToXContent {

        EnumSet<XContentContext> context();

        /**
         * @return true if this custom could be restored from snapshot
         */
        default boolean isRestorable() {
            return context().contains(XContentContext.SNAPSHOT);
        }
    }

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

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, Set<Index>> aliasedIndices;
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, Custom> customs;
    private final Map<String, ReservedStateMetadata> reservedStateMetadata;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private volatile SortedMap<String, IndexAbstraction> indicesLookup;
    private final Map<String, MappingMetadata> mappingsByHash;

    private final Version oldestIndexVersion;

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
        ImmutableOpenMap<String, Custom> customs,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, MappingMetadata> mappingsByHash,
        Version oldestIndexVersion,
        Map<String, ReservedStateMetadata> reservedStateMetadata
    ) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = settings;
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.indices = indices;
        this.aliasedIndices = aliasedIndices;
        this.customs = customs;
        this.templates = templates;
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;
        this.allIndices = allIndices;
        this.visibleIndices = visibleIndices;
        this.allOpenIndices = allOpenIndices;
        this.visibleOpenIndices = visibleOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.visibleClosedIndices = visibleClosedIndices;
        this.indicesLookup = indicesLookup;
        this.mappingsByHash = mappingsByHash;
        this.oldestIndexVersion = oldestIndexVersion;
        this.reservedStateMetadata = reservedStateMetadata;
        assert assertConsistent();
    }

    private boolean assertConsistent() {
        final var lookup = this.indicesLookup;
        final var dsMetadata = custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY);
        assert lookup == null || lookup.equals(Builder.buildIndicesLookup(dsMetadata, indices));
        try {
            Builder.ensureNoNameCollisions(aliasedIndices.keySet(), indices, dsMetadata);
        } catch (Exception e) {
            assert false : e;
        }
        assert Builder.assertDataStreams(indices, dsMetadata);
        assert Set.of(allIndices).equals(indices.keySet());
        final Function<Predicate<IndexMetadata>, Set<String>> indicesByPredicate = predicate -> indices.entrySet()
            .stream()
            .filter(entry -> predicate.test(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
        assert Set.of(allOpenIndices).equals(indicesByPredicate.apply(idx -> idx.getState() == IndexMetadata.State.OPEN));
        assert Set.of(allClosedIndices).equals(indicesByPredicate.apply(idx -> idx.getState() == IndexMetadata.State.CLOSE));
        assert Set.of(visibleIndices).equals(indicesByPredicate.apply(idx -> idx.isHidden() == false));
        assert Set.of(visibleOpenIndices)
            .equals(indicesByPredicate.apply(idx -> idx.isHidden() == false && idx.getState() == IndexMetadata.State.OPEN));
        assert Set.of(visibleClosedIndices)
            .equals(indicesByPredicate.apply(idx -> idx.isHidden() == false && idx.getState() == IndexMetadata.State.CLOSE));
        return true;
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
            totalNumberOfShards,
            totalOpenIndexShards,
            indices,
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
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

        IndexMetadata indexMetadata = getIndexSafe(index);
        if (lifecycleState.equals(indexMetadata.getLifecycleExecutionState())) {
            return this;
        }

        // build a new index metadata with the version incremented and the new lifecycle state
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
        indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
        indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap());

        // drop it into the indices
        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
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
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            totalNumberOfShards,
            totalOpenIndexShards,
            builder.build(),
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
            reservedStateMetadata
        );
    }

    public Metadata withIndexSettingsUpdates(final Map<Index, Settings> updates) {
        Objects.requireNonNull(updates, "no indices to update settings for");

        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
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
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            totalNumberOfShards,
            totalOpenIndexShards,
            builder.build(),
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
            reservedStateMetadata
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
            totalNumberOfShards,
            totalOpenIndexShards,
            indices,
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
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
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            totalNumberOfShards,
            totalOpenIndexShards,
            indices,
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
            reservedStateMetadata
        );
    }

    /**
     * Creates a copy of this instance updated with the given {@link IndexMetadata} that must only contain changes to primary terms
     * and in-sync allocation ids relative to the existing entries. This method is only used by
     * {@link org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater#applyChanges(Metadata, RoutingTable)}.
     * @param updates map of index name to {@link IndexMetadata}.
     * @return updated metadata instance
     */
    public Metadata withAllocationAndTermUpdatesOnly(Map<String, IndexMetadata> updates) {
        if (updates.isEmpty()) {
            return this;
        }
        final var updatedIndicesBuilder = ImmutableOpenMap.builder(indices);
        updatedIndicesBuilder.putAllFromMap(updates);
        return new Metadata(
            clusterUUID,
            clusterUUIDCommitted,
            version,
            coordinationMetadata,
            transientSettings,
            persistentSettings,
            settings,
            hashesOfConsistentSettings,
            totalNumberOfShards,
            totalOpenIndexShards,
            updatedIndicesBuilder.build(),
            aliasedIndices,
            templates,
            customs,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            oldestIndexVersion,
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
        final String[] updatedVisibleIndices = getUpdatedVisibleIndices(index, indexName);

        final String[] updatedAllIndices = ArrayUtils.append(allIndices, indexName);
        UpdatedOpenCloseIndices updatedOpenCloseIndices = getUpdatedOpenCloseIndices(index, indexName);

        final MappingMetadata mappingMetadata = index.mapping();
        IndexAndUpdatedMapByHash indexAndUpdatedMapByHash = getIndexAndUpdatedMapByHash(index, mappingMetadata);

        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
        builder.put(indexName, indexAndUpdatedMapByHash.index());
        final ImmutableOpenMap<String, IndexMetadata> indicesMap = builder.build();
        for (var entry : updatedAliases.entrySet()) {
            List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
            Builder.validateAlias(entry.getKey(), aliasIndices);
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
            totalNumberOfShards + indexAndUpdatedMapByHash.index().getTotalNumberOfShards(),
            totalOpenIndexShards + (indexAndUpdatedMapByHash.index().getState() == IndexMetadata.State.OPEN ? indexAndUpdatedMapByHash.index().getTotalNumberOfShards() : 0),
            indicesMap,
            updatedAliases,
            templates,
            customs,
            updatedAllIndices,
            updatedVisibleIndices,
            updatedOpenCloseIndices.updatedOpenIndices(),
            updatedOpenCloseIndices.updatedVisibleOpenIndices(),
            updatedOpenCloseIndices.updatedClosedIndices(),
            updatedOpenCloseIndices.updatedVisibleClosedIndices(),
            null,
            indexAndUpdatedMapByHash.updatedMappingsByHash(),
            indexAndUpdatedMapByHash.index().getCompatibilityVersion().before(oldestIndexVersion) ? indexAndUpdatedMapByHash.index().getCompatibilityVersion() : oldestIndexVersion,
            reservedStateMetadata
        );
    }

    private IndexAndUpdatedMapByHash getIndexAndUpdatedMapByHash(IndexMetadata index, MappingMetadata mappingMetadata) {
        final Map<String, MappingMetadata> updatedMappingsByHash;
        if (mappingMetadata == null) {
            updatedMappingsByHash = mappingsByHash;
        } else {
            final MappingMetadata existingMapping = mappingsByHash.get(mappingMetadata.getSha256());
            if (existingMapping != null) {
                index = index.withMappingMetadata(existingMapping);
                updatedMappingsByHash = mappingsByHash;
            } else {
                updatedMappingsByHash = Maps.copyMapWithAddedEntry(mappingsByHash, mappingMetadata.getSha256(), mappingMetadata);
            }
        }
        IndexAndUpdatedMapByHash indexAndUpdatedMapByHash = new IndexAndUpdatedMapByHash(index, updatedMappingsByHash);
        return indexAndUpdatedMapByHash;
    }

    private record IndexAndUpdatedMapByHash(IndexMetadata index, Map<String, MappingMetadata> updatedMappingsByHash) {
    }

    private UpdatedOpenCloseIndices getUpdatedOpenCloseIndices(IndexMetadata index, String indexName) {
        final String[] updatedVisibleOpenIndices;
        final String[] updatedOpenIndices;
        final String[] updatedVisibleClosedIndices;
        final String[] updatedClosedIndices;
        switch (index.getState()) {
            case OPEN -> {
                updatedOpenIndices = ArrayUtils.append(allOpenIndices, indexName);
                updatedVisibleOpenIndices = getUpdatedVisibleOpenIndices(index, indexName);
                updatedVisibleClosedIndices = visibleClosedIndices;
                updatedClosedIndices = allClosedIndices;
            }
            case CLOSE -> {
                updatedOpenIndices = allOpenIndices;
                updatedClosedIndices = ArrayUtils.append(allClosedIndices, indexName);
                updatedVisibleOpenIndices = visibleOpenIndices;
                updatedVisibleClosedIndices = getUpdatedVisibleClosedIndices(index, indexName);
            }
            default -> throw new AssertionError("impossible, index is either open or closed");
        }
        UpdatedOpenCloseIndices updatedOpenCloseIndices = new UpdatedOpenCloseIndices(updatedOpenIndices, updatedClosedIndices, updatedVisibleOpenIndices, updatedVisibleClosedIndices);
        return updatedOpenCloseIndices;
    }

    private record UpdatedOpenCloseIndices(String[] updatedOpenIndices, String[] updatedClosedIndices, String[] updatedVisibleOpenIndices, String[] updatedVisibleClosedIndices) {
    }

    private String[] getUpdatedVisibleClosedIndices(IndexMetadata index, String indexName) {
        final String[] updatedVisibleClosedIndices;
        if (index.isHidden() == false) {
            updatedVisibleClosedIndices = ArrayUtils.append(visibleClosedIndices, indexName);
        } else {
            updatedVisibleClosedIndices = visibleClosedIndices;
        }
        return updatedVisibleClosedIndices;
    }

    private String[] getUpdatedVisibleOpenIndices(IndexMetadata index, String indexName) {
        final String[] updatedVisibleOpenIndices;
        if (index.isHidden() == false) {
            updatedVisibleOpenIndices = ArrayUtils.append(visibleOpenIndices, indexName);
        } else {
            updatedVisibleOpenIndices = visibleOpenIndices;
        }
        return updatedVisibleOpenIndices;
    }

    private String[] getUpdatedVisibleIndices(IndexMetadata index, String indexName) {
        final String[] updatedVisibleIndices;
        if (index.isHidden()) {
            updatedVisibleIndices = visibleIndices;
        } else {
            updatedVisibleIndices = ArrayUtils.append(visibleIndices, indexName);
        }
        return updatedVisibleIndices;
    }

    private ImmutableOpenMap<String, Set<Index>> aliasesAfterAddingIndex(IndexMetadata index, Map<String, AliasMetadata> aliases) {
        if (aliases.isEmpty()) {
            return aliasedIndices;
        }
        final String indexName = index.getIndex().getName();
        final ImmutableOpenMap.Builder<String, Set<Index>> aliasesBuilder = ImmutableOpenMap.builder(aliasedIndices);
        for (String alias : aliases.keySet()) {
            ensureNoNameCollision(alias);
            if (aliasedIndices.containsKey(indexName)) {
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
        if (indices.containsKey(indexName)) {
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

    public Version oldestIndexVersion() {
        return this.oldestIndexVersion;
    }

    public boolean equalsAliases(Metadata other) {
        if (areIndexAliasesEqual(other)) return false;

        if (compareDataStreamAliases(other)) return false;

        return true;
    }

    private boolean compareDataStreamAliases(Metadata other) {
        if (other.dataStreamAliases().size() != dataStreamAliases().size()) {
            return true;
        }
        for (DataStreamAlias otherAlias : other.dataStreamAliases().values()) {
            DataStreamAlias thisAlias = dataStreamAliases().get(otherAlias.getName());
            if (thisAlias == null) {
                return true;
            }
            if (thisAlias.equals(otherAlias) == false) {
                return true;
            }
        }
        return false;
    }

    private boolean areIndexAliasesEqual(Metadata other) {
        for (IndexMetadata otherIndex : other.indices().values()) {
            IndexMetadata thisIndex = index(otherIndex.getIndex());
            if (thisIndex == null) {
                return true;
            }
            if (otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return true;
            }
        }
        return false;
    }

    public boolean indicesLookupInitialized() {
        return indicesLookup != null;
    }

    public SortedMap<String, IndexAbstraction> getIndicesLookup() {
        SortedMap<String, IndexAbstraction> lookup = indicesLookup;
        if (lookup == null) {
            lookup = buildIndicesLookup();
        }
        return lookup;
    }

    private synchronized SortedMap<String, IndexAbstraction> buildIndicesLookup() {
        SortedMap<String, IndexAbstraction> i = indicesLookup;
        if (i != null) {
            return i;
        }
        i = Builder.buildIndicesLookup(custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY), indices);
        indicesLookup = i;
        return i;
    }

    public boolean sameIndicesLookup(Metadata other) {
        return this.indicesLookup == other.indicesLookup;
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
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }
        String[] patterns = new String[aliases.length];
        boolean[] include = populateIncludes(aliases, patterns);
        boolean matchAllAliases = patterns.length == 0;
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();
        Map<String, List<AliasMetadata>> indexAlias = getIndexAliases(concreteIndices, patterns, include, matchAllAliases);
        mapBuilder.putAllFromMap(indexAlias);
        return mapBuilder.build();
    }

    private static boolean[] populateIncludes(String[] aliases, String[] patterns) {
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
        return include;
    }

    private Map<String, List<AliasMetadata>>  getIndexAliases(String[] concreteIndices, String[] patterns, boolean[] include, boolean matchAllAliases) {
        Map<String, List<AliasMetadata>> indexAlias = new HashMap<>();
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = indices.get(index);
            List<AliasMetadata> filteredValues = getFilteredValues(patterns, include, matchAllAliases, indexMetadata);
            if (filteredValues.isEmpty() == false) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasMetadata::alias));
                indexAlias.put(index, Collections.unmodifiableList(filteredValues));
            }
        }
        return indexAlias;
    }

    private static List<AliasMetadata> getFilteredValues(String[] patterns, boolean[] include, boolean matchAllAliases, IndexMetadata indexMetadata) {
        List<AliasMetadata> filteredValues = new ArrayList<>();
        for (AliasMetadata aliasMetadata : indexMetadata.getAliases().values()) {
            boolean matched = isMatched(patterns, include, matchAllAliases, aliasMetadata);
            if (matched) {
                filteredValues.add(aliasMetadata);
            }
        }
        return filteredValues;
    }

    private static boolean isMatched(String[] patterns, boolean[] include, boolean matchAllAliases, AliasMetadata aliasMetadata) {
        boolean matched = matchAllAliases;
        String alias = aliasMetadata.alias();
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
        return matched;
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
        Function<String, Predicate<String>> fieldFilter,
        Runnable onNextIndex
    ) {
        assert Transports.assertNotTransportThread("decompressing mappings is too expensive for a transport thread");

        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, MappingMetadata> indexMapBuilder = ImmutableOpenMap.builder();
        Set<String> indicesKeys = indices.keySet();
        Stream.of(concreteIndices).filter(indicesKeys::contains).forEach(index -> {
            onNextIndex.run();
            IndexMetadata indexMetadata = indices.get(index);
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
        final SortedMap<String, IndexAbstraction> lookup = getIndicesLookup();
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
        if (fieldPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
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
        assert fieldPredicate != MapperPlugin.NOOP_FIELD_PREDICATE;
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
     * Returns all the concrete indices.
     */
    public String[] getConcreteAllIndices() {
        return allIndices;
    }

    /**
     * Returns all the concrete indices that are not hidden.
     */
    public String[] getConcreteVisibleIndices() {
        return visibleIndices;
    }

    /**
     * Returns all of the concrete indices that are open.
     */
    public String[] getConcreteAllOpenIndices() {
        return allOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are open and not hidden.
     */
    public String[] getConcreteVisibleOpenIndices() {
        return visibleOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are closed.
     */
    public String[] getConcreteAllClosedIndices() {
        return allClosedIndices;
    }

    /**
     * Returns all of the concrete indices that are closed and not hidden.
     */
    public String[] getConcreteVisibleClosedIndices() {
        return visibleClosedIndices;
    }

    /**
     * Returns indexing routing for the given <code>aliasOrIndex</code>. Resolves routing from the alias metadata used
     * in the write index.
     */
    public String resolveWriteIndexRouting(@Nullable String routing, String aliasOrIndex) {
        if (aliasOrIndex == null) {
            return routing;
        }

        IndexAbstraction result = getIndicesLookup().get(aliasOrIndex);
        if (result == null || result.getType() != IndexAbstraction.Type.ALIAS) {
            return routing;
        }
        Index writeIndexName = result.getWriteIndex();
        if (writeIndexName == null) {
            throw new IllegalArgumentException("alias [" + aliasOrIndex + "] does not have a write index");
        }
        AliasMetadata writeIndexAliasMetadata = index(writeIndexName).getAliases().get(result.getName());
        if (writeIndexAliasMetadata != null) {
            return resolveRouting(routing, aliasOrIndex, writeIndexAliasMetadata);
        } else {
            return routing;
        }
    }

    /**
     * Returns indexing routing for the given index.
     */
    // TODO: This can be moved to IndexNameExpressionResolver too, but this means that we will support wildcards and other expressions
    // in the index,bulk,update and delete apis.
    public String resolveIndexRouting(@Nullable String routing, String aliasOrIndex) {
        if (aliasOrIndex == null) {
            return routing;
        }

        IndexAbstraction result = getIndicesLookup().get(aliasOrIndex);
        if (result == null || result.getType() != IndexAbstraction.Type.ALIAS) {
            return routing;
        }
        if (result.getIndices().size() > 1) {
            rejectSingleIndexOperation(aliasOrIndex, result);
        }
        return resolveRouting(routing, aliasOrIndex, AliasMetadata.getFirstAliasMetadata(this, result));
    }

    private static String resolveRouting(@Nullable String routing, String aliasOrIndex, AliasMetadata aliasMd) {
        if (aliasMd.indexRouting() != null) {
            validateRoutingValue(aliasOrIndex, aliasMd);
            validateRoutingAndAlias(routing, aliasOrIndex, aliasMd);
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
    }

    private static void validateRoutingAndAlias(String routing, String aliasOrIndex, AliasMetadata aliasMd) {
        if (routing != null) {
            if (routing.equals(aliasMd.indexRouting()) == false) {
                throw new IllegalArgumentException(
                    "Alias ["
                        + aliasOrIndex
                        + "] has index routing associated with it ["
                        + aliasMd.indexRouting()
                        + "], and was provided with routing value ["
                        + routing
                        + "], rejecting operation"
                );
            }
        }
    }

    private static void validateRoutingValue(String aliasOrIndex, AliasMetadata aliasMd) {
        if (aliasMd.indexRouting().indexOf(',') != -1) {
            throw new IllegalArgumentException(
                "index/alias ["
                    + aliasOrIndex
                    + "] provided with routing value ["
                    + aliasMd.getIndexRouting()
                    + "] that resolved to several routing values, rejecting operation"
            );
        }
    }

    private static void rejectSingleIndexOperation(String aliasOrIndex, IndexAbstraction result) {
        String[] indexNames = new String[result.getIndices().size()];
        int i = 0;
        for (Index indexName : result.getIndices()) {
            indexNames[i++] = indexName.getName();
        }
        throw new IllegalArgumentException(
            "Alias ["
                + aliasOrIndex
                + "] has more than one index associated with it ["
                + Arrays.toString(indexNames)
                + "], can't execute a single index op"
        );
    }

    /**
     * Checks whether an index exists (as of this {@link Metadata} with the given name. Does not check aliases or data streams.
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if a concrete index with that name exists, {@code false} otherwise.
     */
    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    /**
     * Checks whether an index exists. Similar to {@link Metadata#hasIndex(String)}, but ensures that the index has the same UUID as
     * the given {@link Index}.
     * @param index An {@link Index} object that may or may not exist in the cluster.
     * @return {@code true} if an index exists with the same name and UUID as the given index object, {@code false} otherwise.
     */
    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    /**
     * Checks whether an index abstraction (that is, index, alias, or data stream) exists (as of this {@link Metadata} with the given name.
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if an index abstraction with that name exists, {@code false} otherwise.
     */
    public boolean hasIndexAbstraction(String index) {
        return getIndicesLookup().containsKey(index);
    }

    public IndexMetadata index(String index) {
        return indices.get(index);
    }

    public IndexMetadata index(Index index) {
        IndexMetadata metadata = index(index.getName());
        if (metadata != null && metadata.getIndexUUID().equals(index.getUUID())) {
            return metadata;
        }
        return null;
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetadata getIndexSafe(Index index) {
        IndexMetadata metadata = index(index.getName());
        if (metadata != null) {
            if (metadata.getIndexUUID().equals(index.getUUID())) {
                return metadata;
            }
            throw new IndexNotFoundException(
                index,
                new IllegalStateException(
                    "index uuid doesn't match expected: [" + index.getUUID() + "] but got: [" + metadata.getIndexUUID() + "]"
                )
            );
        }
        throw new IndexNotFoundException(index);
    }

    public Map<String, IndexMetadata> indices() {
        return this.indices;
    }

    public Map<String, IndexMetadata> getIndices() {
        return indices();
    }

    /**
     * Returns whether an alias exists with provided alias name.
     *
     * @param aliasName The provided alias name
     * @return whether an alias exists with provided alias name
     */
    public boolean hasAlias(String aliasName) {
        return aliasedIndices.containsKey(aliasName) || dataStreamAliases().containsKey(aliasName);
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
        Objects.requireNonNull(aliasName);
        return aliasedIndices.getOrDefault(aliasName, Set.of());
    }

    /**
     * @return the names of all indices aliases.
     */
    public Set<String> aliasedIndices() {
        return aliasedIndices.keySet();
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return this.templates;
    }

    public Map<String, IndexTemplateMetadata> getTemplates() {
        return templates();
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return Optional.ofNullable((ComponentTemplateMetadata) this.custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) this.custom(ComposableIndexTemplateMetadata.TYPE))
            .map(ComposableIndexTemplateMetadata::indexTemplates)
            .orElse(Collections.emptyMap());
    }

    public boolean isTimeSeriesTemplate(ComposableIndexTemplate indexTemplate) {
        var template = indexTemplate.template();
        if (indexTemplate.getDataStreamTemplate() == null || template == null) {
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
        return this.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).dataStreams();
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return this.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).getDataStreamAliases();
    }

    public Map<String, SingleNodeShutdownMetadata> nodeShutdowns() {
        return this.custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY).getAllNodeMetadataMap();
    }

    /**
     * Indicates if the provided index is managed by ILM. This takes into account if the index is part of
     * data stream that's potentially managed by DLM and the value of the {@link org.elasticsearch.index.IndexSettings#PREFER_ILM_SETTING}
     */
    public boolean isIndexManagedByILM(IndexMetadata indexMetadata) {
        if (Strings.hasText(indexMetadata.getLifecyclePolicyName()) == false) {
            // no ILM policy configured so short circuit this to *not* managed by ILM
            return false;
        }

        IndexAbstraction indexAbstraction = getIndicesLookup().get(indexMetadata.getIndex().getName());
        if (indexAbstraction == null) {
            // index doesn't exist anymore
            return false;
        }

        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        if (parentDataStream != null && parentDataStream.getLifecycle() != null) {
            // index has both ILM and DLM configured so let's check which is preferred
            return PREFER_ILM_SETTING.get(indexMetadata.getSettings());
        }

        return true;
    }

    public Map<String, Custom> customs() {
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
        return custom(IndexGraveyard.TYPE);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenIndexShards;
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.values().iterator();
    }

    @Override
    public int size() {
        return indices.size();
    }

    public static boolean isGlobalStateEquals(Metadata metadata1, Metadata metadata2) {
        if (compareCoordinationAndPersistent(metadata1, metadata2)) return false;
        if (compareHashesOfConsistentAndTemplate(metadata1, metadata2)) return false;
        if (compareClusterParameter(metadata1, metadata2)) return false;
        // Check if any persistent metadata needs to be saved
        if (compareCustomMetadata(metadata1, metadata2)) return false;
        if (Objects.equals(metadata1.reservedStateMetadata, metadata2.reservedStateMetadata) == false) {
            return false;
        }
        return true;
    }

    private static boolean compareCustomMetadata(Metadata metadata1, Metadata metadata2) {
        int customCount1 = 0;
        for (Map.Entry<String, Custom> cursor : metadata1.customs.entrySet()) {
            if (cursor.getValue().context().contains(XContentContext.GATEWAY)) {
                if (cursor.getValue().equals(metadata2.custom(cursor.getKey())) == false) {
                    return true;
                }
                customCount1++;
            }
        }
        int customCount2 = 0;
        customCount2 = getCustomCount2(metadata2, customCount2);
        if (customCount1 != customCount2) {
            return true;
        }
        return false;
    }

    private static int getCustomCount2(Metadata metadata2, int customCount2) {
        for (Custom custom : metadata2.customs.values()) {
            if (custom.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        return customCount2;
    }

    private static boolean compareClusterParameter(Metadata metadata1, Metadata metadata2) {
        if (metadata1.clusterUUID.equals(metadata2.clusterUUID) == false) {
            return true;
        }
        if (metadata1.clusterUUIDCommitted != metadata2.clusterUUIDCommitted) {
            return true;
        }
        return false;
    }

    private static boolean compareHashesOfConsistentAndTemplate(Metadata metadata1, Metadata metadata2) {
        if (metadata1.hashesOfConsistentSettings.equals(metadata2.hashesOfConsistentSettings) == false) {
            return true;
        }
        if (metadata1.templates.equals(metadata2.templates()) == false) {
            return true;
        }
        return false;
    }

    private static boolean compareCoordinationAndPersistent(Metadata metadata1, Metadata metadata2) {
        if (metadata1.coordinationMetadata.equals(metadata2.coordinationMetadata) == false) {
            return true;
        }
        if (metadata1.persistentSettings.equals(metadata2.persistentSettings) == false) {
            return true;
        }
        return false;
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
        final Iterator<? extends ToXContent> start = getStart(context);

        final Iterator<? extends ToXContent> persistentSettings = context != XContentContext.API && persistentSettings().isEmpty() == false
            ? Iterators.single((builder, params) -> {
                builder.startObject("settings");
                persistentSettings().toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                return builder.endObject();
            })
            : Collections.emptyIterator();

        final Iterator<? extends ToXContent> indices = context == XContentContext.API
            ? ChunkedToXContentHelper.wrapWithObject("indices", indices().values().iterator())
            : Collections.emptyIterator();

        return Iterators.concat(start, Iterators.<ToXContent>single((builder, params) -> {
            builder.field("cluster_uuid", clusterUUID);
            builder.field("cluster_uuid_committed", clusterUUIDCommitted);
            builder.startObject("cluster_coordination");
            coordinationMetadata().toXContent(builder, params);
            return builder.endObject();
        }),
            persistentSettings,
            ChunkedToXContentHelper.wrapWithObject(
                "templates",
                templates().values()
                    .stream()
                    .map(
                        template -> (ToXContent) (builder, params) -> IndexTemplateMetadata.Builder.toXContentWithTypes(
                            template,
                            builder,
                            params
                        )
                    )
                    .iterator()
            ),
            indices,
            Iterators.flatMap(
                customs.entrySet().iterator(),
                entry -> entry.getValue().context().contains(context)
                    ? ChunkedToXContentHelper.wrapWithObject(entry.getKey(), entry.getValue().toXContentChunked(p))
                    : Collections.emptyIterator()
            ),
            ChunkedToXContentHelper.wrapWithObject("reserved_state", reservedStateMetadata().values().iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    private Iterator<? extends ToXContent> getStart(XContentContext context) {
        final Iterator<? extends ToXContent> start = context == XContentContext.API
            ? ChunkedToXContentHelper.startObject("metadata")
            : Iterators.single((builder, params) -> builder.startObject("meta-data").field("version", version()));
        return start;
    }

    public Map<String, MappingMetadata> getMappingsByHash() {
        return mappingsByHash;
    }

    private static class MetadataDiff implements Diff<Metadata> {

        private static final TransportVersion NOOP_METADATA_DIFF_VERSION = TransportVersion.V_8_5_0;
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
        private final Diff<ImmutableOpenMap<String, Custom>> customs;
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
                customs = DiffableUtils.emptyDiff();
                reservedStateMetadata = DiffableUtils.emptyDiff();
            } else {
                hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
                indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
                templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
                customs = DiffableUtils.diff(
                    before.customs,
                    after.customs,
                    DiffableUtils.getStringKeySerializer(),
                    CUSTOM_VALUE_SERIALIZER
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
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_3_0)) {
                hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            } else {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
            }
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                reservedStateMetadata = DiffableUtils.readJdkMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    RESERVED_DIFF_VALUE_READER
                );
            } else {
                reservedStateMetadata = DiffableUtils.emptyDiff();
            }
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
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_3_0)) {
                hashesOfConsistentSettings.writeTo(out);
            }
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                reservedStateMetadata.writeTo(out);
            }
        }

        @Override
        public Metadata apply(Metadata part) {
            if (empty) {
                return part;
            }
            // create builder from existing mappings hashes so we don't change existing index metadata instances when deduplicating
            // mappings in the builder
            final var updatedIndices = indices.apply(part.indices);
            Builder builder = new Builder(part.mappingsByHash, updatedIndices.size());
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(updatedIndices);
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            builder.put(reservedStateMetadata.apply(part.reservedStateMetadata));
            if (part.indices == updatedIndices
                && builder.dataStreamMetadata() == part.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY)) {
                builder.previousIndicesLookup = part.indicesLookup;
            }
            return builder.build(true);
        }
    }

    public static final TransportVersion MAPPINGS_AS_HASH_VERSION = TransportVersion.V_8_1_0;

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_3_0)) {
            builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        }
        final Function<String, MappingMetadata> mappingLookup = buildMappingLookup(in);
        createIndexFromMetadata(in, builder, mappingLookup);
        processReservedMetadata(in, builder);
        return builder.build();
    }

    private static void processReservedMetadata(StreamInput in, Builder builder) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            int reservedStateSize = in.readVInt();
            for (int i = 0; i < reservedStateSize; i++) {
                builder.put(ReservedStateMetadata.readFrom(in));
            }
        }
    }

    private static void createIndexFromMetadata(StreamInput in, Builder builder, Function<String, MappingMetadata> mappingLookup) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in, mappingLookup), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetadata.readFrom(in));
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
    }

    private static Function<String, MappingMetadata> buildMappingLookup(StreamInput in) throws IOException {
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
        return mappingLookup;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        transientSettings.writeTo(out);
        persistentSettings.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_3_0)) {
            hashesOfConsistentSettings.writeTo(out);
        }
        // Starting in #MAPPINGS_AS_HASH_VERSION we write the mapping metadata first and then write the indices without metadata so that
        // we avoid writing duplicate mappings twice
        if (out.getTransportVersion().onOrAfter(MAPPINGS_AS_HASH_VERSION)) {
            out.writeMapValues(mappingsByHash);
        }
        out.writeVInt(indices.size());
        final boolean writeMappingsHash = out.getTransportVersion().onOrAfter(MAPPINGS_AS_HASH_VERSION);
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out, writeMappingsHash);
        }
        out.writeCollection(templates.values());
        VersionedNamedWriteable.writeVersionedWritables(out, customs);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
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

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, Set<Index>> aliasedIndices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        private SortedMap<String, IndexAbstraction> previousIndicesLookup;

        private final Map<String, ReservedStateMetadata> reservedStateMetadata;

        // If this is set to false we can skip checking #mappingsByHash for unused entries in #build(). Used as an optimization to save
        // the rather expensive logic for removing unused mappings when building from another instance and we know that no mappings can
        // have become unused because no indices were updated or removed from this builder in a way that would cause unused entries in
        // #mappingsByHash.
        private boolean checkForUnusedMappings = true;

        private final Map<String, MappingMetadata> mappingsByHash;

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
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.aliasedIndices = ImmutableOpenMap.builder(metadata.aliasedIndices);
            this.templates = ImmutableOpenMap.builder(metadata.templates);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.previousIndicesLookup = metadata.indicesLookup;
            this.mappingsByHash = new HashMap<>(metadata.mappingsByHash);
            this.checkForUnusedMappings = false;
            this.reservedStateMetadata = new HashMap<>(metadata.reservedStateMetadata);
        }

        private Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder(indexCountHint);
            aliasedIndices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            reservedStateMetadata = new HashMap<>();
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
            previousIndicesLookup = null;
            this.mappingsByHash = new HashMap<>(mappingsByHash);
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            dedupeMapping(indexMetadataBuilder);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            IndexMetadata previous = indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            updateAliases(previous, indexMetadata);
            if (unsetPreviousIndicesLookup(previous, indexMetadata)) {
                previousIndicesLookup = null;
            }
            maybeSetMappingPurgeFlag(previous, indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            final String name = indexMetadata.getIndex().getName();
            indexMetadata = dedupeMapping(indexMetadata);
            IndexMetadata previous;
            if (incrementVersion) {
                if (indices.get(name) == indexMetadata) {
                    return this;
                }
                // if we put a new index metadata, increment its version
                indexMetadata = indexMetadata.withIncrementedVersion();
                previous = indices.put(name, indexMetadata);
            } else {
                previous = indices.put(name, indexMetadata);
                if (previous == indexMetadata) {
                    return this;
                }
            }
            updateAliases(previous, indexMetadata);
            if (unsetPreviousIndicesLookup(previous, indexMetadata)) {
                previousIndicesLookup = null;
            }
            maybeSetMappingPurgeFlag(previous, indexMetadata);
            return this;
        }

        private void maybeSetMappingPurgeFlag(@Nullable IndexMetadata previous, IndexMetadata updated) {
            if (checkForUnusedMappings) {
                return;
            }
            if (previous == null) {
                return;
            }
            final MappingMetadata mapping = previous.mapping();
            if (mapping == null) {
                return;
            }
            final MappingMetadata updatedMapping = updated.mapping();
            if (updatedMapping == null) {
                return;
            }
            if (mapping.getSha256().equals(updatedMapping.getSha256()) == false) {
                checkForUnusedMappings = true;
            }
        }

        private static boolean unsetPreviousIndicesLookup(IndexMetadata previous, IndexMetadata current) {
            if (previous == null) {
                return true;
            }

            if (previous.getAliases().equals(current.getAliases()) == false) {
                return true;
            }

            if (previous.isHidden() != current.isHidden()) {
                return true;
            }

            if (previous.isSystem() != current.isSystem()) {
                return true;
            }

            if (previous.getState() != current.getState()) {
                return true;
            }

            return false;
        }

        public IndexMetadata get(String index) {
            return indices.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            IndexMetadata indexMetadata = get(index.getName());
            if (indexMetadata != null) {
                if (indexMetadata.getIndexUUID().equals(index.getUUID())) {
                    return indexMetadata;
                }
                throw new IndexNotFoundException(
                    index,
                    new IllegalStateException(
                        "index uuid doesn't match expected: [" + index.getUUID() + "] but got: [" + indexMetadata.getIndexUUID() + "]"
                    )
                );
            }
            throw new IndexNotFoundException(index);
        }

        public Builder remove(String index) {
            previousIndicesLookup = null;
            checkForUnusedMappings = true;
            IndexMetadata previous = indices.remove(index);
            updateAliases(previous, null);
            return this;
        }

        public Builder removeAllIndices() {
            previousIndicesLookup = null;
            checkForUnusedMappings = true;

            indices.clear();
            mappingsByHash.clear();
            aliasedIndices.clear();
            return this;
        }

        public Builder indices(Map<String, IndexMetadata> indices) {
            for (var value : indices.values()) {
                put(value, false);
            }
            return this;
        }

        void updateAliases(IndexMetadata previous, IndexMetadata current) {
            if (previous == null && current != null) {
                includeAliases(current);
            } else if (previous != null && current == null) {
                excludeAliases(previous);
            } else if (previous != null && current != null) {
                synchronizeAliases(previous, current);
            }
        }

        private void synchronizeAliases(IndexMetadata previous, IndexMetadata current) {
            if (!Objects.equals(previous.getAliases(), current.getAliases())) {
                addMissingAliases(previous, current);
                removeUnusedAliases(previous, current);
            }
        }

        private void removeUnusedAliases(IndexMetadata previous, IndexMetadata current) {
            for (var key : previous.getAliases().keySet()) {
                if (current.getAliases().containsKey(key) == false) {
                    removeAlias(key, current.getIndex());
                }
            }
        }

        private void addMissingAliases(IndexMetadata previous, IndexMetadata current) {
            for (var key : current.getAliases().keySet()) {
                if (previous.getAliases().containsKey(key) == false) {
                    putAlias(key, current.getIndex());
                }
            }
        }

        private void excludeAliases(IndexMetadata previous) {
            for (var key : previous.getAliases().keySet()) {
                removeAlias(key, previous.getIndex());
            }
        }

        private void includeAliases(IndexMetadata current) {
            for (var key : current.getAliases().keySet()) {
                putAlias(key, current.getIndex());
            }
        }

        private Builder putAlias(String alias, Index index) {
            Objects.requireNonNull(alias);
            Objects.requireNonNull(index);

            Set<Index> indices = new HashSet<>(aliasedIndices.getOrDefault(alias, Set.of()));
            if (indices.add(index) == false) {
                return this; // indices already contained this index
            }
            aliasedIndices.put(alias, Collections.unmodifiableSet(indices));
            return this;
        }

        private Builder removeAlias(String alias, Index index) {
            Objects.requireNonNull(alias);
            Objects.requireNonNull(index);

            Set<Index> indices = aliasedIndices.get(alias);
            if (indices == null || indices.isEmpty()) {
                throw new IllegalStateException("Cannot remove non-existent alias [" + alias + "] for index [" + index.getName() + "]");
            }

            indices = new HashSet<>(indices);
            if (indices.remove(index) == false) {
                throw new IllegalStateException("Cannot remove non-existent alias [" + alias + "] for index [" + index.getName() + "]");
            }

            if (indices.isEmpty()) {
                aliasedIndices.remove(alias); // for consistency, we don't store empty sets, so null it out
            } else {
                aliasedIndices.put(alias, Collections.unmodifiableSet(indices));
            }
            return this;
        }

        public Builder put(IndexTemplateMetadata.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            this.templates.putAllFromMap(templates);
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            Objects.requireNonNull(componentTemplate, "it is invalid to add a null component template: " + name);
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, componentTemplate);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(componentTemplates));
            return this;
        }

        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(indexTemplates));
            return this;
        }

        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            Objects.requireNonNull(indexTemplate, "it is invalid to add a null index template: " + name);
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, indexTemplate);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            return dataStreamMetadata().dataStreams().get(dataStreamName);
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            previousIndicesLookup = null;

            // Only perform data stream validation only when data streams are modified in Metadata:
            for (DataStream dataStream : dataStreams.values()) {
                dataStream.validate(indices::get);
            }

            this.customs.put(
                DataStreamMetadata.TYPE,
                new DataStreamMetadata(
                    ImmutableOpenMap.<String, DataStream>builder().putAllFromMap(dataStreams).build(),
                    ImmutableOpenMap.<String, DataStreamAlias>builder().putAllFromMap(dataStreamAliases).build()
                )
            );
            return this;
        }

        public Builder put(DataStream dataStream) {
            previousIndicesLookup = null;
            Objects.requireNonNull(dataStream, "it is invalid to add a null data stream");

            // Every time the backing indices of a data stream is modified a new instance will be created and
            // that instance needs to be added here. So this is a good place to do data stream validation for
            // the data stream and all of its backing indices. Doing this validation in the build() method would
            // trigger this validation on each new Metadata creation, even if there are no changes to data streams.
            dataStream.validate(indices::get);

            this.customs.put(DataStreamMetadata.TYPE, dataStreamMetadata().withAddedDatastream(dataStream));
            return this;
        }

        public DataStreamMetadata dataStreamMetadata() {
            return (DataStreamMetadata) this.customs.getOrDefault(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY);
        }

        public boolean put(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
            previousIndicesLookup = null;
            final DataStreamMetadata existing = dataStreamMetadata();
            final DataStreamMetadata updated = existing.withAlias(aliasName, dataStream, isWriteDataStream, filter);
            if (existing == updated) {
                return false;
            }
            this.customs.put(DataStreamMetadata.TYPE, updated);
            return true;
        }

        public Builder removeDataStream(String name) {
            previousIndicesLookup = null;
            this.customs.put(DataStreamMetadata.TYPE, dataStreamMetadata().withRemovedDataStream(name));
            return this;
        }

        public boolean removeDataStreamAlias(String aliasName, String dataStreamName, boolean mustExist) {
            previousIndicesLookup = null;

            final DataStreamMetadata existing = dataStreamMetadata();
            final DataStreamMetadata updated = existing.withRemovedAlias(aliasName, dataStreamName, mustExist);
            if (existing == updated) {
                return false;
            }
            this.customs.put(DataStreamMetadata.TYPE, updated);
            return true;
        }

        public Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder removeCustomIf(BiPredicate<String, Custom> p) {
            customs.removeAll(p);
            return this;
        }

        public Builder customs(Map<String, Custom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.customs.putAllFromMap(customs);
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
            return (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(new String[0]);
            }
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                // Updating version is required when updating settings.
                // Otherwise, settings changes may not be replicated to remote clusters.
                long newVersion = indexMetadata.getSettingsVersion() + 1;
                put(
                    IndexMetadata.builder(indexMetadata)
                        .settings(Settings.builder().put(indexMetadata.getSettings()).put(settings))
                        .settingsVersion(newVersion)
                );
            }
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
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfReplicas(numberOfReplicas));
            }
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
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will be rebuilt only when needed. Now during serializing we rebuild these datastructures
            // while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.
            final List<String> visibleIndices = new ArrayList<>();
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> visibleOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final List<String> visibleClosedIndices = new ArrayList<>();
            final ImmutableOpenMap<String, IndexMetadata> indicesMap = indices.build();

            int oldestIndexVersionId = Version.CURRENT.id;
            int totalNumberOfShards = 0;
            int totalOpenIndexShards = 0;

            final String[] allIndicesArray = new String[indicesMap.size()];
            int i = 0;
            final Set<String> sha256HashesInUse = checkForUnusedMappings ? Sets.newHashSetWithExpectedSize(mappingsByHash.size()) : null;
            for (var entry : indicesMap.entrySet()) {
                allIndicesArray[i++] = entry.getKey();
                final IndexMetadata indexMetadata = entry.getValue();
                totalNumberOfShards += indexMetadata.getTotalNumberOfShards();
                final String name = indexMetadata.getIndex().getName();
                final boolean visible = isVisible(visibleIndices, indexMetadata, name);
                totalOpenIndexShards = getOpenIndexShards(allOpenIndices, visibleOpenIndices, allClosedIndices, visibleClosedIndices, totalOpenIndexShards, indexMetadata, name, visible);
                oldestIndexVersionId = Math.min(oldestIndexVersionId, indexMetadata.getCompatibilityVersion().id);
                populateSha256HashesInUse(sha256HashesInUse, indexMetadata);
            }

            ImmutableOpenMap<String, Set<Index>> aliasedIndices = getAliasedIndices(indicesMap);
            SortedMap<String, IndexAbstraction> indicesLookup = getIndicesLookup(skipNameCollisionChecks, indicesMap, aliasedIndices);

            if (sha256HashesInUse != null) {
                mappingsByHash.keySet().retainAll(sha256HashesInUse);
            }

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] visibleIndicesArray = visibleIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allOpenIndicesArray = allOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allClosedIndicesArray = allClosedIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(Strings.EMPTY_ARRAY);

            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                Settings.builder().put(persistentSettings).put(transientSettings).build(),
                hashesOfConsistentSettings,
                totalNumberOfShards,
                totalOpenIndexShards,
                indicesMap,
                aliasedIndices,
                templates.build(),
                customs.build(),
                allIndicesArray,
                visibleIndicesArray,
                allOpenIndicesArray,
                visibleOpenIndicesArray,
                allClosedIndicesArray,
                visibleClosedIndicesArray,
                indicesLookup,
                Collections.unmodifiableMap(mappingsByHash),
                Version.fromId(oldestIndexVersionId),
                Collections.unmodifiableMap(reservedStateMetadata)
            );
        }

        private static boolean isVisible(List<String> visibleIndices, IndexMetadata indexMetadata, String name) {
            final boolean visible = indexMetadata.isHidden() == false;
            if (visible) {
                visibleIndices.add(name);
            }
            return visible;
        }

        private SortedMap<String, IndexAbstraction> getIndicesLookup(boolean skipNameCollisionChecks, ImmutableOpenMap<String, IndexMetadata> indicesMap, ImmutableOpenMap<String, Set<Index>> aliasedIndices) {
            SortedMap<String, IndexAbstraction> indicesLookup = null;
            if (previousIndicesLookup != null) {
                // no changes to the names of indices, datastreams, and their aliases so we can reuse the previous lookup
                assert previousIndicesLookup.equals(buildIndicesLookup(dataStreamMetadata(), indicesMap));
                indicesLookup = previousIndicesLookup;
            } else if (skipNameCollisionChecks == false) {
                // we have changes to the the entity names so we ensure we have no naming collisions
                ensureNoNameCollisions(aliasedIndices.keySet(), indicesMap, dataStreamMetadata());
            }
            assert assertDataStreams(indicesMap, dataStreamMetadata());
            return indicesLookup;
        }

        private ImmutableOpenMap<String, Set<Index>> getAliasedIndices(ImmutableOpenMap<String, IndexMetadata> indicesMap) {
            var aliasedIndices = this.aliasedIndices.build();
            for (var entry : aliasedIndices.entrySet()) {
                List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
                validateAlias(entry.getKey(), aliasIndices);
            }
            return aliasedIndices;
        }

        private static void populateSha256HashesInUse(Set<String> sha256HashesInUse, IndexMetadata indexMetadata) {
            if (sha256HashesInUse != null) {
                final var mapping = indexMetadata.mapping();
                if (mapping != null) {
                    sha256HashesInUse.add(mapping.getSha256());
                }
            }
        }

        private static int getOpenIndexShards(List<String> allOpenIndices, List<String> visibleOpenIndices, List<String> allClosedIndices, List<String> visibleClosedIndices, int totalOpenIndexShards, IndexMetadata indexMetadata, String name, boolean visible) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                totalOpenIndexShards += indexMetadata.getTotalNumberOfShards();
                updateIndices(allOpenIndices, visibleOpenIndices, name, visible);
            } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                updateIndices(allClosedIndices, visibleClosedIndices, name, visible);
            }
            return totalOpenIndexShards;
        }

        private static void updateIndices(List<String> allOpenIndices, List<String> visibleOpenIndices, String name, boolean visible) {
            allOpenIndices.add(name);
            if (visible) {
                visibleOpenIndices.add(name);
            }
        }

        private static void ensureNoNameCollisions(
            Set<String> indexAliases,
            ImmutableOpenMap<String, IndexMetadata> indicesMap,
            DataStreamMetadata dataStreamMetadata
        ) {
            final ArrayList<String> duplicates = new ArrayList<>();
            final Set<String> aliasDuplicatesWithIndices = new HashSet<>();
            final Set<String> aliasDuplicatesWithDataStreams = new HashSet<>();
            final var allDataStreams = dataStreamMetadata.dataStreams();
            // Adding data stream aliases:
            for (String dataStreamAlias : dataStreamMetadata.getDataStreamAliases().keySet()) {
                findDuplicates(indexAliases, duplicates, dataStreamAlias);
                findAliasDuplicatesWithIndices(indicesMap, aliasDuplicatesWithIndices, dataStreamAlias);
                findAliasDuplicatesWithDataStreams(aliasDuplicatesWithDataStreams, allDataStreams, dataStreamAlias);
            }
            for (String alias : indexAliases) {
                findAliasDuplicatesWithDataStreams(aliasDuplicatesWithDataStreams, allDataStreams, alias);
                findAliasDuplicatesWithIndices(indicesMap, aliasDuplicatesWithIndices, alias);
            }
            allDataStreams.forEach((key, value) -> {
                if (indicesMap.containsKey(key)) {
                    duplicates.add("data stream [" + key + "] conflicts with index");
                }
            });
            if (aliasDuplicatesWithIndices.isEmpty() == false) {
                collectAliasDuplicates(indicesMap, aliasDuplicatesWithIndices, duplicates);
            }
            if (aliasDuplicatesWithDataStreams.isEmpty() == false) {
                collectAliasDuplicates(indicesMap, dataStreamMetadata, aliasDuplicatesWithDataStreams, duplicates);
            }
            if (duplicates.isEmpty() == false) {
                throw new IllegalStateException(
                    "index, alias, and data stream names need to be unique, but the following duplicates "
                        + "were found ["
                        + Strings.collectionToCommaDelimitedString(duplicates)
                        + "]"
                );
            }
        }

        private static void findAliasDuplicatesWithDataStreams(Set<String> aliasDuplicatesWithDataStreams, Map<String, DataStream> allDataStreams, String dataStreamAlias) {
            if (allDataStreams.containsKey(dataStreamAlias)) {
                aliasDuplicatesWithDataStreams.add(dataStreamAlias);
            }
        }

        private static void findAliasDuplicatesWithIndices(ImmutableOpenMap<String, IndexMetadata> indicesMap, Set<String> aliasDuplicatesWithIndices, String dataStreamAlias) {
            if (indicesMap.containsKey(dataStreamAlias)) {
                aliasDuplicatesWithIndices.add(dataStreamAlias);
            }
        }

        private static void findDuplicates(Set<String> indexAliases, ArrayList<String> duplicates, String dataStreamAlias) {
            if (indexAliases.contains(dataStreamAlias)) {
                duplicates.add("data stream alias and indices alias have the same name (" + dataStreamAlias + ")");
            }
        }

        /**
         * Iterates the detected duplicates between datastreams and aliases and collects them into the duplicates list as helpful messages.
         */
        private static void collectAliasDuplicates(
            ImmutableOpenMap<String, IndexMetadata> indicesMap,
            DataStreamMetadata dataStreamMetadata,
            Set<String> aliasDuplicatesWithDataStreams,
            ArrayList<String> duplicates
        ) {
            for (String alias : aliasDuplicatesWithDataStreams) {
                // reported var avoids adding a message twice if an index alias has the same name as a data stream.
                boolean reported = false;
                for (IndexMetadata cursor : indicesMap.values()) {
                    if (cursor.getAliases().containsKey(alias)) {
                        duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with data stream");
                        reported = true;
                    }
                }
                // This is for adding an error message for when a data steam alias has the same name as a data stream.
                if (reported == false && dataStreamMetadata != null && dataStreamMetadata.dataStreams().containsKey(alias)) {
                    duplicates.add("data stream alias and data stream have the same name (" + alias + ")");
                }
            }
        }

        /**
         * Collect all duplicate names across indices and aliases that were detected into a list of helpful duplicate failure messages.
         */
        private static void collectAliasDuplicates(
            ImmutableOpenMap<String, IndexMetadata> indicesMap,
            Set<String> aliasDuplicatesWithIndices,
            ArrayList<String> duplicates
        ) {
            for (IndexMetadata cursor : indicesMap.values()) {
                for (String alias : aliasDuplicatesWithIndices) {
                    if (cursor.getAliases().containsKey(alias)) {
                        duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with index");
                    }
                }
            }
        }

        static SortedMap<String, IndexAbstraction> buildIndicesLookup(
            DataStreamMetadata dataStreamMetadata,
            ImmutableOpenMap<String, IndexMetadata> indices
        ) {
            if (indices.isEmpty()) {
                return Collections.emptySortedMap();
            }
            SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
            Map<String, DataStream> indexToDataStreamLookup = new HashMap<>();
            final var dataStreams = dataStreamMetadata.dataStreams();
            buildIndexLookupMap(dataStreamMetadata, indicesLookup, indexToDataStreamLookup, dataStreams);

            Map<String, List<IndexMetadata>> aliasToIndices = new HashMap<>();
            getAliasToIndices(indices, indicesLookup, indexToDataStreamLookup, aliasToIndices);

            populateIndicesWithAliases(indicesLookup, aliasToIndices);

            return Collections.unmodifiableSortedMap(indicesLookup);
        }

        private static void populateIndicesWithAliases(SortedMap<String, IndexAbstraction> indicesLookup, Map<String, List<IndexMetadata>> aliasToIndices) {
            for (var entry : aliasToIndices.entrySet()) {
                AliasMetadata alias = entry.getValue().get(0).getAliases().get(entry.getKey());
                IndexAbstraction existing = indicesLookup.put(entry.getKey(), new IndexAbstraction.Alias(alias, entry.getValue()));
                assert existing == null : "duplicate for " + entry.getKey();
            }
        }

        private static void getAliasToIndices(ImmutableOpenMap<String, IndexMetadata> indices, SortedMap<String, IndexAbstraction> indicesLookup, Map<String, DataStream> indexToDataStreamLookup, Map<String, List<IndexMetadata>> aliasToIndices) {
            for (var entry : indices.entrySet()) {
                final String name = entry.getKey();
                final IndexMetadata indexMetadata = entry.getValue();
                final DataStream parent = indexToDataStreamLookup.get(name);
                assert parent == null || parent.getIndices().stream().anyMatch(index -> name.equals(index.getName()))
                    : "Expected data stream [" + parent.getName() + "] to contain index " + indexMetadata.getIndex();
                IndexAbstraction existing = indicesLookup.put(name, new ConcreteIndex(indexMetadata, parent));
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (var aliasMetadata : indexMetadata.getAliases().values()) {
                    List<IndexMetadata> aliasIndices = aliasToIndices.computeIfAbsent(aliasMetadata.getAlias(), k -> new ArrayList<>());
                    aliasIndices.add(indexMetadata);
                }
            }
        }

        private static void buildIndexLookupMap(DataStreamMetadata dataStreamMetadata, SortedMap<String, IndexAbstraction> indicesLookup, Map<String, DataStream> indexToDataStreamLookup, Map<String, DataStream> dataStreams) {
            for (DataStreamAlias alias : dataStreamMetadata.getDataStreamAliases().values()) {
                IndexAbstraction existing = indicesLookup.put(alias.getName(), makeDsAliasAbstraction(dataStreams, alias));
                assert existing == null : "duplicate data stream alias for " + alias.getName();
            }
            for (DataStream dataStream : dataStreams.values()) {
                assert dataStream.getIndices().isEmpty() == false;

                IndexAbstraction existing = indicesLookup.put(dataStream.getName(), dataStream);
                assert existing == null : "duplicate data stream for " + dataStream.getName();

                for (Index i : dataStream.getIndices()) {
                    indexToDataStreamLookup.put(i.getName(), dataStream);
                }
            }
        }

        private static IndexAbstraction.Alias makeDsAliasAbstraction(Map<String, DataStream> dataStreams, DataStreamAlias alias) {
            Index writeIndexOfWriteDataStream = null;
            if (alias.getWriteDataStream() != null) {
                DataStream writeDataStream = dataStreams.get(alias.getWriteDataStream());
                writeIndexOfWriteDataStream = writeDataStream.getWriteIndex();
            }
            return new IndexAbstraction.Alias(
                alias,
                alias.getDataStreams().stream().flatMap(name -> dataStreams.get(name).getIndices().stream()).toList(),
                writeIndexOfWriteDataStream
            );
        }

        private static boolean isNonEmpty(List<IndexMetadata> idxMetas) {
            return (Objects.isNull(idxMetas) || idxMetas.isEmpty()) == false;
        }

        private static void validateAlias(String aliasName, List<IndexMetadata> indexMetadatas) {
            // Validate write indices
            validateSingleWriteIndex(aliasName, indexMetadatas);

            // Validate hidden status
            validateHiddenStatus(aliasName, indexMetadatas);

            // Validate system status
            validateSystemStatus(aliasName, indexMetadatas);
        }

        private static void validateSystemStatus(String aliasName, List<IndexMetadata> indexMetadatas) {
            final Map<Boolean, List<IndexMetadata>> groupedBySystemStatus = indexMetadatas.stream()
                .collect(Collectors.groupingBy(IndexMetadata::isSystem));
            // If the alias has either all system or all non-system, then no more validation is required
            if (isNonEmpty(groupedBySystemStatus.get(false)) && isNonEmpty(groupedBySystemStatus.get(true))) {
                final List<String> newVersionSystemIndices = groupedBySystemStatus.get(true)
                    .stream()
                    .filter(i -> i.getCreationVersion().onOrAfter(IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_VERSION))
                    .map(i -> i.getIndex().getName())
                    .sorted() // reliable error message for testing
                    .toList();

                if (newVersionSystemIndices.isEmpty() == false) {
                    final List<String> nonSystemIndices = groupedBySystemStatus.get(false)
                        .stream()
                        .map(i -> i.getIndex().getName())
                        .sorted() // reliable error message for testing
                        .toList();
                    throw new IllegalStateException(
                        "alias ["
                            + aliasName
                            + "] refers to both system indices "
                            + newVersionSystemIndices
                            + " and non-system indices: "
                            + nonSystemIndices
                            + ", but aliases must refer to either system or"
                            + " non-system indices, not both"
                    );
                }
            }
        }

        private static void validateHiddenStatus(String aliasName, List<IndexMetadata> indexMetadatas) {
            final Map<Boolean, List<IndexMetadata>> groupedByHiddenStatus = indexMetadatas.stream()
                .collect(Collectors.groupingBy(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).isHidden())));
            if (isNonEmpty(groupedByHiddenStatus.get(true)) && isNonEmpty(groupedByHiddenStatus.get(false))) {
                List<String> hiddenOn = groupedByHiddenStatus.get(true).stream().map(idx -> idx.getIndex().getName()).toList();
                List<String> nonHiddenOn = groupedByHiddenStatus.get(false).stream().map(idx -> idx.getIndex().getName()).toList();
                throw new IllegalStateException(
                    "alias ["
                        + aliasName
                        + "] has is_hidden set to true on indices ["
                        + Strings.collectionToCommaDelimitedString(hiddenOn)
                        + "] but does not have is_hidden set to true on indices ["
                        + Strings.collectionToCommaDelimitedString(nonHiddenOn)
                        + "]; alias must have the same is_hidden setting "
                        + "on all indices"
                );
            }
        }

        private static void validateSingleWriteIndex(String aliasName, List<IndexMetadata> indexMetadatas) {
            List<String> writeIndices = indexMetadatas.stream()
                .filter(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).writeIndex()))
                .map(im -> im.getIndex().getName())
                .toList();
            if (writeIndices.size() > 1) {
                throw new IllegalStateException(
                    "alias ["
                        + aliasName
                        + "] has more than one write index ["
                        + Strings.collectionToCommaDelimitedString(writeIndices)
                        + "]"
                );
            }
        }

        static boolean assertDataStreams(Map<String, IndexMetadata> indices, DataStreamMetadata dsMetadata) {
            // Sanity check, because elsewhere a more user friendly error should have occurred:
            List<String> conflictingAliases = getConflictingAliases(indices, dsMetadata);
            if (conflictingAliases != null) {
                throw new AssertionError("aliases " + conflictingAliases + " cannot refer to backing indices of data streams");
            }

            return true;
        }

        private static List<String> getConflictingAliases(Map<String, IndexMetadata> indices, DataStreamMetadata dsMetadata) {
            List<String> conflictingAliases = null;

            for (var dataStream : dsMetadata.dataStreams().values()) {
                for (var index : dataStream.getIndices()) {
                    conflictingAliases = populateConflictingAliases(indices, conflictingAliases, index);
                }
            }
            return conflictingAliases;
        }

        private static List<String> populateConflictingAliases(Map<String, IndexMetadata> indices, List<String> conflictingAliases, Index index) {
            IndexMetadata im = indices.get(index.getName());
            if (im != null && im.getAliases().isEmpty() == false) {
                for (var alias : im.getAliases().values()) {
                    if (conflictingAliases == null) {
                        conflictingAliases = new LinkedList<>();
                    }
                    conflictingAliases.add(alias.alias());
                }
            }
            return conflictingAliases;
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
            processTokenForBuilder(parser, builder, currentFieldName);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            return builder.build();
        }

        private static void processTokenForBuilder(XContentParser parser, Builder builder, String currentFieldName) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    parseMetadataToBuilder(parser, builder, currentFieldName);
                } else if (token.isValue()) {
                    alignBuilderProperties(parser, builder, currentFieldName);
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
        }

        private static void alignBuilderProperties(XContentParser parser, Builder builder, String currentFieldName) throws IOException {
            if ("version".equals(currentFieldName)) {
                builder.version = parser.longValue();
            } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                builder.clusterUUID = parser.text();
            } else if ("cluster_uuid_committed".equals(currentFieldName)) {
                builder.clusterUUIDCommitted = parser.booleanValue();
            } else {
                throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
            }
        }

        private static void parseMetadataToBuilder(XContentParser parser, Builder builder, String currentFieldName) throws IOException {
            XContentParser.Token token;
            if ("cluster_coordination".equals(currentFieldName)) {
                builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
            } else if ("settings".equals(currentFieldName)) {
                builder.persistentSettings(Settings.fromXContent(parser));
            } else if ("indices".equals(currentFieldName)) {
                parseIndexMetadataToken(parser, builder);
            } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                builder.hashesOfConsistentSettings(parser.mapStrings());
            } else if ("templates".equals(currentFieldName)) {
                parseTemplateMetadataTokens(parser, builder);
            } else if ("reserved_state".equals(currentFieldName)) {
                parseReservedStateMetadataTokens(parser, builder);
            } else {
                parseAndPutCustom(parser, builder, currentFieldName);
            }
        }

        private static void parseAndPutCustom(XContentParser parser, Builder builder, String currentFieldName) throws IOException {
            try {
                Custom custom = parser.namedObject(Custom.class, currentFieldName, null);
                builder.putCustom(custom.getWriteableName(), custom);
            } catch (NamedObjectNotFoundException ex) {
                logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                parser.skipChildren();
            }
        }

        private static void parseReservedStateMetadataTokens(XContentParser parser, Builder builder) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                builder.put(ReservedStateMetadata.fromXContent(parser));
            }
        }

        private static void parseTemplateMetadataTokens(XContentParser parser, Builder builder) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
            }
        }

        private static void parseIndexMetadataToken(XContentParser parser, Builder builder) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                builder.put(IndexMetadata.Builder.fromXContent(parser), false);
            }
        }

        /**
         * Dedupes {@link MappingMetadata} instance from the provided indexMetadata parameter using the sha256
         * hash from the compressed source of the mapping. If there is a mapping with the same sha256 hash then
         * a new {@link IndexMetadata} is returned with the found {@link MappingMetadata} instance, otherwise
         * the {@link MappingMetadata} instance of the indexMetadata parameter is recorded and the indexMetadata
         * parameter is then returned.
         */
        private IndexMetadata dedupeMapping(IndexMetadata indexMetadata) {
            if (indexMetadata.mapping() == null) {
                return indexMetadata;
            }

            String digest = indexMetadata.mapping().getSha256();
            MappingMetadata entry = mappingsByHash.get(digest);
            if (entry != null) {
                return indexMetadata.withMappingMetadata(entry);
            } else {
                mappingsByHash.put(digest, indexMetadata.mapping());
                return indexMetadata;
            }
        }

        /**
         * Similar to {@link #dedupeMapping(IndexMetadata)}.
         */
        private void dedupeMapping(IndexMetadata.Builder indexMetadataBuilder) {
            if (indexMetadataBuilder.mapping() == null) {
                return;
            }

            String digest = indexMetadataBuilder.mapping().getSha256();
            MappingMetadata entry = mappingsByHash.get(digest);
            if (entry != null) {
                indexMetadataBuilder.putMapping(entry);
            } else {
                mappingsByHash.put(digest, indexMetadataBuilder.mapping());
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
