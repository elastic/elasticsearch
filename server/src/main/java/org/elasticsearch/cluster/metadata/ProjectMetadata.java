/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.cluster.metadata.Metadata.ALL;
import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

public class ProjectMetadata implements Iterable<IndexMetadata>, Diffable<ProjectMetadata>, ChunkedToXContent {

    private static final NamedDiffableValueSerializer<Metadata.ProjectCustom> PROJECT_CUSTOM_VALUE_SERIALIZER =
        new NamedDiffableValueSerializer<>(Metadata.ProjectCustom.class);

    private final ProjectId id;

    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, Set<Index>> aliasedIndices;
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, Metadata.ProjectCustom> customs;
    private final ImmutableOpenMap<String, ReservedStateMetadata> reservedStateMetadata;

    private final int totalNumberOfShards;
    private final int totalOpenIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private volatile SortedMap<String, IndexAbstraction> indicesLookup;
    private final Map<String, MappingMetadata> mappingsByHash;

    private final Settings settings;

    private final IndexVersion oldestIndexVersion;

    @SuppressWarnings("this-escape")
    private ProjectMetadata(
        ProjectId id,
        ImmutableOpenMap<String, IndexMetadata> indices,
        ImmutableOpenMap<String, Set<Index>> aliasedIndices,
        ImmutableOpenMap<String, IndexTemplateMetadata> templates,
        ImmutableOpenMap<String, Metadata.ProjectCustom> customs,
        ImmutableOpenMap<String, ReservedStateMetadata> reservedStateMetadata,
        int totalNumberOfShards,
        int totalOpenIndexShards,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, MappingMetadata> mappingsByHash,
        Settings settings,
        IndexVersion oldestIndexVersion
    ) {
        this.id = id;
        this.indices = indices;
        this.aliasedIndices = aliasedIndices;
        this.templates = templates;
        this.customs = customs;
        this.reservedStateMetadata = reservedStateMetadata;
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
        this.settings = settings;
        this.oldestIndexVersion = oldestIndexVersion;
        assert assertConsistent();
    }

    @SuppressWarnings("this-escape")
    private boolean assertConsistent() {
        final var lookup = indicesLookup;
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
    public ProjectMetadata withLifecycleState(Index index, LifecycleExecutionState lifecycleState) {
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
        ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
        builder.put(index.getName(), indexMetadataBuilder.build());

        // construct a new ProjectMetadata object directly rather than using ProjectMetadata.builder(this).[...].build().
        // the ProjectMetadata.Builder validation needs to handle the general case where anything at all could
        // have changed, and hence it is expensive -- since we are changing so little about the metadata
        // (and at a leaf in the object tree), we can bypass that validation for efficiency's sake
        return new ProjectMetadata(
            id,
            builder.build(),
            aliasedIndices,
            templates,
            customs,
            reservedStateMetadata,
            totalNumberOfShards,
            totalOpenIndexShards,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            settings,
            oldestIndexVersion
        );
    }

    public ProjectMetadata withIndexSettingsUpdates(Map<Index, Settings> updates) {
        Objects.requireNonNull(updates, "no indices to update settings for");

        ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
        updates.forEach((index, settings) -> {
            IndexMetadata previous = builder.remove(index.getName());
            assert previous != null : index;
            builder.put(
                index.getName(),
                IndexMetadata.builder(previous).settingsVersion(previous.getSettingsVersion() + 1L).settings(settings).build()
            );
        });
        return new ProjectMetadata(
            id,
            builder.build(),
            aliasedIndices,
            templates,
            customs,
            reservedStateMetadata,
            totalNumberOfShards,
            totalOpenIndexShards,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            settings,
            oldestIndexVersion
        );
    }

    /**
     * Creates a copy of this instance updated with the given {@link IndexMetadata} that must only contain changes to primary terms
     * and in-sync allocation ids relative to the existing entries. This method is only used by
     * {@link IndexMetadataUpdater#applyChanges(Metadata, GlobalRoutingTable)}.
     * @param updates map of index name to {@link IndexMetadata}.
     * @return updated metadata instance
     */
    public ProjectMetadata withAllocationAndTermUpdatesOnly(Map<String, IndexMetadata> updates) {
        if (updates.isEmpty()) {
            return this;
        }
        var updatedIndicesBuilder = ImmutableOpenMap.builder(indices);
        updatedIndicesBuilder.putAllFromMap(updates);
        return new ProjectMetadata(
            id,
            updatedIndicesBuilder.build(),
            aliasedIndices,
            templates,
            customs,
            reservedStateMetadata,
            totalNumberOfShards,
            totalOpenIndexShards,
            allIndices,
            visibleIndices,
            allOpenIndices,
            visibleOpenIndices,
            allClosedIndices,
            visibleClosedIndices,
            indicesLookup,
            mappingsByHash,
            settings,
            oldestIndexVersion
        );
    }

    /**
     * Creates a copy of this instance with the given {@code index} added.
     * @param index index to add
     * @return copy with added index
     */
    public ProjectMetadata withAddedIndex(IndexMetadata index) {
        String indexName = index.getIndex().getName();
        ensureNoNameCollision(indexName);
        Map<String, AliasMetadata> aliases = index.getAliases();
        ImmutableOpenMap<String, Set<Index>> updatedAliases = aliasesAfterAddingIndex(index, aliases);
        String[] updatedVisibleIndices;
        if (index.isHidden()) {
            updatedVisibleIndices = visibleIndices;
        } else {
            updatedVisibleIndices = ArrayUtils.append(visibleIndices, indexName);
        }

        String[] updatedAllIndices = ArrayUtils.append(allIndices, indexName);
        String[] updatedOpenIndices;
        String[] updatedClosedIndices;
        String[] updatedVisibleOpenIndices;
        String[] updatedVisibleClosedIndices;
        switch (index.getState()) {
            case OPEN -> {
                updatedOpenIndices = ArrayUtils.append(allOpenIndices, indexName);
                if (index.isHidden() == false) {
                    updatedVisibleOpenIndices = ArrayUtils.append(visibleOpenIndices, indexName);
                } else {
                    updatedVisibleOpenIndices = visibleOpenIndices;
                }
                updatedVisibleClosedIndices = visibleClosedIndices;
                updatedClosedIndices = allClosedIndices;
            }
            case CLOSE -> {
                updatedOpenIndices = allOpenIndices;
                updatedClosedIndices = ArrayUtils.append(allClosedIndices, indexName);
                updatedVisibleOpenIndices = visibleOpenIndices;
                if (index.isHidden() == false) {
                    updatedVisibleClosedIndices = ArrayUtils.append(visibleClosedIndices, indexName);
                } else {
                    updatedVisibleClosedIndices = visibleClosedIndices;
                }
            }
            default -> throw new AssertionError("impossible, index is either open or closed");
        }

        MappingMetadata mappingMetadata = index.mapping();
        Map<String, MappingMetadata> updatedMappingsByHash;
        if (mappingMetadata == null) {
            updatedMappingsByHash = mappingsByHash;
        } else {
            MappingMetadata existingMapping = mappingsByHash.get(mappingMetadata.getSha256());
            if (existingMapping != null) {
                index = index.withMappingMetadata(existingMapping);
                updatedMappingsByHash = mappingsByHash;
            } else {
                updatedMappingsByHash = Maps.copyMapWithAddedEntry(mappingsByHash, mappingMetadata.getSha256(), mappingMetadata);
            }
        }

        ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
        builder.put(indexName, index);
        ImmutableOpenMap<String, IndexMetadata> indicesMap = builder.build();
        for (var entry : updatedAliases.entrySet()) {
            List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
            ProjectMetadata.Builder.validateAlias(entry.getKey(), aliasIndices);
        }
        return new ProjectMetadata(
            id,
            indicesMap,
            updatedAliases,
            templates,
            customs,
            reservedStateMetadata,
            totalNumberOfShards + index.getTotalNumberOfShards(),
            totalOpenIndexShards + (index.getState() == IndexMetadata.State.OPEN ? index.getTotalNumberOfShards() : 0),
            updatedAllIndices,
            updatedVisibleIndices,
            updatedOpenIndices,
            updatedVisibleOpenIndices,
            updatedClosedIndices,
            updatedVisibleClosedIndices,
            null,
            updatedMappingsByHash,
            settings,
            IndexVersion.min(index.getCompatibilityVersion(), oldestIndexVersion)
        );
    }

    private ImmutableOpenMap<String, Set<Index>> aliasesAfterAddingIndex(IndexMetadata index, Map<String, AliasMetadata> aliases) {
        if (aliases.isEmpty()) {
            return aliasedIndices;
        }
        String indexName = index.getIndex().getName();
        ImmutableOpenMap.Builder<String, Set<Index>> aliasesBuilder = ImmutableOpenMap.builder(aliasedIndices);
        for (String alias : aliases.keySet()) {
            ensureNoNameCollision(alias);
            if (aliasedIndices.containsKey(indexName)) {
                throw new IllegalArgumentException("alias with name [" + indexName + "] already exists");
            }
            Set<Index> found = aliasesBuilder.get(alias);
            Set<Index> updated;
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

    /**
     * @return The identifier of this project
     */
    public ProjectId id() {
        return this.id;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + this.id.id() + "}";
    }

    /**
     * Checks whether an index exists (as of this {@link ProjectMetadata} with the given name. Does not check aliases or data streams.
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if a concrete index with that name exists, {@code false} otherwise.
     */
    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    /**
     * Checks whether an index exists. Similar to {@link ProjectMetadata#hasIndex(String)}, but ensures that the index has the same UUID as
     * the given {@link Index}.
     * @param index An {@link Index} object that may or may not exist in the cluster.
     * @return {@code true} if an index exists with the same name and UUID as the given index object, {@code false} otherwise.
     */
    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
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

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return custom(IndexGraveyard.TYPE);
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
        throw new IndexNotFoundException(index, id);
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

    public boolean sameIndicesLookup(ProjectMetadata other) {
        return indicesLookup == other.indicesLookup;
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
     * Finds the specific index aliases that point to the requested concrete indices directly
     * or that match with the indices via wildcards.
     *
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAllAliases(String[] concreteIndices) {
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
    public Map<String, List<AliasMetadata>> findAliases(String[] aliases, String[] concreteIndices) {
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();

        Function<String, List<AliasMetadata>> getter = index -> List.copyOf(indices.get(index).getAliases().values());
        findAliasInfo(aliases, concreteIndices, getter, mapBuilder::put);

        return mapBuilder.build();
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

    public boolean equalsAliases(ProjectMetadata other) {
        for (IndexMetadata otherIndex : other.indices().values()) {
            IndexMetadata thisIndex = index(otherIndex.getIndex());
            if (thisIndex == null || otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return false;
            }
        }

        var thisAliases = dataStreamAliases();
        var otherAliases = other.dataStreamAliases();
        if (otherAliases.size() != thisAliases.size()) {
            return false;
        }
        for (DataStreamAlias otherAlias : otherAliases.values()) {
            DataStreamAlias thisAlias = thisAliases.get(otherAlias.getName());
            if (thisAlias == null || thisAlias.equals(otherAlias) == false) {
                return false;
            }
        }

        return true;
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
        Stream.of(concreteIndices).filter(indices::containsKey).forEach(index -> {
            onNextIndex.run();
            IndexMetadata indexMetadata = indices.get(index);
            Predicate<String> fieldPredicate = fieldFilter.apply(index);
            indexMapBuilder.put(index, filterFields(indexMetadata.mapping(), fieldPredicate));
        });
        return indexMapBuilder.build();
    }

    public Map<String, MappingMetadata> getMappingsByHash() {
        return mappingsByHash;
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
                    assert subFields.isEmpty() == false;
                    map.put("properties", subFields);
                    map.remove("fields");
                    map.remove("type");
                }
            }
        }
        // return true if the ancestor may be removed, as it has no sub-fields left
        return fields.isEmpty();
    }

    private static String mergePaths(String path, String field) {
        return path.isEmpty() ? field : path + "." + field;
    }

    public Map<String, IndexMetadata> indices() {
        return indices;
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return templates;
    }

    public Settings settings() {
        return settings;
    }

    /**
     * Checks whether the provided index is a data stream.
     */
    public boolean indexIsADataStream(String indexName) {
        final SortedMap<String, IndexAbstraction> lookup = getIndicesLookup();
        IndexAbstraction abstraction = lookup.get(indexName);
        return abstraction != null && abstraction.getType() == IndexAbstraction.Type.DATA_STREAM;
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     */
    public int getTotalNumberOfShards() {
        return totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     */
    public int getTotalOpenIndexShards() {
        return totalOpenIndexShards;
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

    public boolean indicesLookupInitialized() {
        return indicesLookup != null;
    }

    public IndexVersion oldestIndexVersion() {
        return oldestIndexVersion;
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
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException(
                    "index/alias ["
                        + aliasOrIndex
                        + "] provided with routing value ["
                        + aliasMd.getIndexRouting()
                        + "] that resolved to several routing values, rejecting operation"
                );
            }
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
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
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

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.values().iterator();
    }

    public Stream<IndexMetadata> stream() {
        return indices.values().stream();
    }

    public int size() {
        return indices.size();
    }

    @SuppressWarnings("unchecked")
    public <T extends Metadata.ProjectCustom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Metadata.ProjectCustom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    public ImmutableOpenMap<String, Metadata.ProjectCustom> customs() {
        return customs;
    }

    public Map<String, ReservedStateMetadata> reservedStateMetadata() {
        return reservedStateMetadata;
    }

    public Map<String, DataStream> dataStreams() {
        return custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).dataStreams();
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).getDataStreamAliases();
    }

    /**
     * Return a map of DataStreamAlias objects by DataStream name
     */
    public Map<String, List<DataStreamAlias>> dataStreamAliasesByDataStream() {
        return dataStreamAliases().values()
            .stream()
            .flatMap(dsa -> dsa.getDataStreams().stream().map(ds -> Map.entry(ds, dsa)))
            .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
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

        findAliasInfo(aliases, dataStreams, dataStream -> dataStreamAliases.getOrDefault(dataStream, List.of()), mapBuilder::put);

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
    private <T extends AliasInfo> void findAliasInfo(
        String[] aliases,
        String[] possibleMatches,
        Function<String, List<T>> getter,
        BiConsumer<String, List<T>> setter
    ) {
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
            List<T> filteredValues = new ArrayList<>();

            List<T> entities = getter.apply(index);
            for (T aliasInfo : entities) {
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
                setter.accept(index, Collections.unmodifiableList(filteredValues));
            }
        }
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return Optional.ofNullable((ComponentTemplateMetadata) custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) custom(ComposableIndexTemplateMetadata.TYPE))
            .map(ComposableIndexTemplateMetadata::indexTemplates)
            .orElse(Collections.emptyMap());
    }

    public IndexMode retrieveIndexModeFromTemplate(ComposableIndexTemplate indexTemplate) {
        if (indexTemplate.getDataStreamTemplate() == null) {
            return null;
        }

        var settings = MetadataIndexTemplateService.resolveSettings(indexTemplate, componentTemplates());
        // Not using IndexSettings.MODE.get() to avoid validation that may fail at this point.
        var rawIndexMode = settings.get(IndexSettings.MODE.getKey());
        return rawIndexMode != null ? Enum.valueOf(IndexMode.class, rawIndexMode.toUpperCase(Locale.ROOT)) : null;
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

        IndexAbstraction indexAbstraction = getIndicesLookup().get(indexMetadata.getIndex().getName());
        if (indexAbstraction == null) {
            // index doesn't exist anymore
            return false;
        }

        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        // Only data streams can be managed by data stream lifecycle
        if (parentDataStream == null) {
            return true;
        }
        DataStreamLifecycle lifecycle = parentDataStream.getDataLifecycleForIndex(indexMetadata.getIndex());
        if (lifecycle != null && lifecycle.enabled()) {
            // index has both ILM and data stream lifecycle configured so let's check which is preferred
            return PREFER_ILM_SETTING.get(indexMetadata.getSettings());
        }

        return true;
    }

    static boolean isStateEquals(ProjectMetadata project1, ProjectMetadata project2) {
        if (project1.settings().equals(project2.settings()) == false) {
            return false;
        }
        if (project1.templates().equals(project2.templates()) == false) {
            return false;
        }
        if (Metadata.customsEqual(project1.customs(), project2.customs()) == false) {
            return false;
        }
        return true;
    }

    public Optional<SecureString> getSecret(String key) {
        return Optional.ofNullable(this.<ProjectSecrets>custom(ProjectSecrets.TYPE)).map(secrets -> secrets.getSettings().getString(key));
    }

    public static ProjectMetadata.Builder builder(ProjectId id) {
        return new ProjectMetadata.Builder().id(id);
    }

    public static ProjectMetadata.Builder builder(ProjectMetadata projectMetadata) {
        return new ProjectMetadata.Builder(projectMetadata);
    }

    public ProjectMetadata copyAndUpdate(Consumer<Builder> updater) {
        var builder = builder(this);
        updater.accept(builder);
        return builder.build();
    }

    public static class Builder {

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Metadata.ProjectCustom> customs;
        private final ImmutableOpenMap.Builder<String, ReservedStateMetadata> reservedStateMetadata;
        private Settings settings = Settings.EMPTY;

        private SortedMap<String, IndexAbstraction> previousIndicesLookup;

        private final Map<String, MappingMetadata> mappingsByHash;
        // If this is set to false we can skip checking #mappingsByHash for unused entries in #build(). Used as an optimization to save
        // the rather expensive logic for removing unused mappings when building from another instance and we know that no mappings can
        // have become unused because no indices were updated or removed from this builder in a way that would cause unused entries in
        // #mappingsByHash.
        private boolean checkForUnusedMappings = true;
        private ProjectId id;

        Builder(ProjectMetadata projectMetadata) {
            this.id = projectMetadata.id;
            this.indices = ImmutableOpenMap.builder(projectMetadata.indices);
            this.templates = ImmutableOpenMap.builder(projectMetadata.templates);
            this.customs = ImmutableOpenMap.builder(projectMetadata.customs);
            this.reservedStateMetadata = ImmutableOpenMap.builder(projectMetadata.reservedStateMetadata);
            this.settings = projectMetadata.settings;
            this.previousIndicesLookup = projectMetadata.indicesLookup;
            this.mappingsByHash = new HashMap<>(projectMetadata.mappingsByHash);
            this.checkForUnusedMappings = false;
        }

        Builder() {
            this(Map.of(), 0);
        }

        Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            indices = ImmutableOpenMap.builder(indexCountHint);
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            reservedStateMetadata = ImmutableOpenMap.builder();
            previousIndicesLookup = null;
            this.mappingsByHash = new HashMap<>(mappingsByHash);
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder id(ProjectId id) {
            assert this.id == null : "a project's ID cannot be changed";
            this.id = id;
            return this;
        }

        public ProjectId getId() {
            return id;
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            dedupeMapping(indexMetadataBuilder);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            IndexMetadata previous = indices.put(indexMetadata.getIndex().getName(), indexMetadata);
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
            if (unsetPreviousIndicesLookup(previous, indexMetadata)) {
                previousIndicesLookup = null;
            }
            maybeSetMappingPurgeFlag(previous, indexMetadata);
            return this;
        }

        public Builder indices(Map<String, IndexMetadata> indices) {
            for (var value : indices.values()) {
                put(value, false);
            }
            return this;
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
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            previousIndicesLookup = null;
            checkForUnusedMappings = true;

            indices.clear();
            mappingsByHash.clear();
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

            var ctm = (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE);
            Map<String, ComponentTemplate> existingTemplates = ctm != null ? new HashMap<>(ctm.componentTemplates()) : new HashMap<>();
            existingTemplates.put(name, componentTemplate);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            var ctm = (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE);
            if (ctm != null) {
                var existingTemplates = new HashMap<>(ctm.componentTemplates());
                if (existingTemplates.remove(name) != null) {
                    this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
                }
            }
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

            var itmd = (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE);
            Map<String, ComposableIndexTemplate> existingTemplates = itmd != null ? new HashMap<>(itmd.indexTemplates()) : new HashMap<>();
            existingTemplates.put(name, indexTemplate);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            var itmd = (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE);
            if (itmd != null) {
                var existingTemplates = new HashMap<>(itmd.indexTemplates());
                if (existingTemplates.remove(name) != null) {
                    this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
                }
            }
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
            Objects.requireNonNull(dataStream, "it is invalid to add a null data stream");
            previousIndicesLookup = null;

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
            DataStreamMetadata existing = dataStreamMetadata();
            DataStreamMetadata updated = existing.withAlias(aliasName, dataStream, isWriteDataStream, filter);
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

            DataStreamMetadata existing = dataStreamMetadata();
            DataStreamMetadata updated = existing.withRemovedAlias(aliasName, dataStreamName, mustExist);
            if (existing == updated) {
                return false;
            }
            this.customs.put(DataStreamMetadata.TYPE, updated);
            return true;
        }

        @SuppressWarnings("unchecked")
        public <T extends Metadata.ProjectCustom> T getCustom(String type) {
            return (T) customs.get(type);
        }

        public Builder putCustom(String type, Metadata.ProjectCustom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder removeCustomIf(BiPredicate<String, ? super Metadata.ProjectCustom> p) {
            customs.removeAll(p);
            return this;
        }

        public Builder clearCustoms() {
            customs.clear();
            return this;
        }

        public Builder customs(Map<String, Metadata.ProjectCustom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.customs.putAllFromMap(customs);
            return this;
        }

        public Builder put(Map<String, ReservedStateMetadata> reservedStateMetadata) {
            this.reservedStateMetadata.putAllFromMap(reservedStateMetadata);
            return this;
        }

        public Builder put(ReservedStateMetadata metadata) {
            reservedStateMetadata.put(metadata.namespace(), metadata);
            return this;
        }

        public Builder removeReservedState(ReservedStateMetadata metadata) {
            reservedStateMetadata.remove(metadata.namespace());
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            return putCustom(IndexGraveyard.TYPE, indexGraveyard);
        }

        public IndexGraveyard indexGraveyard() {
            return (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(String[]::new);
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
        public Builder updateNumberOfReplicas(int numberOfReplicas, String... indices) {
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfReplicas(numberOfReplicas));
            }
            return this;
        }

        public ProjectMetadata build() {
            return build(false);
        }

        public ProjectMetadata build(boolean skipNameCollisionChecks) {
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

            int oldestIndexVersionId = IndexVersion.current().id();
            int totalNumberOfShards = 0;
            int totalOpenIndexShards = 0;

            ImmutableOpenMap.Builder<String, Set<Index>> aliasedIndicesBuilder = ImmutableOpenMap.builder();
            final String[] allIndicesArray = new String[indicesMap.size()];
            int i = 0;
            final Set<String> sha256HashesInUse = checkForUnusedMappings ? Sets.newHashSetWithExpectedSize(mappingsByHash.size()) : null;
            for (var entry : indicesMap.entrySet()) {
                allIndicesArray[i++] = entry.getKey();
                final IndexMetadata indexMetadata = entry.getValue();
                totalNumberOfShards += indexMetadata.getTotalNumberOfShards();
                final String name = indexMetadata.getIndex().getName();
                final boolean visible = indexMetadata.isHidden() == false;
                if (visible) {
                    visibleIndices.add(name);
                }
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                    totalOpenIndexShards += indexMetadata.getTotalNumberOfShards();
                    allOpenIndices.add(name);
                    if (visible) {
                        visibleOpenIndices.add(name);
                    }
                } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    allClosedIndices.add(name);
                    if (visible) {
                        visibleClosedIndices.add(name);
                    }
                }
                oldestIndexVersionId = Math.min(oldestIndexVersionId, indexMetadata.getCompatibilityVersion().id());
                if (sha256HashesInUse != null) {
                    final var mapping = indexMetadata.mapping();
                    if (mapping != null) {
                        sha256HashesInUse.add(mapping.getSha256());
                    }
                }
                for (var alias : indexMetadata.getAliases().keySet()) {
                    var indices = aliasedIndicesBuilder.get(alias);
                    if (indices == null) {
                        indices = new HashSet<>();
                        aliasedIndicesBuilder.put(alias, indices);
                    }
                    indices.add(indexMetadata.getIndex());
                }
            }

            for (String alias : aliasedIndicesBuilder.keys()) {
                aliasedIndicesBuilder.put(alias, Collections.unmodifiableSet(aliasedIndicesBuilder.get(alias)));
            }
            var aliasedIndices = aliasedIndicesBuilder.build();
            for (var entry : aliasedIndices.entrySet()) {
                List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
                validateAlias(entry.getKey(), aliasIndices);
            }
            SortedMap<String, IndexAbstraction> indicesLookup = null;
            if (previousIndicesLookup != null) {
                // no changes to the names of indices, datastreams, and their aliases so we can reuse the previous lookup
                assert previousIndicesLookup.equals(buildIndicesLookup(dataStreamMetadata(), indicesMap));
                indicesLookup = previousIndicesLookup;
            } else if (skipNameCollisionChecks == false) {
                // we have changes to the entity names so we ensure we have no naming collisions
                ensureNoNameCollisions(aliasedIndices.keySet(), indicesMap, dataStreamMetadata());
            }
            assert assertDataStreams(indicesMap, dataStreamMetadata());

            if (sha256HashesInUse != null) {
                mappingsByHash.keySet().retainAll(sha256HashesInUse);
            }

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] visibleIndicesArray = visibleIndices.toArray(String[]::new);
            String[] allOpenIndicesArray = allOpenIndices.toArray(String[]::new);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(String[]::new);
            String[] allClosedIndicesArray = allClosedIndices.toArray(String[]::new);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(String[]::new);

            return new ProjectMetadata(
                id,
                indicesMap,
                aliasedIndices,
                templates.build(),
                customs.build(),
                reservedStateMetadata.build(),
                totalNumberOfShards,
                totalOpenIndexShards,
                allIndicesArray,
                visibleIndicesArray,
                allOpenIndicesArray,
                visibleOpenIndicesArray,
                allClosedIndicesArray,
                visibleClosedIndicesArray,
                indicesLookup,
                Collections.unmodifiableMap(mappingsByHash),
                settings,
                IndexVersion.fromId(oldestIndexVersionId)
            );
        }

        static void ensureNoNameCollisions(
            Set<String> indexAliases,
            ImmutableOpenMap<String, IndexMetadata> indicesMap,
            DataStreamMetadata dataStreamMetadata
        ) {
            List<String> duplicates = new ArrayList<>();
            Set<String> aliasDuplicatesWithIndices = new HashSet<>();
            Set<String> aliasDuplicatesWithDataStreams = new HashSet<>();
            var allDataStreams = dataStreamMetadata.dataStreams();
            // Adding data stream aliases:
            for (String dataStreamAlias : dataStreamMetadata.getDataStreamAliases().keySet()) {
                if (indexAliases.contains(dataStreamAlias)) {
                    duplicates.add("data stream alias and indices alias have the same name (" + dataStreamAlias + ")");
                }
                if (indicesMap.containsKey(dataStreamAlias)) {
                    aliasDuplicatesWithIndices.add(dataStreamAlias);
                }
                if (allDataStreams.containsKey(dataStreamAlias)) {
                    aliasDuplicatesWithDataStreams.add(dataStreamAlias);
                }
            }
            for (String alias : indexAliases) {
                if (allDataStreams.containsKey(alias)) {
                    aliasDuplicatesWithDataStreams.add(alias);
                }
                if (indicesMap.containsKey(alias)) {
                    aliasDuplicatesWithIndices.add(alias);
                }
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

        /**
         * Iterates the detected duplicates between datastreams and aliases and collects them into the duplicates list as helpful messages.
         */
        private static void collectAliasDuplicates(
            ImmutableOpenMap<String, IndexMetadata> indicesMap,
            DataStreamMetadata dataStreamMetadata,
            Set<String> aliasDuplicatesWithDataStreams,
            List<String> duplicates
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
                // This is for adding an error message for when a data stream alias has the same name as a data stream.
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
            List<String> duplicates
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
            Map<String, IndexAbstraction> indicesLookup = new HashMap<>();
            Map<String, DataStream> indexToDataStreamLookup = new HashMap<>();
            collectDataStreams(dataStreamMetadata, indicesLookup, indexToDataStreamLookup);

            Map<String, List<IndexMetadata>> aliasToIndices = new HashMap<>();
            collectIndices(indices, indexToDataStreamLookup, indicesLookup, aliasToIndices);
            collectAliases(aliasToIndices, indicesLookup);

            // We do a ton of lookups on this map but also need its sorted properties at times.
            // Using this hybrid of a sorted and a hash-map trades some heap overhead relative to just using a TreeMap
            // for much faster O(1) lookups in large clusters.
            return new SortedMap<>() {

                private final SortedMap<String, IndexAbstraction> sortedMap = Collections.unmodifiableSortedMap(
                    new TreeMap<>(indicesLookup)
                );

                @Override
                public Comparator<? super String> comparator() {
                    return sortedMap.comparator();
                }

                @Override
                public SortedMap<String, IndexAbstraction> subMap(String fromKey, String toKey) {
                    return sortedMap.subMap(fromKey, toKey);
                }

                @Override
                public SortedMap<String, IndexAbstraction> headMap(String toKey) {
                    return sortedMap.headMap(toKey);
                }

                @Override
                public SortedMap<String, IndexAbstraction> tailMap(String fromKey) {
                    return sortedMap.tailMap(fromKey);
                }

                @Override
                public String firstKey() {
                    return sortedMap.firstKey();
                }

                @Override
                public String lastKey() {
                    return sortedMap.lastKey();
                }

                @Override
                public Set<String> keySet() {
                    return sortedMap.keySet();
                }

                @Override
                public Collection<IndexAbstraction> values() {
                    return sortedMap.values();
                }

                @Override
                public Set<Entry<String, IndexAbstraction>> entrySet() {
                    return sortedMap.entrySet();
                }

                @Override
                public int size() {
                    return indicesLookup.size();
                }

                @Override
                public boolean isEmpty() {
                    return indicesLookup.isEmpty();
                }

                @Override
                public boolean containsKey(Object key) {
                    return indicesLookup.containsKey(key);
                }

                @Override
                public boolean containsValue(Object value) {
                    return indicesLookup.containsValue(value);
                }

                @Override
                public IndexAbstraction get(Object key) {
                    return indicesLookup.get(key);
                }

                @Override
                public IndexAbstraction put(String key, IndexAbstraction value) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public IndexAbstraction remove(Object key) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void putAll(Map<? extends String, ? extends IndexAbstraction> m) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void clear() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean equals(Object obj) {
                    if (obj == null) {
                        return false;
                    }
                    if (getClass() != obj.getClass()) {
                        return false;
                    }
                    return indicesLookup.equals(obj);
                }

                @Override
                public int hashCode() {
                    return indicesLookup.hashCode();
                }
            };
        }

        private static void collectAliases(Map<String, List<IndexMetadata>> aliasToIndices, Map<String, IndexAbstraction> indicesLookup) {
            for (var entry : aliasToIndices.entrySet()) {
                AliasMetadata alias = entry.getValue().get(0).getAliases().get(entry.getKey());
                IndexAbstraction existing = indicesLookup.put(entry.getKey(), new IndexAbstraction.Alias(alias, entry.getValue()));
                assert existing == null : "duplicate for " + entry.getKey();
            }
        }

        private static void collectIndices(
            Map<String, IndexMetadata> indices,
            Map<String, DataStream> indexToDataStreamLookup,
            Map<String, IndexAbstraction> indicesLookup,
            Map<String, List<IndexMetadata>> aliasToIndices
        ) {
            for (var entry : indices.entrySet()) {
                String name = entry.getKey();
                IndexMetadata indexMetadata = entry.getValue();
                DataStream parent = indexToDataStreamLookup.get(name);
                assert assertContainsIndexIfDataStream(parent, indexMetadata);
                IndexAbstraction existing = indicesLookup.put(name, new IndexAbstraction.ConcreteIndex(indexMetadata, parent));
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (var aliasMetadata : indexMetadata.getAliases().values()) {
                    List<IndexMetadata> aliasIndices = aliasToIndices.computeIfAbsent(aliasMetadata.getAlias(), k -> new ArrayList<>());
                    aliasIndices.add(indexMetadata);
                }
            }
        }

        private static boolean assertContainsIndexIfDataStream(DataStream parent, IndexMetadata indexMetadata) {
            assert parent == null
                || parent.getIndices().stream().anyMatch(index -> indexMetadata.getIndex().getName().equals(index.getName()))
                || parent.getFailureComponent()
                    .getIndices()
                    .stream()
                    .anyMatch(index -> indexMetadata.getIndex().getName().equals(index.getName()))
                : "Expected data stream [" + parent.getName() + "] to contain index " + indexMetadata.getIndex();
            return true;
        }

        private static void collectDataStreams(
            DataStreamMetadata dataStreamMetadata,
            Map<String, IndexAbstraction> indicesLookup,
            Map<String, DataStream> indexToDataStreamLookup
        ) {
            var dataStreams = dataStreamMetadata.dataStreams();
            for (DataStreamAlias alias : dataStreamMetadata.getDataStreamAliases().values()) {
                IndexAbstraction existing = indicesLookup.put(alias.getName(), makeDsAliasAbstraction(dataStreams, alias));
                assert existing == null : "duplicate data stream alias for " + alias.getName();
            }
            for (DataStream dataStream : dataStreams.values()) {
                IndexAbstraction existing = indicesLookup.put(dataStream.getName(), dataStream);
                assert existing == null : "duplicate data stream for " + dataStream.getName();

                for (Index i : dataStream.getIndices()) {
                    indexToDataStreamLookup.put(i.getName(), dataStream);
                }
                for (Index i : dataStream.getFailureIndices()) {
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
                writeIndexOfWriteDataStream,
                alias.getDataStreams()
            );
        }

        private static boolean isNonEmpty(List<IndexMetadata> idxMetas) {
            return (Objects.isNull(idxMetas) || idxMetas.isEmpty()) == false;
        }

        static void validateAlias(String aliasName, List<IndexMetadata> indexMetadatas) {
            // Validate write indices
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

            // Validate hidden status
            Map<Boolean, List<IndexMetadata>> groupedByHiddenStatus = indexMetadatas.stream()
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

            // Validate system status
            Map<Boolean, List<IndexMetadata>> groupedBySystemStatus = indexMetadatas.stream()
                .collect(Collectors.groupingBy(IndexMetadata::isSystem));
            // If the alias has either all system or all non-system, then no more validation is required
            if (isNonEmpty(groupedBySystemStatus.get(false)) && isNonEmpty(groupedBySystemStatus.get(true))) {
                final List<String> newVersionSystemIndices = groupedBySystemStatus.get(true)
                    .stream()
                    .filter(i -> i.getCreationVersion().onOrAfter(IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION))
                    .map(i -> i.getIndex().getName())
                    .sorted() // reliable error message for testing
                    .toList();

                if (newVersionSystemIndices.isEmpty() == false) {
                    List<String> nonSystemIndices = groupedBySystemStatus.get(false)
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

        static boolean assertDataStreams(Map<String, IndexMetadata> indices, DataStreamMetadata dsMetadata) {
            // Sanity check, because elsewhere a more user friendly error should have occurred:
            List<String> conflictingAliases = dsMetadata.dataStreams()
                .values()
                .stream()
                .flatMap(ds -> ds.getIndices().stream())
                .map(index -> indices.get(index.getName()))
                .filter(Objects::nonNull)
                .flatMap(im -> im.getAliases().values().stream())
                .map(AliasMetadata::alias)
                .toList();

            if (conflictingAliases.isEmpty() == false) {
                throw new AssertionError("aliases " + conflictingAliases + " cannot refer to backing indices of data streams");
            }

            return true;
        }

        public static ProjectMetadata fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = null;

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            ProjectMetadata.Builder projectBuilder = new Builder();

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case "id" -> projectBuilder.id(ProjectId.fromXContent(parser));
                        default -> throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case "reserved_state" -> {
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                projectBuilder.put(ReservedStateMetadata.fromXContent(parser));
                            }
                        }
                        case "indices" -> {
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                projectBuilder.put(IndexMetadata.Builder.fromXContent(parser), false);
                            }
                        }
                        case "templates" -> {
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                projectBuilder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                            }
                        }
                        case "settings" -> {
                            projectBuilder.settings(Settings.fromXContent(parser));
                        }
                        default -> Metadata.Builder.parseCustomObject(
                            parser,
                            currentFieldName,
                            Metadata.ProjectCustom.class,
                            projectBuilder::putCustom
                        );
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            return projectBuilder.build();
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params p) {
        Metadata.XContentContext context = Metadata.XContentContext.from(p);

        Iterator<? extends ToXContent> indices = context == Metadata.XContentContext.API
            ? ChunkedToXContentHelper.object("indices", indices().values().iterator())
            : Collections.emptyIterator();

        final var multiProject = p.paramAsBoolean("multi-project", false);
        Iterator<ToXContent> customs = Iterators.flatMap(customs().entrySet().iterator(), entry -> {
            if (entry.getValue().context().contains(context)
                // Include persistent tasks in the output only when multi-project=true.
                // In single-project-mode (multi-project=false), we already output them in Metadata.
                && (multiProject || PersistentTasksCustomMetadata.TYPE.equals(entry.getKey()) == false)) {
                return ChunkedToXContentHelper.object(entry.getKey(), entry.getValue().toXContentChunked(p));
            } else {
                return Collections.emptyIterator();
            }
        });

        return Iterators.concat(
            ChunkedToXContentHelper.object(
                "templates",
                Iterators.map(
                    templates().values().iterator(),
                    template -> (builder, params) -> IndexTemplateMetadata.Builder.toXContentWithTypes(template, builder, params)
                )
            ),
            indices,
            customs,
            multiProject ? Iterators.single((builder, params) -> {
                builder.startObject("settings");
                settings.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                return builder.endObject();
            }) : Collections.emptyIterator(),
            multiProject
                ? ChunkedToXContentHelper.object("reserved_state", reservedStateMetadata().values().iterator())
                : Collections.emptyIterator()
        );
    }

    public static ProjectMetadata readFrom(StreamInput in) throws IOException {
        ProjectId id = ProjectId.readFrom(in);
        Builder builder = builder(id);
        Function<String, MappingMetadata> mappingLookup;
        Map<String, MappingMetadata> mappingMetadataMap = in.readMapValues(MappingMetadata::new, MappingMetadata::getSha256);
        if (mappingMetadataMap.isEmpty() == false) {
            mappingLookup = mappingMetadataMap::get;
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

        readProjectCustoms(in, builder);

        int reservedStateSize = in.readVInt();
        for (int i = 0; i < reservedStateSize; i++) {
            builder.put(ReservedStateMetadata.readFrom(in));
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_METADATA_SETTINGS)) {
            builder.settings(Settings.readSettingsFromStream(in));
        }

        return builder.build();
    }

    private static void readProjectCustoms(StreamInput in, Builder builder) throws IOException {
        Set<String> clusterScopedNames = in.namedWriteableRegistry().getReaders(Metadata.ProjectCustom.class).keySet();
        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            String name = in.readString();
            if (clusterScopedNames.contains(name)) {
                Metadata.ProjectCustom custom = in.readNamedWriteable(Metadata.ProjectCustom.class, name);
                builder.putCustom(custom.getWriteableName(), custom);
            } else {
                throw new IllegalArgumentException("Unknown project custom name [" + name + "]");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        id.writeTo(out);
        // we write the mapping metadata first and then write the indices without metadata so that
        // we avoid writing duplicate mappings twice
        out.writeMapValues(mappingsByHash);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out, true);
        }
        out.writeCollection(templates.values());
        Collection<Metadata.ProjectCustom> filteredCustoms = customs.values();
        if (out.getTransportVersion().before(TransportVersions.REPOSITORIES_METADATA_AS_PROJECT_CUSTOM)) {
            // RepositoriesMetadata is sent as part of Metadata#customs for version before RepositoriesMetadata migration
            // So we exclude it from the project level customs
            if (custom(RepositoriesMetadata.TYPE) != null) {
                assert ProjectId.DEFAULT.equals(id)
                    : "Only default project can have repositories metadata. Otherwise the code should have thrown before it reaches here";
                filteredCustoms = filteredCustoms.stream().filter(custom -> custom instanceof RepositoriesMetadata == false).toList();
            }
        }
        VersionedNamedWriteable.writeVersionedWriteables(out, filteredCustoms);
        out.writeCollection(reservedStateMetadata.values());

        if (out.getTransportVersion().onOrAfter(TransportVersions.PROJECT_METADATA_SETTINGS)) {
            settings.writeTo(out);
        }
    }

    // this needs to be package accessible for bwc serialization in Metadata.java
    static class ProjectMetadataDiff implements Diff<ProjectMetadata> {

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, ReservedStateMetadata> RESERVED_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(ReservedStateMetadata::readFrom, ReservedStateMetadata::readDiffFrom);

        private final DiffableUtils.MapDiff<String, IndexMetadata, ImmutableOpenMap<String, IndexMetadata>> indices;
        private final DiffableUtils.MapDiff<String, IndexTemplateMetadata, ImmutableOpenMap<String, IndexTemplateMetadata>> templates;
        private final DiffableUtils.MapDiff<String, Metadata.ProjectCustom, ImmutableOpenMap<String, Metadata.ProjectCustom>> customs;
        private final DiffableUtils.MapDiff<
            String,
            ReservedStateMetadata,
            ImmutableOpenMap<String, ReservedStateMetadata>> reservedStateMetadata;
        private final Diff<Settings> settingsDiff;

        private ProjectMetadataDiff(ProjectMetadata before, ProjectMetadata after) {
            if (before == after) {
                indices = DiffableUtils.emptyDiff();
                templates = DiffableUtils.emptyDiff();
                customs = DiffableUtils.emptyDiff();
                reservedStateMetadata = DiffableUtils.emptyDiff();
                settingsDiff = Settings.EMPTY_DIFF;
            } else {
                indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
                templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
                customs = DiffableUtils.diff(
                    before.customs,
                    after.customs,
                    DiffableUtils.getStringKeySerializer(),
                    PROJECT_CUSTOM_VALUE_SERIALIZER
                );
                reservedStateMetadata = DiffableUtils.diff(
                    before.reservedStateMetadata,
                    after.reservedStateMetadata,
                    DiffableUtils.getStringKeySerializer()
                );
                settingsDiff = after.settings.diff(before.settings);
            }
        }

        ProjectMetadataDiff(
            DiffableUtils.MapDiff<String, IndexMetadata, ImmutableOpenMap<String, IndexMetadata>> indices,
            DiffableUtils.MapDiff<String, IndexTemplateMetadata, ImmutableOpenMap<String, IndexTemplateMetadata>> templates,
            DiffableUtils.MapDiff<String, Metadata.ProjectCustom, ImmutableOpenMap<String, Metadata.ProjectCustom>> customs,
            DiffableUtils.MapDiff<String, ReservedStateMetadata, ImmutableOpenMap<String, ReservedStateMetadata>> reservedStateMetadata,
            Diff<Settings> settingsDiff
        ) {
            this.indices = indices;
            this.templates = templates;
            this.customs = customs;
            this.reservedStateMetadata = reservedStateMetadata;
            this.settingsDiff = settingsDiff;
        }

        ProjectMetadataDiff(StreamInput in) throws IOException {
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), PROJECT_CUSTOM_VALUE_SERIALIZER);
            reservedStateMetadata = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                RESERVED_DIFF_VALUE_READER
            );
            if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_METADATA_SETTINGS)) {
                settingsDiff = Settings.readSettingsDiffFromStream(in);
            } else {
                settingsDiff = Settings.EMPTY_DIFF;
            }
        }

        Diff<ImmutableOpenMap<String, IndexMetadata>> indices() {
            return indices;
        }

        Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates() {
            return templates;
        }

        DiffableUtils.MapDiff<String, Metadata.ProjectCustom, ImmutableOpenMap<String, Metadata.ProjectCustom>> customs() {
            return customs;
        }

        DiffableUtils.MapDiff<String, ReservedStateMetadata, ImmutableOpenMap<String, ReservedStateMetadata>> reservedStateMetadata() {
            return reservedStateMetadata;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
            reservedStateMetadata.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.PROJECT_METADATA_SETTINGS)) {
                settingsDiff.writeTo(out);
            }
        }

        @Override
        public ProjectMetadata apply(ProjectMetadata part) {
            if (indices.isEmpty()
                && templates.isEmpty()
                && customs.isEmpty()
                && reservedStateMetadata.isEmpty()
                && settingsDiff == Settings.EMPTY_DIFF) {
                // nothing to do
                return part;
            }
            var updatedIndices = indices.apply(part.indices);
            Builder builder = new Builder(part.mappingsByHash, updatedIndices.size());
            builder.id(part.id);
            builder.indices(updatedIndices);
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            builder.put(reservedStateMetadata.apply(part.reservedStateMetadata));
            if (part.indices == updatedIndices
                && builder.dataStreamMetadata() == part.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY)) {
                builder.previousIndicesLookup = part.indicesLookup;
            }
            builder.settings = settingsDiff.apply(part.settings);
            return builder.build(true);
        }

        ProjectMetadataDiff withCustoms(
            DiffableUtils.MapDiff<String, Metadata.ProjectCustom, ImmutableOpenMap<String, Metadata.ProjectCustom>> customs
        ) {
            return new ProjectMetadataDiff(indices, templates, customs, reservedStateMetadata, settingsDiff);
        }
    }

    @Override
    public ProjectMetadataDiff diff(ProjectMetadata previousState) {
        return new ProjectMetadataDiff(previousState, this);
    }
}
