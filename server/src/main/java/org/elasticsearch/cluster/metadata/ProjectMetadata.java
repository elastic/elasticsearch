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
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
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
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

/**
 * {@link ProjectMetadata} is the part of the {@link ClusterState} which is specific to projects - that is it deals with data-like concepts
 * (indices, templates) rather than topology concepts (nodes, etc)
 */
public class ProjectMetadata implements Iterable<IndexMetadata>, Diffable<ProjectMetadata>, ChunkedToXContent {

    private static final Logger logger = LogManager.getLogger(ProjectMetadata.class);

    public static final TransportVersion MAPPINGS_AS_HASH_VERSION = TransportVersions.V_8_1_0;

    /**
     * Project-level custom metadata that persists (via XContent) across restarts.
     * The deserialization method for each implementation must be registered with the {@link NamedXContentRegistry}.
     */
    public interface ProjectCustom extends Metadata.MetadataCustom<ProjectCustom> {}

    /*TODO private*/ static final NamedDiffableValueSerializer<ProjectCustom> PROJECT_CUSTOM_VALUE_SERIALIZER =
        new NamedDiffableValueSerializer<>(ProjectCustom.class);

    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, Set<Index>> aliasedIndices;
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, ProjectCustom> customs;

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

    private final IndexVersion oldestIndexVersion;

    // Used in the findAliases and findDataStreamAliases functions
    private interface AliasInfoGetter {
        List<? extends AliasInfo> get(String entityName);
    }

    // Used in the findAliases and findDataStreamAliases functions
    private interface AliasInfoSetter {
        void put(String entityName, List<AliasInfo> aliases);
    }

    ProjectMetadata(
        int totalNumberOfShards,
        int totalOpenIndexShards,
        ImmutableOpenMap<String, IndexMetadata> indices,
        ImmutableOpenMap<String, Set<Index>> aliasedIndices,
        ImmutableOpenMap<String, IndexTemplateMetadata> templates,
        ImmutableOpenMap<String, ProjectMetadata.ProjectCustom> customs,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, MappingMetadata> mappingsByHash,
        IndexVersion oldestIndexVersion
    ) {
        this.indices = indices;
        this.aliasedIndices = aliasedIndices;
        this.templates = templates;
        this.customs = customs;
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
        assert assertConsistent();
    }

    boolean assertConsistent() {
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

    /**
     * Given an index and lifecycle state, returns a metadata where the lifecycle state will be
     * associated with the given index.
     * <p>
     * The passed-in index must already be present in the cluster state, this method cannot
     * be used to add an index.
     *
     * @param index          A non-null index
     * @param lifecycleState A non-null lifecycle execution state
     * @return a <code>Metadata</code> instance where the index has the provided lifecycle state
     */
    public ProjectMetadata withLifecycleState(final Index index, final LifecycleExecutionState lifecycleState) {
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

        // construct a new object directly rather than using the builder.
        // The Builder validation needs to handle the general case where anything at all could
        // have changed, and hence it is expensive -- since we are changing so little about the metadata
        // (and at a leaf in the object tree), we can bypass that validation for efficiency's sake
        return new ProjectMetadata(
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
            oldestIndexVersion
        );
    }

    public ProjectMetadata withIndexSettingsUpdates(final Map<Index, Settings> updates) {
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
        return new ProjectMetadata(
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
            oldestIndexVersion
        );
    }

    /**
     * Creates a copy of this instance updated with the given {@link IndexMetadata} that must only contain changes to primary terms
     * and in-sync allocation ids relative to the existing entries. This method is only used by
     * {@link org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater#applyChanges(Metadata, RoutingTable)}.
     *
     * @param updates map of index name to {@link IndexMetadata}.
     * @return updated metadata instance
     */
    public ProjectMetadata withAllocationAndTermUpdatesOnly(Map<String, IndexMetadata> updates) {
        if (updates.isEmpty()) {
            return this;
        }
        final var updatedIndicesBuilder = ImmutableOpenMap.builder(indices);
        updatedIndicesBuilder.putAllFromMap(updates);
        return new ProjectMetadata(
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
            oldestIndexVersion
        );
    }

    /**
     * Creates a copy of this instance with the given {@code index} added.
     *
     * @param index index to add
     * @return copy with added index
     */
    public ProjectMetadata withAddedIndex(IndexMetadata index) {
        final String indexName = index.getIndex().getName();
        ensureNoNameCollision(indexName);
        final Map<String, AliasMetadata> aliases = index.getAliases();
        final ImmutableOpenMap<String, Set<Index>> updatedAliases = aliasesAfterAddingIndex(index, aliases);
        final String[] updatedVisibleIndices;
        if (index.isHidden()) {
            updatedVisibleIndices = visibleIndices;
        } else {
            updatedVisibleIndices = ArrayUtils.append(visibleIndices, indexName);
        }

        final String[] updatedAllIndices = ArrayUtils.append(allIndices, indexName);
        final String[] updatedOpenIndices;
        final String[] updatedClosedIndices;
        final String[] updatedVisibleOpenIndices;
        final String[] updatedVisibleClosedIndices;
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

        final MappingMetadata mappingMetadata = index.mapping();
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

        final ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder(indices);
        builder.put(indexName, index);
        final ImmutableOpenMap<String, IndexMetadata> indicesMap = builder.build();
        for (var entry : updatedAliases.entrySet()) {
            List<IndexMetadata> aliasIndices = entry.getValue().stream().map(idx -> indicesMap.get(idx.getName())).toList();
            Builder.validateAlias(entry.getKey(), aliasIndices);
        }
        return new ProjectMetadata(
            totalNumberOfShards + index.getTotalNumberOfShards(),
            totalOpenIndexShards + (index.getState() == IndexMetadata.State.OPEN ? index.getTotalNumberOfShards() : 0),
            indicesMap,
            updatedAliases,
            templates,
            customs,
            updatedAllIndices,
            updatedVisibleIndices,
            updatedOpenIndices,
            updatedVisibleOpenIndices,
            updatedClosedIndices,
            updatedVisibleClosedIndices,
            null,
            updatedMappingsByHash,
            IndexVersion.min(index.getCompatibilityVersion(), oldestIndexVersion)
        );
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

    public IndexVersion oldestIndexVersion() {
        return this.oldestIndexVersion;
    }

    public boolean equalsAliases(ProjectMetadata other) {
        for (IndexMetadata otherIndex : other.indices().values()) {
            IndexMetadata thisIndex = index(otherIndex.getIndex());
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

    public boolean sameIndicesLookup(ProjectMetadata other) {
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
     * @param aliases         The aliases to look for. Might contain include or exclude wildcards.
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAliases(final String[] aliases, final String[] concreteIndices) {
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();

        AliasInfoGetter getter = index -> indices.get(index).getAliases().values().stream().toList();

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
     * @param aliases     The aliases to look for. Might contain include or exclude wildcards.
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
     * @param aliases         The aliases to look for. Might contain include or exclude wildcards.
     * @param possibleMatches The data streams or indices that the aliases must point to in order to be returned
     * @param getter          A function that is used to get the alises for a given data stream or index
     * @param setter          A function that is used to keep track of the found aliases
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
                            matched = Metadata.ALL.equals(pattern) || Regex.simpleMatch(pattern, alias);
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
     * @param onNextIndex a hook that gets notified for each index that's processed
     * @see MapperPlugin#getFieldFilter()
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

    /**
     * Checks whether the provided index is a data stream.
     */
    public boolean indexIsADataStream(String indexName) {
        final SortedMap<String, IndexAbstraction> lookup = getIndicesLookup();
        IndexAbstraction abstraction = lookup.get(indexName);
        return abstraction != null && abstraction.getType() == IndexAbstraction.Type.DATA_STREAM;
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

    /**
     * Checks whether an index exists (as of this {@link Metadata} with the given name. Does not check aliases or data streams.
     *
     * @param index An index name that may or may not exist in the cluster.
     * @return {@code true} if a concrete index with that name exists, {@code false} otherwise.
     */
    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    /**
     * Checks whether an index exists. Similar to {@link Metadata#hasIndex(String)}, but ensures that the index has the same UUID as
     * the given {@link Index}.
     *
     * @param index An {@link Index} object that may or may not exist in the cluster.
     * @return {@code true} if an index exists with the same name and UUID as the given index object, {@code false} otherwise.
     */
    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    /**
     * Checks whether an index abstraction (that is, index, alias, or data stream) exists (as of this {@link Metadata} with the given name.
     *
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
     * Returns true iff existing index has the same {@link IndexMetadata} instance
     */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     *
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
        return Optional.ofNullable(this.<ComponentTemplateMetadata>custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) this.custom(ComposableIndexTemplateMetadata.TYPE))
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
        return this.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).dataStreams();
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return this.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY).getDataStreamAliases();
    }

    /**
     * Return a map of DataStreamAlias objects by DataStream name
     *
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
        if (parentDataStream != null && parentDataStream.getLifecycle() != null && parentDataStream.getLifecycle().isEnabled()) {
            // index has both ILM and data stream lifecycle configured so let's check which is preferred
            return PREFER_ILM_SETTING.get(indexMetadata.getSettings());
        }

        return true;
    }

    public Map<String, ProjectMetadata.ProjectCustom> customs() {
        return this.customs;
    }

    @SuppressWarnings("unchecked")
    public <T extends ProjectCustom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends ProjectCustom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return custom(IndexGraveyard.TYPE);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     *
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     *
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenIndexShards;
    }

    public Map<String, MappingMetadata> getMappingsByHash() {
        return mappingsByHash;
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

    public static boolean isGlobalStateEquals(ProjectMetadata metadata1, ProjectMetadata metadata2) {
        if (metadata1.templates.equals(metadata2.templates()) == false) {
            return false;
        }
        if (Metadata.customsEqual(metadata1.customs, metadata2.customs) == false) {
            return false;
        }
        return true;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeMapValues(mappingsByHash);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out, true);
        }
        out.writeCollection(templates.values());
        VersionedNamedWriteable.writeVersionedWritables(out, customs);
    }

    public static ProjectMetadata readFrom(StreamInput in) throws IOException {
        var builder = new Builder();

        final Map<String, MappingMetadata> mappingMetadataMap = in.readMapValues(MappingMetadata::new, MappingMetadata::getSha256);
        final Function<String, MappingMetadata> mappingLookup;
        if (mappingMetadataMap.size() > 0) {
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

        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            var customIndexMetadata = in.readNamedWriteable(ProjectCustom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }

        return builder.build();
    }

    public Diff<ProjectMetadata> diff(ProjectMetadata previousState) {
        return new ProjectMetadataDiff(previousState, this);
    }

    public static Diff<ProjectMetadata> readDiffFrom(StreamInput in) throws IOException {
        final boolean empty = in.readBoolean();
        return empty ? SimpleDiffable.empty() : new ProjectMetadataDiff(in);
    }

    static class ProjectMetadataDiff implements Diff<ProjectMetadata> {
        private final Diff<ImmutableOpenMap<String, IndexMetadata>> indices;
        private final Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates;
        private final DiffableUtils.MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> customs;

        /**
         * true if this diff is a noop because before and after were the same instance
         */
        private final boolean empty;

        ProjectMetadataDiff(ProjectMetadata before, ProjectMetadata after) {
            this.empty = before == after;
            if (empty) {
                indices = DiffableUtils.emptyDiff();
                templates = DiffableUtils.emptyDiff();
                customs = DiffableUtils.emptyDiff();
            } else {
                indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
                templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
                customs = DiffableUtils.diff(
                    before.customs,
                    after.customs,
                    DiffableUtils.getStringKeySerializer(),
                    PROJECT_CUSTOM_VALUE_SERIALIZER
                );
            }
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);

        // Used by Metadata for BWC
        ProjectMetadataDiff(
            Diff<ImmutableOpenMap<String, IndexMetadata>> indices,
            Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates,
            DiffableUtils.MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> customs
        ) {
            this.empty = false;
            this.indices = indices;
            this.templates = templates;
            this.customs = customs;
        }

        private ProjectMetadataDiff(StreamInput in) throws IOException {
            empty = false;
            indices = readIndices(in);
            templates = readTemplates(in);
            customs = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ProjectMetadata.PROJECT_CUSTOM_VALUE_SERIALIZER
            );
        }

        static Diff<ImmutableOpenMap<String, IndexMetadata>> readIndices(StreamInput in) throws IOException {
            return DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
        }

        static Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> readTemplates(StreamInput in) throws IOException {
            return DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(empty);
            if (empty) {
                // noop diff
                return;
            }
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public ProjectMetadata apply(ProjectMetadata part) {
            if (empty) {
                return part;
            }
            // create builder from existing mappings hashes so we don't change existing index metadata instances when deduplicating
            // mappings in the builder
            final var updatedIndices = indices.apply(part.indices);
            var builder = new Builder(part.mappingsByHash, updatedIndices.size());
            builder.indices(updatedIndices);
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            if (part.indices == updatedIndices
                && builder.dataStreamMetadata() == part.custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY)) {
                builder.previousIndicesLookup = part.indicesLookup;
            }
            return builder.build(true);
        }

        // To support BWC writing in Metadata
        static Diff<ImmutableOpenMap<String, IndexMetadata>> indices(Diff<ProjectMetadata> diff) {
            if (diff instanceof ProjectMetadataDiff pmd) {
                return pmd.indices;
            } else {
                return DiffableUtils.emptyDiff();
            }
        }

        static Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates(Diff<ProjectMetadata> diff) {
            if (diff instanceof ProjectMetadataDiff pmd) {
                return pmd.templates;
            } else {
                return DiffableUtils.emptyDiff();
            }
        }

        static DiffableUtils.MapDiff<String, ProjectCustom, ImmutableOpenMap<String, ProjectCustom>> customs(Diff<ProjectMetadata> diff) {
            if (diff instanceof ProjectMetadataDiff pmd) {
                return pmd.customs;
            } else {
                return DiffableUtils.emptyDiff();
            }
        }
    }

    public static class Builder {
        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, Set<Index>> aliasedIndices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, ProjectCustom> customs;

        private SortedMap<String, IndexAbstraction> previousIndicesLookup;

        // If this is set to false we can skip checking #mappingsByHash for unused entries in #build(). Used as an optimization to save
        // the rather expensive logic for removing unused mappings when building from another instance and we know that no mappings can
        // have become unused because no indices were updated or removed from this builder in a way that would cause unused entries in
        // #mappingsByHash.
        private boolean checkForUnusedMappings = true;

        private final Map<String, MappingMetadata> mappingsByHash;

        public Builder() {
            this(Map.of(), 0);
        }

        Builder(ProjectMetadata metadata) {
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.aliasedIndices = ImmutableOpenMap.builder(metadata.aliasedIndices);
            this.templates = ImmutableOpenMap.builder(metadata.templates);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.previousIndicesLookup = metadata.indicesLookup;
            this.mappingsByHash = new HashMap<>(metadata.mappingsByHash);
            this.checkForUnusedMappings = false;
        }

        Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            indices = ImmutableOpenMap.builder(indexCountHint);
            aliasedIndices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
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
                for (var key : current.getAliases().keySet()) {
                    putAlias(key, current.getIndex());
                }
            } else if (previous != null && current == null) {
                for (var key : previous.getAliases().keySet()) {
                    removeAlias(key, previous.getIndex());
                }
            } else if (previous != null && current != null) {
                if (Objects.equals(previous.getAliases(), current.getAliases())) {
                    return;
                }

                for (var key : current.getAliases().keySet()) {
                    if (previous.getAliases().containsKey(key) == false) {
                        putAlias(key, current.getIndex());
                    }
                }
                for (var key : previous.getAliases().keySet()) {
                    if (current.getAliases().containsKey(key) == false) {
                        removeAlias(key, current.getIndex());
                    }
                }
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

        public ProjectMetadata.ProjectCustom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, ProjectMetadata.ProjectCustom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder removeCustomIf(BiPredicate<String, ? super ProjectMetadata.ProjectCustom> p) {
            customs.removeAll(p);
            return this;
        }

        public Builder customs(Map<String, ProjectMetadata.ProjectCustom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.customs.putAllFromMap(customs);
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

        /**
         * @return a new <code>Metadata</code> instance
         */
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
            }

            var aliasedIndices = this.aliasedIndices.build();
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
                // we have changes to the the entity names so we ensure we have no naming collisions
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
            String[] visibleIndicesArray = visibleIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allOpenIndicesArray = allOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allClosedIndicesArray = allClosedIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(Strings.EMPTY_ARRAY);

            return new ProjectMetadata(
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
                IndexVersion.fromId(oldestIndexVersionId)
            );
        }

        static void ensureNoNameCollisions(
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
            collectDataStreams(dataStreamMetadata, indicesLookup, indexToDataStreamLookup);

            Map<String, List<IndexMetadata>> aliasToIndices = new HashMap<>();
            collectIndices(indices, indexToDataStreamLookup, indicesLookup, aliasToIndices);
            collectAliases(aliasToIndices, indicesLookup);

            return Collections.unmodifiableSortedMap(indicesLookup);
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
                final String name = entry.getKey();
                final IndexMetadata indexMetadata = entry.getValue();
                final DataStream parent = indexToDataStreamLookup.get(name);
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
                || (DataStream.isFailureStoreFeatureFlagEnabled()
                    && parent.isFailureStoreEnabled()
                    && parent.getFailureIndices()
                        .getIndices()
                        .stream()
                        .anyMatch(index -> indexMetadata.getIndex().getName().equals(index.getName())))
                : "Expected data stream [" + parent.getName() + "] to contain index " + indexMetadata.getIndex();
            return true;
        }

        private static void collectDataStreams(
            DataStreamMetadata dataStreamMetadata,
            Map<String, IndexAbstraction> indicesLookup,
            Map<String, DataStream> indexToDataStreamLookup
        ) {
            final var dataStreams = dataStreamMetadata.dataStreams();
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
                if (DataStream.isFailureStoreFeatureFlagEnabled() && dataStream.isFailureStoreEnabled()) {
                    for (Index i : dataStream.getFailureIndices().getIndices()) {
                        indexToDataStreamLookup.put(i.getName(), dataStream);
                    }
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

        // TODO: Move to project
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

            // Validate system status
            final Map<Boolean, List<IndexMetadata>> groupedBySystemStatus = indexMetadatas.stream()
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

        static boolean assertDataStreams(Map<String, IndexMetadata> indices, DataStreamMetadata dsMetadata) {
            // Sanity check, because elsewhere a more user friendly error should have occurred:
            List<String> conflictingAliases = null;

            for (var dataStream : dsMetadata.dataStreams().values()) {
                for (var index : dataStream.getIndices()) {
                    IndexMetadata im = indices.get(index.getName());
                    if (im != null && im.getAliases().isEmpty() == false) {
                        for (var alias : im.getAliases().values()) {
                            if (conflictingAliases == null) {
                                conflictingAliases = new LinkedList<>();
                            }
                            conflictingAliases.add(alias.alias());
                        }
                    }
                }
            }
            if (conflictingAliases != null) {
                throw new AssertionError("aliases " + conflictingAliases + " cannot refer to backing indices of data streams");
            }

            return true;
        }

        public static ProjectMetadata.Builder fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                        }
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else {
                        parseCustoms(parser, currentFieldName, builder);
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            return builder;
        }

        private static void parseCustoms(XContentParser parser, String currentFieldName, Builder builder) throws IOException {
            try {
                ProjectCustom custom = parser.namedObject(ProjectCustom.class, currentFieldName, null);
                builder.putCustom(custom.getWriteableName(), custom);
            } catch (NamedObjectNotFoundException _ex) {
                logger.warn("Skipping unknown custom [{}] object with type {}", ProjectCustom.class.getSimpleName(), currentFieldName);
                parser.skipChildren();
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

    /**
     * This produces the inner content of an object (no start/end object) so that it can embedded as fields
     *  in another object
     */
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return toXContentChunked(params, Metadata.XContentContext.from(params));
    }

    private Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params p, Metadata.XContentContext context) {
        final Iterator<? extends ToXContent> indices = context == Metadata.XContentContext.API
            ? ChunkedToXContentHelper.wrapWithObject("indices", this.indices.values().iterator())
            : Collections.emptyIterator();

        final Iterator<ToXContent> customs = Iterators.flatMap(
            this.customs.entrySet().iterator(),
            entry -> entry.getValue().context().contains(context)
                ? ChunkedToXContentHelper.wrapWithObject(entry.getKey(), entry.getValue().toXContentChunked(p))
                : Collections.emptyIterator()
        );

        final Iterator<ToXContent> templates = ChunkedToXContentHelper.wrapWithObject(
            "templates",
            Iterators.map(
                this.templates.values().iterator(),
                template -> (builder, params) -> IndexTemplateMetadata.Builder.toXContentWithTypes(template, builder, params)
            )
        );

        return Iterators.concat(templates, indices, customs);
    }
}
