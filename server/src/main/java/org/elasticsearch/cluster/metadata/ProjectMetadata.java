/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProjectMetadata implements Iterable<IndexMetadata> {

    final ImmutableOpenMap<String, IndexMetadata> indices;
    final ImmutableOpenMap<String, Set<Index>> aliasedIndices;
    final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    final ImmutableOpenMap<String, Metadata.Custom> customs;

    final int totalNumberOfShards;
    final int totalOpenIndexShards;

    final String[] allIndices;
    final String[] visibleIndices;
    final String[] allOpenIndices;
    final String[] visibleOpenIndices;
    final String[] allClosedIndices;
    final String[] visibleClosedIndices;

    volatile SortedMap<String, IndexAbstraction> indicesLookup;
    final Map<String, MappingMetadata> mappingsByHash;

    final IndexVersion oldestIndexVersion;

    public ProjectMetadata(
        ImmutableOpenMap<String, IndexMetadata> indices,
        ImmutableOpenMap<String, Set<Index>> aliasedIndices,
        ImmutableOpenMap<String, IndexTemplateMetadata> templates,
        ImmutableOpenMap<String, Metadata.Custom> customs,
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

    private boolean assertConsistent() {
        final var lookup = indicesLookup;
        final var dsMetadata = custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY);
        assert lookup == null || lookup.equals(Metadata.Builder.buildIndicesLookup(dsMetadata, indices));
        try {
            Metadata.Builder.ensureNoNameCollisions(aliasedIndices.keySet(), indices, dsMetadata);
        } catch (Exception e) {
            assert false : e;
        }
        assert Metadata.Builder.assertDataStreams(indices, dsMetadata);
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
        i = Metadata.Builder.buildIndicesLookup(custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY), indices);
        indicesLookup = i;
        return i;
    }

    public boolean sameIndicesLookup(ProjectMetadata other) {
        return indicesLookup == other.indicesLookup;
    }

    public Map<String, IndexMetadata> indices() {
        return indices;
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return templates;
    }

    @SuppressWarnings("unchecked")
    public <T extends Metadata.Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Metadata.Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
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
}
