/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;
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
        i = Builder.buildIndicesLookup(custom(DataStreamMetadata.TYPE, DataStreamMetadata.EMPTY), indices);
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

    public static ProjectMetadata.Builder builder() {
        return new ProjectMetadata.Builder(Map.of(), 0);
    }

    public static ProjectMetadata.Builder builder(ProjectMetadata projectMetadata) {
        return new ProjectMetadata.Builder(projectMetadata);
    }

    public static class Builder {

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, Set<Index>> aliasedIndices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Metadata.Custom> customs;

        SortedMap<String, IndexAbstraction> previousIndicesLookup;

        private final Map<String, MappingMetadata> mappingsByHash;
        // If this is set to false we can skip checking #mappingsByHash for unused entries in #build(). Used as an optimization to save
        // the rather expensive logic for removing unused mappings when building from another instance and we know that no mappings can
        // have become unused because no indices were updated or removed from this builder in a way that would cause unused entries in
        // #mappingsByHash.
        private boolean checkForUnusedMappings = true;

        Builder(ProjectMetadata projectMetadata) {
            this.indices = ImmutableOpenMap.builder(projectMetadata.indices);
            this.aliasedIndices = ImmutableOpenMap.builder(projectMetadata.aliasedIndices);
            this.templates = ImmutableOpenMap.builder(projectMetadata.templates);
            this.customs = ImmutableOpenMap.builder(projectMetadata.customs);
            this.previousIndicesLookup = projectMetadata.indicesLookup;
            this.mappingsByHash = new HashMap<>(projectMetadata.mappingsByHash);
            this.checkForUnusedMappings = false;
        }

        Builder(Map<String, MappingMetadata> mappingsByHash, int indexCountHint) {
            indices = ImmutableOpenMap.builder(indexCountHint);
            aliasedIndices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
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

        public Metadata.Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Metadata.Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder removeCustomIf(BiPredicate<String, Metadata.Custom> p) {
            customs.removeAll(p);
            return this;
        }

        public Builder customs(Map<String, Metadata.Custom> customs) {
            customs.forEach((key, value) -> Objects.requireNonNull(value, key));
            this.customs.putAllFromMap(customs);
            return this;
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
            List<String> visibleIndices = new ArrayList<>();
            List<String> allOpenIndices = new ArrayList<>();
            List<String> visibleOpenIndices = new ArrayList<>();
            List<String> allClosedIndices = new ArrayList<>();
            List<String> visibleClosedIndices = new ArrayList<>();
            ImmutableOpenMap<String, IndexMetadata> indicesMap = indices.build();

            int oldestIndexVersionId = IndexVersion.current().id();
            int totalNumberOfShards = 0;
            int totalOpenIndexShards = 0;

            String[] allIndicesArray = new String[indicesMap.size()];
            int i = 0;
            Set<String> sha256HashesInUse = checkForUnusedMappings ? Sets.newHashSetWithExpectedSize(mappingsByHash.size()) : null;
            for (var entry : indicesMap.entrySet()) {
                allIndicesArray[i++] = entry.getKey();
                IndexMetadata indexMetadata = entry.getValue();
                totalNumberOfShards += indexMetadata.getTotalNumberOfShards();
                String name = indexMetadata.getIndex().getName();
                boolean visible = indexMetadata.isHidden() == false;
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
                    var mapping = indexMetadata.mapping();
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
            String[] visibleIndicesArray = visibleIndices.toArray(String[]::new);
            String[] allOpenIndicesArray = allOpenIndices.toArray(String[]::new);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(String[]::new);
            String[] allClosedIndicesArray = allClosedIndices.toArray(String[]::new);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(String[]::new);

            return new ProjectMetadata(
                indicesMap,
                aliasedIndices,
                templates.build(),
                customs.build(),
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
    }
}
