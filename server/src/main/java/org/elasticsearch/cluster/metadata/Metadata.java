/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * {@link Metadata} is the part of the {@link ClusterState} which persists across restarts. This persistence is XContent-based, so a
 * round-trip through XContent must be faithful in {@link XContentContext#GATEWAY} context.
 */
public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata>, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(Metadata.class);

    public static final Runnable ON_NEXT_INDEX_FIND_MAPPINGS_NOOP = () -> { };
    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = "_na_";

    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
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
    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment {

        EnumSet<XContentContext> context();
    }

    public interface NonRestorableCustom extends Custom {
    }

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING =
        Setting.boolSetting("cluster.blocks.read_only", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)",
        false, false, false, RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING =
        Setting.boolSetting("cluster.blocks.read_only_allow_delete", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK =
        new ClusterBlock(13, "cluster read-only / allow delete (api)",
        false, false, true, RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String CONTEXT_MODE_API = XContentContext.API.toString();

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
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private final SortedMap<String, IndexAbstraction> indicesLookup;

    Metadata(String clusterUUID, boolean clusterUUIDCommitted, long version, CoordinationMetadata coordinationMetadata,
             Settings transientSettings, Settings persistentSettings, DiffableStringMap hashesOfConsistentSettings,
             ImmutableOpenMap<String, IndexMetadata> indices, ImmutableOpenMap<String, IndexTemplateMetadata> templates,
             ImmutableOpenMap<String, Custom> customs, String[] allIndices, String[] visibleIndices, String[] allOpenIndices,
             String[] visibleOpenIndices, String[] allClosedIndices, String[] visibleClosedIndices,
             SortedMap<String, IndexAbstraction> indicesLookup) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.indices = indices;
        this.customs = customs;
        this.templates = templates;
        int totalNumberOfShards = 0;
        int totalOpenIndexShards = 0;
        for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
            totalNumberOfShards += cursor.value.getTotalNumberOfShards();
            if (IndexMetadata.State.OPEN.equals(cursor.value.getState())) {
                totalOpenIndexShards += cursor.value.getTotalNumberOfShards();
            }
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;

        this.allIndices = allIndices;
        this.visibleIndices = visibleIndices;
        this.allOpenIndices = allOpenIndices;
        this.visibleOpenIndices = visibleOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.visibleClosedIndices = visibleClosedIndices;
        this.indicesLookup = indicesLookup;
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

    public boolean hasAlias(String alias) {
        IndexAbstraction indexAbstraction = getIndicesLookup().get(alias);
        if (indexAbstraction != null) {
            return indexAbstraction.getType() == IndexAbstraction.Type.ALIAS;
        } else {
            return false;
        }
    }

    public boolean equalsAliases(Metadata other) {
        for (ObjectCursor<IndexMetadata> cursor : other.indices().values()) {
            IndexMetadata otherIndex = cursor.value;
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

    public SortedMap<String, IndexAbstraction> getIndicesLookup() {
        return indicesLookup;
    }

    /**
     * Finds the specific index aliases that point to the requested concrete indices directly
     * or that match with the indices via wildcards.
     *
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public ImmutableOpenMap<String, List<AliasMetadata>> findAllAliases(final String[] concreteIndices) {
        return findAliases(Strings.EMPTY_ARRAY, concreteIndices);
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards, and
     * that point to the specified concrete indices (directly or matching indices via wildcards).
     *
     * @param aliasesRequest The request to find aliases for
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public ImmutableOpenMap<String, List<AliasMetadata>> findAliases(final AliasesRequest aliasesRequest, final String[] concreteIndices) {
        return findAliases(aliasesRequest.aliases(), concreteIndices);
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
    private ImmutableOpenMap<String, List<AliasMetadata>> findAliases(final String[] aliases, final String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }
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
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = indices.get(index);
            List<AliasMetadata> filteredValues = new ArrayList<>();
            for (ObjectCursor<AliasMetadata> cursor : indexMetadata.getAliases().values()) {
                AliasMetadata value = cursor.value;
                boolean matched = matchAllAliases;
                String alias = value.alias();
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
                    filteredValues.add(value);
                }
            }
            if (filteredValues.isEmpty() == false) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasMetadata::alias));
                mapBuilder.put(index, Collections.unmodifiableList(filteredValues));
            }
        }
        return mapBuilder.build();
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
    public ImmutableOpenMap<String, MappingMetadata> findMappings(String[] concreteIndices,
                                                                  Function<String, Predicate<String>> fieldFilter,
                                                                  Runnable onNextIndex) {
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, MappingMetadata> indexMapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            onNextIndex.run();
            IndexMetadata indexMetadata = indices.get(index);
            Predicate<String> fieldPredicate = fieldFilter.apply(index);
            indexMapBuilder.put(index, filterFields(indexMetadata.mapping(), fieldPredicate));
        }
        return indexMapBuilder.build();
    }

    /**
     * Finds the parent data streams, if any, for the specified concrete indices.
     */
    public ImmutableOpenMap<String, IndexAbstraction.DataStream> findDataStreams(String[] concreteIndices) {
        assert concreteIndices != null;
        final ImmutableOpenMap.Builder<String, IndexAbstraction.DataStream> builder = ImmutableOpenMap.builder();
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

        Map<String, Object> properties = (Map<String, Object>)mapping.get("properties");
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
                Map<String, Object> properties = (Map<String, Object>)map.get("properties");
                if (properties != null) {
                    mayRemove = filterFields(newPath, properties, fieldPredicate);
                } else {
                    Map<String, Object> subFields = (Map<String, Object>)map.get("fields");
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

            //only remove a field if it has no sub-fields left and it has to be excluded
            if (fieldPredicate.test(newPath) == false) {
                if (mayRemove) {
                    entryIterator.remove();
                } else if (isMultiField) {
                    //multi fields that should be excluded but hold subfields that don't have to be excluded are converted to objects
                    Map<String, Object> map = (Map<String, Object>) value;
                    Map<String, Object> subFields = (Map<String, Object>)map.get("fields");
                    assert subFields.size() > 0;
                    map.put("properties", subFields);
                    map.remove("fields");
                    map.remove("type");
                }
            }
        }
        //return true if the ancestor may be removed, as it has no sub-fields left
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
        IndexMetadata writeIndex = result.getWriteIndex();
        if (writeIndex == null) {
            throw new IllegalArgumentException("alias [" + aliasOrIndex + "] does not have a write index");
        }
        AliasMetadata writeIndexAliasMetadata = writeIndex.getAliases().get(result.getName());
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
        return resolveRouting(routing, aliasOrIndex, AliasMetadata.getFirstAliasMetadata(result));
    }

    private static String resolveRouting(@Nullable String routing, String aliasOrIndex, AliasMetadata aliasMd) {
        if (aliasMd.indexRouting() != null) {
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value [" +
                    aliasMd.getIndexRouting() + "] that resolved to several routing values, rejecting operation");
            }
            if (routing != null) {
                if (routing.equals(aliasMd.indexRouting()) == false) {
                    throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it [" +
                        aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
    }

    private void rejectSingleIndexOperation(String aliasOrIndex, IndexAbstraction result) {
        String[] indexNames = new String[result.getIndices().size()];
        int i = 0;
        for (IndexMetadata indexMetadata : result.getIndices()) {
            indexNames[i++] = indexMetadata.getIndex().getName();
        }
        throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" +
            Arrays.toString(indexNames) + "], can't execute a single index op");
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    public boolean hasConcreteIndex(String index) {
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
            if(metadata.getIndexUUID().equals(index.getUUID())) {
                return metadata;
            }
            throw new IndexNotFoundException(index,
                new IllegalStateException("index uuid doesn't match expected: [" + index.getUUID()
                    + "] but got: [" + metadata.getIndexUUID() +"]"));
        }
        throw new IndexNotFoundException(index);
    }

    public ImmutableOpenMap<String, IndexMetadata> indices() {
        return this.indices;
    }

    public ImmutableOpenMap<String, IndexMetadata> getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, IndexTemplateMetadata> templates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, IndexTemplateMetadata> getTemplates() {
        return this.templates;
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

    public Map<String, DataStream> dataStreams() {
        return Optional.ofNullable((DataStreamMetadata) this.custom(DataStreamMetadata.TYPE))
            .map(DataStreamMetadata::dataStreams)
            .orElse(Collections.emptyMap());
    }

    public Map<String, DataStreamAlias> dataStreamAliases() {
        return Optional.ofNullable((DataStreamMetadata) this.custom(DataStreamMetadata.TYPE))
            .map(DataStreamMetadata::getDataStreamAliases)
            .orElse(Collections.emptyMap());
    }

    public Map<String, SingleNodeShutdownMetadata> nodeShutdowns() {
        return Optional.ofNullable((NodesShutdownMetadata) this.custom(NodesShutdownMetadata.TYPE))
            .map(NodesShutdownMetadata::getAllNodeMetadataMap)
            .orElse(Collections.emptyMap());
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
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

    /**
     * @param concreteIndex The concrete index to check if routing is required
     * @return Whether routing is required according to the mapping for the specified index and type
     */
    public boolean routingRequired(String concreteIndex) {
        IndexMetadata indexMetadata = indices.get(concreteIndex);
        if (indexMetadata != null) {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                return mappingMetadata.routingRequired();
            }
        }
        return false;
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.valuesIt();
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
        if (metadata1.templates.equals(metadata2.templates()) == false) {
            return false;
        }
        if (metadata1.clusterUUID.equals(metadata2.clusterUUID) == false) {
            return false;
        }
        if (metadata1.clusterUUIDCommitted != metadata2.clusterUUIDCommitted) {
            return false;
        }
        // Check if any persistent metadata needs to be saved
        int customCount1 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metadata1.customs) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                if (cursor.value.equals(metadata2.custom(cursor.key)) == false) {
                    return false;
                }
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (ObjectCursor<Custom> cursor : metadata2.customs.values()) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        if (customCount1 != customCount2) {
            return false;
        }
        return true;
    }

    /**
     * Reconciles the cluster state metadata taken at the end of a snapshot with the data streams and indices
     * contained in the snapshot. Certain actions taken during a snapshot such as rolling over a data stream
     * or deleting a backing index may result in situations where some reconciliation is required.
     *
     * @return Reconciled {@link Metadata} instance
     */
    public static Metadata snapshot(Metadata metadata, List<String> dataStreams, List<String> indices) {
        var builder = Metadata.builder(metadata);
        for (var dsName : dataStreams) {
            var dataStream = metadata.dataStreams().get(dsName);
            if (dataStream == null) {
                // should never occur since data streams cannot be deleted while they have snapshots underway
                throw new IllegalArgumentException("unable to find data stream [" + dsName + "]");
            }
            builder.put(dataStream.snapshot(indices));
        }
        return builder.build();
    }

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class MetadataDiff implements Diff<Metadata> {

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

        MetadataDiff(Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            } else {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
            }
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                hashesOfConsistentSettings.writeTo(out);
            }
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public Metadata apply(Metadata part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in), false);
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
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            hashesOfConsistentSettings.writeTo(out);
        }
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out);
        }
        out.writeVInt(templates.size());
        for (ObjectCursor<IndexTemplateMetadata> cursor : templates.values()) {
            cursor.value.writeTo(out);
        }
        VersionedNamedWriteable.writeVersionedWritables(out, customs);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;
        private DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(Collections.emptyMap());

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.hashesOfConsistentSettings = metadata.hashesOfConsistentSettings;
            this.version = metadata.version;
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.templates = ImmutableOpenMap.builder(metadata.templates);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            if (indices.get(indexMetadata.getIndex().getName()) == indexMetadata) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetadata = IndexMetadata.builder(indexMetadata).version(indexMetadata.getVersion() + 1).build();
            }
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public IndexMetadata get(String index) {
            return indices.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            IndexMetadata indexMetadata = get(index.getName());
            if (indexMetadata != null) {
                if(indexMetadata.getIndexUUID().equals(index.getUUID())) {
                    return indexMetadata;
                }
                throw new IndexNotFoundException(index,
                    new IllegalStateException("index uuid doesn't match expected: [" + index.getUUID()
                        + "] but got: [" + indexMetadata.getIndexUUID() +"]"));
            }
            throw new IndexNotFoundException(index);
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            indices.clear();
            return this;
        }

        public Builder indices(ImmutableOpenMap<String, IndexMetadata> indices) {
            this.indices.putAll(indices);
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

        public Builder templates(ImmutableOpenMap<String, IndexTemplateMetadata> templates) {
            this.templates.putAll(templates);
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            Objects.requireNonNull(componentTemplate, "it is invalid to add a null component template: " + name);
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComponentTemplate> existingTemplates =
                Optional.ofNullable((ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE))
                    .map(ctm -> new HashMap<>(ctm.componentTemplates()))
                    .orElse(new HashMap<>());
            existingTemplates.put(name, componentTemplate);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComponentTemplate> existingTemplates =
                Optional.ofNullable((ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE))
                    .map(ctm -> new HashMap<>(ctm.componentTemplates()))
                    .orElse(new HashMap<>());
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
            Map<String, ComposableIndexTemplate> existingTemplates =
                Optional.ofNullable((ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE))
                    .map(itmd -> new HashMap<>(itmd.indexTemplates()))
                    .orElse(new HashMap<>());
            existingTemplates.put(name, indexTemplate);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            // ಠ_ಠ at ImmutableOpenMap
            Map<String, ComposableIndexTemplate> existingTemplates =
                Optional.ofNullable((ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE))
                    .map(itmd -> new HashMap<>(itmd.indexTemplates()))
                    .orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) customs.get(DataStreamMetadata.TYPE);
            if (dataStreamMetadata != null) {
                return dataStreamMetadata.dataStreams().get(dataStreamName);
            } else {
                return null;
            }
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams, Map<String, DataStreamAlias> dataStreamAliases) {
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(dataStreams, dataStreamAliases));
            return this;
        }

        public Builder put(DataStream dataStream) {
            Objects.requireNonNull(dataStream, "it is invalid to add a null data stream");
            Map<String, DataStream> existingDataStreams =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.dataStreams()))
                    .orElse(new HashMap<>());
            existingDataStreams.put(dataStream.getName(), dataStream);
            Map<String, DataStreamAlias> existingDataStreamAliases =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                .map(dsmd -> new HashMap<>(dsmd.getDataStreamAliases()))
                .orElse(new HashMap<>());

            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams, existingDataStreamAliases));
            return this;
        }

        public boolean put(String aliasName, String dataStream, Boolean isWriteDataStream, String filter) {
            Map<String, DataStream> existingDataStream =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.dataStreams()))
                    .orElse(new HashMap<>());
            Map<String, DataStreamAlias> dataStreamAliases =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.getDataStreamAliases()))
                    .orElse(new HashMap<>());

            if (existingDataStream.containsKey(dataStream) == false) {
                throw new IllegalArgumentException("alias [" + aliasName + "] refers to a non existing data stream [" + dataStream + "]");
            }

            Map<String, Object> filterAsMap;
            if (filter != null) {
                filterAsMap = XContentHelper.convertToMap(XContentFactory.xContent(filter), filter, true);
            } else {
                filterAsMap = null;
            }

            DataStreamAlias alias = dataStreamAliases.get(aliasName);
            if (alias == null) {
                String writeDataStream = isWriteDataStream != null && isWriteDataStream ? dataStream : null;
                alias = new DataStreamAlias(aliasName, List.of(dataStream), writeDataStream, filterAsMap);
            } else {
                DataStreamAlias copy = alias.update(dataStream, isWriteDataStream, filterAsMap);
                if (copy == alias) {
                    return false;
                }
                alias = copy;
            }
            dataStreamAliases.put(aliasName, alias);

            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStream, dataStreamAliases));
            return true;
        }

        public Builder removeDataStream(String name) {
            Map<String, DataStream> existingDataStreams =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.dataStreams()))
                    .orElse(new HashMap<>());
            existingDataStreams.remove(name);

            Map<String, DataStreamAlias> existingDataStreamAliases =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.getDataStreamAliases()))
                    .orElse(new HashMap<>());

            Set<String> aliasesToDelete = new HashSet<>();
            List<DataStreamAlias> aliasesToUpdate = new ArrayList<>();
            for (var alias : existingDataStreamAliases.values()) {
                DataStreamAlias copy = alias.removeDataStream(name);
                if (copy != null) {
                    if (copy == alias) {
                        continue;
                    }
                    aliasesToUpdate.add(copy);
                } else {
                    aliasesToDelete.add(alias.getName());
                }
            }
            for (DataStreamAlias alias : aliasesToUpdate) {
                existingDataStreamAliases.put(alias.getName(), alias);
            }
            for (String aliasToDelete : aliasesToDelete) {
                existingDataStreamAliases.remove(aliasToDelete);
            }

            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams, existingDataStreamAliases));
            return this;
        }

        public boolean removeDataStreamAlias(String aliasName, String dataStreamName, boolean mustExist) {
            Map<String, DataStreamAlias> dataStreamAliases =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.getDataStreamAliases()))
                    .orElse(new HashMap<>());

            DataStreamAlias existing = dataStreamAliases.get(aliasName);
            if (mustExist && existing == null) {
                throw new ResourceNotFoundException("alias [" + aliasName + "] doesn't exist");
            } else if (existing == null) {
                return false;
            }
            DataStreamAlias copy = existing.removeDataStream(dataStreamName);
            if (copy == existing) {
                return false;
            }
            if (copy != null) {
                dataStreamAliases.put(aliasName, copy);
            } else {
                dataStreamAliases.remove(aliasName);
            }
            Map<String, DataStream> existingDataStream =
                Optional.ofNullable((DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE))
                    .map(dsmd -> new HashMap<>(dsmd.dataStreams()))
                    .orElse(new HashMap<>());
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStream, dataStreamAliases));
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

        public Builder customs(ImmutableOpenMap<String, Custom> customs) {
            StreamSupport.stream(customs.spliterator(), false).forEach(cursor -> Objects.requireNonNull(cursor.value, cursor.key));
            this.customs.putAll(customs);
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
                indices = this.indices.keys().toArray(String.class);
            }
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata)
                        .settings(Settings.builder().put(indexMetadata.getSettings()).put(settings)));
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

        public Metadata build() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will be rebuilt only when needed. Now during serializing we rebuild these datastructures
            //    while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final List<String> visibleIndices = new ArrayList<>();
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> visibleOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final List<String> visibleClosedIndices = new ArrayList<>();
            final Set<String> allAliases = new HashSet<>();
            for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                final IndexMetadata indexMetadata = cursor.value;
                final String name = indexMetadata.getIndex().getName();
                boolean added = allIndices.add(name);
                assert added : "double index named [" + name + "]";
                final boolean visible = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings()) == false;
                if (visible) {
                    visibleIndices.add(name);
                }
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
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
                indexMetadata.getAliases().keysIt().forEachRemaining(allAliases::add);
            }

            final Set<String> allDataStreams = new HashSet<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            if (dataStreamMetadata != null) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    allDataStreams.add(dataStream.getName());
                }
            }

            final Set<String> aliasDuplicatesWithIndices = new HashSet<>(allAliases);
            aliasDuplicatesWithIndices.retainAll(allIndices);
            ArrayList<String> duplicates = new ArrayList<>();
            if (aliasDuplicatesWithIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithIndices) {
                        if (cursor.value.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.value.getIndex() + ") conflicts with index");
                        }
                    }
                }
            }

            final Set<String> aliasDuplicatesWithDataStreams = new HashSet<>(allAliases);
            aliasDuplicatesWithDataStreams.retainAll(allDataStreams);
            if (aliasDuplicatesWithDataStreams.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithDataStreams) {
                        if (cursor.value.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.value.getIndex() + ") conflicts with data stream");
                        }
                    }
                }
            }

            final Set<String> dataStreamDuplicatesWithIndices = new HashSet<>(allDataStreams);
            dataStreamDuplicatesWithIndices.retainAll(allIndices);
            if (dataStreamDuplicatesWithIndices.isEmpty() == false) {
                for (String dataStream : dataStreamDuplicatesWithIndices) {
                    duplicates.add("data stream [" + dataStream + "] conflicts with index");
                }
            }

            if (duplicates.size() > 0) {
                throw new IllegalStateException("index, alias, and data stream names need to be unique, but the following duplicates " +
                    "were found [" + Strings.collectionToCommaDelimitedString(duplicates) + "]");
            }

            SortedMap<String, IndexAbstraction> indicesLookup = Collections.unmodifiableSortedMap(buildIndicesLookup());

            validateDataStreams(indicesLookup, (DataStreamMetadata) customs.get(DataStreamMetadata.TYPE));

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] allIndicesArray = allIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleIndicesArray = visibleIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allOpenIndicesArray = allOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allClosedIndicesArray = allClosedIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(Strings.EMPTY_ARRAY);

            return new Metadata(clusterUUID, clusterUUIDCommitted, version, coordinationMetadata, transientSettings, persistentSettings,
                hashesOfConsistentSettings, indices.build(), templates.build(), customs.build(), allIndicesArray, visibleIndicesArray,
                allOpenIndicesArray, visibleOpenIndicesArray, allClosedIndicesArray, visibleClosedIndicesArray, indicesLookup);
        }

        private SortedMap<String, IndexAbstraction> buildIndicesLookup() {
            SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
            Map<String, DataStream> indexToDataStreamLookup = new HashMap<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            // If there are no indices, then skip data streams. This happens only when metadata is read from disk
            if (dataStreamMetadata != null && indices.size() > 0) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    List<IndexMetadata> backingIndices = dataStream.getIndices().stream()
                        .map(index -> indices.get(index.getName()))
                        .collect(Collectors.toList());
                    assert backingIndices.isEmpty() == false;
                    assert backingIndices.contains(null) == false;

                    IndexAbstraction existing = indicesLookup.put(dataStream.getName(),
                        new IndexAbstraction.DataStream(dataStream, backingIndices));
                    assert existing == null : "duplicate data stream for " + dataStream.getName();

                    for (Index i : dataStream.getIndices()) {
                        indexToDataStreamLookup.put(i.getName(), dataStream);
                    }
                }
                for (DataStreamAlias alias : dataStreamMetadata.getDataStreamAliases().values()) {
                    List<IndexMetadata> allIndicesOfAllDataStreams = alias.getDataStreams().stream()
                        .map(name -> dataStreamMetadata.dataStreams().get(name))
                        .flatMap(ds -> ds.getIndices().stream())
                        .map(index -> indices.get(index.getName()))
                        .collect(Collectors.toList());
                    IndexMetadata writeIndexOfWriteDataStream = null;
                    if (alias.getWriteDataStream() != null) {
                        DataStream writeDataStream = dataStreamMetadata.dataStreams().get(alias.getWriteDataStream());
                        writeIndexOfWriteDataStream = indices.get(writeDataStream.getWriteIndex().getName());
                    }
                    IndexAbstraction existing = indicesLookup.put(alias.getName(),
                        new IndexAbstraction.Alias(alias, allIndicesOfAllDataStreams, writeIndexOfWriteDataStream));
                    assert existing == null : "duplicate data stream alias for " + alias.getName();
                }
            }

            Map<String, List<IndexMetadata>> aliasToIndices = new HashMap<>();
            for (var cursor : indices.values()) {
                IndexMetadata indexMetadata = cursor.value;

                IndexAbstraction.Index index;
                DataStream parent = indexToDataStreamLookup.get(indexMetadata.getIndex().getName());
                if (parent != null) {
                    assert parent.getIndices().stream()
                        .map(Index::getName)
                        .collect(Collectors.toList())
                        .contains(indexMetadata.getIndex().getName()) :
                        "Expected data stream [" + parent.getName() + "] to contain index " + indexMetadata.getIndex();
                    index = new IndexAbstraction.Index(indexMetadata, (IndexAbstraction.DataStream) indicesLookup.get(parent.getName()));
                } else {
                    index = new IndexAbstraction.Index(indexMetadata);
                }

                IndexAbstraction existing = indicesLookup.put(indexMetadata.getIndex().getName(), index);
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (var aliasCursor : indexMetadata.getAliases()) {
                    AliasMetadata aliasMetadata = aliasCursor.value;
                    aliasToIndices.compute(aliasMetadata.getAlias(), (aliasName, indices) -> {
                       if (indices == null) {
                           indices = new ArrayList<>();
                       }
                       indices.add(indexMetadata);
                       return indices;
                    });
                }
            }

            for (var entry : aliasToIndices.entrySet()) {
                AliasMetadata alias = entry.getValue().get(0).getAliases().get(entry.getKey());
                IndexAbstraction existing = indicesLookup.put(entry.getKey(), new IndexAbstraction.Alias(alias, entry.getValue()));
                assert existing == null : "duplicate for " + entry.getKey();
            }

            return indicesLookup;
        }

        static void validateDataStreams(SortedMap<String, IndexAbstraction> indicesLookup, @Nullable DataStreamMetadata dsMetadata) {
            if (dsMetadata != null) {
                // Sanity check, because elsewhere a more user friendly error should have occurred:
                List<String> conflictingAliases = indicesLookup.values().stream()
                    .filter(ia -> ia.getType() == IndexAbstraction.Type.ALIAS)
                    .filter(ia -> ia.isDataStreamRelated() == false)
                    .filter(ia -> {
                        for (IndexMetadata index : ia.getIndices()) {
                            if (indicesLookup.get(index.getIndex().getName()).getParentDataStream() != null) {
                                return true;
                            }
                        }

                        return false;
                    })
                    .map(IndexAbstraction::getName)
                    .collect(Collectors.toList());
                if (conflictingAliases.isEmpty() == false) {
                    throw new IllegalStateException("aliases " + conflictingAliases + " cannot refer to backing indices of data streams");
                }
            }
        }

        public static void toXContent(Metadata metadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));

            if (context == XContentContext.API) {
                builder.startObject("metadata");
            } else {
                builder.startObject("meta-data");
                builder.field("version", metadata.version());
            }

            builder.field("cluster_uuid", metadata.clusterUUID);
            builder.field("cluster_uuid_committed", metadata.clusterUUIDCommitted);

            builder.startObject("cluster_coordination");
            metadata.coordinationMetadata().toXContent(builder, params);
            builder.endObject();

            if (context != XContentContext.API && metadata.persistentSettings().isEmpty() == false) {
                builder.startObject("settings");
                metadata.persistentSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetadata> cursor : metadata.templates().values()) {
                IndexTemplateMetadata.Builder.toXContentWithTypes(cursor.value, builder, params);
            }
            builder.endObject();

            if (context == XContentContext.API) {
                builder.startObject("indices");
                for (IndexMetadata indexMetadata : metadata) {
                    IndexMetadata.Builder.toXContent(indexMetadata, builder, params);
                }
                builder.endObject();
            }

            for (ObjectObjectCursor<String, Custom> cursor : metadata.customs()) {
                if (cursor.value.context().contains(context)) {
                    builder.startObject(cursor.key);
                    cursor.value.toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
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
                    } else {
                        try {
                            Custom custom = parser.namedObject(Custom.class, currentFieldName, null);
                            builder.putCustom(custom.getWriteableName(), custom);
                        } catch (NamedObjectNotFoundException ex) {
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
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * State format for {@link Metadata} to write to and load from disk
     */
    public static final MetadataStateFormat<Metadata> FORMAT = new MetadataStateFormat<>(GLOBAL_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, Metadata state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public Metadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

}
