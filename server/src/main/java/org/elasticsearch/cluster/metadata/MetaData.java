/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class MetaData implements Iterable<IndexMetaData>, Diffable<MetaData>, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(MetaData.class);

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

    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment {

        EnumSet<XContentContext> context();
    }

    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE =
        Setting.intSetting("cluster.max_shards_per_node", 1000, 1, Property.Dynamic, Property.NodeScope);

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

    public static final MetaData EMPTY_META_DATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetaData coordinationMetaData;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    private final ImmutableOpenMap<String, IndexMetaData> indices;
    private final ImmutableOpenMap<String, IndexTemplateMetaData> templates;
    private final ImmutableOpenMap<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;

    MetaData(String clusterUUID, boolean clusterUUIDCommitted, long version, CoordinationMetaData coordinationMetaData,
             Settings transientSettings, Settings persistentSettings, DiffableStringMap hashesOfConsistentSettings,
             ImmutableOpenMap<String, IndexMetaData> indices, ImmutableOpenMap<String, IndexTemplateMetaData> templates,
             ImmutableOpenMap<String, Custom> customs, String[] allIndices, String[] visibleIndices, String[] allOpenIndices,
             String[] visibleOpenIndices, String[] allClosedIndices, String[] visibleClosedIndices,
             SortedMap<String, AliasOrIndex> aliasAndIndexLookup) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetaData = coordinationMetaData;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.indices = indices;
        this.customs = customs;
        this.templates = templates;
        int totalNumberOfShards = 0;
        int totalOpenIndexShards = 0;
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            totalNumberOfShards += cursor.value.getTotalNumberOfShards();
            if (IndexMetaData.State.OPEN.equals(cursor.value.getState())) {
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
        this.aliasAndIndexLookup = aliasAndIndexLookup;
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

    public CoordinationMetaData coordinationMetaData() {
        return this.coordinationMetaData;
    }

    public boolean hasAlias(String alias) {
        AliasOrIndex aliasOrIndex = getAliasAndIndexLookup().get(alias);
        if (aliasOrIndex != null) {
            return aliasOrIndex.isAlias();
        } else {
            return false;
        }
    }

    public boolean equalsAliases(MetaData other) {
        for (ObjectCursor<IndexMetaData> cursor : other.indices().values()) {
            IndexMetaData otherIndex = cursor.value;
            IndexMetaData thisIndex = index(otherIndex.getIndex());
            if (thisIndex == null) {
                return false;
            }
            if (otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return false;
            }
        }

        return true;
    }

    public SortedMap<String, AliasOrIndex> getAliasAndIndexLookup() {
        return aliasAndIndexLookup;
    }

    /**
     * Finds the specific index aliases that point to the requested concrete indices directly
     * or that match with the indices via wildcards.
     *
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public ImmutableOpenMap<String, List<AliasMetaData>> findAllAliases(final String[] concreteIndices) {
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
    public ImmutableOpenMap<String, List<AliasMetaData>> findAliases(final AliasesRequest aliasesRequest, final String[] concreteIndices) {
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
    private ImmutableOpenMap<String, List<AliasMetaData>> findAliases(final String[] aliases, final String[] concreteIndices) {
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
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> mapBuilder = ImmutableOpenMap.builder();
        for (String index : concreteIndices) {
            IndexMetaData indexMetaData = indices.get(index);
            List<AliasMetaData> filteredValues = new ArrayList<>();
            for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
                AliasMetaData value = cursor.value;
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
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasMetaData::alias));
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
     */
    public ImmutableOpenMap<String, MappingMetaData> findMappings(String[] concreteIndices,
                                                                  Function<String, Predicate<String>> fieldFilter)
            throws IOException {
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, MappingMetaData> indexMapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            Predicate<String> fieldPredicate = fieldFilter.apply(index);
            indexMapBuilder.put(index, filterFields(indexMetaData.mapping(), fieldPredicate));
        }
        return indexMapBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private static MappingMetaData filterFields(MappingMetaData mappingMetaData, Predicate<String> fieldPredicate) {
        if (mappingMetaData == null) {
            return MappingMetaData.EMPTY_MAPPINGS;
        }
        if (fieldPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
            return mappingMetaData;
        }
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(mappingMetaData.source().compressedReference(), true).v2();
        Map<String, Object> mapping;
        if (sourceAsMap.size() == 1 && sourceAsMap.containsKey(mappingMetaData.type())) {
            mapping = (Map<String, Object>) sourceAsMap.get(mappingMetaData.type());
        } else {
            mapping = sourceAsMap;
        }

        Map<String, Object> properties = (Map<String, Object>)mapping.get("properties");
        if (properties == null || properties.isEmpty()) {
            return mappingMetaData;
        }

        filterFields("", properties, fieldPredicate);

        return new MappingMetaData(mappingMetaData.type(), sourceAsMap);
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

        AliasOrIndex result = getAliasAndIndexLookup().get(aliasOrIndex);
        if (result == null || result.isAlias() == false) {
            return routing;
        }
        AliasOrIndex.Alias alias = (AliasOrIndex.Alias) result;
        IndexMetaData writeIndex = alias.getWriteIndex();
        if (writeIndex == null) {
            throw new IllegalArgumentException("alias [" + aliasOrIndex + "] does not have a write index");
        }
        AliasMetaData aliasMd = writeIndex.getAliases().get(alias.getAliasName());
        if (aliasMd.indexRouting() != null) {
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value ["
                    + aliasMd.getIndexRouting() + "] that resolved to several routing values, rejecting operation");
            }
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it ["
                        + aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
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

        AliasOrIndex result = getAliasAndIndexLookup().get(aliasOrIndex);
        if (result == null || result.isAlias() == false) {
            return routing;
        }
        AliasOrIndex.Alias alias = (AliasOrIndex.Alias) result;
        if (result.getIndices().size() > 1) {
            rejectSingleIndexOperation(aliasOrIndex, result);
        }
        AliasMetaData aliasMd = alias.getFirstAliasMetaData();
        if (aliasMd.indexRouting() != null) {
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value [" +
                    aliasMd.getIndexRouting() + "] that resolved to several routing values, rejecting operation");
            }
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it [" +
                        aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
    }

    private void rejectSingleIndexOperation(String aliasOrIndex, AliasOrIndex result) {
        String[] indexNames = new String[result.getIndices().size()];
        int i = 0;
        for (IndexMetaData indexMetaData : result.getIndices()) {
            indexNames[i++] = indexMetaData.getIndex().getName();
        }
        throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" +
            Arrays.toString(indexNames) + "], can't execute a single index op");
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasConcreteIndex(String index) {
        return getAliasAndIndexLookup().containsKey(index);
    }

    public IndexMetaData index(String index) {
        return indices.get(index);
    }

    public IndexMetaData index(Index index) {
        IndexMetaData metaData = index(index.getName());
        if (metaData != null && metaData.getIndexUUID().equals(index.getUUID())) {
            return metaData;
        }
        return null;
    }

    /** Returns true iff existing index has the same {@link IndexMetaData} instance */
    public boolean hasIndexMetaData(final IndexMetaData indexMetaData) {
        return indices.get(indexMetaData.getIndex().getName()) == indexMetaData;
    }

    /**
     * Returns the {@link IndexMetaData} for this index.
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetaData getIndexSafe(Index index) {
        IndexMetaData metaData = index(index.getName());
        if (metaData != null) {
            if(metaData.getIndexUUID().equals(index.getUUID())) {
                return metaData;
            }
            throw new IndexNotFoundException(index,
                new IllegalStateException("index uuid doesn't match expected: [" + index.getUUID()
                    + "] but got: [" + metaData.getIndexUUID() +"]"));
        }
        throw new IndexNotFoundException(index);
    }

    public ImmutableOpenMap<String, IndexMetaData> indices() {
        return this.indices;
    }

    public ImmutableOpenMap<String, IndexMetaData> getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, IndexTemplateMetaData> templates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, IndexTemplateMetaData> getTemplates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
    }

    public ImmutableOpenMap<String, Custom> getCustoms() {
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
     * Identifies whether the array containing type names given as argument refers to all types
     * The empty or null array identifies all types
     *
     * @param types the array containing types
     * @return true if the provided array maps to all types, false otherwise
     */
    public static boolean isAllTypes(String[] types) {
        return types == null || types.length == 0 || isExplicitAllType(types);
    }

    /**
     * Identifies whether the array containing type names given as argument explicitly refers to all types
     * The empty or null array doesn't explicitly map to all types
     *
     * @param types the array containing index names
     * @return true if the provided array explicitly maps to all types, false otherwise
     */
    public static boolean isExplicitAllType(String[] types) {
        return types != null && types.length == 1 && ALL.equals(types[0]);
    }

    /**
     * @param concreteIndex The concrete index to check if routing is required
     * @return Whether routing is required according to the mapping for the specified index and type
     */
    public boolean routingRequired(String concreteIndex) {
        IndexMetaData indexMetaData = indices.get(concreteIndex);
        if (indexMetaData != null) {
            MappingMetaData mappingMetaData = indexMetaData.mapping();
            if (mappingMetaData != null) {
                return mappingMetaData.routingRequired();
            }
        }
        return false;
    }

    @Override
    public Iterator<IndexMetaData> iterator() {
        return indices.valuesIt();
    }

    public static boolean isGlobalStateEquals(MetaData metaData1, MetaData metaData2) {
        if (!metaData1.coordinationMetaData.equals(metaData2.coordinationMetaData)) {
            return false;
        }
        if (!metaData1.persistentSettings.equals(metaData2.persistentSettings)) {
            return false;
        }
        if (!metaData1.hashesOfConsistentSettings.equals(metaData2.hashesOfConsistentSettings)) {
            return false;
        }
        if (!metaData1.templates.equals(metaData2.templates())) {
            return false;
        }
        if (!metaData1.clusterUUID.equals(metaData2.clusterUUID)) {
            return false;
        }
        if (metaData1.clusterUUIDCommitted != metaData2.clusterUUIDCommitted) {
            return false;
        }
        // Check if any persistent metadata needs to be saved
        int customCount1 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metaData1.customs) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                if (!cursor.value.equals(metaData2.custom(cursor.key))) return false;
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (ObjectCursor<Custom> cursor : metaData2.customs.values()) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        if (customCount1 != customCount2) return false;
        return true;
    }

    @Override
    public Diff<MetaData> diff(MetaData previousState) {
        return new MetaDataDiff(previousState, this);
    }

    public static Diff<MetaData> readDiffFrom(StreamInput in) throws IOException {
        return new MetaDataDiff(in);
    }

    public static MetaData fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser, false);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class MetaDataDiff implements Diff<MetaData> {

        private long version;
        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private CoordinationMetaData coordinationMetaData;
        private Settings transientSettings;
        private Settings persistentSettings;
        private Diff<DiffableStringMap> hashesOfConsistentSettings;
        private Diff<ImmutableOpenMap<String, IndexMetaData>> indices;
        private Diff<ImmutableOpenMap<String, IndexTemplateMetaData>> templates;
        private Diff<ImmutableOpenMap<String, Custom>> customs;

        MetaDataDiff(MetaData before, MetaData after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetaData = after.coordinationMetaData;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        MetaDataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetaData = new CoordinationMetaData(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            } else {
                hashesOfConsistentSettings = DiffableStringMap.DiffableStringMapDiff.EMPTY;
            }
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexMetaData::readFrom,
                IndexMetaData::readDiffFrom);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexTemplateMetaData::readFrom,
                IndexTemplateMetaData::readDiffFrom);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetaData.writeTo(out);
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
        public MetaData apply(MetaData part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetaData(coordinationMetaData);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    public static MetaData readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetaData(new CoordinationMetaData(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            builder.hashesOfConsistentSettings(new DiffableStringMap(in));
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetaData.readFrom(in), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetaData.readFrom(in));
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetaData = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetaData.getWriteableName(), customIndexMetaData);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetaData.writeTo(out);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            hashesOfConsistentSettings.writeTo(out);
        }
        out.writeVInt(indices.size());
        for (IndexMetaData indexMetaData : this) {
            indexMetaData.writeTo(out);
        }
        out.writeVInt(templates.size());
        for (ObjectCursor<IndexTemplateMetaData> cursor : templates.values()) {
            cursor.value.writeTo(out);
        }
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                out.writeNamedWriteable(cursor.value);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MetaData metaData) {
        return new Builder(metaData);
    }

    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetaData coordinationMetaData = CoordinationMetaData.EMPTY_META_DATA;
        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;
        private DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(Collections.emptyMap());

        private final ImmutableOpenMap.Builder<String, IndexMetaData> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetaData> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(MetaData metaData) {
            this.clusterUUID = metaData.clusterUUID;
            this.clusterUUIDCommitted = metaData.clusterUUIDCommitted;
            this.coordinationMetaData = metaData.coordinationMetaData;
            this.transientSettings = metaData.transientSettings;
            this.persistentSettings = metaData.persistentSettings;
            this.hashesOfConsistentSettings = metaData.hashesOfConsistentSettings;
            this.version = metaData.version;
            this.indices = ImmutableOpenMap.builder(metaData.indices);
            this.templates = ImmutableOpenMap.builder(metaData.templates);
            this.customs = ImmutableOpenMap.builder(metaData.customs);
        }

        public Builder put(IndexMetaData.Builder indexMetaDataBuilder) {
            // we know its a new one, increment the version and store
            indexMetaDataBuilder.version(indexMetaDataBuilder.version() + 1);
            IndexMetaData indexMetaData = indexMetaDataBuilder.build();
            indices.put(indexMetaData.getIndex().getName(), indexMetaData);
            return this;
        }

        public Builder put(IndexMetaData indexMetaData, boolean incrementVersion) {
            if (indices.get(indexMetaData.getIndex().getName()) == indexMetaData) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetaData = IndexMetaData.builder(indexMetaData).version(indexMetaData.getVersion() + 1).build();
            }
            indices.put(indexMetaData.getIndex().getName(), indexMetaData);
            return this;
        }

        public IndexMetaData get(String index) {
            return indices.get(index);
        }

        public IndexMetaData getSafe(Index index) {
            IndexMetaData indexMetaData = get(index.getName());
            if (indexMetaData != null) {
                if(indexMetaData.getIndexUUID().equals(index.getUUID())) {
                    return indexMetaData;
                }
                throw new IndexNotFoundException(index,
                    new IllegalStateException("index uuid doesn't match expected: [" + index.getUUID()
                        + "] but got: [" + indexMetaData.getIndexUUID() +"]"));
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

        public Builder indices(ImmutableOpenMap<String, IndexMetaData> indices) {
            this.indices.putAll(indices);
            return this;
        }

        public Builder put(IndexTemplateMetaData.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetaData template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder templates(ImmutableOpenMap<String, IndexTemplateMetaData> templates) {
            this.templates.putAll(templates);
            return this;
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
            IndexGraveyard graveyard = (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
            return graveyard;
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(String.class);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetaData.builder(indexMetaData)
                        .settings(Settings.builder().put(indexMetaData.getSettings()).put(settings)));
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
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetaData.builder(indexMetaData).numberOfReplicas(numberOfReplicas));
            }
            return this;
        }

        public Builder coordinationMetaData(CoordinationMetaData coordinationMetaData) {
            this.coordinationMetaData = coordinationMetaData;
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

        public DiffableStringMap hashesOfConsistentSettings() {
            return this.hashesOfConsistentSettings;
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

        public MetaData build() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will only be rebuilded when needed. Now during serializing we rebuild these datastructures
            //    while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final List<String> visibleIndices = new ArrayList<>();
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> visibleOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final List<String> visibleClosedIndices = new ArrayList<>();
            final Set<String> duplicateAliasesIndices = new HashSet<>();
            for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                final IndexMetaData indexMetaData = cursor.value;
                final String name = indexMetaData.getIndex().getName();
                boolean added = allIndices.add(name);
                assert added : "double index named [" + name + "]";
                final boolean visible = IndexMetaData.INDEX_HIDDEN_SETTING.get(indexMetaData.getSettings()) == false;
                if (visible) {
                    visibleIndices.add(name);
                }
                if (indexMetaData.getState() == IndexMetaData.State.OPEN) {
                    allOpenIndices.add(name);
                    if (visible) {
                        visibleOpenIndices.add(name);
                    }
                } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                    allClosedIndices.add(name);
                    if (visible) {
                        visibleClosedIndices.add(name);
                    }
                }
                indexMetaData.getAliases().keysIt().forEachRemaining(duplicateAliasesIndices::add);
            }
            duplicateAliasesIndices.retainAll(allIndices);
            if (duplicateAliasesIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                ArrayList<String> duplicates = new ArrayList<>();
                for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                    for (String alias: duplicateAliasesIndices) {
                        if (cursor.value.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.value.getIndex() + ")");
                        }
                    }
                }
                assert duplicates.size() > 0;
                throw new IllegalStateException("index and alias names need to be unique, but the following duplicates were found ["
                    + Strings.collectionToCommaDelimitedString(duplicates)+ "]");

            }

            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = Collections.unmodifiableSortedMap(buildAliasAndIndexLookup());


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

            return new MetaData(clusterUUID, clusterUUIDCommitted, version, coordinationMetaData, transientSettings, persistentSettings,
                hashesOfConsistentSettings, indices.build(), templates.build(), customs.build(), allIndicesArray, visibleIndicesArray,
                allOpenIndicesArray, visibleOpenIndicesArray, allClosedIndicesArray, visibleClosedIndicesArray, aliasAndIndexLookup);
        }

        private SortedMap<String, AliasOrIndex> buildAliasAndIndexLookup() {
            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = new TreeMap<>();
            for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                IndexMetaData indexMetaData = cursor.value;
                AliasOrIndex existing = aliasAndIndexLookup.put(indexMetaData.getIndex().getName(), new AliasOrIndex.Index(indexMetaData));
                assert existing == null : "duplicate for " + indexMetaData.getIndex();

                for (ObjectObjectCursor<String, AliasMetaData> aliasCursor : indexMetaData.getAliases()) {
                    AliasMetaData aliasMetaData = aliasCursor.value;
                    aliasAndIndexLookup.compute(aliasMetaData.getAlias(), (aliasName, alias) -> {
                        if (alias == null) {
                            return new AliasOrIndex.Alias(aliasMetaData, indexMetaData);
                        } else {
                            assert alias instanceof AliasOrIndex.Alias : alias.getClass().getName();
                            ((AliasOrIndex.Alias) alias).addIndex(indexMetaData);
                            return alias;
                        }
                    });
                }
            }
            aliasAndIndexLookup.values().stream().filter(AliasOrIndex::isAlias)
                .forEach(alias -> ((AliasOrIndex.Alias) alias).computeAndValidateWriteIndex());
            return aliasAndIndexLookup;
        }

        public static String toXContent(MetaData metaData) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }

        public static void toXContent(MetaData metaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, "API"));

            builder.startObject("meta-data");

            builder.field("version", metaData.version());
            builder.field("cluster_uuid", metaData.clusterUUID);
            builder.field("cluster_uuid_committed", metaData.clusterUUIDCommitted);

            builder.startObject("cluster_coordination");
            metaData.coordinationMetaData().toXContent(builder, params);
            builder.endObject();

            if (!metaData.persistentSettings().isEmpty()) {
                builder.startObject("settings");
                metaData.persistentSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            if (context == XContentContext.API && !metaData.transientSettings().isEmpty()) {
                builder.startObject("transient_settings");
                metaData.transientSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates().values()) {
                IndexTemplateMetaData.Builder.toXContentWithTypes(cursor.value, builder, params);
            }
            builder.endObject();

            if (context == XContentContext.API && !metaData.indices().isEmpty()) {
                builder.startObject("indices");
                for (IndexMetaData indexMetaData : metaData) {
                    IndexMetaData.Builder.toXContent(indexMetaData, builder, params);
                }
                builder.endObject();
            }

            for (ObjectObjectCursor<String, Custom> cursor : metaData.customs()) {
                if (cursor.value.context().contains(context)) {
                    builder.startObject(cursor.key);
                    cursor.value.toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }

        public static MetaData fromXContent(XContentParser parser, boolean preserveUnknownCustoms) throws IOException {
            Builder builder = new Builder();

            // we might get here after the meta-data element, or on a fresh parser
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name (meta-data)
                    token = parser.nextToken();
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new IllegalArgumentException("Expected a field name but got " + token);
                    }
                    // move to the next object
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
            }

            if (!"meta-data".equals(parser.currentName())) {
                throw new IllegalArgumentException("Expected [meta-data] as a field name but got " + currentFieldName);
            }
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a START_OBJECT but got " + token);
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("cluster_coordination".equals(currentFieldName)) {
                        builder.coordinationMetaData(CoordinationMetaData.fromXContent(parser));
                    } else if ("settings".equals(currentFieldName)) {
                        builder.persistentSettings(Settings.fromXContent(parser));
                    } else if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetaData.Builder.fromXContent(parser), false);
                        }
                    } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                        builder.hashesOfConsistentSettings(parser.mapStrings());
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetaData.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else {
                        try {
                            Custom custom = parser.namedObject(Custom.class, currentFieldName, null);
                            builder.putCustom(custom.getWriteableName(), custom);
                        } catch (NamedObjectNotFoundException ex) {
                            if (preserveUnknownCustoms) {
                                logger.warn("Adding unknown custom object with type {}", currentFieldName);
                                builder.putCustom(currentFieldName, new UnknownGatewayOnlyCustom(parser.mapOrdered()));
                            } else {
                                logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                                parser.skipChildren();
                            }
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
            return builder.build();
        }
    }

    public static class UnknownGatewayOnlyCustom implements Custom {

        private final Map<String, Object> contents;

        UnknownGatewayOnlyCustom(Map<String, Object> contents) {
            this.contents = contents;
        }

        @Override
        public EnumSet<XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.API, MetaData.XContentContext.GATEWAY);
        }

        @Override
        public Diff<Custom> diff(Custom previousState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Version getMinimalSupportedVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.mapContents(contents);
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * State format for {@link MetaData} to write to and load from disk
     */
    public static final MetaDataStateFormat<MetaData> FORMAT = createMetaDataStateFormat(false);

    /**
     * Special state format for {@link MetaData} to write to and load from disk, preserving unknown customs
     */
    public static final MetaDataStateFormat<MetaData> FORMAT_PRESERVE_CUSTOMS = createMetaDataStateFormat(true);

    private static MetaDataStateFormat<MetaData> createMetaDataStateFormat(boolean preserveUnknownCustoms) {
        return new MetaDataStateFormat<MetaData>(GLOBAL_STATE_FILE_PREFIX) {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                Builder.toXContent(state, builder, FORMAT_PARAMS);
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return Builder.fromXContent(parser, preserveUnknownCustoms);
            }
        };
    }
}
