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

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 *
 */
public class MetaData implements Iterable<IndexMetaData> {

    public static final String ALL = "_all";

    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT;
    }

    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    public interface Custom {

        abstract class Factory<T extends Custom> {

            public abstract String type();

            public abstract T readFrom(StreamInput in) throws IOException;

            public abstract void writeTo(T customIndexMetaData, StreamOutput out) throws IOException;

            public abstract T fromXContent(XContentParser parser) throws IOException;

            public abstract void toXContent(T customIndexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException;

            public EnumSet<XContentContext> context() {
                return API_ONLY;
            }
        }
    }

    public static Map<String, Custom.Factory> customFactories = new HashMap<>();

    static {
        // register non plugin custom metadata
        registerFactory(RepositoriesMetaData.TYPE, RepositoriesMetaData.FACTORY);
        registerFactory(SnapshotMetaData.TYPE, SnapshotMetaData.FACTORY);
        registerFactory(RestoreMetaData.TYPE, RestoreMetaData.FACTORY);
    }

    /**
     * Register a custom index meta data factory. Make sure to call it from a static block.
     */
    public static void registerFactory(String type, Custom.Factory factory) {
        customFactories.put(type, factory);
    }

    @Nullable
    public static <T extends Custom> Custom.Factory<T> lookupFactory(String type) {
        return customFactories.get(type);
    }

    public static <T extends Custom> Custom.Factory<T> lookupFactorySafe(String type) throws ElasticsearchIllegalArgumentException {
        Custom.Factory<T> factory = customFactories.get(type);
        if (factory == null) {
            throw new ElasticsearchIllegalArgumentException("No custom index metadata factory registered for type [" + type + "]");
        }
        return factory;
    }


    public static final String SETTING_READ_ONLY = "cluster.blocks.read_only";

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final MetaData EMPTY_META_DATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    private final String uuid;
    private final long version;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final ImmutableOpenMap<String, IndexMetaData> indices;
    private final ImmutableOpenMap<String, IndexTemplateMetaData> templates;
    private final ImmutableOpenMap<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int numberOfShards;


    private final String[] allIndices;
    private final String[] allOpenIndices;
    private final String[] allClosedIndices;

    private final ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> aliases;
    private final ImmutableOpenMap<String, String[]> aliasAndIndexToIndexMap;

    @SuppressWarnings("unchecked")
    MetaData(String uuid, long version, Settings transientSettings, Settings persistentSettings, ImmutableOpenMap<String, IndexMetaData> indices, ImmutableOpenMap<String, IndexTemplateMetaData> templates, ImmutableOpenMap<String, Custom> customs) {
        this.uuid = uuid;
        this.version = version;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = ImmutableSettings.settingsBuilder().put(persistentSettings).put(transientSettings).build();
        this.indices = indices;
        this.customs = customs;
        this.templates = templates;
        int totalNumberOfShards = 0;
        int numberOfShards = 0;
        int numAliases = 0;
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            totalNumberOfShards += cursor.value.totalNumberOfShards();
            numberOfShards += cursor.value.numberOfShards();
            numAliases += cursor.value.aliases().size();
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.numberOfShards = numberOfShards;

        // build all indices map
        List<String> allIndicesLst = Lists.newArrayList();
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            allIndicesLst.add(cursor.value.index());
        }
        allIndices = allIndicesLst.toArray(new String[allIndicesLst.size()]);
        int numIndices = allIndicesLst.size();

        List<String> allOpenIndices = Lists.newArrayList();
        List<String> allClosedIndices = Lists.newArrayList();
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                allOpenIndices.add(indexMetaData.index());
            } else if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                allClosedIndices.add(indexMetaData.index());
            }
        }
        this.allOpenIndices = allOpenIndices.toArray(new String[allOpenIndices.size()]);
        this.allClosedIndices = allClosedIndices.toArray(new String[allClosedIndices.size()]);

        // build aliases map
        ImmutableOpenMap.Builder<String, Object> tmpAliases = ImmutableOpenMap.builder(numAliases);
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            String index = indexMetaData.index();
            for (ObjectCursor<AliasMetaData> aliasCursor : indexMetaData.aliases().values()) {
                AliasMetaData aliasMd = aliasCursor.value;
                ImmutableOpenMap.Builder<String, AliasMetaData> indexAliasMap = (ImmutableOpenMap.Builder<String, AliasMetaData>) tmpAliases.get(aliasMd.alias());
                if (indexAliasMap == null) {
                    indexAliasMap = ImmutableOpenMap.builder(1); // typically, there is 1 alias pointing to an index
                    tmpAliases.put(aliasMd.alias(), indexAliasMap);
                }
                indexAliasMap.put(index, aliasMd);
            }
        }

        for (ObjectCursor<String> cursor : tmpAliases.keys()) {
            String alias = cursor.value;
            // if there is access to the raw values buffer of the map that the immutable maps wraps, then we don't need to use put, and just set array slots
            ImmutableOpenMap<String, AliasMetaData> map = ((ImmutableOpenMap.Builder) tmpAliases.get(alias)).cast().build();
            tmpAliases.put(alias, map);
        }

        this.aliases = tmpAliases.<String, ImmutableOpenMap<String, AliasMetaData>>cast().build();
        ImmutableOpenMap.Builder<String, Object> aliasAndIndexToIndexMap = ImmutableOpenMap.builder(numAliases + numIndices);
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            ObjectArrayList<String> indicesLst = (ObjectArrayList<String>) aliasAndIndexToIndexMap.get(indexMetaData.index());
            if (indicesLst == null) {
                indicesLst = new ObjectArrayList<>();
                aliasAndIndexToIndexMap.put(indexMetaData.index(), indicesLst);
            }
            indicesLst.add(indexMetaData.index());

            for (ObjectCursor<String> cursor1 : indexMetaData.aliases().keys()) {
                String alias = cursor1.value;
                indicesLst = (ObjectArrayList<String>) aliasAndIndexToIndexMap.get(alias);
                if (indicesLst == null) {
                    indicesLst = new ObjectArrayList<>();
                    aliasAndIndexToIndexMap.put(alias, indicesLst);
                }
                indicesLst.add(indexMetaData.index());
            }
        }

        for (ObjectObjectCursor<String, Object> cursor : aliasAndIndexToIndexMap) {
            String[] indicesLst = ((ObjectArrayList<String>) cursor.value).toArray(String.class);
            aliasAndIndexToIndexMap.put(cursor.key, indicesLst);
        }

        this.aliasAndIndexToIndexMap = aliasAndIndexToIndexMap.<String, String[]>cast().build();
    }

    public long version() {
        return this.version;
    }

    public String uuid() {
        return this.uuid;
    }

    /**
     * Returns the merges transient and persistent settings.
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

    public ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> aliases() {
        return this.aliases;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> getAliases() {
        return aliases();
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards and
     * that point to the specified concrete indices or match partially with the indices via wildcards.
     *
     * @param aliases         The names of the index aliases to find
     * @param concreteIndices The concrete indexes the index aliases must point to order to be returned.
     * @return the found index aliases grouped by index
     */
    public ImmutableOpenMap<String, ImmutableList<AliasMetaData>> findAliases(final String[] aliases, String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        boolean matchAllAliases = matchAllAliases(aliases);
        ImmutableOpenMap.Builder<String, ImmutableList<AliasMetaData>> mapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectOpenHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            List<AliasMetaData> filteredValues = Lists.newArrayList();
            for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
                AliasMetaData value = cursor.value;
                if (matchAllAliases || Regex.simpleMatch(aliases, value.alias())) {
                    filteredValues.add(value);
                }
            }

            if (!filteredValues.isEmpty()) {
                mapBuilder.put(index, ImmutableList.copyOf(filteredValues));
            }
        }
        return mapBuilder.build();
    }

    private static boolean matchAllAliases(final String[] aliases) {
        for (String alias : aliases) {
            if (alias.equals(ALL)) {
                return true;
            }
        }
        return aliases.length == 0;
    }

    /**
     * Checks if at least one of the specified aliases exists in the specified concrete indices. Wildcards are supported in the
     * alias names for partial matches.
     *
     * @param aliases         The names of the index aliases to find
     * @param concreteIndices The concrete indexes the index aliases must point to order to be returned.
     * @return whether at least one of the specified aliases exists in one of the specified concrete indices.
     */
    public boolean hasAliases(final String[] aliases, String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return false;
        }

        Iterable<String> intersection = HppcMaps.intersection(ObjectOpenHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            List<AliasMetaData> filteredValues = Lists.newArrayList();
            for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
                AliasMetaData value = cursor.value;
                if (Regex.simpleMatch(aliases, value.alias())) {
                    filteredValues.add(value);
                }
            }
            if (!filteredValues.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /*
     * Finds all mappings for types and concrete indices. Types are expanded to
     * include all types that match the glob patterns in the types array. Empty
     * types array, null or {"_all"} will be expanded to all types available for
     * the given indices.
     */
    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> findMappings(String[] concreteIndices, final String[] types) {
        assert types != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexMapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectOpenHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            ImmutableOpenMap.Builder<String, MappingMetaData> filteredMappings;
            if (isAllTypes(types)) {
                indexMapBuilder.put(index, indexMetaData.getMappings()); // No types specified means get it all

            } else {
                filteredMappings = ImmutableOpenMap.builder();
                for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.mappings()) {
                    if (Regex.simpleMatch(types, cursor.key)) {
                        filteredMappings.put(cursor.key, cursor.value);
                    }
                }
                if (!filteredMappings.isEmpty()) {
                    indexMapBuilder.put(index, filteredMappings.build());
                }
            }
        }
        return indexMapBuilder.build();
    }

    public ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> findWarmers(String[] concreteIndices, final String[] types, final String[] uncheckedWarmers) {
        assert uncheckedWarmers != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }
        // special _all check to behave the same like not specifying anything for the warmers (not for the indices)
        final String[] warmers = Strings.isAllOrWildcard(uncheckedWarmers) ? Strings.EMPTY_ARRAY : uncheckedWarmers;

        ImmutableOpenMap.Builder<String, ImmutableList<IndexWarmersMetaData.Entry>> mapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectOpenHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            IndexWarmersMetaData indexWarmersMetaData = indexMetaData.custom(IndexWarmersMetaData.TYPE);
            if (indexWarmersMetaData == null || indexWarmersMetaData.entries().isEmpty()) {
                continue;
            }

            Collection<IndexWarmersMetaData.Entry> filteredWarmers = Collections2.filter(indexWarmersMetaData.entries(), new Predicate<IndexWarmersMetaData.Entry>() {

                @Override
                public boolean apply(IndexWarmersMetaData.Entry warmer) {
                    if (warmers.length != 0 && types.length != 0) {
                        return Regex.simpleMatch(warmers, warmer.name()) && Regex.simpleMatch(types, warmer.types());
                    } else if (warmers.length != 0) {
                        return Regex.simpleMatch(warmers, warmer.name());
                    } else if (types.length != 0) {
                        return Regex.simpleMatch(types, warmer.types());
                    } else {
                        return true;
                    }
                }

            });
            if (!filteredWarmers.isEmpty()) {
                mapBuilder.put(index, ImmutableList.copyOf(filteredWarmers));
            }
        }
        return mapBuilder.build();
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] concreteAllIndices() {
        return allIndices;
    }

    public String[] getConcreteAllIndices() {
        return concreteAllIndices();
    }

    public String[] concreteAllOpenIndices() {
        return allOpenIndices;
    }

    public String[] getConcreteAllOpenIndices() {
        return allOpenIndices;
    }

    public String[] concreteAllClosedIndices() {
        return allClosedIndices;
    }

    public String[] getConcreteAllClosedIndices() {
        return allClosedIndices;
    }

    /**
     * Returns indexing routing for the given index.
     */
    public String resolveIndexRouting(@Nullable String routing, String aliasOrIndex) {
        // Check if index is specified by an alias
        ImmutableOpenMap<String, AliasMetaData> indexAliases = aliases.get(aliasOrIndex);
        if (indexAliases == null || indexAliases.isEmpty()) {
            return routing;
        }
        if (indexAliases.size() > 1) {
            throw new ElasticsearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" + Arrays.toString(indexAliases.keys().toArray(String.class)) + "], can't execute a single index op");
        }
        AliasMetaData aliasMd = indexAliases.values().iterator().next().value;
        if (aliasMd.indexRouting() != null) {
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new ElasticsearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it [" + aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            routing = aliasMd.indexRouting();
        }
        if (routing != null) {
            if (routing.indexOf(',') != -1) {
                throw new ElasticsearchIllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value [" + routing + "] that resolved to several routing values, rejecting operation");
            }
        }
        return routing;
    }

    public Map<String, Set<String>> resolveSearchRouting(@Nullable String routing, String aliasOrIndex) {
        return resolveSearchRouting(routing, convertFromWildcards(new String[]{aliasOrIndex}, IndicesOptions.lenientExpandOpen()));
    }

    public Map<String, Set<String>> resolveSearchRouting(@Nullable String routing, String[] aliasesOrIndices) {
        if (isAllIndices(aliasesOrIndices)) {
            return resolveSearchRoutingAllIndices(routing);
        }

        aliasesOrIndices = convertFromWildcards(aliasesOrIndices, IndicesOptions.lenientExpandOpen());

        if (aliasesOrIndices.length == 1) {
            return resolveSearchRoutingSingleValue(routing, aliasesOrIndices[0]);
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
        }

        for (String aliasOrIndex : aliasesOrIndices) {
            ImmutableOpenMap<String, AliasMetaData> indexToRoutingMap = aliases.get(aliasOrIndex);
            if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
                for (ObjectObjectCursor<String, AliasMetaData> indexRouting : indexToRoutingMap) {
                    if (!norouting.contains(indexRouting.key)) {
                        if (!indexRouting.value.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = newHashMap();
                            }
                            Set<String> r = routings.get(indexRouting.key);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(indexRouting.key, r);
                            }
                            r.addAll(indexRouting.value.searchRoutingValues());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(indexRouting.key);
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(indexRouting.key)) {
                                norouting.add(indexRouting.key);
                                if (paramRouting != null) {
                                    Set<String> r = new HashSet<>(paramRouting);
                                    if (routings == null) {
                                        routings = newHashMap();
                                    }
                                    routings.put(indexRouting.key, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(indexRouting.key);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Index
                if (!norouting.contains(aliasOrIndex)) {
                    norouting.add(aliasOrIndex);
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(aliasOrIndex, r);
                    } else {
                        if (routings != null) {
                            routings.remove(aliasOrIndex);
                        }
                    }
                }
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    private Map<String, Set<String>> resolveSearchRoutingSingleValue(@Nullable String routing, String aliasOrIndex) {
        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
        }

        ImmutableOpenMap<String, AliasMetaData> indexToRoutingMap = aliases.get(aliasOrIndex);
        if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
            // It's an alias
            for (ObjectObjectCursor<String, AliasMetaData> indexRouting : indexToRoutingMap) {
                if (!indexRouting.value.searchRoutingValues().isEmpty()) {
                    // Routing alias
                    Set<String> r = new HashSet<>(indexRouting.value.searchRoutingValues());
                    if (paramRouting != null) {
                        r.retainAll(paramRouting);
                    }
                    if (!r.isEmpty()) {
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.key, r);
                    }
                } else {
                    // Non-routing alias
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.key, r);
                    }
                }
            }
        } else {
            // It's an index
            if (paramRouting != null) {
                routings = ImmutableMap.of(aliasOrIndex, paramRouting);
            }
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     */
    private Map<String, Set<String>> resolveSearchRoutingAllIndices(String routing) {
        if (routing != null) {
            Set<String> r = Strings.splitStringByCommaToSet(routing);
            Map<String, Set<String>> routings = newHashMap();
            String[] concreteIndices = concreteAllIndices();
            for (String index : concreteIndices) {
                routings.put(index, r);
            }
            return routings;
        }
        return null;
    }


    /**
     * Translates the provided indices or aliases, eventually containing wildcard expressions, into actual indices.
     *
     * @param indicesOptions how the aliases or indices need to be resolved to concrete indices
     * @param aliasesOrIndices the aliases or indices to be resolved to concrete indices
     * @return the obtained concrete indices
     * @throws IndexMissingException if one of the aliases or indices is missing and the provided indices options
     * don't allow such a case, or if the final result of the indices resolution is no indices and the indices options
     * don't allow such a case.
     * @throws ElasticsearchIllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     * indices options don't allow such a case.
     */
    public String[] concreteIndices(IndicesOptions indicesOptions, String... aliasesOrIndices) throws IndexMissingException, ElasticsearchIllegalArgumentException {
        if (indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed()) {
            if (isAllIndices(aliasesOrIndices)) {
                String[] concreteIndices;
                if (indicesOptions.expandWildcardsOpen() && indicesOptions.expandWildcardsClosed()) {
                    concreteIndices = concreteAllIndices();
                } else if (indicesOptions.expandWildcardsOpen()) {
                    concreteIndices = concreteAllOpenIndices();
                } else {
                    concreteIndices = concreteAllClosedIndices();
                }

                if (!indicesOptions.allowNoIndices() && concreteIndices.length == 0) {
                    throw new IndexMissingException(new Index("_all"));
                }
                return concreteIndices;
            }

            aliasesOrIndices = convertFromWildcards(aliasesOrIndices, indicesOptions);
        }

        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            if (!indicesOptions.allowNoIndices()) {
                throw new ElasticsearchIllegalArgumentException("no indices were specified and wildcard expansion is disabled.");
            } else {
                return Strings.EMPTY_ARRAY;
            }
        }

        boolean failClosed = indicesOptions.forbidClosedIndices() && !indicesOptions.ignoreUnavailable();

        // optimize for single element index (common case)
        if (aliasesOrIndices.length == 1) {
            return concreteIndices(aliasesOrIndices[0], indicesOptions, !indicesOptions.allowNoIndices());
        }

        // check if its a possible aliased index, if not, just return the passed array
        boolean possiblyAliased = false;
        boolean closedIndices = false;
        for (String index : aliasesOrIndices) {
            IndexMetaData indexMetaData = indices.get(index);
            if (indexMetaData == null) {
                possiblyAliased = true;
                break;
            } else {
                if (indicesOptions.forbidClosedIndices() && indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(new Index(index));
                    } else {
                        closedIndices = true;
                    }
                }
            }
        }
        if (!possiblyAliased) {
            if (closedIndices) {
                Set<String> actualIndices = new HashSet<>(Arrays.asList(aliasesOrIndices));
                actualIndices.retainAll(new HashSet<Object>(Arrays.asList(allOpenIndices)));
                return actualIndices.toArray(new String[actualIndices.size()]);
            } else {
                return aliasesOrIndices;
            }
        }

        Set<String> actualIndices = new HashSet<>();
        for (String aliasOrIndex : aliasesOrIndices) {
            String[] indices = concreteIndices(aliasOrIndex, indicesOptions, !indicesOptions.ignoreUnavailable());
            Collections.addAll(actualIndices, indices);
        }

        if (!indicesOptions.allowNoIndices() && actualIndices.isEmpty()) {
            throw new IndexMissingException(new Index(Arrays.toString(aliasesOrIndices)));
        }
        return actualIndices.toArray(new String[actualIndices.size()]);
    }

    /**
     * Utility method that allows to resolve an index or alias to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link org.elasticsearch.ElasticsearchIllegalArgumentException} gets thrown otherwise.
     *
     * @param indexOrAlias   the index or alias to be resolved to concrete index
     * @param indicesOptions the indices options to be used for the index resolution
     * @return the concrete index obtained as a result of the index resolution
     * @throws IndexMissingException                 if the index or alias provided doesn't exist
     * @throws ElasticsearchIllegalArgumentException if the index resolution lead to more than one index
     */
    public String concreteSingleIndex(String indexOrAlias, IndicesOptions indicesOptions) throws IndexMissingException, ElasticsearchIllegalArgumentException {
        String[] indices = concreteIndices(indicesOptions, indexOrAlias);
        if (indices.length != 1) {
            throw new ElasticsearchIllegalArgumentException("unable to return a single index as the index and options provided got resolved to multiple indices");
        }
        return indices[0];
    }

    private String[] concreteIndices(String aliasOrIndex, IndicesOptions options, boolean failNoIndices) throws IndexMissingException, ElasticsearchIllegalArgumentException {
        boolean failClosed = options.forbidClosedIndices() && !options.ignoreUnavailable();

        // a quick check, if this is an actual index, if so, return it
        IndexMetaData indexMetaData = indices.get(aliasOrIndex);
        if (indexMetaData != null) {
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                if (failClosed) {
                    throw new IndexClosedException(new Index(aliasOrIndex));
                } else {
                    return options.forbidClosedIndices() ? Strings.EMPTY_ARRAY : new String[]{aliasOrIndex};
                }
            } else {
                return new String[]{aliasOrIndex};
            }
        }
        // not an actual index, fetch from an alias
        String[] indices = aliasAndIndexToIndexMap.getOrDefault(aliasOrIndex, Strings.EMPTY_ARRAY);
        if (indices.length == 0 && failNoIndices) {
            throw new IndexMissingException(new Index(aliasOrIndex));
        }
        if (indices.length > 1 && !options.allowAliasesToMultipleIndices()) {
            throw new ElasticsearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one indices associated with it [" + Arrays.toString(indices) + "], can't execute a single index op");
        }

        // No need to check whether indices referred by aliases are closed, because there are no closed indices.
        if (allClosedIndices.length == 0) {
            return indices;
        }

        switch (indices.length) {
            case 0:
                return indices;
            case 1:
                indexMetaData = this.indices.get(indices[0]);
                if (indexMetaData != null && indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                    if (failClosed) {
                        throw new IndexClosedException(new Index(indexMetaData.getIndex()));
                    } else {
                        if (options.forbidClosedIndices()) {
                            return Strings.EMPTY_ARRAY;
                        }
                    }
                }
                return indices;
            default:
                ObjectArrayList<String> concreteIndices = new ObjectArrayList<>();
                for (String index : indices) {
                    indexMetaData = this.indices.get(index);
                    if (indexMetaData != null) {
                        if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                            if (failClosed) {
                                throw new IndexClosedException(new Index(indexMetaData.getIndex()));
                            } else if (!options.forbidClosedIndices()) {
                                concreteIndices.add(index);
                            }
                        } else if (indexMetaData.getState() == IndexMetaData.State.OPEN) {
                            concreteIndices.add(index);
                        } else {
                            throw new IllegalStateException("index state [" + indexMetaData.getState() + "] not supported");
                        }
                    }
                }
                return concreteIndices.toArray(String.class);
        }
    }

    /**
     * Converts a list of indices or aliases wildcards, and special +/- signs, into their respective full matches. It
     * won't convert only to indices, but also to aliases. For example, alias_* will expand to alias_1 and alias_2, not
     * to the respective indices those aliases point to.
     */
    public String[] convertFromWildcards(String[] aliasesOrIndices, IndicesOptions indicesOptions) {
        if (aliasesOrIndices == null) {
            return null;
        }
        Set<String> result = null;
        for (int i = 0; i < aliasesOrIndices.length; i++) {
            String aliasOrIndex = aliasesOrIndices[i];
            if (aliasAndIndexToIndexMap.containsKey(aliasOrIndex)) {
                if (result != null) {
                    result.add(aliasOrIndex);
                }
                continue;
            }
            boolean add = true;
            if (aliasOrIndex.charAt(0) == '+') {
                // if its the first, add empty result set
                if (i == 0) {
                    result = new HashSet<>();
                }
                add = true;
                aliasOrIndex = aliasOrIndex.substring(1);
            } else if (aliasOrIndex.charAt(0) == '-') {
                // if its the first, fill it with all the indices...
                if (i == 0) {
                    String[] concreteIndices;
                    if (indicesOptions.expandWildcardsOpen() && indicesOptions.expandWildcardsClosed()) {
                        concreteIndices = concreteAllIndices();
                    } else if (indicesOptions.expandWildcardsOpen()) {
                        concreteIndices = concreteAllOpenIndices();
                    } else if (indicesOptions.expandWildcardsClosed()) {
                        concreteIndices = concreteAllClosedIndices();
                    } else {
                        assert false : "Shouldn't end up here";
                        concreteIndices = Strings.EMPTY_ARRAY;
                    }
                    result = new HashSet<>(Arrays.asList(concreteIndices));
                }
                add = false;
                aliasOrIndex = aliasOrIndex.substring(1);
            }
            if (!Regex.isSimpleMatchPattern(aliasOrIndex)) {
                if (!indicesOptions.ignoreUnavailable() && !aliasAndIndexToIndexMap.containsKey(aliasOrIndex)) {
                    throw new IndexMissingException(new Index(aliasOrIndex));
                }
                if (result != null) {
                    if (add) {
                        result.add(aliasOrIndex);
                    } else {
                        result.remove(aliasOrIndex);
                    }
                }
                continue;
            }
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>();
                result.addAll(Arrays.asList(aliasesOrIndices).subList(0, i));
            }
            String[] indices;
            if (indicesOptions.expandWildcardsOpen() && indicesOptions.expandWildcardsClosed()) {
                indices = concreteAllIndices();
            } else if (indicesOptions.expandWildcardsOpen()) {
                indices = concreteAllOpenIndices();
            } else if (indicesOptions.expandWildcardsClosed()) {
                indices = concreteAllClosedIndices();
            } else {
                assert false : "convertFromWildcards shouldn't get called if wildcards expansion is disabled";
                indices = Strings.EMPTY_ARRAY;
            }
            boolean found = false;
            // iterating over all concrete indices and see if there is a wildcard match
            for (String index : indices) {
                if (Regex.simpleMatch(aliasOrIndex, index)) {
                    found = true;
                    if (add) {
                        result.add(index);
                    } else {
                        result.remove(index);
                    }
                }
            }
            // iterating over all aliases and see if there is a wildcard match
            for (ObjectCursor<String> cursor : aliases.keys()) {
                String alias = cursor.value;
                if (Regex.simpleMatch(aliasOrIndex, alias)) {
                    found = true;
                    if (add) {
                        result.add(alias);
                    } else {
                        result.remove(alias);
                    }
                }
            }
            if (!found && !indicesOptions.allowNoIndices()) {
                throw new IndexMissingException(new Index(aliasOrIndex));
            }
        }
        if (result == null) {
            return aliasesOrIndices;
        }
        if (result.isEmpty() && !indicesOptions.allowNoIndices()) {
            throw new IndexMissingException(new Index(Arrays.toString(aliasesOrIndices)));
        }
        return result.toArray(new String[result.size()]);
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasConcreteIndex(String index) {
        return aliasAndIndexToIndexMap.containsKey(index);
    }

    public IndexMetaData index(String index) {
        return indices.get(index);
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

    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    public int totalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards();
    }

    public int numberOfShards() {
        return this.numberOfShards;
    }

    public int getNumberOfShards() {
        return numberOfShards();
    }


    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p/>
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns <tt>null</tt> if no filtering is required.</p>
     */
    public String[] filteringAliases(String index, String... indicesOrAliases) {
        // expand the aliases wildcard
        indicesOrAliases = convertFromWildcards(indicesOrAliases, IndicesOptions.lenientExpandOpen());

        if (isAllIndices(indicesOrAliases)) {
            return null;
        }
        // optimize for the most common single index/alias scenario
        if (indicesOrAliases.length == 1) {
            String alias = indicesOrAliases[0];
            IndexMetaData indexMetaData = this.indices.get(index);
            if (indexMetaData == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }
            AliasMetaData aliasMetaData = indexMetaData.aliases().get(alias);
            boolean filteringRequired = aliasMetaData != null && aliasMetaData.filteringRequired();
            if (!filteringRequired) {
                return null;
            }
            return new String[]{alias};
        }
        List<String> filteringAliases = null;
        for (String alias : indicesOrAliases) {
            if (alias.equals(index)) {
                return null;
            }

            IndexMetaData indexMetaData = this.indices.get(index);
            if (indexMetaData == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }

            AliasMetaData aliasMetaData = indexMetaData.aliases().get(alias);
            // Check that this is an alias for the current index
            // Otherwise - skip it
            if (aliasMetaData != null) {
                boolean filteringRequired = aliasMetaData.filteringRequired();
                if (filteringRequired) {
                    // If filtering required - add it to the list of filters
                    if (filteringAliases == null) {
                        filteringAliases = newArrayList();
                    }
                    filteringAliases.add(alias);
                } else {
                    // If not, we have a non filtering alias for this index - no filtering needed
                    return null;
                }
            }
        }
        if (filteringAliases == null) {
            return null;
        }
        return filteringAliases.toArray(new String[filteringAliases.size()]);
    }

    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(String[] aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.length == 0 || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing type names given as argument refers to all types
     * The empty or null array identifies all types
     *
     * @param types the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllTypes(String[] types) {
        return types == null || types.length == 0 || isExplicitAllPattern(types);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    public static boolean isExplicitAllPattern(String[] aliasesOrIndices) {
        return aliasesOrIndices != null && aliasesOrIndices.length == 1 && ALL.equals(aliasesOrIndices[0]);
    }

    /**
     * Identifies whether the first argument (an array containing index names) is a pattern that matches all indices
     *
     * @param indicesOrAliases the array containing index names
     * @param concreteIndices  array containing the concrete indices that the first argument refers to
     * @return true if the first argument is a pattern that maps to all available indices, false otherwise
     */
    public boolean isPatternMatchingAllIndices(String[] indicesOrAliases, String[] concreteIndices) {
        // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
        if (concreteIndices.length == concreteAllIndices().length && indicesOrAliases.length > 0) {

            //we might have something like /-test1,+test1 that would identify all indices
            //or something like /-test1 with test1 index missing and IndicesOptions.lenient()
            if (indicesOrAliases[0].charAt(0) == '-') {
                return true;
            }

            //otherwise we check if there's any simple regex
            for (String indexOrAlias : indicesOrAliases) {
                if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param concreteIndex The concrete index to check if routing is required
     * @param type          The type to check if routing is required
     * @return Whether routing is required according to the mapping for the specified index and type
     */
    public boolean routingRequired(String concreteIndex, String type) {
        IndexMetaData indexMetaData = indices.get(concreteIndex);
        if (indexMetaData != null) {
            MappingMetaData mappingMetaData = indexMetaData.getMappings().get(type);
            if (mappingMetaData != null) {
                return mappingMetaData.routing().required();
            }
        }
        return false;
    }

    @Override
    public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.valuesIt();
    }

    public static boolean isGlobalStateEquals(MetaData metaData1, MetaData metaData2) {
        if (!metaData1.persistentSettings.equals(metaData2.persistentSettings)) {
            return false;
        }
        if (!metaData1.templates.equals(metaData2.templates())) {
            return false;
        }
        // Check if any persistent metadata needs to be saved
        int customCount1 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metaData1.customs) {
            if (customFactories.get(cursor.key).context().contains(XContentContext.GATEWAY)) {
                if (!cursor.value.equals(metaData2.custom(cursor.key))) return false;
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metaData2.customs) {
            if (customFactories.get(cursor.key).context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        if (customCount1 != customCount2) return false;
        return true;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MetaData metaData) {
        return new Builder(metaData);
    }

    public static class Builder {

        private String uuid;
        private long version;

        private Settings transientSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private final ImmutableOpenMap.Builder<String, IndexMetaData> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetaData> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        public Builder() {
            uuid = "_na_";
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
        }

        public Builder(MetaData metaData) {
            this.uuid = metaData.uuid;
            this.transientSettings = metaData.transientSettings;
            this.persistentSettings = metaData.persistentSettings;
            this.version = metaData.version;
            this.indices = ImmutableOpenMap.builder(metaData.indices);
            this.templates = ImmutableOpenMap.builder(metaData.templates);
            this.customs = ImmutableOpenMap.builder(metaData.customs);
        }

        public Builder put(IndexMetaData.Builder indexMetaDataBuilder) {
            // we know its a new one, increment the version and store
            indexMetaDataBuilder.version(indexMetaDataBuilder.version() + 1);
            IndexMetaData indexMetaData = indexMetaDataBuilder.build();
            indices.put(indexMetaData.index(), indexMetaData);
            return this;
        }

        public Builder put(IndexMetaData indexMetaData, boolean incrementVersion) {
            if (indices.get(indexMetaData.index()) == indexMetaData) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetaData = IndexMetaData.builder(indexMetaData).version(indexMetaData.version() + 1).build();
            }
            indices.put(indexMetaData.index(), indexMetaData);
            return this;
        }

        public IndexMetaData get(String index) {
            return indices.get(index);
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            indices.clear();
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

        public Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, custom);
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(String.class);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.builder(indexMetaData)
                        .settings(settingsBuilder().put(indexMetaData.settings()).put(settings)));
            }
            return this;
        }

        public Builder updateNumberOfReplicas(int numberOfReplicas, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(String.class);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.builder(indexMetaData).numberOfReplicas(numberOfReplicas));
            }
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

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder generateUuidIfNeeded() {
            if (uuid.equals("_na_")) {
                uuid = Strings.randomBase64UUID();
            }
            return this;
        }

        public MetaData build() {
            return new MetaData(uuid, version, transientSettings, persistentSettings, indices.build(), templates.build(), customs.build());
        }

        public static String toXContent(MetaData metaData) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        }

        public static void toXContent(MetaData metaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, "API"));

            builder.startObject("meta-data");

            builder.field("version", metaData.version());
            builder.field("uuid", metaData.uuid);

            if (!metaData.persistentSettings().getAsMap().isEmpty()) {
                builder.startObject("settings");
                for (Map.Entry<String, String> entry : metaData.persistentSettings().getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            if (context == XContentContext.API && !metaData.transientSettings().getAsMap().isEmpty()) {
                builder.startObject("transient_settings");
                for (Map.Entry<String, String> entry : metaData.transientSettings().getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates().values()) {
                IndexTemplateMetaData.Builder.toXContent(cursor.value, builder, params);
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
                Custom.Factory factory = lookupFactorySafe(cursor.key);
                if (factory.context().contains(context)) {
                    builder.startObject(cursor.key);
                    factory.toXContent(cursor.value, builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }

        public static MetaData fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            // we might get here after the meta-data element, or on a fresh parser
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name (meta-data)
                    token = parser.nextToken();
                    // move to the next object
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
                if (token == null) {
                    // no data...
                    return builder.build();
                }
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        builder.persistentSettings(ImmutableSettings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build());
                    } else if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetaData.Builder.fromXContent(parser), false);
                        }
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetaData.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else {
                        // check if its a custom index metadata
                        Custom.Factory<Custom> factory = lookupFactory(currentFieldName);
                        if (factory == null) {
                            //TODO warn
                            parser.skipChildren();
                        } else {
                            builder.putCustom(factory.type(), factory.fromXContent(parser));
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    } else if ("uuid".equals(currentFieldName)) {
                        builder.uuid = parser.text();
                    }
                }
            }
            return builder.build();
        }

        public static MetaData readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            builder.version = in.readLong();
            builder.uuid = in.readString();
            builder.transientSettings(readSettingsFromStream(in));
            builder.persistentSettings(readSettingsFromStream(in));
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexMetaData.Builder.readFrom(in), false);
            }
            size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexTemplateMetaData.Builder.readFrom(in));
            }
            int customSize = in.readVInt();
            for (int i = 0; i < customSize; i++) {
                String type = in.readString();
                Custom customIndexMetaData = lookupFactorySafe(type).readFrom(in);
                builder.putCustom(type, customIndexMetaData);
            }
            return builder.build();
        }

        public static void writeTo(MetaData metaData, StreamOutput out) throws IOException {
            out.writeLong(metaData.version);
            out.writeString(metaData.uuid);
            writeSettingsToStream(metaData.transientSettings(), out);
            writeSettingsToStream(metaData.persistentSettings(), out);
            out.writeVInt(metaData.indices.size());
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
            out.writeVInt(metaData.templates.size());
            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates.values()) {
                IndexTemplateMetaData.Builder.writeTo(cursor.value, out);
            }
            out.writeVInt(metaData.customs().size());
            for (ObjectObjectCursor<String, Custom> cursor : metaData.customs()) {
                out.writeString(cursor.key);
                lookupFactorySafe(cursor.key).writeTo(cursor.value, out);
            }
        }
    }
}
