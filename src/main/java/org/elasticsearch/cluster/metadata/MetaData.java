/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IgnoreIndices;
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
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 *
 */
public class MetaData implements Iterable<IndexMetaData> {

    public interface Custom {

        interface Factory<T extends Custom> {

            String type();

            T readFrom(StreamInput in) throws IOException;

            void writeTo(T customIndexMetaData, StreamOutput out) throws IOException;

            T fromXContent(XContentParser parser) throws IOException;

            void toXContent(T customIndexMetaData, XContentBuilder builder, ToXContent.Params params);
        }
    }

    public static Map<String, Custom.Factory> customFactories = new HashMap<String, Custom.Factory>();

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

    public static <T extends Custom> Custom.Factory<T> lookupFactorySafe(String type) throws ElasticSearchIllegalArgumentException {
        Custom.Factory<T> factory = customFactories.get(type);
        if (factory == null) {
            throw new ElasticSearchIllegalArgumentException("No custom index metadata factory registered for type [" + type + "]");
        }
        return factory;
    }


    public static final String SETTING_READ_ONLY = "cluster.blocks.read_only";

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA);

    public static final MetaData EMPTY_META_DATA = builder().build();

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
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                allOpenIndices.add(indexMetaData.index());
            }
        }
        this.allOpenIndices = allOpenIndices.toArray(new String[allOpenIndices.size()]);

        // build aliases map
        ImmutableOpenMap.Builder<String, Object> tmpAliases = ImmutableOpenMap.builder(numAliases);
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            IndexMetaData indexMetaData = cursor.value;
            String index = indexMetaData.index();
            for (ObjectCursor<AliasMetaData> aliasCursor : indexMetaData.aliases().values()) {
                AliasMetaData aliasMd = aliasCursor.value;
                ImmutableOpenMap.Builder<String, AliasMetaData> indexAliasMap = (ImmutableOpenMap.Builder<String, AliasMetaData>) tmpAliases.get(aliasMd.alias());
                if (indexAliasMap == null) {
                    indexAliasMap = ImmutableOpenMap.builder(indices.size());
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
                indicesLst = new ObjectArrayList<String>();
                aliasAndIndexToIndexMap.put(indexMetaData.index(), indicesLst);
            }
            indicesLst.add(indexMetaData.index());

            for (ObjectCursor<String> cursor1 : indexMetaData.aliases().keys()) {
                String alias = cursor1.value;
                indicesLst = (ObjectArrayList<String>) aliasAndIndexToIndexMap.get(alias);
                if (indicesLst == null) {
                    indicesLst = new ObjectArrayList<String>();
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

        ImmutableOpenMap.Builder<String, ImmutableList<AliasMetaData>> mapBuilder = ImmutableOpenMap.builder();
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
                mapBuilder.put(index, ImmutableList.copyOf(filteredValues));
            }
        }
        return mapBuilder.build();
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
            throw new ElasticSearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" + Arrays.toString(indexAliases.keys().toArray(String.class)) + "], can't execute a single index op");
        }
        AliasMetaData aliasMd = indexAliases.values().iterator().next().value;
        if (aliasMd.indexRouting() != null) {
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new ElasticSearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it [" + aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            routing = aliasMd.indexRouting();
        }
        if (routing != null) {
            if (routing.indexOf(',') != -1) {
                throw new ElasticSearchIllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value [" + routing + "] that resolved to several routing values, rejecting operation");
            }
        }
        return routing;
    }

    public Map<String, Set<String>> resolveSearchRouting(@Nullable String routing, String aliasOrIndex) {
        return resolveSearchRouting(routing, convertFromWildcards(new String[]{aliasOrIndex}, true, IgnoreIndices.MISSING));
    }

    public Map<String, Set<String>> resolveSearchRouting(@Nullable String routing, String[] aliasesOrIndices) {
        if (isAllIndices(aliasesOrIndices)) {
            return resolveSearchRoutingAllIndices(routing);
        }

        aliasesOrIndices = convertFromWildcards(aliasesOrIndices, true, IgnoreIndices.MISSING);

        if (aliasesOrIndices.length == 1) {
            return resolveSearchRoutingSingleValue(routing, aliasesOrIndices[0]);
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<String>();
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
                                r = new HashSet<String>();
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
                                    Set<String> r = new HashSet<String>(paramRouting);
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
                        Set<String> r = new HashSet<String>(paramRouting);
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
                    Set<String> r = new HashSet<String>(indexRouting.value.searchRoutingValues());
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
                        Set<String> r = new HashSet<String>(paramRouting);
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
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndices(String[] indices) throws IndexMissingException {
        return concreteIndices(indices, IgnoreIndices.NONE, false);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndicesIgnoreMissing(String[] indices) {
        return concreteIndices(indices, IgnoreIndices.MISSING, false);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndices(String[] aliasesOrIndices, IgnoreIndices ignoreIndices, boolean allOnlyOpen) throws IndexMissingException {
        if (isAllIndices(aliasesOrIndices)) {
            return allOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices();
        }
        aliasesOrIndices = convertFromWildcards(aliasesOrIndices, allOnlyOpen, ignoreIndices);
        // optimize for single element index (common case)
        if (aliasesOrIndices.length == 1) {
            String aliasOrIndex = aliasesOrIndices[0];
            // if a direct index name, just return the array provided
            if (this.indices.containsKey(aliasOrIndex)) {
                return aliasesOrIndices;
            }
            String[] actualLst = aliasAndIndexToIndexMap.get(aliasOrIndex);
            if (actualLst == null) {
                throw new IndexMissingException(new Index(aliasOrIndex));
            } else {
                return actualLst;
            }
        }

        // check if its a possible aliased index, if not, just return the
        // passed array
        boolean possiblyAliased = false;
        for (String index : aliasesOrIndices) {
            if (!this.indices.containsKey(index)) {
                possiblyAliased = true;
                break;
            }
        }
        if (!possiblyAliased) {
            return aliasesOrIndices;
        }

        Set<String> actualIndices = new HashSet<String>();
        for (String index : aliasesOrIndices) {
            String[] actualLst = aliasAndIndexToIndexMap.get(index);
            if (actualLst == null) {
                if (ignoreIndices != IgnoreIndices.MISSING) {
                    throw new IndexMissingException(new Index(index));
                }
            } else {
                for (String x : actualLst) {
                    actualIndices.add(x);
                }
            }
        }

        if (actualIndices.isEmpty()) {
            throw new IndexMissingException(new Index(Arrays.toString(aliasesOrIndices)));
        }
        return actualIndices.toArray(new String[actualIndices.size()]);
    }

    public String concreteIndex(String index) throws IndexMissingException, ElasticSearchIllegalArgumentException {
        // a quick check, if this is an actual index, if so, return it
        if (indices.containsKey(index)) {
            return index;
        }
        // not an actual index, fetch from an alias
        String[] lst = aliasAndIndexToIndexMap.get(index);
        if (lst == null) {
            throw new IndexMissingException(new Index(index));
        }
        if (lst.length > 1) {
            throw new ElasticSearchIllegalArgumentException("Alias [" + index + "] has more than one indices associated with it [" + Arrays.toString(lst) + "], can't execute a single index op");
        }
        return lst[0];
    }

    /**
     * Converts a list of indices or aliases wildcards, and special +/- signs, into their respective full matches. It
     * won't convert only to indices, but also to aliases. For example, alias_* will expand to alias_1 and alias_2, not
     * to the respective indices those aliases point to.
     */
    public String[] convertFromWildcards(String[] aliasesOrIndices, boolean wildcardOnlyOpen, IgnoreIndices ignoreIndices) {
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
                    result = new HashSet<String>();
                }
                add = true;
                aliasOrIndex = aliasOrIndex.substring(1);
            } else if (aliasOrIndex.charAt(0) == '-') {
                // if its the first, fill it with all the indices...
                if (i == 0) {
                    result = new HashSet<String>(Arrays.asList(wildcardOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices()));
                }
                add = false;
                aliasOrIndex = aliasOrIndex.substring(1);
            }
            if (!Regex.isSimpleMatchPattern(aliasOrIndex)) {
                if (ignoreIndices != IgnoreIndices.MISSING && !aliasAndIndexToIndexMap.containsKey(aliasOrIndex)) {
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
                result = new HashSet<String>();
                result.addAll(Arrays.asList(aliasesOrIndices).subList(0, i));
            }
            String[] indices = wildcardOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices();
            boolean found = false;
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
            if (!found && ignoreIndices != IgnoreIndices.MISSING) {
                throw new IndexMissingException(new Index(aliasOrIndex));
            }
        }
        if (result == null) {
            return aliasesOrIndices;
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
        indicesOrAliases = convertFromWildcards(indicesOrAliases, true, IgnoreIndices.MISSING);

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

    private boolean isAllIndices(String[] aliasesOrIndices) {
        return (aliasesOrIndices == null || aliasesOrIndices.length == 0 || (aliasesOrIndices.length == 1 && "_all".equals(aliasesOrIndices[0])));
    }


    @Override
    public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.valuesIt();
    }

    public static boolean isGlobalStateEquals(MetaData metaData1, MetaData metaData2) {
        if (!metaData1.persistentSettings.equals(metaData2.persistentSettings)) return false;
        if (!metaData1.templates.equals(metaData2.templates())) return false;
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

            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates().values()) {
                IndexTemplateMetaData.Builder.toXContent(cursor.value, builder, params);
            }
            builder.endObject();

            if (!metaData.indices().isEmpty()) {
                builder.startObject("indices");
                for (IndexMetaData indexMetaData : metaData) {
                    IndexMetaData.Builder.toXContent(indexMetaData, builder, params);
                }
                builder.endObject();
            }

            for (ObjectObjectCursor<String, Custom> cursor : metaData.customs()) {
                builder.startObject(cursor.key);
                lookupFactorySafe(cursor.key).toXContent(cursor.value, builder, params);
                builder.endObject();
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
                            builder.put(IndexTemplateMetaData.Builder.fromXContent(parser));
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
            if (in.getVersion().after(Version.V_0_90_7)) {
                builder.uuid = in.readString();
            }
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
            if (out.getVersion().after(Version.V_0_90_7)) {
                out.writeString(metaData.uuid);
            }
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
