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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import gnu.trove.set.hash.THashSet;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
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
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
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
            throw new ElasticSearchIllegalArgumentException("No custom index metadata factoy registered for type [" + type + "]");
        }
        return factory;
    }


    public static final String SETTING_READ_ONLY = "cluster.blocks.read_only";

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA);

    private static ImmutableSet<String> dynamicSettings = ImmutableSet.<String>builder()
            .add(SETTING_READ_ONLY)
            .build();

    public static ImmutableSet<String> dynamicSettings() {
        return dynamicSettings;
    }

    public static boolean hasDynamicSetting(String key) {
        for (String dynamicSetting : dynamicSettings) {
            if (Regex.simpleMatch(dynamicSetting, key)) {
                return true;
            }
        }
        return false;
    }

    public static synchronized void addDynamicSettings(String... settings) {
        HashSet<String> updatedSettings = new HashSet<String>(dynamicSettings);
        updatedSettings.addAll(Arrays.asList(settings));
        dynamicSettings = ImmutableSet.copyOf(updatedSettings);
    }

    public static final MetaData EMPTY_META_DATA = newMetaDataBuilder().build();

    private final long version;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final ImmutableMap<String, IndexMetaData> indices;
    private final ImmutableMap<String, IndexTemplateMetaData> templates;
    private final ImmutableMap<String, Custom> customs;

    private final transient int totalNumberOfShards;

    private final String[] allIndices;
    private final String[] allOpenIndices;

    private final ImmutableMap<String, ImmutableMap<String, AliasMetaData>> aliases;

    private final ImmutableMap<String, ImmutableMap<String, ImmutableSet<String>>> aliasToIndexToSearchRoutingMap;

    // This map indicates if an alias associated with an index is filtering alias
    private final ImmutableMap<String, ImmutableMap<String, Boolean>> indexToAliasFilteringRequiredMap;

    private final ImmutableMap<String, String[]> aliasAndIndexToIndexMap;

    MetaData(long version, Settings transientSettings, Settings persistentSettings, ImmutableMap<String, IndexMetaData> indices, ImmutableMap<String, IndexTemplateMetaData> templates, ImmutableMap<String, Custom> customs) {
        this.version = version;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = ImmutableSettings.settingsBuilder().put(persistentSettings).put(transientSettings).build();
        this.indices = ImmutableMap.copyOf(indices);
        this.customs = customs;
        this.templates = templates;
        int totalNumberOfShards = 0;
        for (IndexMetaData indexMetaData : indices.values()) {
            totalNumberOfShards += indexMetaData.totalNumberOfShards();
        }
        this.totalNumberOfShards = totalNumberOfShards;

        // build all indices map
        List<String> allIndicesLst = Lists.newArrayList();
        for (IndexMetaData indexMetaData : indices.values()) {
            allIndicesLst.add(indexMetaData.index());
        }
        allIndices = allIndicesLst.toArray(new String[allIndicesLst.size()]);

        List<String> allOpenIndices = Lists.newArrayList();
        for (IndexMetaData indexMetaData : indices.values()) {
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                allOpenIndices.add(indexMetaData.index());
            }
        }
        this.allOpenIndices = allOpenIndices.toArray(new String[allOpenIndices.size()]);

        // build aliases map
        MapBuilder<String, MapBuilder<String, AliasMetaData>> tmpAliasesMap = newMapBuilder();
        for (IndexMetaData indexMetaData : indices.values()) {
            String index = indexMetaData.index();
            for (AliasMetaData aliasMd : indexMetaData.aliases().values()) {
                MapBuilder<String, AliasMetaData> indexAliasMap = tmpAliasesMap.get(aliasMd.alias());
                if (indexAliasMap == null) {
                    indexAliasMap = newMapBuilder();
                    tmpAliasesMap.put(aliasMd.alias(), indexAliasMap);
                }
                indexAliasMap.put(index, aliasMd);
            }
        }
        MapBuilder<String, ImmutableMap<String, AliasMetaData>> aliases = newMapBuilder();
        for (Map.Entry<String, MapBuilder<String, AliasMetaData>> alias : tmpAliasesMap.map().entrySet()) {
            aliases.put(alias.getKey(), alias.getValue().immutableMap());
        }
        this.aliases = aliases.immutableMap();

        // build routing aliases set
        MapBuilder<String, MapBuilder<String, ImmutableSet<String>>> tmpAliasToIndexToSearchRoutingMap = newMapBuilder();
        for (IndexMetaData indexMetaData : indices.values()) {
            for (AliasMetaData aliasMd : indexMetaData.aliases().values()) {
                MapBuilder<String, ImmutableSet<String>> indexToSearchRoutingMap = tmpAliasToIndexToSearchRoutingMap.get(aliasMd.alias());
                if (indexToSearchRoutingMap == null) {
                    indexToSearchRoutingMap = newMapBuilder();
                    tmpAliasToIndexToSearchRoutingMap.put(aliasMd.alias(), indexToSearchRoutingMap);
                }
                if (aliasMd.searchRouting() != null) {
                    indexToSearchRoutingMap.put(indexMetaData.index(), ImmutableSet.copyOf(Strings.splitStringByCommaToSet(aliasMd.searchRouting())));
                } else {
                    indexToSearchRoutingMap.put(indexMetaData.index(), ImmutableSet.<String>of());
                }
            }
        }
        MapBuilder<String, ImmutableMap<String, ImmutableSet<String>>> aliasToIndexToSearchRoutingMap = newMapBuilder();
        for (Map.Entry<String, MapBuilder<String, ImmutableSet<String>>> alias : tmpAliasToIndexToSearchRoutingMap.map().entrySet()) {
            aliasToIndexToSearchRoutingMap.put(alias.getKey(), alias.getValue().immutableMap());
        }
        this.aliasToIndexToSearchRoutingMap = aliasToIndexToSearchRoutingMap.immutableMap();

        // build filtering required map
        MapBuilder<String, ImmutableMap<String, Boolean>> filteringRequiredMap = newMapBuilder();
        for (IndexMetaData indexMetaData : indices.values()) {
            MapBuilder<String, Boolean> indexFilteringRequiredMap = newMapBuilder();
            // Filtering is not required for the index itself
            indexFilteringRequiredMap.put(indexMetaData.index(), false);
            for (AliasMetaData aliasMetaData : indexMetaData.aliases().values()) {
                if (aliasMetaData.filter() != null) {
                    indexFilteringRequiredMap.put(aliasMetaData.alias(), true);
                } else {
                    indexFilteringRequiredMap.put(aliasMetaData.alias(), false);
                }
            }
            filteringRequiredMap.put(indexMetaData.index(), indexFilteringRequiredMap.immutableMap());
        }
        indexToAliasFilteringRequiredMap = filteringRequiredMap.immutableMap();

        // build aliasAndIndex to Index map
        MapBuilder<String, Set<String>> tmpAliasAndIndexToIndexBuilder = newMapBuilder();
        for (IndexMetaData indexMetaData : indices.values()) {
            Set<String> lst = tmpAliasAndIndexToIndexBuilder.get(indexMetaData.index());
            if (lst == null) {
                lst = newHashSet();
                tmpAliasAndIndexToIndexBuilder.put(indexMetaData.index(), lst);
            }
            lst.add(indexMetaData.index());

            for (String alias : indexMetaData.aliases().keySet()) {
                lst = tmpAliasAndIndexToIndexBuilder.get(alias);
                if (lst == null) {
                    lst = newHashSet();
                    tmpAliasAndIndexToIndexBuilder.put(alias, lst);
                }
                lst.add(indexMetaData.index());
            }
        }

        MapBuilder<String, String[]> aliasAndIndexToIndexBuilder = newMapBuilder();
        for (Map.Entry<String, Set<String>> entry : tmpAliasAndIndexToIndexBuilder.map().entrySet()) {
            aliasAndIndexToIndexBuilder.put(entry.getKey(), entry.getValue().toArray(new String[entry.getValue().size()]));
        }
        this.aliasAndIndexToIndexMap = aliasAndIndexToIndexBuilder.immutableMap();
    }

    public long version() {
        return this.version;
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

    public ImmutableMap<String, ImmutableMap<String, AliasMetaData>> aliases() {
        return this.aliases;
    }

    public ImmutableMap<String, ImmutableMap<String, AliasMetaData>> getAliases() {
        return aliases();
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
        ImmutableMap<String, AliasMetaData> indexAliases = aliases.get(aliasOrIndex);
        if (indexAliases == null || indexAliases.isEmpty()) {
            return routing;
        }
        if (indexAliases.size() > 1) {
            throw new ElasticSearchIllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" + indexAliases.keySet() + "], can't execute a single index op");
        }
        AliasMetaData aliasMd = indexAliases.values().iterator().next();
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
        return resolveSearchRouting(routing, convertFromWildcards(new String[]{aliasOrIndex}, true, true));
    }

    public Map<String, Set<String>> resolveSearchRouting(@Nullable String routing, String[] aliasesOrIndices) {
        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            return resolveSearchRoutingAllIndices(routing);
        }

        aliasesOrIndices = convertFromWildcards(aliasesOrIndices, true, true);

        if (aliasesOrIndices.length == 1) {
            if (aliasesOrIndices[0].equals("_all")) {
                return resolveSearchRoutingAllIndices(routing);
            } else {
                return resolveSearchRoutingSingleValue(routing, aliasesOrIndices[0]);
            }
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new THashSet<String>();
        if (routing != null) {
            paramRouting = Strings.splitStringByCommaToSet(routing);
        }

        for (String aliasOrIndex : aliasesOrIndices) {
            ImmutableMap<String, ImmutableSet<String>> indexToRoutingMap = aliasToIndexToSearchRoutingMap.get(aliasOrIndex);
            if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
                for (Map.Entry<String, ImmutableSet<String>> indexRouting : indexToRoutingMap.entrySet()) {
                    if (!norouting.contains(indexRouting.getKey())) {
                        if (!indexRouting.getValue().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = newHashMap();
                            }
                            Set<String> r = routings.get(indexRouting.getKey());
                            if (r == null) {
                                r = new THashSet<String>();
                                routings.put(indexRouting.getKey(), r);
                            }
                            r.addAll(indexRouting.getValue());
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(indexRouting.getKey());
                            }
                        } else {
                            // Non-routing alias
                            if (!norouting.contains(indexRouting.getKey())) {
                                norouting.add(indexRouting.getKey());
                                if (paramRouting != null) {
                                    Set<String> r = new THashSet<String>(paramRouting);
                                    if (routings == null) {
                                        routings = newHashMap();
                                    }
                                    routings.put(indexRouting.getKey(), r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(indexRouting.getKey());
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
                        Set<String> r = new THashSet<String>(paramRouting);
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

        ImmutableMap<String, ImmutableSet<String>> indexToRoutingMap = aliasToIndexToSearchRoutingMap.get(aliasOrIndex);
        if (indexToRoutingMap != null && !indexToRoutingMap.isEmpty()) {
            // It's an alias
            for (Map.Entry<String, ImmutableSet<String>> indexRouting : indexToRoutingMap.entrySet()) {
                if (!indexRouting.getValue().isEmpty()) {
                    // Routing alias
                    Set<String> r = new THashSet<String>(indexRouting.getValue());
                    if (paramRouting != null) {
                        r.retainAll(paramRouting);
                    }
                    if (!r.isEmpty()) {
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.getKey(), r);
                    }
                } else {
                    // Non-routing alias
                    if (paramRouting != null) {
                        Set<String> r = new THashSet<String>(paramRouting);
                        if (routings == null) {
                            routings = newHashMap();
                        }
                        routings.put(indexRouting.getKey(), r);
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
        return concreteIndices(indices, false, false);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndicesIgnoreMissing(String[] indices) {
        return concreteIndices(indices, true, false);
    }

    /**
     * Translates the provided indices (possibly aliased) into actual indices.
     */
    public String[] concreteIndices(String[] aliasesOrIndices, boolean ignoreMissing, boolean allOnlyOpen) throws IndexMissingException {
        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            return allOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices();
        }
        aliasesOrIndices = convertFromWildcards(aliasesOrIndices, allOnlyOpen, false);
        // optimize for single element index (common case)
        if (aliasesOrIndices.length == 1) {
            String aliasOrIndex = aliasesOrIndices[0];
            if (aliasOrIndex.length() == 0) {
                return allOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices();
            }
            if (aliasOrIndex.equals("_all")) {
                return allOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices();
            }
            // if a direct index name, just return the array provided
            if (this.indices.containsKey(aliasOrIndex)) {
                return aliasesOrIndices;
            }
            String[] actualLst = aliasAndIndexToIndexMap.get(aliasOrIndex);
            if (actualLst == null) {
                if (!ignoreMissing) {
                    throw new IndexMissingException(new Index(aliasOrIndex));
                } else {
                    return Strings.EMPTY_ARRAY;
                }
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

        Set<String> actualIndices = new THashSet<String>();
        for (String index : aliasesOrIndices) {
            String[] actualLst = aliasAndIndexToIndexMap.get(index);
            if (actualLst == null) {
                if (!ignoreMissing) {
                    throw new IndexMissingException(new Index(index));
                }
            } else {
                for (String x : actualLst) {
                    actualIndices.add(x);
                }
            }
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

    public String[] convertFromWildcards(String[] aliasesOrIndices, boolean wildcardOnlyOpen, boolean ignoreMissing) {
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
                add = true;
                aliasOrIndex = aliasOrIndex.substring(1);
            } else if (aliasOrIndex.charAt(0) == '-') {
                // if its the first, fill it with all the indices...
                if (i == 0) {
                    result = new THashSet<String>(Arrays.asList(wildcardOnlyOpen ? concreteAllOpenIndices() : concreteAllIndices()));
                }
                add = false;
                aliasOrIndex = aliasOrIndex.substring(1);
            }
            if (!Regex.isSimpleMatchPattern(aliasOrIndex)) {
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
                result = new THashSet<String>();
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
            for (String alias : aliases.keySet()) {
                if (Regex.simpleMatch(aliasOrIndex, alias)) {
                    found = true;
                    if (add) {
                        result.add(alias);
                    } else {
                        result.remove(alias);
                    }
                }
            }
            if (!found && !ignoreMissing) {
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

    public ImmutableMap<String, IndexMetaData> indices() {
        return this.indices;
    }

    public ImmutableMap<String, IndexMetaData> getIndices() {
        return indices();
    }

    public ImmutableMap<String, IndexTemplateMetaData> templates() {
        return this.templates;
    }

    public ImmutableMap<String, IndexTemplateMetaData> getTemplates() {
        return this.templates;
    }

    public ImmutableMap<String, Custom> customs() {
        return this.customs;
    }

    public ImmutableMap<String, Custom> getCustoms() {
        return this.customs;
    }

    public int totalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards();
    }


    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p/>
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns <tt>null</tt> if no filtering is required.</p>
     */
    public String[] filteringAliases(String index, String... indices) {
        if (indices == null || indices.length == 0) {
            return null;
        }
        // optimize for the most common single index/alias scenario
        if (indices.length == 1) {
            String alias = indices[0];
            // This list contains "_all" - no filtering needed
            if (alias.equals("_all")) {
                return null;
            }
            ImmutableMap<String, Boolean> aliasToFilteringRequiredMap = indexToAliasFilteringRequiredMap.get(index);
            if (aliasToFilteringRequiredMap == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }
            Boolean filteringRequired = aliasToFilteringRequiredMap.get(alias);
            if (filteringRequired == null || !filteringRequired) {
                return null;
            }
            return new String[]{alias};
        }
        List<String> filteringAliases = null;
        for (String alias : indices) {
            ImmutableMap<String, Boolean> aliasToFilteringRequiredMap = indexToAliasFilteringRequiredMap.get(index);
            if (aliasToFilteringRequiredMap == null) {
                // Shouldn't happen
                throw new IndexMissingException(new Index(index));
            }
            Boolean filteringRequired = aliasToFilteringRequiredMap.get(alias);
            // Check that this is an alias for the current index
            // Otherwise - skip it
            if (filteringRequired != null) {
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


    @Override
    public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.values().iterator();
    }

    public static boolean isGlobalStateEquals(MetaData metaData1, MetaData metaData2) {
        if (!metaData1.persistentSettings.equals(metaData2.persistentSettings)) return false;
        if (!metaData1.templates.equals(metaData2.templates())) return false;
        return true;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static Builder newMetaDataBuilder() {
        return new Builder();
    }

    public static class Builder {

        private long version;

        private Settings transientSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private MapBuilder<String, IndexMetaData> indices = newMapBuilder();

        private MapBuilder<String, IndexTemplateMetaData> templates = newMapBuilder();

        private MapBuilder<String, Custom> customs = newMapBuilder();

        public Builder metaData(MetaData metaData) {
            this.transientSettings = metaData.transientSettings;
            this.persistentSettings = metaData.persistentSettings;
            this.version = metaData.version;
            this.indices.putAll(metaData.indices);
            this.templates.putAll(metaData.templates);
            this.customs.putAll(metaData.customs);
            return this;
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
                indexMetaData = IndexMetaData.newIndexMetaDataBuilder(indexMetaData).version(indexMetaData.version() + 1).build();
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
                indices = this.indices.map().keySet().toArray(new String[this.indices.map().keySet().size()]);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.newIndexMetaDataBuilder(indexMetaData)
                        .settings(settingsBuilder().put(indexMetaData.settings()).put(settings)));
            }
            return this;
        }

        public Builder updateNumberOfReplicas(int numberOfReplicas, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.map().keySet().toArray(new String[this.indices.map().keySet().size()]);
            }
            for (String index : indices) {
                IndexMetaData indexMetaData = this.indices.get(index);
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(index));
                }
                put(IndexMetaData.newIndexMetaDataBuilder(indexMetaData).numberOfReplicas(numberOfReplicas));
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

        public MetaData build() {
            return new MetaData(version, transientSettings, persistentSettings, indices.immutableMap(), templates.immutableMap(), customs.immutableMap());
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

            if (!metaData.persistentSettings().getAsMap().isEmpty()) {
                builder.startObject("settings");
                for (Map.Entry<String, String> entry : metaData.persistentSettings().getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            builder.startObject("templates");
            for (IndexTemplateMetaData template : metaData.templates().values()) {
                IndexTemplateMetaData.Builder.toXContent(template, builder, params);
            }
            builder.endObject();

            if (!metaData.indices().isEmpty()) {
                builder.startObject("indices");
                for (IndexMetaData indexMetaData : metaData) {
                    IndexMetaData.Builder.toXContent(indexMetaData, builder, params);
                }
                builder.endObject();
            }

            for (Map.Entry<String, Custom> entry : metaData.customs().entrySet()) {
                builder.startObject(entry.getKey());
                lookupFactorySafe(entry.getKey()).toXContent(entry.getValue(), builder, params);
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
                    }
                }
            }
            return builder.build();
        }

        public static MetaData readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            builder.version = in.readLong();
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
                String type = in.readUTF();
                Custom customIndexMetaData = lookupFactorySafe(type).readFrom(in);
                builder.putCustom(type, customIndexMetaData);
            }
            return builder.build();
        }

        public static void writeTo(MetaData metaData, StreamOutput out) throws IOException {
            out.writeLong(metaData.version);
            writeSettingsToStream(metaData.transientSettings(), out);
            writeSettingsToStream(metaData.persistentSettings(), out);
            out.writeVInt(metaData.indices.size());
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
            out.writeVInt(metaData.templates.size());
            for (IndexTemplateMetaData template : metaData.templates.values()) {
                IndexTemplateMetaData.Builder.writeTo(template, out);
            }
            out.writeVInt(metaData.customs().size());
            for (Map.Entry<String, Custom> entry : metaData.customs().entrySet()) {
                out.writeUTF(entry.getKey());
                lookupFactorySafe(entry.getKey()).writeTo(entry.getValue(), out);
            }
        }
    }
}
