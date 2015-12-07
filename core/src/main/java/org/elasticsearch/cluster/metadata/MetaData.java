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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.FromXContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

public class MetaData implements Iterable<IndexMetaData>, Diffable<MetaData>, FromXContentBuilder<MetaData>, ToXContent {

    public static final MetaData PROTO = builder().build();

    public static final String ALL = "_all";

    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT
    }

    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    public interface Custom extends Diffable<Custom>, ToXContent {

        String type();

        Custom fromXContent(XContentParser parser) throws IOException;

        EnumSet<XContentContext> context();
    }

    public static Map<String, Custom> customPrototypes = new HashMap<>();

    static {
        // register non plugin custom metadata
        registerPrototype(RepositoriesMetaData.TYPE, RepositoriesMetaData.PROTO);
    }

    /**
     * Register a custom index meta data factory. Make sure to call it from a static block.
     */
    public static void registerPrototype(String type, Custom proto) {
        customPrototypes.put(type, proto);
    }

    @Nullable
    public static <T extends Custom> T lookupPrototype(String type) {
        //noinspection unchecked
        return (T) customPrototypes.get(type);
    }

    public static <T extends Custom> T lookupPrototypeSafe(String type) {
        //noinspection unchecked
        T proto = (T) customPrototypes.get(type);
        if (proto == null) {
            throw new IllegalArgumentException("No custom metadata prototype registered for type [" + type + "]");
        }
        return proto;
    }


    public static final String SETTING_READ_ONLY = "cluster.blocks.read_only";

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final MetaData EMPTY_META_DATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    private final String clusterUUID;
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

    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;

    @SuppressWarnings("unchecked")
    MetaData(String clusterUUID, long version, Settings transientSettings, Settings persistentSettings, ImmutableOpenMap<String, IndexMetaData> indices, ImmutableOpenMap<String, IndexTemplateMetaData> templates, ImmutableOpenMap<String, Custom> customs, String[] allIndices, String[] allOpenIndices, String[] allClosedIndices, SortedMap<String, AliasOrIndex> aliasAndIndexLookup) {
        this.clusterUUID = clusterUUID;
        this.version = version;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.settingsBuilder().put(persistentSettings).put(transientSettings).build();
        this.indices = indices;
        this.customs = customs;
        this.templates = templates;
        int totalNumberOfShards = 0;
        int numberOfShards = 0;
        for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
            totalNumberOfShards += cursor.value.getTotalNumberOfShards();
            numberOfShards += cursor.value.getNumberOfShards();
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.numberOfShards = numberOfShards;

        this.allIndices = allIndices;
        this.allOpenIndices = allOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.aliasAndIndexLookup = aliasAndIndexLookup;
    }

    public long version() {
        return this.version;
    }

    public String clusterUUID() {
        return this.clusterUUID;
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
            IndexMetaData thisIndex= indices().get(otherIndex.getIndex());
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
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards and
     * that point to the specified concrete indices or match partially with the indices via wildcards.
     *
     * @param aliases         The names of the index aliases to find
     * @param concreteIndices The concrete indexes the index aliases must point to order to be returned.
     * @return the found index aliases grouped by index
     */
    public ImmutableOpenMap<String, List<AliasMetaData>> findAliases(final String[] aliases, String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        boolean matchAllAliases = matchAllAliases(aliases);
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> mapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            List<AliasMetaData> filteredValues = new ArrayList<>();
            for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
                AliasMetaData value = cursor.value;
                if (matchAllAliases || Regex.simpleMatch(aliases, value.alias())) {
                    filteredValues.add(value);
                }
            }

            if (!filteredValues.isEmpty()) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, new Comparator<AliasMetaData>() {
                    @Override
                    public int compare(AliasMetaData o1, AliasMetaData o2) {
                        return o1.alias().compareTo(o2.alias());
                    }
                });
                mapBuilder.put(index, Collections.unmodifiableList(filteredValues));
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

        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            List<AliasMetaData> filteredValues = new ArrayList<>();
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
        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            ImmutableOpenMap.Builder<String, MappingMetaData> filteredMappings;
            if (isAllTypes(types)) {
                indexMapBuilder.put(index, indexMetaData.getMappings()); // No types specified means get it all

            } else {
                filteredMappings = ImmutableOpenMap.builder();
                for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.getMappings()) {
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

    public ImmutableOpenMap<String, List<IndexWarmersMetaData.Entry>> findWarmers(String[] concreteIndices, final String[] types, final String[] uncheckedWarmers) {
        assert uncheckedWarmers != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }
        // special _all check to behave the same like not specifying anything for the warmers (not for the indices)
        final String[] warmers = Strings.isAllOrWildcard(uncheckedWarmers) ? Strings.EMPTY_ARRAY : uncheckedWarmers;

        ImmutableOpenMap.Builder<String, List<IndexWarmersMetaData.Entry>> mapBuilder = ImmutableOpenMap.builder();
        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetaData indexMetaData = indices.get(index);
            IndexWarmersMetaData indexWarmersMetaData = indexMetaData.custom(IndexWarmersMetaData.TYPE);
            if (indexWarmersMetaData == null || indexWarmersMetaData.entries().isEmpty()) {
                continue;
            }

            // TODO: make this a List so we don't have to copy below
            Collection<IndexWarmersMetaData.Entry> filteredWarmers =
                    indexWarmersMetaData
                            .entries()
                            .stream()
                            .filter(warmer -> {
                                if (warmers.length != 0 && types.length != 0) {
                                    return Regex.simpleMatch(warmers, warmer.name()) && Regex.simpleMatch(types, warmer.types());
                                } else if (warmers.length != 0) {
                                    return Regex.simpleMatch(warmers, warmer.name());
                                } else if (types.length != 0) {
                                    return Regex.simpleMatch(types, warmer.types());
                                } else {
                                    return true;
                                }
                            })
                            .collect(Collectors.toCollection(ArrayList::new));

            if (!filteredWarmers.isEmpty()) {
                mapBuilder.put(index, Collections.unmodifiableList(new ArrayList<>(filteredWarmers)));
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
            String[] indexNames = new String[result.getIndices().size()];
            int i = 0;
            for (IndexMetaData indexMetaData : result.getIndices()) {
                indexNames[i++] = indexMetaData.getIndex();
            }
            throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has more than one index associated with it [" + Arrays.toString(indexNames) + "], can't execute a single index op");
        }
        AliasMetaData aliasMd = alias.getFirstAliasMetaData();
        if (aliasMd.indexRouting() != null) {
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new IllegalArgumentException("Alias [" + aliasOrIndex + "] has index routing associated with it [" + aliasMd.indexRouting() + "], and was provided with routing value [" + routing + "], rejecting operation");
                }
            }
            routing = aliasMd.indexRouting();
        }
        if (routing != null) {
            if (routing.indexOf(',') != -1) {
                throw new IllegalArgumentException("index/alias [" + aliasOrIndex + "] provided with routing value [" + routing + "] that resolved to several routing values, rejecting operation");
            }
        }
        return routing;
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
    public Iterator<IndexMetaData> iterator() {
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
            if (customPrototypes.get(cursor.key).context().contains(XContentContext.GATEWAY)) {
                if (!cursor.value.equals(metaData2.custom(cursor.key))) return false;
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metaData2.customs) {
            if (customPrototypes.get(cursor.key).context().contains(XContentContext.GATEWAY)) {
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

    @Override
    public Diff<MetaData> readDiffFrom(StreamInput in) throws IOException {
        return new MetaDataDiff(in);
    }

    @Override
    public MetaData fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class MetaDataDiff implements Diff<MetaData> {

        private long version;

        private String clusterUUID;

        private Settings transientSettings;
        private Settings persistentSettings;
        private Diff<ImmutableOpenMap<String, IndexMetaData>> indices;
        private Diff<ImmutableOpenMap<String, IndexTemplateMetaData>> templates;
        private Diff<ImmutableOpenMap<String, Custom>> customs;


        public MetaDataDiff(MetaData before, MetaData after) {
            clusterUUID = after.clusterUUID;
            version = after.version;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer());
        }

        public MetaDataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            version = in.readLong();
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexMetaData.PROTO);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexTemplateMetaData.PROTO);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(),
                    new DiffableUtils.DiffableValueSerializer<String, Custom>() {
                @Override
                public Custom read(StreamInput in, String key) throws IOException {
                    return lookupPrototypeSafe(key).readFrom(in);
                }

                @Override
                public Diff<Custom> readDiff(StreamInput in, String key) throws IOException {
                    return lookupPrototypeSafe(key).readDiffFrom(in);
                }
            });
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeLong(version);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public MetaData apply(MetaData part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.version(version);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    @Override
    public MetaData readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
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
            Custom customIndexMetaData = lookupPrototypeSafe(type).readFrom(in);
            builder.putCustom(type, customIndexMetaData);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        out.writeVInt(indices.size());
        for (IndexMetaData indexMetaData : this) {
            indexMetaData.writeTo(out);
        }
        out.writeVInt(templates.size());
        for (ObjectCursor<IndexTemplateMetaData> cursor : templates.values()) {
            cursor.value.writeTo(out);
        }
        out.writeVInt(customs.size());
        for (ObjectObjectCursor<String, Custom> cursor : customs) {
            out.writeString(cursor.key);
            cursor.value.writeTo(out);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MetaData metaData) {
        return new Builder(metaData);
    }

    /** All known byte-sized cluster settings. */
    public static final Set<String> CLUSTER_BYTES_SIZE_SETTINGS = unmodifiableSet(newHashSet(
        IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC,
        RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE,
        RecoverySettings.INDICES_RECOVERY_TRANSLOG_SIZE,
        RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC));


    /** All known time cluster settings. */
    public static final Set<String> CLUSTER_TIME_SETTINGS = unmodifiableSet(newHashSet(
                                    IndicesTTLService.INDICES_TTL_INTERVAL,
                                    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC,
                                    RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK,
                                    RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT,
                                    RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT,
                                    RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT,
                                    DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL,
                                    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL,
                                    InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT,
                                    DiscoverySettings.PUBLISH_TIMEOUT,
                                    InternalClusterService.SETTING_CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD));

    /** As of 2.0 we require units for time and byte-sized settings.  This methods adds default units to any cluster settings that don't
     *  specify a unit. */
    public static MetaData addDefaultUnitsIfNeeded(ESLogger logger, MetaData metaData) {
        Settings.Builder newPersistentSettings = null;
        for(Map.Entry<String,String> ent : metaData.persistentSettings().getAsMap().entrySet()) {
            String settingName = ent.getKey();
            String settingValue = ent.getValue();
            if (CLUSTER_BYTES_SIZE_SETTINGS.contains(settingName)) {
                try {
                    Long.parseLong(settingValue);
                } catch (NumberFormatException nfe) {
                    continue;
                }
                // It's a naked number that previously would be interpreted as default unit (bytes); now we add it:
                logger.warn("byte-sized cluster setting [{}] with value [{}] is missing units; assuming default units (b) but in future versions this will be a hard error", settingName, settingValue);
                if (newPersistentSettings == null) {
                    newPersistentSettings = Settings.builder();
                    newPersistentSettings.put(metaData.persistentSettings());
                }
                newPersistentSettings.put(settingName, settingValue + "b");
            }
            if (CLUSTER_TIME_SETTINGS.contains(settingName)) {
                try {
                    Long.parseLong(settingValue);
                } catch (NumberFormatException nfe) {
                    continue;
                }
                // It's a naked number that previously would be interpreted as default unit (ms); now we add it:
                logger.warn("time cluster setting [{}] with value [{}] is missing units; assuming default units (ms) but in future versions this will be a hard error", settingName, settingValue);
                if (newPersistentSettings == null) {
                    newPersistentSettings = Settings.builder();
                    newPersistentSettings.put(metaData.persistentSettings());
                }
                newPersistentSettings.put(settingName, settingValue + "ms");
            }
        }

        if (newPersistentSettings != null) {
            return new MetaData(
                    metaData.clusterUUID(),
                    metaData.version(),
                    metaData.transientSettings(),
                    newPersistentSettings.build(),
                    metaData.getIndices(),
                    metaData.getTemplates(),
                    metaData.getCustoms(),
                    metaData.concreteAllIndices(),
                    metaData.concreteAllOpenIndices(),
                    metaData.concreteAllClosedIndices(),
                    metaData.getAliasAndIndexLookup());
        } else {
            // No changes:
            return metaData;
        }
    }

    public static class Builder {

        private String clusterUUID;
        private long version;

        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;

        private final ImmutableOpenMap.Builder<String, IndexMetaData> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetaData> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        public Builder() {
            clusterUUID = "_na_";
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
        }

        public Builder(MetaData metaData) {
            this.clusterUUID = metaData.clusterUUID;
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
            indices.put(indexMetaData.getIndex(), indexMetaData);
            return this;
        }

        public Builder put(IndexMetaData indexMetaData, boolean incrementVersion) {
            if (indices.get(indexMetaData.getIndex()) == indexMetaData) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetaData = IndexMetaData.builder(indexMetaData).version(indexMetaData.getVersion() + 1).build();
            }
            indices.put(indexMetaData.getIndex(), indexMetaData);
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
            customs.put(type, custom);
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(ImmutableOpenMap<String, Custom> customs) {
            this.customs.putAll(customs);
            return this;
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
                        .settings(settingsBuilder().put(indexMetaData.getSettings()).put(settings)));
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
                    throw new IndexNotFoundException(index);
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

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder generateClusterUuidIfNeeded() {
            if (clusterUUID.equals("_na_")) {
                clusterUUID = Strings.randomBase64UUID();
            }
            return this;
        }

        public MetaData build() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will only be rebuilded when needed. Now during serailizing we rebuild these datastructures
            //    while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            List<String> allIndicesLst = new ArrayList<>();
            for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                allIndicesLst.add(cursor.value.getIndex());
            }
            String[] allIndices = allIndicesLst.toArray(new String[allIndicesLst.size()]);

            List<String> allOpenIndicesLst = new ArrayList<>();
            List<String> allClosedIndicesLst = new ArrayList<>();
            for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                IndexMetaData indexMetaData = cursor.value;
                if (indexMetaData.getState() == IndexMetaData.State.OPEN) {
                    allOpenIndicesLst.add(indexMetaData.getIndex());
                } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                    allClosedIndicesLst.add(indexMetaData.getIndex());
                }
            }
            String[] allOpenIndices = allOpenIndicesLst.toArray(new String[allOpenIndicesLst.size()]);
            String[] allClosedIndices = allClosedIndicesLst.toArray(new String[allClosedIndicesLst.size()]);

            // build all indices map
            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = new TreeMap<>();
            for (ObjectCursor<IndexMetaData> cursor : indices.values()) {
                IndexMetaData indexMetaData = cursor.value;
                aliasAndIndexLookup.put(indexMetaData.getIndex(), new AliasOrIndex.Index(indexMetaData));

                for (ObjectObjectCursor<String, AliasMetaData> aliasCursor : indexMetaData.getAliases()) {
                    AliasMetaData aliasMetaData = aliasCursor.value;
                    AliasOrIndex aliasOrIndex = aliasAndIndexLookup.get(aliasMetaData.getAlias());
                    if (aliasOrIndex == null) {
                        aliasOrIndex = new AliasOrIndex.Alias(aliasMetaData, indexMetaData);
                        aliasAndIndexLookup.put(aliasMetaData.getAlias(), aliasOrIndex);
                    } else if (aliasOrIndex instanceof AliasOrIndex.Alias) {
                        AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
                        alias.addIndex(indexMetaData);
                    } else if (aliasOrIndex instanceof AliasOrIndex.Index) {
                        AliasOrIndex.Index index = (AliasOrIndex.Index) aliasOrIndex;
                        throw new IllegalStateException("index and alias names need to be unique, but alias [" + aliasMetaData.getAlias() + "] and index [" + index.getIndex().getIndex() + "] have the same name");
                    } else {
                        throw new IllegalStateException("unexpected alias [" + aliasMetaData.getAlias() + "][" + aliasOrIndex + "]");
                    }
                }
            }
            aliasAndIndexLookup = Collections.unmodifiableSortedMap(aliasAndIndexLookup);
            return new MetaData(clusterUUID, version, transientSettings, persistentSettings, indices.build(), templates.build(), customs.build(), allIndices, allOpenIndices, allClosedIndices, aliasAndIndexLookup);
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
            builder.field("cluster_uuid", metaData.clusterUUID);

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
                Custom proto = lookupPrototypeSafe(cursor.key);
                if (proto.context().contains(context)) {
                    builder.startObject(cursor.key);
                    cursor.value.toXContent(builder, params);
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
                        builder.persistentSettings(Settings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build());
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
                        Custom proto = lookupPrototype(currentFieldName);
                        if (proto == null) {
                            //TODO warn
                            parser.skipChildren();
                        } else {
                            Custom custom = proto.fromXContent(parser);
                            builder.putCustom(custom.type(), custom);
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                        builder.clusterUUID = parser.text();
                    }
                }
            }
            return builder.build();
        }

        public static MetaData readFrom(StreamInput in) throws IOException {
            return PROTO.readFrom(in);
        }
    }
}
