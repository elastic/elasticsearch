/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeRegistry;
import org.elasticsearch.xpack.ql.type.DateEsField;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.TextEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.common.regex.Regex.simpleMatch;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.util.StringUtils.qualifyAndJoinIndices;
import static org.elasticsearch.xpack.ql.util.StringUtils.splitQualifiedIndex;

public class IndexResolver {

    public enum IndexType {
        STANDARD_INDEX(SQL_TABLE, "INDEX"),
        ALIAS(SQL_VIEW, "ALIAS"),
        FROZEN_INDEX(SQL_TABLE, "FROZEN INDEX"),
        // value for user types unrecognized
        UNKNOWN("UNKNOWN", "UNKNOWN");

        public static final EnumSet<IndexType> VALID_INCLUDE_FROZEN = EnumSet.of(STANDARD_INDEX, ALIAS, FROZEN_INDEX);
        public static final EnumSet<IndexType> VALID_REGULAR = EnumSet.of(STANDARD_INDEX, ALIAS);

        private final String toSql;
        private final String toNative;

        IndexType(String sql, String toNative) {
            this.toSql = sql;
            this.toNative = toNative;
        }

        public String toSql() {
            return toSql;
        }

        public String toNative() {
            return toNative;
        }
    }

    public static class IndexInfo {
        private final String cluster;
        private final String name;
        private final IndexType type;

        public IndexInfo(String cluster, String name, IndexType type) {
            this.cluster = cluster;
            this.name = name;
            this.type = type;
        }

        public String cluster() {
            return cluster;
        }

        public String name() {
            return name;
        }

        public IndexType type() {
            return type;
        }

        @Override
        public String toString() {
            return buildRemoteIndexName(cluster, name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, name, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            IndexResolver.IndexInfo other = (IndexResolver.IndexInfo) obj;
            return Objects.equals(cluster, other.cluster) && Objects.equals(name, other.name) && Objects.equals(type, other.type);
        }
    }

    public static final String SQL_TABLE = "TABLE";
    public static final String SQL_VIEW = "VIEW";

    private static final IndicesOptions INDICES_ONLY_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_ALIASES, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    private static final IndicesOptions FROZEN_INDICES_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_ALIASES),
        EnumSet.of(WildcardStates.OPEN)
    );

    public static final IndicesOptions FIELD_CAPS_INDICES_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions FIELD_CAPS_FROZEN_INDICES_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN)
    );

    private static final String UNMAPPED = "unmapped";

    private final Client client;
    private final String clusterName;
    private final DataTypeRegistry typeRegistry;

    private final Supplier<Set<String>> remoteClusters;

    public IndexResolver(Client client, String clusterName, DataTypeRegistry typeRegistry, Supplier<Set<String>> remoteClusters) {
        this.client = client;
        this.clusterName = clusterName;
        this.typeRegistry = typeRegistry;
        this.remoteClusters = remoteClusters;
    }

    public String clusterName() {
        return clusterName;
    }

    public Set<String> remoteClusters() {
        return remoteClusters.get();
    }

    /**
     * Resolves only the names, differentiating between indices and aliases.
     * This method is required since the other methods rely on mapping which is tied to an index (not an alias).
     */
    public void resolveNames(
        String clusterWildcard,
        String indexWildcard,
        String javaRegex,
        EnumSet<IndexType> types,
        ActionListener<Set<IndexInfo>> listener
    ) {

        // first get aliases (if specified)
        boolean retrieveAliases = CollectionUtils.isEmpty(types) || types.contains(IndexType.ALIAS);
        boolean retrieveIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.STANDARD_INDEX);
        boolean retrieveFrozenIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.FROZEN_INDEX);

        String[] indexWildcards = Strings.commaDelimitedListToStringArray(indexWildcard);
        Set<IndexInfo> indexInfos = new HashSet<>();
        if (retrieveAliases && clusterIsLocal(clusterWildcard)) {
            ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(indexWildcards, IndicesOptions.lenientExpandOpen());
            client.admin().indices().resolveIndex(resolveRequest, wrap(response -> {
                for (ResolveIndexAction.ResolvedAlias alias : response.getAliases()) {
                    indexInfos.add(new IndexInfo(clusterName, alias.getName(), IndexType.ALIAS));
                }
                for (ResolveIndexAction.ResolvedDataStream dataStream : response.getDataStreams()) {
                    indexInfos.add(new IndexInfo(clusterName, dataStream.getName(), IndexType.ALIAS));
                }
                resolveIndices(clusterWildcard, indexWildcards, javaRegex, retrieveIndices, retrieveFrozenIndices, indexInfos, listener);
            }, ex -> {
                // with security, two exception can be thrown:
                // INFE - if no alias matches
                // security exception is the user cannot access aliases

                // in both cases, that is allowed and we continue with the indices request
                if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                    resolveIndices(
                        clusterWildcard,
                        indexWildcards,
                        javaRegex,
                        retrieveIndices,
                        retrieveFrozenIndices,
                        indexInfos,
                        listener
                    );
                } else {
                    listener.onFailure(ex);
                }
            }));
        } else {
            resolveIndices(clusterWildcard, indexWildcards, javaRegex, retrieveIndices, retrieveFrozenIndices, indexInfos, listener);
        }
    }

    private void resolveIndices(
        String clusterWildcard,
        String[] indexWildcards,
        String javaRegex,
        boolean retrieveIndices,
        boolean retrieveFrozenIndices,
        Set<IndexInfo> indexInfos,
        ActionListener<Set<IndexInfo>> listener
    ) {
        if (retrieveIndices || retrieveFrozenIndices) {
            if (clusterIsLocal(clusterWildcard)) { // resolve local indices
                GetIndexRequest indexRequest = new GetIndexRequest().local(true)
                    .indices(indexWildcards)
                    .features(Feature.SETTINGS)
                    .includeDefaults(false)
                    .indicesOptions(INDICES_ONLY_OPTIONS);

                // if frozen indices are requested, make sure to update the request accordingly
                if (retrieveFrozenIndices) {
                    indexRequest.indicesOptions(FROZEN_INDICES_OPTIONS);
                }

                client.admin().indices().getIndex(indexRequest, wrap(indices -> {
                    if (indices != null) {
                        for (String indexName : indices.getIndices()) {
                            boolean isFrozen = retrieveFrozenIndices
                                && indices.getSettings().get(indexName).getAsBoolean("index.frozen", false);
                            indexInfos.add(
                                new IndexInfo(clusterName, indexName, isFrozen ? IndexType.FROZEN_INDEX : IndexType.STANDARD_INDEX)
                            );
                        }
                    }
                    resolveRemoteIndices(clusterWildcard, indexWildcards, javaRegex, retrieveFrozenIndices, indexInfos, listener);
                }, listener::onFailure));
            } else {
                resolveRemoteIndices(clusterWildcard, indexWildcards, javaRegex, retrieveFrozenIndices, indexInfos, listener);
            }
        } else {
            filterResults(javaRegex, indexInfos, listener);
        }
    }

    private void resolveRemoteIndices(
        String clusterWildcard,
        String[] indexWildcards,
        String javaRegex,
        boolean retrieveFrozenIndices,
        Set<IndexInfo> indexInfos,
        ActionListener<Set<IndexInfo>> listener
    ) {
        if (hasText(clusterWildcard)) {
            IndicesOptions indicesOptions = retrieveFrozenIndices ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS;
            FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(
                qualifyAndJoinIndices(clusterWildcard, indexWildcards),
                indicesOptions,
                emptyMap()
            );
            client.fieldCaps(fieldRequest, wrap(response -> {
                String[] indices = response.getIndices();
                if (indices != null) {
                    for (String indexName : indices) {
                        // TODO: perform two requests w/ & w/o frozen option to retrieve (by diff) the throttling status?
                        Tuple<String, String> splitRef = splitQualifiedIndex(indexName);
                        // Field caps on "remote:foo" should always return either empty or remote indices. But in case cluster's
                        // detail is missing, it's going to be a local index. TODO: why would this happen?
                        String cluster = splitRef.v1() == null ? clusterName : splitRef.v1();
                        indexInfos.add(new IndexInfo(cluster, splitRef.v2(), IndexType.STANDARD_INDEX));
                    }
                }
                filterResults(javaRegex, indexInfos, listener);
            }, ex -> {
                // see comment in resolveNames()
                if (ex instanceof NoSuchRemoteClusterException || ex instanceof ElasticsearchSecurityException) {
                    filterResults(javaRegex, indexInfos, listener);
                } else {
                    listener.onFailure(ex);
                }
            }));
        } else {
            filterResults(javaRegex, indexInfos, listener);
        }
    }

    private void filterResults(String javaRegex, Set<IndexInfo> indexInfos, ActionListener<Set<IndexInfo>> listener) {

        // since the index name does not support ?, filter the results manually
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        Set<IndexInfo> result = new TreeSet<>(Comparator.comparing(IndexInfo::cluster).thenComparing(IndexInfo::name));
        for (IndexInfo indexInfo : indexInfos) {
            if (pattern == null || pattern.matcher(indexInfo.name()).matches()) {
                result.add(indexInfo);
            }
        }
        listener.onResponse(result);
    }

    private boolean clusterIsLocal(String clusterWildcard) {
        return clusterWildcard == null || simpleMatch(clusterWildcard, clusterName);
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(
        String indexWildcard,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings,
        ActionListener<IndexResolution> listener
    ) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, indicesOptions, runtimeMappings);
        client.fieldCaps(
            fieldRequest,
            ActionListener.wrap(response -> listener.onResponse(mergedMappings(typeRegistry, indexWildcard, response)), listener::onFailure)
        );
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(
        String indexWildcard,
        boolean includeFrozen,
        Map<String, Object> runtimeMappings,
        ActionListener<IndexResolution> listener
    ) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen, runtimeMappings);
        client.fieldCaps(
            fieldRequest,
            ActionListener.wrap(response -> listener.onResponse(mergedMappings(typeRegistry, indexWildcard, response)), listener::onFailure)
        );
    }

    public static IndexResolution mergedMappings(
        DataTypeRegistry typeRegistry,
        String indexPattern,
        FieldCapabilitiesResponse fieldCapsResponse
    ) {

        if (fieldCapsResponse.getIndices().length == 0) {
            return IndexResolution.notFound(indexPattern);
        }

        // merge all indices onto the same one
        List<EsIndex> indices = buildIndices(typeRegistry, null, fieldCapsResponse, null, i -> indexPattern, (n, types) -> {
            StringBuilder errorMessage = new StringBuilder();

            boolean hasUnmapped = types.containsKey(UNMAPPED);

            if (types.size() > (hasUnmapped ? 2 : 1)) {
                // build the error message
                // and create a MultiTypeField

                for (Entry<String, FieldCapabilities> type : types.entrySet()) {
                    // skip unmapped
                    if (UNMAPPED.equals(type.getKey())) {
                        continue;
                    }

                    if (errorMessage.length() > 0) {
                        errorMessage.append(", ");
                    }
                    errorMessage.append("[");
                    errorMessage.append(type.getKey());
                    errorMessage.append("] in ");
                    errorMessage.append(Arrays.toString(type.getValue().indices()));
                }

                errorMessage.insert(0, "mapped as [" + (types.size() - (hasUnmapped ? 1 : 0)) + "] incompatible types: ");

                return new InvalidMappedField(n, errorMessage.toString());
            }
            // type is okay, check aggregation
            else {
                FieldCapabilities fieldCap = types.values().iterator().next();

                // validate search/agg-able
                if (fieldCap.isAggregatable() && fieldCap.nonAggregatableIndices() != null) {
                    errorMessage.append("mapped as aggregatable except in ");
                    errorMessage.append(Arrays.toString(fieldCap.nonAggregatableIndices()));
                }
                if (fieldCap.isSearchable() && fieldCap.nonSearchableIndices() != null) {
                    if (errorMessage.length() > 0) {
                        errorMessage.append(",");
                    }
                    errorMessage.append("mapped as searchable except in ");
                    errorMessage.append(Arrays.toString(fieldCap.nonSearchableIndices()));
                }

                if (errorMessage.length() > 0) {
                    return new InvalidMappedField(n, errorMessage.toString());
                }
            }

            // everything checks
            return null;
        });

        if (indices.size() > 1) {
            throw new QlIllegalArgumentException(
                "Incorrect merging of mappings (likely due to a bug) - expect at most one but found [{}]",
                indices.size()
            );
        }

        String[] indexNames = fieldCapsResponse.getIndices();
        if (indices.isEmpty()) {
            return IndexResolution.valid(new EsIndex(indexNames[0], emptyMap(), Set.of()));
        } else {
            EsIndex idx = indices.get(0);
            return IndexResolution.valid(new EsIndex(idx.name(), idx.mapping(), Set.of(indexNames)));
        }
    }

    private static EsField createField(
        DataTypeRegistry typeRegistry,
        String fieldName,
        Map<String, Map<String, FieldCapabilities>> globalCaps,
        Map<String, EsField> hierarchicalMapping,
        Map<String, EsField> flattedMapping,
        Function<String, EsField> field
    ) {

        Map<String, EsField> parentProps = hierarchicalMapping;

        int dot = fieldName.lastIndexOf('.');
        String fullFieldName = fieldName;
        EsField parent = null;

        if (dot >= 0) {
            String parentName = fieldName.substring(0, dot);
            fieldName = fieldName.substring(dot + 1);
            parent = flattedMapping.get(parentName);
            if (parent == null) {
                Map<String, FieldCapabilities> map = globalCaps.get(parentName);
                Function<String, EsField> fieldFunction;

                // lack of parent implies the field is an alias
                if (map == null) {
                    // as such, create the field manually, marking the field to also be an alias
                    fieldFunction = s -> createField(typeRegistry, s, OBJECT.esType(), new TreeMap<>(), false, true);
                } else {
                    Iterator<FieldCapabilities> iterator = map.values().iterator();
                    FieldCapabilities parentCap = iterator.next();
                    if (iterator.hasNext() && UNMAPPED.equals(parentCap.getType())) {
                        parentCap = iterator.next();
                    }
                    final FieldCapabilities parentC = parentCap;
                    fieldFunction = s -> createField(typeRegistry, s, parentC.getType(), new TreeMap<>(), parentC.isAggregatable(), false);
                }

                parent = createField(typeRegistry, parentName, globalCaps, hierarchicalMapping, flattedMapping, fieldFunction);
            }
            parentProps = parent.getProperties();
        }

        EsField esField = field.apply(fieldName);

        if (parent != null && parent instanceof UnsupportedEsField unsupportedParent) {
            String inherited = unsupportedParent.getInherited();
            String type = unsupportedParent.getOriginalType();

            if (inherited == null) {
                // mark the sub-field as unsupported, just like its parent, setting the first unsupported parent as the current one
                esField = new UnsupportedEsField(esField.getName(), type, unsupportedParent.getName(), esField.getProperties());
            } else {
                // mark the sub-field as unsupported, just like its parent, but setting the first unsupported parent
                // as the parent's first unsupported grandparent
                esField = new UnsupportedEsField(esField.getName(), type, inherited, esField.getProperties());
            }
        }

        parentProps.put(fieldName, esField);
        flattedMapping.put(fullFieldName, esField);

        return esField;
    }

    private static EsField createField(
        DataTypeRegistry typeRegistry,
        String fieldName,
        String typeName,
        Map<String, EsField> props,
        boolean isAggregateable,
        boolean isAlias
    ) {
        DataType esType = typeRegistry.fromEs(typeName);

        if (esType == TEXT) {
            return new TextEsField(fieldName, props, false, isAlias);
        }
        if (esType == KEYWORD) {
            int length = Short.MAX_VALUE;
            // TODO: to check whether isSearchable/isAggregateable takes into account the presence of the normalizer
            boolean normalized = false;
            return new KeywordEsField(fieldName, props, isAggregateable, length, normalized, isAlias);
        }
        if (esType == DATETIME) {
            return DateEsField.dateEsField(fieldName, props, isAggregateable);
        }
        if (esType == UNSUPPORTED) {
            return new UnsupportedEsField(fieldName, typeName, null, props);
        }

        return new EsField(fieldName, esType, props, isAggregateable, isAlias);
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(
        String index,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings
    ) {
        return new FieldCapabilitiesRequest().indices(Strings.commaDelimitedListToStringArray(index))
            .fields("*")
            .includeUnmapped(true)
            .runtimeFields(runtimeMappings)
            // lenient because we throw our own errors looking at the response e.g. if something was not resolved
            // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
            .indicesOptions(indicesOptions);
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(
        String index,
        boolean includeFrozen,
        Map<String, Object> runtimeMappings
    ) {
        IndicesOptions indicesOptions = includeFrozen ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS;
        return createFieldCapsRequest(index, indicesOptions, runtimeMappings);
    }

    /**
     * Resolves a pattern to multiple, separate indices. Doesn't perform validation.
     */
    public void resolveAsSeparateMappings(
        String indexWildcard,
        String javaRegex,
        boolean includeFrozen,
        Map<String, Object> runtimeMappings,
        ActionListener<List<EsIndex>> listener
    ) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen, runtimeMappings);
        client.fieldCaps(fieldRequest, wrap(response -> {
            client.admin().indices().getAliases(createGetAliasesRequest(response, includeFrozen), wrap(aliases -> {
                listener.onResponse(separateMappings(typeRegistry, javaRegex, response, aliases.getAliases()));
            }, ex -> {
                if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                    listener.onResponse(separateMappings(typeRegistry, javaRegex, response, null));
                } else {
                    listener.onFailure(ex);
                }
            }));
        }, listener::onFailure));

    }

    private GetAliasesRequest createGetAliasesRequest(FieldCapabilitiesResponse response, boolean includeFrozen) {
        return new GetAliasesRequest().local(true)
            .aliases("*")
            .indices(response.getIndices())
            .indicesOptions(includeFrozen ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS);
    }

    public static List<EsIndex> separateMappings(
        DataTypeRegistry typeRegistry,
        String javaRegex,
        FieldCapabilitiesResponse fieldCaps,
        Map<String, List<AliasMetadata>> aliases
    ) {
        return buildIndices(typeRegistry, javaRegex, fieldCaps, aliases, Function.identity(), (s, cap) -> null);
    }

    private static class Fields {
        final Map<String, EsField> hierarchicalMapping = new TreeMap<>();
        final Map<String, EsField> flattedMapping = new LinkedHashMap<>();
    }

    /**
     * Assemble an index-based mapping from the field caps (which is field based) by looking at the indices associated with
     * each field.
     */
    private static List<EsIndex> buildIndices(
        DataTypeRegistry typeRegistry,
        String javaRegex,
        FieldCapabilitiesResponse fieldCapsResponse,
        Map<String, List<AliasMetadata>> aliases,
        Function<String, String> indexNameProcessor,
        BiFunction<String, Map<String, FieldCapabilities>, InvalidMappedField> validityVerifier
    ) {

        if ((fieldCapsResponse.getIndices() == null || fieldCapsResponse.getIndices().length == 0)
            && (aliases == null || aliases.isEmpty())) {
            return emptyList();
        }

        Set<String> resolvedAliases = new HashSet<>();
        if (aliases != null) {
            for (var aliasList : aliases.values()) {
                for (AliasMetadata alias : aliasList) {
                    resolvedAliases.add(alias.getAlias());
                }
            }
        }

        List<String> resolvedIndices = new ArrayList<>(asList(fieldCapsResponse.getIndices()));
        int mapSize = CollectionUtils.mapSize(resolvedIndices.size() + resolvedAliases.size());
        Map<String, Fields> indices = Maps.newLinkedHashMapWithExpectedSize(mapSize);
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        // sort fields in reverse order to build the field hierarchy
        Set<Entry<String, Map<String, FieldCapabilities>>> sortedFields = new TreeSet<>(
            Collections.reverseOrder(Comparator.comparing(Entry::getKey))
        );
        final Map<String, Map<String, FieldCapabilities>> fieldCaps = fieldCapsResponse.get();
        sortedFields.addAll(fieldCaps.entrySet());

        for (Entry<String, Map<String, FieldCapabilities>> entry : sortedFields) {
            String fieldName = entry.getKey();
            if (fieldCapsResponse.isMetadataField(fieldName)) {
                // skip metadata field!
                continue;
            }
            Map<String, FieldCapabilities> types = new LinkedHashMap<>(entry.getValue());
            final InvalidMappedField invalidField = validityVerifier.apply(fieldName, types);
            // apply verification for fields belonging to index aliases
            Map<String, InvalidMappedField> invalidFieldsForAliases = getInvalidFieldsForAliases(fieldName, types, aliases);

            // filter unmapped
            FieldCapabilities unmapped = types.get(UNMAPPED);
            Set<String> unmappedIndices = unmapped != null ? new HashSet<>(asList(unmapped.indices())) : emptySet();

            // check each type
            for (Entry<String, FieldCapabilities> typeEntry : types.entrySet()) {
                FieldCapabilities typeCap = typeEntry.getValue();
                String[] capIndices = typeCap.indices();

                // compute the actual indices - if any are specified, take into account the unmapped indices
                List<String> concreteIndices = null;
                if (capIndices != null) {
                    if (unmappedIndices.isEmpty()) {
                        concreteIndices = new ArrayList<>(asList(capIndices));
                    } else {
                        concreteIndices = new ArrayList<>(capIndices.length);
                        for (String capIndex : capIndices) {
                            // add only indices that have a mapping
                            if (unmappedIndices.contains(capIndex) == false) {
                                concreteIndices.add(capIndex);
                            }
                        }
                    }
                } else {
                    concreteIndices = resolvedIndices;
                }

                // add to the list of concrete indices the aliases associated with these indices
                Set<String> uniqueAliases = new LinkedHashSet<>();
                if (aliases != null) {
                    for (String concreteIndex : concreteIndices) {
                        if (aliases.containsKey(concreteIndex)) {
                            List<AliasMetadata> concreteIndexAliases = aliases.get(concreteIndex);
                            concreteIndexAliases.stream().forEach(e -> uniqueAliases.add(e.alias()));
                        }
                    }
                    concreteIndices.addAll(uniqueAliases);
                }

                // put the field in their respective mappings
                for (String index : concreteIndices) {
                    boolean isIndexAlias = uniqueAliases.contains(index);
                    // TODO is split still needed?
                    if (pattern == null || pattern.matcher(splitQualifiedIndex(index).v2()).matches() || isIndexAlias) {
                        String indexName = isIndexAlias ? index : indexNameProcessor.apply(index);
                        Fields indexFields = indices.get(indexName);
                        if (indexFields == null) {
                            indexFields = new Fields();
                            indices.put(indexName, indexFields);
                        }
                        EsField field = indexFields.flattedMapping.get(fieldName);
                        boolean createField = false;
                        if (isIndexAlias == false) {
                            if (field == null || (invalidField != null && (field instanceof InvalidMappedField) == false)) {
                                createField = true;
                            }
                        } else {
                            if (field == null && invalidFieldsForAliases.get(index) == null) {
                                createField = true;
                            }
                        }

                        if (createField) {
                            int dot = fieldName.lastIndexOf('.');
                            /*
                             * Looking up the "tree" at the parent fields here to see if the field is an alias.
                             * When the upper elements of the "tree" have no elements in fieldcaps, then this is an alias field. But not
                             * always: if there are two aliases - a.b.c.alias1 and a.b.c.alias2 - only one of them will be considered alias.
                             */
                            Holder<Boolean> isAliasFieldType = new Holder<>(false);
                            if (dot >= 0) {
                                String parentName = fieldName.substring(0, dot);
                                if (indexFields.flattedMapping.get(parentName) == null) {
                                    // lack of parent implies the field is an alias
                                    if (fieldCaps.get(parentName) == null) {
                                        isAliasFieldType.set(true);
                                    }
                                }
                            }

                            createField(
                                typeRegistry,
                                fieldName,
                                fieldCaps,
                                indexFields.hierarchicalMapping,
                                indexFields.flattedMapping,
                                s -> invalidField != null
                                    ? invalidField
                                    : createField(
                                        typeRegistry,
                                        s,
                                        typeCap.getType(),
                                        emptyMap(),
                                        typeCap.isAggregatable(),
                                        isAliasFieldType.get()
                                    )
                            );
                        }
                    }
                }
            }
        }

        // return indices in ascending order
        List<EsIndex> foundIndices = new ArrayList<>(indices.size());
        for (Entry<String, Fields> entry : indices.entrySet()) {
            foundIndices.add(new EsIndex(entry.getKey(), entry.getValue().hierarchicalMapping, Set.of(entry.getKey())));
        }
        foundIndices.sort(Comparator.comparing(EsIndex::name));
        return foundIndices;
    }

    /*
     * Checks if the field is valid (same type and same capabilities - searchable/aggregatable) across indices belonging to a list
     * of aliases.
     * A field can look like the example below (generated by field_caps API).
     *   "name": {
     *       "text": {
     *           "type": "text",
     *           "searchable": false,
     *           "aggregatable": false,
     *           "indices": [
     *               "bar",
     *               "foo"
     *           ],
     *           "non_searchable_indices": [
     *               "foo"
     *           ]
     *       },
     *       "keyword": {
     *           "type": "keyword",
     *           "searchable": false,
     *           "aggregatable": true,
     *           "non_aggregatable_indices": [
     *               "bar", "baz"
     *           ]
     *       }
     *   }
     */
    private static Map<String, InvalidMappedField> getInvalidFieldsForAliases(
        String fieldName,
        Map<String, FieldCapabilities> types,
        Map<String, List<AliasMetadata>> aliases
    ) {
        if (aliases == null || aliases.isEmpty()) {
            return emptyMap();
        }
        Map<String, InvalidMappedField> invalidFields = new HashMap<>();
        Map<String, Set<String>> typesErrors = new HashMap<>(); // map holding aliases and a list of unique field types across its indices
        Map<String, Set<String>> aliasToIndices = new HashMap<>(); // map with aliases and their list of indices

        for (var entry : aliases.entrySet()) {
            for (AliasMetadata aliasMetadata : entry.getValue()) {
                String aliasName = aliasMetadata.alias();
                aliasToIndices.putIfAbsent(aliasName, new HashSet<>());
                aliasToIndices.get(aliasName).add(entry.getKey());
            }
        }

        // iterate over each type
        for (Entry<String, FieldCapabilities> type : types.entrySet()) {
            String esFieldType = type.getKey();
            if (Objects.equals(esFieldType, UNMAPPED)) {
                continue;
            }
            String[] indices = type.getValue().indices();
            // if there is a list of indices where this field type is defined
            if (indices != null) {
                // Look at all these indices' aliases and add the type of the field to a list (Set) with unique elements.
                // A valid mapping for a field in an index alias should contain only one type. If it doesn't, this means that field
                // is mapped as different types across the indices in this index alias.
                for (String index : indices) {
                    List<AliasMetadata> indexAliases = aliases.get(index);
                    if (indexAliases == null) {
                        continue;
                    }
                    for (AliasMetadata aliasMetadata : indexAliases) {
                        String aliasName = aliasMetadata.alias();
                        if (typesErrors.containsKey(aliasName)) {
                            typesErrors.get(aliasName).add(esFieldType);
                        } else {
                            Set<String> fieldTypes = new HashSet<>();
                            fieldTypes.add(esFieldType);
                            typesErrors.put(aliasName, fieldTypes);
                        }
                    }
                }
            }
        }

        for (String aliasName : aliasToIndices.keySet()) {
            // if, for the same index alias, there are multiple field types for this fieldName ie the index alias has indices where the same
            // field name is of different types
            Set<String> esFieldTypes = typesErrors.get(aliasName);
            if (esFieldTypes != null && esFieldTypes.size() > 1) {
                // consider the field as invalid, for the currently checked index alias
                // the error message doesn't actually matter
                invalidFields.put(aliasName, new InvalidMappedField(fieldName));
            } else {
                // if the field type is the same across all this alias' indices, check the field's capabilities (searchable/aggregatable)
                for (Entry<String, FieldCapabilities> type : types.entrySet()) {
                    if (Objects.equals(type.getKey(), UNMAPPED)) {
                        continue;
                    }
                    FieldCapabilities f = type.getValue();

                    // the existence of a list of non_aggregatable_indices is an indication that not all indices have the same capabilities
                    // but this list can contain indices belonging to other aliases, so we need to check only for this alias
                    if (f.nonAggregatableIndices() != null) {
                        Set<String> aliasIndices = aliasToIndices.get(aliasName);
                        int nonAggregatableCount = 0;
                        // either all or none of the non-aggregatable indices belonging to a certain alias should be in this list
                        for (String nonAggIndex : f.nonAggregatableIndices()) {
                            if (aliasIndices.contains(nonAggIndex)) {
                                nonAggregatableCount++;
                            }
                        }
                        if (nonAggregatableCount > 0 && nonAggregatableCount != aliasIndices.size()) {
                            invalidFields.put(aliasName, new InvalidMappedField(fieldName));
                            break;
                        }
                    }

                    // perform the same check for non_searchable_indices list
                    if (f.nonSearchableIndices() != null) {
                        Set<String> aliasIndices = aliasToIndices.get(aliasName);
                        int nonSearchableCount = 0;
                        // either all or none of the non-searchable indices belonging to a certain alias should be in this list
                        for (String nonSearchIndex : f.nonSearchableIndices()) {
                            if (aliasIndices.contains(nonSearchIndex)) {
                                nonSearchableCount++;
                            }
                        }
                        if (nonSearchableCount > 0 && nonSearchableCount != aliasIndices.size()) {
                            invalidFields.put(aliasName, new InvalidMappedField(fieldName));
                            break;
                        }
                    }
                }
            }
        }

        if (invalidFields.size() > 0) {
            return invalidFields;
        }
        // everything checks
        return emptyMap();
    }
}
