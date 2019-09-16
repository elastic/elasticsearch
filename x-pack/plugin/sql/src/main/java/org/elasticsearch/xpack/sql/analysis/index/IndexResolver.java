/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DateEsField;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.InvalidMappedField;
import org.elasticsearch.xpack.sql.type.KeywordEsField;
import org.elasticsearch.xpack.sql.type.TextEsField;
import org.elasticsearch.xpack.sql.type.UnsupportedEsField;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.elasticsearch.xpack.sql.util.Holder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;

public class IndexResolver {

    public enum IndexType {
        STANDARD_INDEX("BASE TABLE", "INDEX"),
        ALIAS("VIEW", "ALIAS"),
        FROZEN_INDEX("BASE TABLE", "FROZEN INDEX"),
        // value for user types unrecognized
        UNKNOWN("UNKNOWN", "UNKNOWN");

        public static final String SQL_BASE_TABLE = "BASE TABLE";
        public static final String SQL_TABLE = "TABLE";
        public static final String SQL_VIEW = "VIEW";

        public static final EnumSet<IndexType> VALID_INCLUDE_FROZEN = EnumSet.of(STANDARD_INDEX, ALIAS, FROZEN_INDEX);
        public static final EnumSet<IndexType> VALID_REGULAR = EnumSet.of(STANDARD_INDEX, ALIAS);
        public static final EnumSet<IndexType> INDICES_ONLY = EnumSet.of(STANDARD_INDEX, FROZEN_INDEX);

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
        private final String name;
        private final IndexType type;

        public IndexInfo(String name, IndexType type) {
            this.name = name;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public IndexType type() {
            return type;
        }
        
        @Override
        public String toString() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
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
            return Objects.equals(name, other.name)
                    && Objects.equals(type, other.type);
        }
    }

    private static final IndicesOptions INDICES_ONLY_OPTIONS = new IndicesOptions(
            EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_ALIASES, Option.IGNORE_THROTTLED),
            EnumSet.of(WildcardStates.OPEN));
    private static final IndicesOptions FROZEN_INDICES_OPTIONS = new IndicesOptions(
            EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_ALIASES), EnumSet.of(WildcardStates.OPEN));

    public static final IndicesOptions FIELD_CAPS_INDICES_OPTIONS = new IndicesOptions(
            EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_THROTTLED), EnumSet.of(WildcardStates.OPEN));
    public static final IndicesOptions FIELD_CAPS_FROZEN_INDICES_OPTIONS = new IndicesOptions(
            EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE), EnumSet.of(WildcardStates.OPEN));


    private static final List<String> FIELD_NAMES_BLACKLIST = Arrays.asList("_size");
    private static final String UNMAPPED = "unmapped";

    private final Client client;
    private final String clusterName;


    public IndexResolver(Client client, String clusterName) {
        this.client = client;
        this.clusterName = clusterName;
    }

    public String clusterName() {
        return clusterName;
    }

    /**
     * Resolves only the names, differentiating between indices and aliases.
     * This method is required since the other methods rely on mapping which is tied to an index (not an alias).
     */
    public void resolveNames(String indexWildcard, String javaRegex, EnumSet<IndexType> types, ActionListener<Set<IndexInfo>> listener) {

        // first get aliases (if specified)
        boolean retrieveAliases = CollectionUtils.isEmpty(types) || types.contains(IndexType.ALIAS);
        boolean retrieveIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.STANDARD_INDEX);
        boolean retrieveFrozenIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.FROZEN_INDEX);

        String[] indices = Strings.commaDelimitedListToStringArray(indexWildcard);
        if (retrieveAliases) {
            GetAliasesRequest aliasRequest = new GetAliasesRequest()
                    .local(true)
                    .aliases(indices)
                    .indicesOptions(IndicesOptions.lenientExpandOpen());
    
            client.admin().indices().getAliases(aliasRequest, wrap(aliases ->
                            resolveIndices(indices, javaRegex, aliases, retrieveIndices, retrieveFrozenIndices, listener),
                            ex -> {
                                // with security, two exception can be thrown:
                                // INFE - if no alias matches
                                // security exception is the user cannot access aliases
    
                                // in both cases, that is allowed and we continue with the indices request
                                if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                                    resolveIndices(indices, javaRegex, null, retrieveIndices, retrieveFrozenIndices, listener);
                                } else {
                                    listener.onFailure(ex);
                                }
                            }));
        } else {
            resolveIndices(indices, javaRegex, null, retrieveIndices, retrieveFrozenIndices, listener);
        }
    }

    private void resolveIndices(String[] indices, String javaRegex, GetAliasesResponse aliases,
            boolean retrieveIndices, boolean retrieveFrozenIndices, ActionListener<Set<IndexInfo>> listener) {

        if (retrieveIndices || retrieveFrozenIndices) {
            
            GetIndexRequest indexRequest = new GetIndexRequest()
                    .local(true)
                    .indices(indices)
                    .features(Feature.SETTINGS)
                    .includeDefaults(false)
                    .indicesOptions(INDICES_ONLY_OPTIONS);

            // if frozen indices are requested, make sure to update the request accordingly
            if (retrieveFrozenIndices) {
                indexRequest.indicesOptions(FROZEN_INDICES_OPTIONS);
            }
    
            client.admin().indices().getIndex(indexRequest,
                    wrap(response -> filterResults(javaRegex, aliases, response, retrieveIndices, retrieveFrozenIndices, listener),
                            listener::onFailure));
            
        } else {
            filterResults(javaRegex, aliases, null, false, false, listener);
        }
    }

    private void filterResults(String javaRegex, GetAliasesResponse aliases, GetIndexResponse indices,
            // these are needed to filter out the different results from the same index response
            boolean retrieveIndices,
            boolean retrieveFrozenIndices,
            ActionListener<Set<IndexInfo>> listener) {
        
        // since the index name does not support ?, filter the results manually
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        Set<IndexInfo> result = new TreeSet<>(Comparator.comparing(IndexInfo::name));
        // filter aliases (if present)
        if (aliases != null) {
            for (ObjectCursor<List<AliasMetaData>> cursor : aliases.getAliases().values()) {
                for (AliasMetaData amd : cursor.value) {
                    String alias = amd.alias();
                    if (alias != null && (pattern == null || pattern.matcher(alias).matches())) {
                        result.add(new IndexInfo(alias, IndexType.ALIAS));
                    }
                }
            }
        }
        
        // filter indices (if present)
        String[] indicesNames = indices != null ? indices.indices() : null;
        if (indicesNames != null) {
            for (String indexName : indicesNames) {
                boolean isFrozen = retrieveFrozenIndices
                        && IndexSettings.INDEX_SEARCH_THROTTLED.get(indices.getSettings().get(indexName)) == Boolean.TRUE;

                if (pattern == null || pattern.matcher(indexName).matches()) {
                    result.add(new IndexInfo(indexName, isFrozen ? IndexType.FROZEN_INDEX : IndexType.STANDARD_INDEX));
                }
            }
        }

        listener.onResponse(result);
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(String indexWildcard, String javaRegex, boolean includeFrozen,
            ActionListener<IndexResolution> listener) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen);
        client.fieldCaps(fieldRequest,
                ActionListener.wrap(
                        response -> listener.onResponse(mergedMappings(indexWildcard, response.getIndices(), response.get())),
                        listener::onFailure));
    }

    static IndexResolution mergedMappings(String indexPattern, String[] indexNames, Map<String, Map<String, FieldCapabilities>> fieldCaps) {
        if (fieldCaps == null || fieldCaps.isEmpty()) {
            return IndexResolution.notFound(indexPattern);
        }

        // merge all indices onto the same one
        List<EsIndex> indices = buildIndices(indexNames, null, fieldCaps, i -> indexPattern, (n, types) -> {
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

        if (indices.size() != 1) {
            throw new SqlIllegalArgumentException("Incorrect merging of mappings (likely due to a bug) - expect 1 but found [{}]",
                    indices.size());
        }

        return IndexResolution.valid(indices.get(0));
    }

    private static EsField createField(String fieldName, Map<String, Map<String, FieldCapabilities>> globalCaps,
            Map<String, EsField> hierarchicalMapping,
            Map<String, EsField> flattedMapping,
            Function<String, EsField> field) {

        Map<String, EsField> parentProps = hierarchicalMapping;

        int dot = fieldName.lastIndexOf('.');
        String fullFieldName = fieldName;

        if (dot >= 0) {
            String parentName = fieldName.substring(0, dot);
            fieldName = fieldName.substring(dot + 1);
            EsField parent = flattedMapping.get(parentName);
            if (parent == null) {
                Map<String, FieldCapabilities> map = globalCaps.get(parentName);
                Function<String, EsField> fieldFunction;

                // lack of parent implies the field is an alias
                if (map == null) {
                    // as such, create the field manually, marking the field to also be an alias
                    fieldFunction = s -> createField(s, DataType.OBJECT.name(), new TreeMap<>(), false, true);
                } else {
                    Iterator<FieldCapabilities> iterator = map.values().iterator();
                    FieldCapabilities parentCap = iterator.next();
                    if (iterator.hasNext() && UNMAPPED.equals(parentCap.getType())) {
                        parentCap = iterator.next();
                    }
                    final FieldCapabilities parentC = parentCap;
                    fieldFunction = s -> createField(s, parentC.getType(), new TreeMap<>(), parentC.isAggregatable(), false);
                }

                parent = createField(parentName, globalCaps, hierarchicalMapping, flattedMapping, fieldFunction);
            }
            parentProps = parent.getProperties();
        }

        EsField esField = field.apply(fieldName);
        
        parentProps.put(fieldName, esField);
        flattedMapping.put(fullFieldName, esField);

        return esField;
    }

    private static EsField createField(String fieldName, String typeName, Map<String, EsField> props,
            boolean isAggregateable, boolean isAlias) {
        DataType esType = DataType.fromTypeName(typeName);
        switch (esType) {
            case TEXT:
                return new TextEsField(fieldName, props, false, isAlias);
            case KEYWORD:
                int length = DataType.KEYWORD.defaultPrecision;
                // TODO: to check whether isSearchable/isAggregateable takes into account the presence of the normalizer
                boolean normalized = false;
                return new KeywordEsField(fieldName, props, isAggregateable, length, normalized, isAlias);
            case DATETIME:
                return new DateEsField(fieldName, props, isAggregateable);
            case UNSUPPORTED:
                return new UnsupportedEsField(fieldName, typeName);
            default:
                return new EsField(fieldName, esType, props, isAggregateable, isAlias);
        }
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(String index, boolean includeFrozen) {
        return new FieldCapabilitiesRequest()
                .indices(Strings.commaDelimitedListToStringArray(index))
                .fields("*")
                .includeUnmapped(true)
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
                .indicesOptions(includeFrozen ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS);
    }

    /**
     * Resolves a pattern to multiple, separate indices. Doesn't perform validation.
     */
    public void resolveAsSeparateMappings(String indexWildcard, String javaRegex, boolean includeFrozen,
            ActionListener<List<EsIndex>> listener) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen);
        client.fieldCaps(fieldRequest,
                ActionListener.wrap(
                        response -> listener.onResponse(separateMappings(indexWildcard, javaRegex, response.getIndices(), response.get())),
                        listener::onFailure));

    }
    
    static List<EsIndex> separateMappings(String indexPattern, String javaRegex, String[] indexNames,
            Map<String, Map<String, FieldCapabilities>> fieldCaps) {
        return buildIndices(indexNames, javaRegex, fieldCaps, Function.identity(), (s, cap) -> null);
    }
    
    private static class Fields {
        final Map<String, EsField> hierarchicalMapping = new TreeMap<>();
        final Map<String, EsField> flattedMapping = new LinkedHashMap<>();
    }

    /**
     * Assemble an index-based mapping from the field caps (which is field based) by looking at the indices associated with
     * each field.
     */
    private static List<EsIndex> buildIndices(String[] indexNames, String javaRegex, Map<String, Map<String, FieldCapabilities>> fieldCaps,
            Function<String, String> indexNameProcessor,
            BiFunction<String, Map<String, FieldCapabilities>, InvalidMappedField> validityVerifier) {

        if (indexNames == null || indexNames.length == 0) {
            return emptyList();
        }

        final List<String> resolvedIndices = asList(indexNames);
        Map<String, Fields> indices = new LinkedHashMap<>(resolvedIndices.size());
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        // sort fields in reverse order to build the field hierarchy
        Set<Entry<String, Map<String, FieldCapabilities>>> sortedFields = new TreeSet<>(
                Collections.reverseOrder(Comparator.comparing(Entry::getKey)));

        sortedFields.addAll(fieldCaps.entrySet());

        for (Entry<String, Map<String, FieldCapabilities>> entry : sortedFields) {
            String fieldName = entry.getKey();
            Map<String, FieldCapabilities> types = entry.getValue();

            // ignore size added by the mapper plugin
            if (FIELD_NAMES_BLACKLIST.contains(fieldName)) {
                continue;
            }

            // apply verification
            final InvalidMappedField invalidField = validityVerifier.apply(fieldName, types);

            // filter meta fields and unmapped
            FieldCapabilities unmapped = types.get(UNMAPPED);
            Set<String> unmappedIndices = unmapped != null ? new HashSet<>(asList(unmapped.indices())) : emptySet();

            // check each type
            for (Entry<String, FieldCapabilities> typeEntry : types.entrySet()) {
                FieldCapabilities typeCap = typeEntry.getValue();
                String[] capIndices = typeCap.indices();

                // Skip internal fields (name starting with underscore and its type reported by field_caps starts
                // with underscore as well). A meta field named "_version", for example, has the type named "_version".
                if (typeEntry.getKey().startsWith("_") && typeCap.getType().startsWith("_")) {
                    continue;
                }

                // compute the actual indices - if any are specified, take into account the unmapped indices
                List<String> concreteIndices = null;
                if (capIndices != null) {
                    if (unmappedIndices.isEmpty() == true) {
                        concreteIndices = asList(capIndices);
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

                // put the field in their respective mappings
                for (String index : concreteIndices) {
                    if (pattern == null || pattern.matcher(index).matches()) {
                        String indexName = indexNameProcessor.apply(index);
                        Fields indexFields = indices.get(indexName);
                        if (indexFields == null) {
                            indexFields = new Fields();
                            indices.put(indexName, indexFields);
                        }
                        EsField field = indexFields.flattedMapping.get(fieldName);
                        if (field == null || (invalidField != null && (field instanceof InvalidMappedField) == false)) {
                            int dot = fieldName.lastIndexOf('.');
                            /*
                             * Looking up the "tree" at the parent fields here to see if the field is an alias.
                             * When the upper elements of the "tree" have no elements in fieldcaps, then this is an alias field. But not
                             * always: if there are two aliases - a.b.c.alias1 and a.b.c.alias2 - only one of them will be considered alias.
                             */
                            Holder<Boolean> isAlias = new Holder<>(false);
                            if (dot >= 0) {
                                String parentName = fieldName.substring(0, dot);
                                if (indexFields.flattedMapping.get(parentName) == null) {
                                    // lack of parent implies the field is an alias
                                    if (fieldCaps.get(parentName) == null) {
                                        isAlias.set(true);
                                    }
                                }
                            }
                            
                            createField(fieldName, fieldCaps, indexFields.hierarchicalMapping, indexFields.flattedMapping,
                                    s -> invalidField != null ? invalidField : createField(s, typeCap.getType(), emptyMap(),
                                            typeCap.isAggregatable(), isAlias.get()));
                        }
                    }
                }
            }
        }

        // return indices in ascending order
        List<EsIndex> foundIndices = new ArrayList<>(indices.size());
        for (Entry<String, Fields> entry : indices.entrySet()) {
            foundIndices.add(new EsIndex(entry.getKey(), entry.getValue().hierarchicalMapping));
        }
        foundIndices.sort(Comparator.comparing(EsIndex::name));
        return foundIndices;
    }
}