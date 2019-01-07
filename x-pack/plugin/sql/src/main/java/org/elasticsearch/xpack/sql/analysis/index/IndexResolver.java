/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

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
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DateEsField;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.InvalidMappedField;
import org.elasticsearch.xpack.sql.type.KeywordEsField;
import org.elasticsearch.xpack.sql.type.TextEsField;
import org.elasticsearch.xpack.sql.type.Types;
import org.elasticsearch.xpack.sql.type.UnsupportedEsField;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;

public class IndexResolver {

    public enum IndexType {

        INDEX("BASE TABLE"),
        ALIAS("ALIAS"),
        // value for user types unrecognized
        UNKNOWN("UNKNOWN");

        public static final EnumSet<IndexType> VALID = EnumSet.of(INDEX, ALIAS);

        private final String toSql;

        IndexType(String sql) {
            this.toSql = sql;
        }

        public String toSql() {
            return toSql;
        }

        public static IndexType from(String name) {
            if (name != null) {
                name = name.toUpperCase(Locale.ROOT);
                for (IndexType type : IndexType.VALID) {
                    if (type.toSql.equals(name)) {
                        return type;
                    }
                }
            }
            return IndexType.UNKNOWN;
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
            EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_ALIASES), EnumSet.of(WildcardStates.OPEN));
    private static final List<String> FIELD_NAMES_BLACKLIST = Arrays.asList("_size");

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
        boolean retrieveIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.INDEX);

        String[] indices = Strings.commaDelimitedListToStringArray(indexWildcard);
        if (retrieveAliases) {
            GetAliasesRequest aliasRequest = new GetAliasesRequest()
                    .local(true)
                    .aliases(indices)
                    .indicesOptions(IndicesOptions.lenientExpandOpen());
    
            client.admin().indices().getAliases(aliasRequest, ActionListener.wrap(aliases ->
                            resolveIndices(indices, javaRegex, aliases, retrieveIndices, listener),
                            ex -> {
                                // with security, two exception can be thrown:
                                // INFE - if no alias matches
                                // security exception is the user cannot access aliases
    
                                // in both cases, that is allowed and we continue with the indices request
                                if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                                    resolveIndices(indices, javaRegex, null, retrieveIndices, listener);
                                } else {
                                    listener.onFailure(ex);
                                }
                            }));
        } else {
            resolveIndices(indices, javaRegex, null, retrieveIndices, listener);
        }
    }

    private void resolveIndices(String[] indices, String javaRegex, GetAliasesResponse aliases,
            boolean retrieveIndices, ActionListener<Set<IndexInfo>> listener) {

        if (retrieveIndices) {
            GetIndexRequest indexRequest = new GetIndexRequest()
                    .local(true)
                    .indices(indices)
                    .features(Feature.SETTINGS)
                    .includeDefaults(false)
                    .indicesOptions(INDICES_ONLY_OPTIONS);
    
            client.admin().indices().getIndex(indexRequest,
                    ActionListener.wrap(response -> filterResults(javaRegex, aliases, response, listener),
                            listener::onFailure));
        } else {
            filterResults(javaRegex, aliases, null, listener);
        }
    }

    private void filterResults(String javaRegex, GetAliasesResponse aliases, GetIndexResponse indices,
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
                if (pattern == null || pattern.matcher(indexName).matches()) {
                    result.add(new IndexInfo(indexName, IndexType.INDEX));
                }
            }
        }

        listener.onResponse(result);
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(String indexWildcard, String javaRegex, ActionListener<IndexResolution> listener) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard);
        client.fieldCaps(fieldRequest,
                ActionListener.wrap(response -> listener.onResponse(mergedMapping(indexWildcard, response.get())), listener::onFailure));
    }

    static IndexResolution mergedMapping(String indexPattern, Map<String, Map<String, FieldCapabilities>> fieldCaps) {
        if (fieldCaps == null || fieldCaps.isEmpty()) {
            return IndexResolution.notFound(indexPattern);
        }

        StringBuilder errorMessage = new StringBuilder();

        NavigableSet<Entry<String, Map<String, FieldCapabilities>>> sortedFields = new TreeSet<>(
                // for some reason .reversed doesn't work (prolly due to inference)
                Collections.reverseOrder(Comparator.comparing(Entry::getKey)));
        sortedFields.addAll(fieldCaps.entrySet());

        Map<String, EsField> hierarchicalMapping = new TreeMap<>();
        Map<String, EsField> flattedMapping = new LinkedHashMap<>();
        
        // sort keys descending in order to easily detect multi-fields (a.b.c multi-field of a.b)
        // without sorting, they can still be detected however without the emptyMap optimization
        // (fields without multi-fields have no children)
        for (Entry<String, Map<String, FieldCapabilities>> entry : sortedFields) {

            InvalidMappedField invalidField = null;
            FieldCapabilities fieldCap = null;
            errorMessage.setLength(0);

            String name = entry.getKey();

            // Skip any of the blacklisted field names.
            if (!FIELD_NAMES_BLACKLIST.contains(name)) {
                Map<String, FieldCapabilities> types = entry.getValue();
                // field is mapped differently across indices
                if (types.size() > 1) {
                    // build the error message
                    // and create a MultiTypeField
                    
                    for (Entry<String, FieldCapabilities> type : types.entrySet()) {
                        if (errorMessage.length() > 0) {
                            errorMessage.append(", ");
                        }
                        errorMessage.append("[");
                        errorMessage.append(type.getKey());
                        errorMessage.append("] in ");
                        errorMessage.append(Arrays.toString(type.getValue().indices()));
                    }

                    errorMessage.insert(0, "mapped as [" + types.size() + "] incompatible types: ");
                    
                    invalidField = new InvalidMappedField(name, errorMessage.toString());
                }
                // type is okay, check aggregation
                else {
                    fieldCap = types.values().iterator().next();
                    
                    // Skip internal fields (name starting with underscore and its type reported by field_caps starts with underscore
                    // as well). A meta field named "_version", for example, has the type named "_version".
                    if (name.startsWith("_") && fieldCap.getType().startsWith("_")) {
                        continue;
                    }
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
                        invalidField = new InvalidMappedField(name, errorMessage.toString());
                    }
                }
                
                // validation passes - create the field
                // if the name wasn't added before
                final InvalidMappedField invalidF = invalidField;
                final FieldCapabilities fieldCapab = fieldCap;
                if (!flattedMapping.containsKey(name)) {
                    createField(name, fieldCaps, hierarchicalMapping, flattedMapping, s -> {
                        return invalidF != null ? invalidF : createField(s, fieldCapab.getType(), emptyMap(), fieldCapab.isAggregatable());
                    });
                }
            }
        }

        return IndexResolution.valid(new EsIndex(indexPattern, hierarchicalMapping));
    }

    private static EsField createField(String fieldName, Map<String, Map<String, FieldCapabilities>> globalCaps,
            Map<String, EsField> hierarchicalMapping, Map<String, EsField> flattedMapping,
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
                if (map == null) {
                    throw new SqlIllegalArgumentException("Cannot find field {}; this is likely a bug", parentName);
                }
                FieldCapabilities parentCap = map.values().iterator().next();
                parent = createField(parentName, globalCaps, hierarchicalMapping, flattedMapping,
                        s -> createField(s, parentCap.getType(), new TreeMap<>(), parentCap.isAggregatable()));
            }
            parentProps = parent.getProperties();
        }

        EsField esField = field.apply(fieldName);
        
        parentProps.put(fieldName, esField);
        flattedMapping.put(fullFieldName, esField);

        return esField;
    }
    
    private static EsField createField(String fieldName, String typeName, Map<String, EsField> props, boolean isAggregateable) {
        DataType esType = DataType.fromTypeName(typeName);
        switch (esType) {
            case TEXT:
                return new TextEsField(fieldName, props, false);
            case KEYWORD:
                int length = DataType.KEYWORD.defaultPrecision;
                // TODO: to check whether isSearchable/isAggregateable takes into account the presence of the normalizer
                boolean normalized = false;
                return new KeywordEsField(fieldName, props, isAggregateable, length, normalized);
            case DATE:
                return new DateEsField(fieldName, props, isAggregateable);
            case UNSUPPORTED:
                return new UnsupportedEsField(fieldName, typeName);
            default:
                return new EsField(fieldName, esType, props, isAggregateable);
        }
    }
    
    private static FieldCapabilitiesRequest createFieldCapsRequest(String index) {
        return new FieldCapabilitiesRequest()
                .indices(Strings.commaDelimitedListToStringArray(index))
                .fields("*")
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    // TODO: Concrete indices still uses get mapping
    // waiting on https://github.com/elastic/elasticsearch/pull/34071
    //
    
    /**
     * Resolves a pattern to multiple, separate indices. Doesn't perform validation.
     */
    public void resolveAsSeparateMappings(String indexWildcard, String javaRegex, ActionListener<List<EsIndex>> listener) {
        GetIndexRequest getIndexRequest = createGetIndexRequest(indexWildcard);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
            List<EsIndex> results = new ArrayList<>(mappings.size());
            Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                /*
                 * We support wildcard expressions here, and it's only for commands that only perform the get index call.
                 * We can and simply have to use the concrete index name and show that to users.
                 * Get index against an alias with security enabled, where the user has only access to get mappings for the alias
                 * and not the concrete index: there is a well known information leak of the concrete index name in the response.
                 */
                String concreteIndex = indexMappings.key;
                if (pattern == null || pattern.matcher(concreteIndex).matches()) {
                    IndexResolution getIndexResult = buildGetIndexResult(concreteIndex, concreteIndex, indexMappings.value);
                    if (getIndexResult.isValid()) {
                        results.add(getIndexResult.get());
                    }
                }
            }
            results.sort(Comparator.comparing(EsIndex::name));
            listener.onResponse(results);
        }, listener::onFailure));
    }
    
    private static GetIndexRequest createGetIndexRequest(String index) {
        return new GetIndexRequest()
                .local(true)
                .indices(Strings.commaDelimitedListToStringArray(index))
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honours ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    private static IndexResolution buildGetIndexResult(String concreteIndex, String indexOrAlias,
            ImmutableOpenMap<String, MappingMetaData> mappings) {

        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            //Default mappings are ignored as they are applied to each type. Each type alone holds all of its fields.
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }

        if (singleType == null) {
            return IndexResolution.invalid("[" + indexOrAlias + "] doesn't have any types so it is incompatible with sql");
        } else if (typeNames != null) {
            Collections.sort(typeNames);
            return IndexResolution.invalid(
                    "[" + indexOrAlias + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        } else {
            try {
                Map<String, EsField> mapping = Types.fromEs(singleType.sourceAsMap());
                return IndexResolution.valid(new EsIndex(indexOrAlias, mapping));
            } catch (MappingException ex) {
                return IndexResolution.invalid(ex.getMessage());
            }
        }
    }
}