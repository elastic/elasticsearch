/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.index;

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
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.ConstantKeywordEsField;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;
import static org.elasticsearch.xpack.ql.type.DataTypes.CONSTANT_KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

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

    public static final String SQL_TABLE = "TABLE";
    public static final String SQL_VIEW = "VIEW";

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
    private static final Set<String> COMPATIBLE_KEYWORD_TYPES = Set.of(KEYWORD.typeName(), CONSTANT_KEYWORD.typeName());
    static final String UNMAPPED = "unmapped";

    private final Client client;
    private final String clusterName;
    private final DataTypeRegistry typeRegistry;

    public IndexResolver(Client client, String clusterName, DataTypeRegistry typeRegistry) {
        this.client = client;
        this.clusterName = clusterName;
        this.typeRegistry = typeRegistry;
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
            for (ObjectCursor<List<AliasMetadata>> cursor : aliases.getAliases().values()) {
                for (AliasMetadata amd : cursor.value) {
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
                        && indices.getSettings().get(indexName).getAsBoolean("index.frozen", false);

                if (pattern == null || pattern.matcher(indexName).matches()) {
                    result.add(new IndexInfo(indexName, isFrozen ? IndexType.FROZEN_INDEX : IndexType.STANDARD_INDEX));
                }
            }
        }

        listener.onResponse(result);
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     * Used for SHOW COLUMNS and SYS COLUMNS with pattern.
     */
    public void resolveAsMergedMapping(String indexWildcard, String javaRegex, boolean includeFrozen,
            ActionListener<IndexResolution> listener) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen);
        client.fieldCaps(fieldRequest,
                ActionListener.wrap(
                        response -> listener.onResponse(mergedMappings(typeRegistry, indexWildcard, response.getIndices(), response.get())),
                        listener::onFailure));
    }

    public static IndexResolution mergedMappings(DataTypeRegistry typeRegistry, String indexPattern, String[] indexNames,
            Map<String, Map<String, FieldCapabilities>> fieldCaps) {

        if (indexNames.length == 0) {
            return IndexResolution.notFound(indexPattern);
        }

        // merge all indices onto the same one
        List<EsIndex> indices = buildIndices(typeRegistry, indexNames, null, fieldCaps, null, i -> indexPattern, true);

        if (indices.size() > 1) {
            throw new QlIllegalArgumentException(
                    "Incorrect merging of mappings (likely due to a bug) - expect at most one but found [{}]",
                    indices.size());
        }

        return IndexResolution.valid(indices.isEmpty() ? new EsIndex(indexNames[0], emptyMap()) : indices.get(0));
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
     * Used for SYS COLUMNS
     */
    public void resolveAsSeparateMappings(String indexWildcard, String javaRegex, boolean includeFrozen,
            ActionListener<List<EsIndex>> listener) {
        FieldCapabilitiesRequest fieldRequest = createFieldCapsRequest(indexWildcard, includeFrozen);
        client.fieldCaps(fieldRequest, wrap(response -> {
            client.admin().indices().getAliases(createGetAliasesRequest(response, includeFrozen), wrap(aliases ->
                listener.onResponse(separateMappings(typeRegistry, javaRegex, response.getIndices(), response.get(), aliases.getAliases())),
                ex -> {
                    if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                        listener.onResponse(separateMappings(typeRegistry, javaRegex, response.getIndices(), response.get(), null));
                    } else {
                        listener.onFailure(ex);
                    }
                }));
            },
            listener::onFailure));

    }

    private GetAliasesRequest createGetAliasesRequest(FieldCapabilitiesResponse response, boolean includeFrozen) {
        return new GetAliasesRequest()
                .local(true)
                .aliases("*")
                .indices(response.getIndices())
                .indicesOptions(includeFrozen ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS);
    }

    public static List<EsIndex> separateMappings(DataTypeRegistry typeRegistry, String javaRegex, String[] indexNames,
            Map<String, Map<String, FieldCapabilities>> fieldCaps, ImmutableOpenMap<String, List<AliasMetadata>> aliases) {
        return buildIndices(typeRegistry, indexNames, javaRegex, fieldCaps, aliases, Function.identity(), false);
    }

    private static Map<String, QlFieldCapabilities> buildQlFieldCaps(String[] indexNames,
            Map<String, Map<String, FieldCapabilities>> fieldCaps, ImmutableOpenMap<String, List<AliasMetadata>> indexToAliases,
            boolean isMergedMapping) {
        Map<String, QlFieldCapabilities> map = new HashMap<>();
        Map<String, Set<String>> aliasToIndices = new HashMap<>(); // map with aliases and their list of indices
        
        if (indexToAliases != null) {
            Iterator<ObjectObjectCursor<String, List<AliasMetadata>>> iter = indexToAliases.iterator();
            while (iter.hasNext()) {
                ObjectObjectCursor<String, List<AliasMetadata>> index = iter.next();
                for (AliasMetadata aliasMetadata : index.value) {
                    String aliasName = aliasMetadata.alias();
                    aliasToIndices.putIfAbsent(aliasName, new HashSet<>());
                    aliasToIndices.get(aliasName).add(index.key);
                }
            }
        } else if (isMergedMapping) {
            // single virtual alias named "*" to be used for a merged mappings
            aliasToIndices.put("*", new HashSet<>(Arrays.asList(indexNames)));
        }

        // create a virtual set of fields capabilities for each alias
        for (String alias : aliasToIndices.keySet()) {
            Set<String> indices = aliasToIndices.get(alias);
            QlFieldCapabilities qlFieldCaps = new QlFieldCapabilities(indices.toArray(new String[indices.size()]));
            Map<String, Map<String, QlFieldCapability>> fields = new HashMap<>();
            
            // check each field
            for (Entry<String, Map<String, FieldCapabilities>> entry : fieldCaps.entrySet()) {
                String fieldName = entry.getKey();
                
                // ignore size added by the mapper plugin
                if (FIELD_NAMES_BLACKLIST.contains(fieldName)) {
                    continue;
                }
                Map<String, QlFieldCapability> qlFieldTypes = new HashMap<>();

                // check each type
                for (Entry<String, FieldCapabilities> typeEntry : entry.getValue().entrySet()) {
                    String type = typeEntry.getKey();
                    FieldCapabilities f = typeEntry.getValue();
                    // Skip internal fields (name starting with underscore and its type reported by field_caps starts
                    // with underscore as well). A meta field named "_version", for example, has the type named "_version".
                    if (type.startsWith("_") && f.getType().startsWith("_")) {
                        continue;
                    }
                    
                    QlFieldCapability qlFieldCap = new QlFieldCapability(type, f.getType(), f.isSearchable(), f.isAggregatable());
                    boolean isValidType = false;

                    // in each list of indices, keep only those belonging to the current alias
                    isValidType = maybeSetIndices(f.indices(), indices, qlFieldCap::setIndices) || isValidType;
                    isValidType = maybeSetIndices(f.nonAggregatableIndices(), indices, qlFieldCap::setNonAggregatableIndices)
                        || isValidType;
                    isValidType = maybeSetIndices(f.nonSearchableIndices(), indices, qlFieldCap::setNonSearchableIndices) || isValidType;

                    // another field type is applicable to this alias, so skip the current field type
                    if (isEmpty(qlFieldCap.getIndices())
                        && isEmpty(qlFieldCap.getNonAggregatableIndices())
                        && isEmpty(qlFieldCap.getNonSearchableIndices())
                        && (isEmpty(f.indices()) == false || isEmpty(f.nonAggregatableIndices()) == false
                            || isEmpty(f.nonSearchableIndices()) == false)) {
                        continue;
                    }
                    
                    // a type that doesn't have any indices is valid by default, for example
                    // "id": {"keyword": {"type": "keyword","searchable": false,"aggregatable": true}}
                    if (isValidType || (isEmpty(f.indices()) && isEmpty(f.nonAggregatableIndices()) && isEmpty(f.nonSearchableIndices()))) {
                        qlFieldTypes.put(type, qlFieldCap);
                    }
                }
                
                if (qlFieldTypes.size() > 0) {
                    fields.put(fieldName, qlFieldTypes);
                }
            }

            if (fields.size() > 0) {
                qlFieldCaps.setFields(fields);
                map.put(alias, qlFieldCaps);
            }
        }

        return map;
    }

    private static boolean maybeSetIndices(String[] originalList, Set<String> virtualList, Consumer<String[]> setter) {
        if (originalList == null) {
            return true;
        }
        
        String[] intersection = intersection(originalList, virtualList);
        // if the indices belonging to the virtual grouping are all in the intersection no need to set the array as it's considered implicit
        if (intersection.length == virtualList.size()) {
            return true;
        }
        // if there are common indices or the original array had nothing in it, all good
        if (intersection.length > 0 || originalList.length == 0) {
            setter.accept(intersection);
            return true;
        }
        return false;
    }

    private static String[] intersection(String[] array, Set<String> set) {
        List<String> list = Arrays.asList(array)
            .stream()
            .filter(set::contains)
            .collect(Collectors.toList());
        return list.toArray(new String[list.size()]);
    }

    private static class Fields {
        final Map<String, EsField> hierarchicalMapping = new TreeMap<>();
        final Map<String, EsField> flattedMapping = new LinkedHashMap<>();
    }


    private static List<EsIndex> buildIndices(DataTypeRegistry typeRegistry, String[] indexNames, String javaRegex,
        Map<String, Map<String, FieldCapabilities>> fieldCaps, ImmutableOpenMap<String, List<AliasMetadata>> indicesToAliases,
        Function<String, String> indexNameProcessor, boolean isMergedMapping) {

        Map<String, QlFieldCapabilities> qlFieldCaps= buildQlFieldCaps(indexNames, fieldCaps, indicesToAliases, isMergedMapping);
        return buildIndices(typeRegistry, indexNames, javaRegex, fieldCaps, indicesToAliases, indexNameProcessor, qlFieldCaps);
    }

    /**
     * Assemble an index-based mapping from the field caps (which is field based) by looking at the indices associated with
     * each field.
     */
    private static List<EsIndex> buildIndices(DataTypeRegistry typeRegistry, String[] indexNames, String javaRegex,
            Map<String, Map<String, FieldCapabilities>> fieldCaps, ImmutableOpenMap<String, List<AliasMetadata>> indicesToAliases,
            Function<String, String> indexNameProcessor, Map<String, QlFieldCapabilities> qlFieldCaps) {

        if ((indexNames == null || indexNames.length == 0) && (indicesToAliases == null || indicesToAliases.isEmpty())) {
            return emptyList();
        }

        Set<String> resolvedAliases = new HashSet<>();
        if (indicesToAliases != null) {
            Iterator<ObjectObjectCursor<String, List<AliasMetadata>>> iterator = indicesToAliases.iterator();
            while (iterator.hasNext()) {
                for (AliasMetadata alias : iterator.next().value) {
                    resolvedAliases.add(alias.getAlias());
                }
            }
        }
        List<String> resolvedIndices = new ArrayList<>(asList(indexNames));

        int mapSize = CollectionUtils.mapSize(resolvedIndices.size() + resolvedAliases.size());
        Map<String, Fields> indicesAndAliases = new LinkedHashMap<>(mapSize);
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        // sort fields in reverse order to build the field hierarchy
        Set<Entry<String, Map<String, FieldCapabilities>>> sortedFields = new TreeSet<>(
                Collections.reverseOrder(Comparator.comparing(Entry::getKey)));
        sortedFields.addAll(fieldCaps.entrySet());
        
        for (Entry<String, Map<String, FieldCapabilities>> entry : sortedFields) {
            String fieldName = entry.getKey();

            // ignore size added by the mapper plugin
            if (FIELD_NAMES_BLACKLIST.contains(fieldName)) {
                continue;
            }

            Map<String, FieldCapabilities> types = new LinkedHashMap<>(entry.getValue());
            Map<String, InvalidMappedField> invalidFields = validateField(fieldName, qlFieldCaps, types);

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
                if (indicesToAliases != null) {
                    for (String concreteIndex : concreteIndices) {
                        if (indicesToAliases.containsKey(concreteIndex)) {
                            List<AliasMetadata> concreteIndexAliases = indicesToAliases.get(concreteIndex);
                            concreteIndexAliases.stream().forEach(e -> uniqueAliases.add(e.alias()));
                        }
                    }
                    concreteIndices.addAll(uniqueAliases);
                }

                // put the field in their respective mappings
                for (String index : concreteIndices) {
                    boolean isIndexAlias = uniqueAliases.contains(index);
                    if (pattern == null || pattern.matcher(index).matches() || isIndexAlias) {
                        String indexName = isIndexAlias ? index : indexNameProcessor.apply(index);
                        Fields indexFields = indicesAndAliases.get(indexName);
                        if (indexFields == null) {
                            indexFields = new Fields();
                            indicesAndAliases.put(indexName, indexFields);
                        }
                        EsField field = indexFields.flattedMapping.get(fieldName);
                        InvalidMappedField invalidField = isIndexAlias ? invalidFields.get(index) : invalidFields.get("*");
                        boolean createField = false;

                        // field not created at all => create the field
                        // field created starting from children as being valid, but having invalid mappings => re-create it as being invalid
                        if (field == null || (invalidField != null && (field instanceof InvalidMappedField) == false)) {
                            createField = true;
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

                            createField(typeRegistry, fieldName, indexName, indicesToAliases, fieldCaps, indexFields.hierarchicalMapping,
                                    indexFields.flattedMapping,
                                    s -> invalidField != null ? invalidField :
                                        createField(typeRegistry, s, typeCap.getType(), emptyMap(), typeCap.isAggregatable(),
                                                isAliasFieldType.get()));
                        }
                    }
                }
            }
        }

        List<EsIndex> foundIndices = new ArrayList<>(indicesAndAliases.size());
        for (Entry<String, Fields> entry : indicesAndAliases.entrySet()) {
            foundIndices.add(new EsIndex(entry.getKey(), entry.getValue().hierarchicalMapping));
        }
        // return indices in ascending order
        foundIndices.sort(Comparator.comparing(EsIndex::name));
        return foundIndices;
    }

    private static Map<String, InvalidMappedField> validateField(String fieldName, Map<String, QlFieldCapabilities> qlFieldCaps,
        Map<String, FieldCapabilities> esTypes)
    {
        Map<String, InvalidMappedField> invalidFields = new HashMap<>();

        // for each indices group
        for (Entry<String, QlFieldCapabilities> capabilities : qlFieldCaps.entrySet()) {
            String indicesGroupName = capabilities.getKey();
            Map<String, QlFieldCapability> types = capabilities.getValue().getFields().get(fieldName);
            
            if (types == null || fieldName.startsWith("_")) {
                continue;
            }
            InvalidMappedField invalidField = validate(fieldName, types);
            if (invalidField != null) {
                invalidFields.put(indicesGroupName, invalidField);
            } else if (qlFieldCaps.size() == 1 && qlFieldCaps.containsKey("*") && esTypes.containsKey(KEYWORD.esType())
                && esTypes.containsKey(CONSTANT_KEYWORD.esType())) {
                esTypes.remove(CONSTANT_KEYWORD.esType());
            }
        }

        if (invalidFields.isEmpty() == false) {
            return invalidFields;
        }
        // everything checks
        return emptyMap();
    }
    
    private static InvalidMappedField validate(String fieldName, Map<String, QlFieldCapability> types) {
        StringBuilder errorMessage = new StringBuilder();
        
        boolean hasUnmapped = types.containsKey(UNMAPPED);
        // a keyword field and a constant_keyword field with the same name in two different indices are considered "compatible"
        // since a common use case of constant_keyword field involves two indices with a field having the same name: one being
        // a keyword, the other being a constant_keyword
        boolean hasCompatibleKeywords = types.containsKey(KEYWORD.esType()) && types.containsKey(CONSTANT_KEYWORD.esType());
        int allowedTypesCount = (hasUnmapped ? 2 : 1) + (hasCompatibleKeywords ? 1 : 0);
    
        if (types.size() > allowedTypesCount) {
            // build the error message
            // and create a MultiTypeField
            for (Entry<String, QlFieldCapability> type : types.entrySet()) {
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
                errorMessage.append(Arrays.toString(type.getValue().getIndices()));
            }
    
            errorMessage.insert(0, "mapped as [" + (types.size() - (hasUnmapped ? 1 : 0)) + "] incompatible types: ");
    
            return new InvalidMappedField(fieldName, errorMessage.toString());
        }
        // type is okay, check aggregation
        else {
            QlFieldCapability fieldCap = types.values().iterator().next();
    
            // validate search/agg-able
            if (fieldCap.getNonAggregatableIndices() != null) {
                errorMessage.append("mapped as aggregatable except in ");
                errorMessage.append(Arrays.toString(fieldCap.getNonAggregatableIndices()));
            }
            if (fieldCap.getNonSearchableIndices() != null) {
                if (errorMessage.length() > 0) {
                    errorMessage.append(",");
                }
                errorMessage.append("mapped as searchable except in ");
                errorMessage.append(Arrays.toString(fieldCap.getNonSearchableIndices()));
            }
    
            if (errorMessage.length() > 0) {
                return new InvalidMappedField(fieldName, errorMessage.toString());
            }
        }
        
        return null;
    }

    private static EsField createField(DataTypeRegistry typeRegistry, String fieldName, String aliasOrIndexName,
            ImmutableOpenMap<String, List<AliasMetadata>> aliases,
            Map<String, Map<String, FieldCapabilities>> globalCaps,
            Map<String, EsField> hierarchicalMapping,
            Map<String, EsField> flattedMapping,
            Function<String, EsField> field) {

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
                    List<String> indices = new ArrayList<>();
                    // this is a field to be built for an alias
                    if (aliases != null) {
                        Iterator<ObjectObjectCursor<String, List<AliasMetadata>>> iter = aliases.iterator();
                        Map<String, Set<String>> aliasToIndices = new HashMap<>();
                        while (iter.hasNext()) {
                            ObjectObjectCursor<String, List<AliasMetadata>> index = iter.next();
                            for (AliasMetadata aliasMetadata : index.value) {
                                String aliasName = aliasMetadata.alias();
                                aliasToIndices.putIfAbsent(aliasName, new HashSet<>());
                                aliasToIndices.get(aliasName).add(index.key);
                            }
                        }
                        if (aliasToIndices.containsKey(aliasOrIndexName)) {
                            indices.addAll(aliasToIndices.get(aliasOrIndexName));
                        }
                    }
                    
                    indices.add(aliasOrIndexName);
                    Iterator<FieldCapabilities> iterator = map.values().iterator();
                    FieldCapabilities parentCap = iterator.next();
                    while (iterator.hasNext()
                            && ((parentCap.indices().length > 0 && Collections.disjoint(indices, Arrays.asList(parentCap.indices()))) 
                                    || UNMAPPED.equals(parentCap.getType()))) {
                        parentCap = iterator.next();
                    }
                    final FieldCapabilities parentC = parentCap;
                    fieldFunction = s -> createField(typeRegistry, s, parentC.getType(), new TreeMap<>(), parentC.isAggregatable(), false);
                }

                parent = createField(typeRegistry, parentName, aliasOrIndexName, aliases, globalCaps, hierarchicalMapping, flattedMapping,
                        fieldFunction);
            }
            parentProps = parent.getProperties();
        }

        EsField esField = field.apply(fieldName);

        if (parent != null && parent instanceof UnsupportedEsField) {
            UnsupportedEsField unsupportedParent = (UnsupportedEsField) parent;
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

    private static EsField createField(DataTypeRegistry typeRegistry, String fieldName, String typeName, Map<String, EsField> props,
            boolean isAggregateable, boolean isAlias) {
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
            return new DateEsField(fieldName, props, isAggregateable);
        }
        if (esType == CONSTANT_KEYWORD) {
            return new ConstantKeywordEsField(fieldName);
        }
        if (esType == UNSUPPORTED) {
            return new UnsupportedEsField(fieldName, typeName, null, props);
        }

        return new EsField(fieldName, esType, props, isAggregateable, isAlias);
    }
}
