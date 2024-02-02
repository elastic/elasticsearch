/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeRegistry;
import org.elasticsearch.xpack.ql.type.DateEsField;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.TextEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public class EsqlIndexResolver {
    public static final IndicesOptions FIELD_CAPS_INDICES_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions FIELD_CAPS_FROZEN_INDICES_OPTIONS = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN)
    );

    private final Client client;
    private final DataTypeRegistry typeRegistry;

    public EsqlIndexResolver(Client client, DataTypeRegistry typeRegistry) {
        this.client = client;
        this.typeRegistry = typeRegistry;
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(
        String indexWildcard,
        Set<String> fieldNames,
        boolean includeFrozen,
        Map<String, Object> runtimeMappings,
        ActionListener<IndexResolution> listener
    ) {
        client.fieldCaps(
            createFieldCapsRequest(indexWildcard, fieldNames, includeFrozen, runtimeMappings),
            listener.delegateFailureAndWrap((l, response) -> l.onResponse(mergedMappings(indexWildcard, response)))
        );
    }

    public IndexResolution mergedMappings(String indexPattern, FieldCapabilitiesResponse fieldCapsResponse) {
        if (fieldCapsResponse.getIndexResponses().isEmpty()) {
            return IndexResolution.notFound(indexPattern);
        }

        Map<String, List<IndexFieldCapabilities>> fieldsCaps = collectFieldCaps(fieldCapsResponse);

        // Build hierarchical fields - it's easier to do it in sorted order so the object fields come first.
        // TODO flattened is simpler - could we get away with that?
        String[] names = fieldsCaps.keySet().toArray(new String[0]);
        Arrays.sort(names);
        Map<String, EsField> rootFields = new HashMap<>();
        for (String name : names) {
            Map<String, EsField> fields = rootFields;
            String fullName = name;
            boolean isAlias = false;
            while (true) {
                int nextDot = name.indexOf('.');
                if (nextDot < 0) {
                    break;
                }
                String parent = name.substring(0, nextDot);
                EsField obj = fields.get(parent);
                if (obj == null) {
                    obj = new EsField(parent, OBJECT, new HashMap<>(), false, true);
                    isAlias = true;
                    fields.put(parent, obj);
                }
                fields = obj.getProperties();
                name = name.substring(nextDot + 1);
            }
            // TODO we're careful to make isAlias match IndexResolver - but do we use it?
            EsField field = createField(fieldCapsResponse, name, fieldsCaps.get(fullName), isAlias);
            fields.put(name, field);
        }

        Set<String> concreteIndices = new HashSet<>(fieldCapsResponse.getIndexResponses().size());
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            concreteIndices.add(ir.getIndexName());
        }
        return IndexResolution.valid(new EsIndex(indexPattern, rootFields, concreteIndices));
    }

    private static Map<String, List<IndexFieldCapabilities>> collectFieldCaps(FieldCapabilitiesResponse fieldCapsResponse) {
        Set<String> seenHashes = new HashSet<>();
        Map<String, List<IndexFieldCapabilities>> fieldsCaps = new HashMap<>();
        for (FieldCapabilitiesIndexResponse response : fieldCapsResponse.getIndexResponses()) {
            if (seenHashes.add(response.getIndexMappingHash()) == false) {
                continue;
            }
            for (IndexFieldCapabilities fc : response.get().values()) {
                if (fc.isMetadatafield()) {
                    // ESQL builds the metadata fields if they are asked for without using the resolution.
                    continue;
                }
                List<IndexFieldCapabilities> all = fieldsCaps.computeIfAbsent(fc.name(), (_key) -> new ArrayList<>());
                all.add(fc);
            }
        }
        return fieldsCaps;
    }

    private EsField createField(
        FieldCapabilitiesResponse fieldCapsResponse,
        String name,
        List<IndexFieldCapabilities> fcs,
        boolean isAlias
    ) {
        IndexFieldCapabilities first = fcs.get(0);
        List<IndexFieldCapabilities> rest = fcs.subList(1, fcs.size());
        DataType type = typeRegistry.fromEs(first.type(), first.metricType());
        if (rest.isEmpty() == false) {
            for (IndexFieldCapabilities fc : rest) {
                if (first.metricType() != fc.metricType()) {
                    return conflictingMetricTypes(first.name(), fieldCapsResponse);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                if (type != typeRegistry.fromEs(fc.type(), fc.metricType())) {
                    return conflictingTypes(first.name(), fieldCapsResponse);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                if (first.isAggregatable() != fc.isAggregatable() || first.isSearchable() != fc.isSearchable()) {
                    // TODO why do we care if there's a conflict in "searchable" - also, can't we just search everything?
                    return conflictingAggregatableOrSearchable(first.name(), fieldCapsResponse);
                }
            }
        }

        // TODO I think we only care about unmapped fields if we're aggregating on them. do we even then?

        if (type == TEXT) {
            return new TextEsField(name, new HashMap<>(), false, isAlias);
        }
        if (type == KEYWORD) {
            int length = Short.MAX_VALUE;
            // TODO: to check whether isSearchable/isAggregateable takes into account the presence of the normalizer
            boolean normalized = false;
            return new KeywordEsField(name, new HashMap<>(), first.isAggregatable(), length, normalized, isAlias);
        }
        if (type == DATETIME) {
            return DateEsField.dateEsField(name, new HashMap<>(), first.isAggregatable());
        }
        if (type == UNSUPPORTED) {
            String originalType = first.metricType() == TimeSeriesParams.MetricType.COUNTER ? "counter" : first.type();
            return new UnsupportedEsField(name, originalType, null, new HashMap<>());
        }

        return new EsField(name, type, new HashMap<>(), first.isAggregatable(), isAlias);
    }

    private EsField conflictingTypes(String name, FieldCapabilitiesResponse fieldCapsResponse) {
        Map<String, Set<String>> typesToIndices = new TreeMap<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(name);
            if (fc != null) {
                typesToIndices.computeIfAbsent(typeRegistry.fromEs(fc.type(), fc.metricType()).name(), _key -> new TreeSet<>())
                    .add(ir.getIndexName());
            }
        }
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size());
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] in ");
            errorMessage.append(e.getValue());
        }
        return new InvalidMappedField(name, errorMessage.toString());
    }

    private EsField conflictingAggregatableOrSearchable(String name, FieldCapabilitiesResponse fieldCapsResponse) {
        Set<String> nonAggregatableIndices = new TreeSet<>();
        Set<String> nonSearchableIndices = new TreeSet<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(name);
            if (fc == null) {
                continue;
            }
            if (fc.isAggregatable() == false) {
                nonAggregatableIndices.add(ir.getIndexName());
            }
            if (fc.isSearchable() == false) {
                nonSearchableIndices.add(ir.getIndexName());
            }
        }
        StringBuilder errorMessage = new StringBuilder();
        if (nonAggregatableIndices.size() > 0) {
            errorMessage.append("mapped as aggregatable except in ");
            errorMessage.append(nonAggregatableIndices);
        }
        if (nonSearchableIndices.size() > 0) {
            if (errorMessage.length() > 0) {
                errorMessage.append(",");
            }
            errorMessage.append("mapped as searchable except in ");
            errorMessage.append(nonSearchableIndices);
        }
        if (errorMessage.isEmpty()) {
            throw new AssertionError("called conflictingAggregatableOrSearchable without conflicts");
        }
        return new InvalidMappedField(name, errorMessage.toString());
    }

    private EsField conflictingMetricTypes(String name, FieldCapabilitiesResponse fieldCapsResponse) {
        TreeSet<String> indices = new TreeSet<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(name);
            if (fc != null) {
                indices.add(ir.getIndexName());
            }
        }
        return new InvalidMappedField(name, "mapped as different metric types in indices: " + indices);
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(
        String index,
        Set<String> fieldNames,
        boolean includeFrozen,
        Map<String, Object> runtimeMappings
    ) {
        IndicesOptions indicesOptions = includeFrozen ? FIELD_CAPS_FROZEN_INDICES_OPTIONS : FIELD_CAPS_INDICES_OPTIONS;
        FieldCapabilitiesRequest req = new FieldCapabilitiesRequest().indices(Strings.commaDelimitedListToStringArray(index));
        req.fields(fieldNames.toArray(String[]::new));
        req.includeUnmapped(true);
        req.runtimeFields(runtimeMappings);
        // lenient because we throw our own errors looking at the response e.g. if something was not resolved
        // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
        req.indicesOptions(indicesOptions);
        req.setMergeResults(false);
        return req;
    }
}
