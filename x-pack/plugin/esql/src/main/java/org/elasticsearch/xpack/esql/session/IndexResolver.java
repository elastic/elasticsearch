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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

public class IndexResolver {
    public static final Set<String> ALL_FIELDS = Set.of("*");
    public static final Set<String> INDEX_METADATA_FIELD = Set.of("_index");
    public static final String UNMAPPED = "unmapped";

    public static final IndicesOptions FIELD_CAPS_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .gatekeeperOptions(
            IndicesOptions.GatekeeperOptions.builder().ignoreThrottled(true).allowClosedIndices(true).allowAliasToMultipleIndices(true)
        )
        .build();

    private final Client client;

    public IndexResolver(Client client) {
        this.client = client;
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveAsMergedMapping(String indexWildcard, Set<String> fieldNames, ActionListener<IndexResolution> listener) {
        client.execute(
            EsqlResolveFieldsAction.TYPE,
            createFieldCapsRequest(indexWildcard, fieldNames),
            listener.delegateFailureAndWrap((l, response) -> l.onResponse(mergedMappings(indexWildcard, response)))
        );
    }

    // public for testing only
    public IndexResolution mergedMappings(String indexPattern, FieldCapabilitiesResponse fieldCapsResponse) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION); // too expensive to run this on a transport worker
        if (fieldCapsResponse.getIndexResponses().isEmpty()) {
            return IndexResolution.notFound(indexPattern);
        }

        Map<String, List<IndexFieldCapabilities>> fieldsCaps = collectFieldCaps(fieldCapsResponse);

        // Build hierarchical fields - it's easier to do it in sorted order so the object fields come first.
        // TODO flattened is simpler - could we get away with that?
        String[] names = fieldsCaps.keySet().toArray(new String[0]);
        Arrays.sort(names);
        Set<String> forbiddenFields = new HashSet<>();
        Map<String, EsField> rootFields = new HashMap<>();
        name: for (String name : names) {
            Map<String, EsField> fields = rootFields;
            String fullName = name;
            boolean isAlias = false;
            UnsupportedEsField firstUnsupportedParent = null;
            while (true) {
                int nextDot = name.indexOf('.');
                if (nextDot < 0) {
                    break;
                }
                String parent = name.substring(0, nextDot);
                if (forbiddenFields.contains(parent)) {
                    continue name;
                }
                EsField obj = fields.get(parent);
                if (obj == null) {
                    obj = new EsField(parent, OBJECT, new HashMap<>(), false, true);
                    isAlias = true;
                    fields.put(parent, obj);
                } else if (firstUnsupportedParent == null && obj instanceof UnsupportedEsField unsupportedParent) {
                    firstUnsupportedParent = unsupportedParent;
                }
                fields = obj.getProperties();
                name = name.substring(nextDot + 1);
            }

            List<IndexFieldCapabilities> caps = fieldsCaps.get(fullName);
            if (allNested(caps)) {
                forbiddenFields.add(name);
                continue;
            }
            // TODO we're careful to make isAlias match IndexResolver - but do we use it?

            EsField field = firstUnsupportedParent == null
                ? createField(fieldCapsResponse, name, fullName, caps, isAlias)
                : new UnsupportedEsField(
                    fullName,
                    firstUnsupportedParent.getOriginalType(),
                    firstUnsupportedParent.getName(),
                    new HashMap<>()
                );
            fields.put(name, field);
        }

        boolean allEmpty = true;
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            allEmpty &= ir.get().isEmpty();
        }
        if (allEmpty) {
            // If all the mappings are empty we return an empty set of resolved indices to line up with QL
            return IndexResolution.valid(new EsIndex(indexPattern, rootFields, Set.of()));
        }

        Set<String> concreteIndices = new HashSet<>(fieldCapsResponse.getIndexResponses().size());
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            concreteIndices.add(ir.getIndexName());
        }
        return IndexResolution.valid(new EsIndex(indexPattern, rootFields, concreteIndices));
    }

    private boolean allNested(List<IndexFieldCapabilities> caps) {
        for (IndexFieldCapabilities cap : caps) {
            if (false == cap.type().equalsIgnoreCase("nested")) {
                return false;
            }
        }
        return true;
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
        String fullName,
        List<IndexFieldCapabilities> fcs,
        boolean isAlias
    ) {
        IndexFieldCapabilities first = fcs.get(0);
        List<IndexFieldCapabilities> rest = fcs.subList(1, fcs.size());
        DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(first.type(), first.metricType());
        boolean aggregatable = first.isAggregatable();
        if (rest.isEmpty() == false) {
            for (IndexFieldCapabilities fc : rest) {
                if (first.metricType() != fc.metricType()) {
                    return conflictingMetricTypes(name, fullName, fieldCapsResponse);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                if (type != EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType())) {
                    return conflictingTypes(name, fullName, fieldCapsResponse);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                aggregatable &= fc.isAggregatable();
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
            return new KeywordEsField(name, new HashMap<>(), aggregatable, length, normalized, isAlias);
        }
        if (type == DATETIME) {
            return DateEsField.dateEsField(name, new HashMap<>(), aggregatable);
        }
        if (type == UNSUPPORTED) {
            return unsupported(name, first);
        }

        return new EsField(name, type, new HashMap<>(), aggregatable, isAlias);
    }

    private UnsupportedEsField unsupported(String name, IndexFieldCapabilities fc) {
        String originalType = fc.metricType() == TimeSeriesParams.MetricType.COUNTER ? "counter" : fc.type();
        return new UnsupportedEsField(name, originalType);
    }

    private EsField conflictingTypes(String name, String fullName, FieldCapabilitiesResponse fieldCapsResponse) {
        Map<String, Set<String>> typesToIndices = new TreeMap<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(fullName);
            if (fc != null) {
                DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType());
                if (type == UNSUPPORTED) {
                    return unsupported(name, fc);
                }
                typesToIndices.computeIfAbsent(type.typeName(), _key -> new TreeSet<>()).add(ir.getIndexName());
            }
        }
        return new InvalidMappedField(name, typesToIndices);
    }

    private EsField conflictingMetricTypes(String name, String fullName, FieldCapabilitiesResponse fieldCapsResponse) {
        TreeSet<String> indices = new TreeSet<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(fullName);
            if (fc != null) {
                indices.add(ir.getIndexName());
            }
        }
        return new InvalidMappedField(name, "mapped as different metric types in indices: " + indices);
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(String index, Set<String> fieldNames) {
        FieldCapabilitiesRequest req = new FieldCapabilitiesRequest().indices(Strings.commaDelimitedListToStringArray(index));
        req.fields(fieldNames.toArray(String[]::new));
        req.includeUnmapped(true);
        // lenient because we throw our own errors looking at the response e.g. if something was not resolved
        // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
        req.indicesOptions(FIELD_CAPS_INDICES_OPTIONS);
        req.setMergeResults(false);
        return req;
    }
}
