/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.Build;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * How a nested object, or a field under one, is mapped. Its leaves are ordinary columns; this is what the nested functions
 * and {@code NESTED_EXPAND} need to read them one value per object.
 *
 * @param path the nested object the field is under, or the field itself if it is the nested object
 * @param indices the indices where the field is under {@code path}
 * @param collection whether the field is the nested object itself
 * @param sourceFailures why the nested functions cannot read the {@code _source} of some of {@code indices}, by index name
 * @param conflict how the indices disagree on the nested mapping of this field, or {@code null} if they agree. Only a nested
 *                 function or expansion that reads the field fails with it.
 */
public record NestedField(
    String path,
    Set<String> indices,
    boolean collection,
    Map<String, String> sourceFailures,
    @Nullable String conflict
) {
    /**
     * Whether a field under a nested object reads, outside the nested functions, as a multivalued column of the values of all
     * of the parent's objects. Snapshot only, with the nested functions; otherwise ES|QL hides such fields.
     */
    public static final boolean FLATTENED_LEAVES = Build.current().isSnapshot();

    /** Added to the field names a query reads when it uses a nested function, so resolution reports {@code _source} failures. */
    public static final String REQUEST_MARKER = "\u0000esql_nested_reductions";

    public NestedField {
        indices = Set.copyOf(indices);
        sourceFailures = Map.copyOf(sourceFailures);
    }

    /** Finds the actual nested ancestor in this index, rather than inferring one from dots in a name. */
    @Nullable
    public static String pathFor(String name, Map<String, IndexFieldCapabilities> fields) {
        String candidate = name;
        while (true) {
            IndexFieldCapabilities field = fields.get(candidate);
            if (field != null && field.type().equals("nested")) {
                return candidate;
            }
            int dot = candidate.lastIndexOf('.');
            if (dot < 0) {
                return null;
            }
            candidate = candidate.substring(0, dot);
        }
    }

    /**
     * The nested objects and the fields under them, keyed by full field name. {@code sourceFailures} holds why the nested
     * functions cannot read each index's {@code _source}, by index name.
     */
    public static Map<String, NestedField> of(List<FieldCapabilitiesIndexResponse> responses, Map<String, String> sourceFailures) {
        Map<String, Set<String>> indices = new HashMap<>();
        Map<String, String> paths = new HashMap<>();
        Map<String, String> pathIndex = new HashMap<>();
        Map<String, Boolean> collections = new HashMap<>();
        Map<String, String> conflicts = new HashMap<>();
        List<FieldCapabilitiesIndexResponse> sorted = new ArrayList<>(responses);
        sorted.sort(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName));
        for (var response : sorted) {
            for (var fc : response.get().values()) {
                String path = pathFor(fc.name(), response.get());
                if (path == null) {
                    continue;
                }
                String oldPath = paths.putIfAbsent(fc.name(), path);
                pathIndex.putIfAbsent(fc.name(), response.getIndexName());
                if (oldPath != null && oldPath.equals(path) == false) {
                    conflicts.putIfAbsent(
                        fc.name(),
                        "its nested path is ["
                            + oldPath
                            + "] in ["
                            + pathIndex.get(fc.name())
                            + "] but ["
                            + path
                            + "] in ["
                            + response.getIndexName()
                            + "]"
                    );
                }
                collections.merge(fc.name(), fc.type().equals("nested"), Boolean::logicalAnd);
                indices.computeIfAbsent(fc.name(), k -> new HashSet<>()).add(response.getIndexName());
            }
        }
        if (paths.isEmpty()) {
            return Map.of();
        }
        // A path that another index maps as an object, or anything else, has no child documents there to read.
        for (String path : new TreeSet<>(paths.values())) {
            for (var response : sorted) {
                IndexFieldCapabilities fc = response.get().get(path);
                if (fc != null && fc.type().equals("nested") == false) {
                    String conflict = "["
                        + path
                        + "] is [nested] in ["
                        + pathIndex.get(path)
                        + "] but ["
                        + fc.type()
                        + "] in ["
                        + response.getIndexName()
                        + "]";
                    paths.forEach((field, fieldPath) -> {
                        if (fieldPath.equals(path)) {
                            conflicts.putIfAbsent(field, conflict);
                        }
                    });
                    break;
                }
            }
        }
        Map<String, NestedField> fields = new HashMap<>();
        paths.forEach((name, path) -> {
            Set<String> fieldIndices = indices.get(name);
            Map<String, String> fieldFailures = new HashMap<>();
            for (String index : fieldIndices) {
                if (sourceFailures.containsKey(index)) {
                    fieldFailures.put(index, sourceFailures.get(index));
                }
            }
            fields.put(name, new NestedField(path, fieldIndices, collections.get(name), fieldFailures, conflicts.get(name)));
        });
        return fields;
    }
}
