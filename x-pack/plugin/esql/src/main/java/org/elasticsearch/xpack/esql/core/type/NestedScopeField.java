/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Analysis-only namespace for fields visible exclusively inside an explicit NESTED expression. */
public final class NestedScopeField extends EsField {
    public static final String NAMESPACE = "$$nested_reductions";
    public static final String PREFIX = NAMESPACE + ".";
    public static final String REQUEST_MARKER = "\u0000esql_nested_reductions";

    private final String path;
    private final Set<String> indices;
    private final boolean collection;
    private final Map<String, String> sourceFailures;
    @Nullable
    private final String conflict;

    /**
     * @param sourceFailures why NESTED cannot read the {@code _source} of some of {@code indices}, by index name
     * @param conflict how the indices disagree on the nested mapping of this field, or {@code null} if they agree. Only a
     *                 NESTED reduction or expansion that reads the field fails with it.
     */
    public NestedScopeField(
        String name,
        DataType type,
        Map<String, EsField> properties,
        String path,
        Set<String> indices,
        boolean collection,
        Map<String, String> sourceFailures,
        @Nullable String conflict
    ) {
        super(name, type, properties, false, TimeSeriesFieldType.NONE);
        this.path = path;
        this.indices = Set.copyOf(indices);
        this.collection = collection;
        this.sourceFailures = Map.copyOf(sourceFailures);
        this.conflict = conflict;
    }

    public String path() {
        return path;
    }

    public Set<String> indices() {
        return indices;
    }

    public boolean collection() {
        return collection;
    }

    public Map<String, String> sourceFailures() {
        return sourceFailures;
    }

    @Nullable
    public String conflict() {
        return conflict;
    }

    @Override
    public EsField withProperties(Map<String, EsField> properties) {
        return new NestedScopeField(getName(), getDataType(), properties, path, indices, collection, sourceFailures, conflict);
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("nested scope fields must be lowered before plan serialization");
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        NestedScopeField other = (NestedScopeField) obj;
        return path.equals(other.path)
            && indices.equals(other.indices)
            && collection == other.collection
            && sourceFailures.equals(other.sourceFailures)
            && Objects.equals(conflict, other.conflict);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, indices, collection, sourceFailures, conflict);
    }

    /** Finds the actual nested ancestor in this index, rather than inferring one from dots in a name. */
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
     * Builds a separate schema so nested fields cannot introduce conflicts into ordinary field resolution.
     * {@code sourceFailures} holds why NESTED cannot read each index's {@code _source}, by index name.
     */
    public static EsField namespace(List<FieldCapabilitiesIndexResponse> responses, Map<String, String> sourceFailures) {
        Map<String, List<IndexFieldCapabilities>> caps = new TreeMap<>();
        Map<String, Set<String>> indices = new HashMap<>();
        Map<String, String> paths = new HashMap<>();
        Map<String, String> pathIndex = new HashMap<>();
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
                caps.computeIfAbsent(fc.name(), k -> new ArrayList<>()).add(fc);
                indices.computeIfAbsent(fc.name(), k -> new HashSet<>()).add(response.getIndexName());
            }
        }
        if (caps.isEmpty()) {
            return null;
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
        Map<String, EsField> roots = new TreeMap<>();
        for (var entry : caps.entrySet()) {
            String fullName = entry.getKey();
            Map<String, EsField> parent = roots;
            String[] parts = fullName.split("\\.");
            for (int i = 0; i < parts.length - 1; i++) {
                EsField object = parent.computeIfAbsent(
                    parts[i],
                    k -> new EsField(k, DataType.OBJECT, new TreeMap<>(), false, TimeSeriesFieldType.NONE)
                );
                parent = object.getProperties();
            }
            String name = parts[parts.length - 1];
            boolean collection = entry.getValue().stream().allMatch(fc -> fc.type().equals("nested"));
            DataType firstType = collection
                ? DataType.SOURCE
                : EsqlDataTypeRegistry.INSTANCE.fromEs(entry.getValue().getFirst().type(), entry.getValue().getFirst().metricType())
                    .widenSmallNumeric();
            boolean conflict = collection == false
                && entry.getValue()
                    .stream()
                    .anyMatch(fc -> EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType()).widenSmallNumeric() != firstType);
            DataType type = conflict ? DataType.UNSUPPORTED : firstType;
            Map<String, EsField> properties = parent.containsKey(name) ? parent.get(name).getProperties() : new TreeMap<>();
            Set<String> fieldIndices = indices.get(fullName);
            Map<String, String> fieldFailures = new HashMap<>();
            for (String index : fieldIndices) {
                if (sourceFailures.containsKey(index)) {
                    fieldFailures.put(index, sourceFailures.get(index));
                }
            }
            parent.put(
                name,
                new NestedScopeField(
                    name,
                    type,
                    properties,
                    paths.get(fullName),
                    fieldIndices,
                    collection,
                    fieldFailures,
                    conflicts.get(fullName)
                )
            );
        }
        return new EsField(NAMESPACE, DataType.OBJECT, roots, false, TimeSeriesFieldType.NONE);
    }
}
