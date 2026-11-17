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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Analysis-only schema for fields under a mapped {@code nested} object. It is requested only by a query that uses
 * {@code NESTED_EXPAND}, and it stays out of ordinary field resolution so a query without that command still hides
 * nested fields.
 */
public final class NestedScopeField extends EsField {
    public static final String NAMESPACE = "$$nested_reductions";
    public static final String PREFIX = NAMESPACE + ".";

    /**
     * How a field in this namespace relates to the nested collection that owns its documents. Ordinary resolution, scope
     * construction, and type-conflict handling all use this classification.
     */
    public enum Kind {
        /** The mapped nested object. */
        COLLECTION,
        /** An object under a collection that is not itself nested. */
        OBJECT,
        /** A physical leaf stored on the collection's child documents. */
        VALUE
    }

    private final String path;
    private final Kind kind;
    @Nullable
    private final String conflict;

    /**
     * @param conflict how the indices disagree on the nested mapping of this field, or {@code null} if they agree. Only a
     *                 {@code NESTED_EXPAND} that reads the field fails with it.
     */
    public NestedScopeField(
        String name,
        DataType type,
        Map<String, EsField> properties,
        String path,
        Kind kind,
        @Nullable String conflict
    ) {
        super(name, type, properties, false, TimeSeriesFieldType.NONE);
        this.path = path;
        this.kind = kind;
        this.conflict = conflict;
    }

    public String path() {
        return path;
    }

    public Kind kind() {
        return kind;
    }

    @Nullable
    public String conflict() {
        return conflict;
    }

    @Override
    public EsField withProperties(Map<String, EsField> properties) {
        return new NestedScopeField(getName(), getDataType(), properties, path, kind, conflict);
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
        return path.equals(other.path) && kind == other.kind && Objects.equals(conflict, other.conflict);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, kind, conflict);
    }

    /**
     * Whether {@code field} belongs to the private nested scope rather than to ordinary resolution.
     * Each shard decides how to read a child it maps as a runtime field.
     */
    public static boolean inNestedScope(IndexFieldCapabilities field, Map<String, IndexFieldCapabilities> fields) {
        return pathFor(field.name(), fields) != null;
    }

    /** The nested collection that owns {@code name} in this index, or {@code null} when none does. */
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
     * Builds a separate schema so nested fields cannot introduce conflicts into ordinary field resolution.
     * Returns {@code null} when no index maps a nested field.
     */
    @Nullable
    public static EsField namespace(List<FieldCapabilitiesIndexResponse> responses) {
        Map<String, List<IndexFieldCapabilities>> caps = new TreeMap<>();
        Map<String, String> paths = new HashMap<>();
        Map<String, String> pathIndex = new HashMap<>();
        Map<String, String> conflicts = new HashMap<>();
        List<FieldCapabilitiesIndexResponse> sorted = new ArrayList<>(responses);
        sorted.sort(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName));
        for (var response : sorted) {
            for (var fc : response.get().values()) {
                if (inNestedScope(fc, response.get()) == false) {
                    continue;
                }
                String path = pathFor(fc.name(), response.get());
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
            boolean typeConflict = collection == false
                && entry.getValue()
                    .stream()
                    .anyMatch(fc -> EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType()).widenSmallNumeric() != firstType);
            DataType type = typeConflict ? DataType.UNSUPPORTED : firstType;
            Kind kind = collection ? Kind.COLLECTION : type == DataType.OBJECT ? Kind.OBJECT : Kind.VALUE;
            Map<String, EsField> properties = parent.containsKey(name) ? parent.get(name).getProperties() : new TreeMap<>();
            parent.put(name, new NestedScopeField(name, type, properties, paths.get(fullName), kind, conflicts.get(fullName)));
        }
        return new EsField(NAMESPACE, DataType.OBJECT, roots, false, TimeSeriesFieldType.NONE);
    }
}
