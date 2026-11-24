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
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Analysis-only schema for fields under a mapped {@code nested} object. It is requested only by a query that uses
 * {@code NESTED_EXPAND}, and it stays out of ordinary field resolution so a query without that command still hides
 * nested fields.
 */
public final class NestedScopeField extends EsField {
    public static final String NAMESPACE = "$$nested_scope";
    public static final String PREFIX = NAMESPACE + ".";

    /**
     * Whether {@code attribute} reads a child of a nested object. Inside {@code NESTED_ANY} such an attribute, and a cast of it,
     * carries the name of the private schema attribute it reads as its parent name, which is where this finds the prefix.
     */
    public static boolean readsChild(FieldAttribute attribute) {
        return attribute.parentName() != null && attribute.parentName().startsWith(PREFIX);
    }

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
    @Nullable
    private final EsField leaf;

    /**
     * @param conflict how the indices disagree on the nested mapping of this field, or {@code null} if they agree. Only a
     *                 {@code NESTED_EXPAND} that reads the field fails with it.
     * @param leaf the field that ordinary resolution makes of the occurrences of a {@link Kind#VALUE}, or {@code null} for the
     *             other kinds
     */
    public NestedScopeField(
        String name,
        DataType type,
        Map<String, EsField> properties,
        String path,
        Kind kind,
        @Nullable String conflict,
        @Nullable EsField leaf
    ) {
        super(name, type, properties, false, TimeSeriesFieldType.NONE);
        this.path = path;
        this.kind = kind;
        this.conflict = conflict;
        this.leaf = leaf;
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

    /** The field ordinary resolution makes of this field's occurrences, or {@code null} unless this is a {@link Kind#VALUE}. */
    @Nullable
    public EsField leaf() {
        return leaf;
    }

    @Override
    public EsField withProperties(Map<String, EsField> properties) {
        return new NestedScopeField(getName(), getDataType(), properties, path, kind, conflict, leaf);
    }

    @Override
    public EsField withDataType(DataType esDataType) {
        return esDataType == getDataType()
            ? this
            : new NestedScopeField(
                getName(),
                esDataType,
                getProperties(),
                path,
                kind,
                conflict,
                leaf == null ? null : leaf.withDataType(esDataType)
            );
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
            && kind == other.kind
            && Objects.equals(conflict, other.conflict)
            && Objects.equals(leaf, other.leaf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, kind, conflict, leaf);
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

    /** Decides whether a name is the key of a field an index maps in nested scope, as the ordinary resolution decides it outside it. */
    @FunctionalInterface
    public interface KeyResolver {
        /** The field the name is as a key of a field in {@code fields} that is in nested scope, or {@code null} if it is none. */
        @Nullable
        IndexFieldCapabilities key(String name, Map<String, IndexFieldCapabilities> fields);
    }

    /**
     * How the field of one name under a nested object is made from what the indices report for it. Ordinary resolution implements
     * it, so a nested leaf gets the same type, conflict handling and specialization as a field outside the nested scope.
     */
    @FunctionalInterface
    public interface LeafMerger {
        /**
         * @param name the last part of the field's name
         * @param fullName the whole name
         * @param occurrences what each distinct mapping reports for the field
         */
        EsField merge(String name, String fullName, List<IndexFieldCapabilities> occurrences);
    }

    /**
     * Builds a separate schema so nested fields cannot introduce conflicts into ordinary field resolution.
     * Returns {@code null} when no index maps a nested field.
     * <p>
     * Indices with the same mapping hash have the same fields, so only the first of them by name is read. The work is linear in
     * the number of fields of the distinct mappings, however many nested paths they hold.
     * <p>
     * The schema is built in three steps. {@link MappingFacts} records what the indices map, without judging it. {@code resolve}
     * decides each field's kind and conflict from those facts, and leaves the leaf's own field to {@code merger}. {@code tree} lays
     * the decided fields out as the analyzer reads them.
     */
    @Nullable
    public static EsField namespace(List<FieldCapabilitiesIndexResponse> responses, LeafMerger merger) {
        return namespace(responses, merger, Set.of(), (name, fields) -> null);
    }

    /**
     * As {@link #namespace(List, LeafMerger)}, with each of {@code keyNames} that {@code keys} reports as a key of a field in nested
     * scope, in an index, a child of the collection that field is in.
     */
    @Nullable
    public static EsField namespace(
        List<FieldCapabilitiesIndexResponse> responses,
        LeafMerger merger,
        Set<String> keyNames,
        KeyResolver keys
    ) {
        MappingFacts facts = MappingFacts.collect(distinctMappings(responses), keyNames, keys);
        if (facts.scoped.isEmpty()) {
            return null;
        }
        return tree(resolve(facts, merger));
    }

    /** The responses in index name order, leaving out each one whose mapping hash an earlier response already has. */
    private static List<FieldCapabilitiesIndexResponse> distinctMappings(List<FieldCapabilitiesIndexResponse> responses) {
        List<FieldCapabilitiesIndexResponse> sorted = new ArrayList<>(responses);
        sorted.sort(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName));
        Set<String> seenMappings = new HashSet<>();
        List<FieldCapabilitiesIndexResponse> distinct = new ArrayList<>(sorted.size());
        for (var response : sorted) {
            String hash = response.getIndexMappingHash();
            if (hash == null || seenMappings.add(hash)) {
                distinct.add(response);
            }
        }
        return distinct;
    }

    /** What the indices map for one field in nested scope. */
    private static final class ScopedMapping {
        final String firstPath;
        final String firstIndex;
        final List<IndexFieldCapabilities> caps = new ArrayList<>();
        @Nullable
        String pathDisagreement;

        ScopedMapping(String firstPath, String firstIndex) {
            this.firstPath = firstPath;
            this.firstIndex = firstIndex;
        }
    }

    /** A nested path that an index maps as something other than {@code nested}. */
    private record OtherMapping(String type, String index) {}

    /** What the indices map, before any of it is judged. */
    private static final class MappingFacts {
        /** Every field under a nested object in some index, by full name. */
        final Map<String, ScopedMapping> scoped = new TreeMap<>();
        /** For each nested path, the first index that maps it as something other than {@code nested}. */
        final Map<String, OtherMapping> otherMappings = new HashMap<>();

        static MappingFacts collect(List<FieldCapabilitiesIndexResponse> responses, Set<String> keyNames, KeyResolver keys) {
            MappingFacts facts = new MappingFacts();
            for (var response : responses) {
                Map<String, IndexFieldCapabilities> fields = response.get();
                for (var fc : fields.values()) {
                    String path = pathFor(fc.name(), fields);
                    if (path == null) {
                        continue;
                    }
                    ScopedMapping mapping = facts.scoped.computeIfAbsent(fc.name(), k -> new ScopedMapping(path, response.getIndexName()));
                    if (mapping.firstPath.equals(path) == false && mapping.pathDisagreement == null) {
                        mapping.pathDisagreement = Strings.format(
                            "its nested path is [%s] in [%s] but [%s] in [%s]",
                            mapping.firstPath,
                            mapping.firstIndex,
                            path,
                            response.getIndexName()
                        );
                    }
                    mapping.caps.add(fc);
                }
            }
            // A name that an index maps as a field is that field in the indices where it is a key: they read the key as its type
            Set<String> mappedNames = new HashSet<>();
            for (var entry : facts.scoped.entrySet()) {
                if (entry.getValue().caps.stream()
                    .anyMatch(fc -> fc.type().equals("object") == false && fc.type().equals("nested") == false)) {
                    mappedNames.add(entry.getKey());
                }
            }
            for (var response : responses) {
                Map<String, IndexFieldCapabilities> fields = response.get();
                for (String name : keyNames) {
                    IndexFieldCapabilities key = mappedNames.contains(name) ? null : keys.key(name, fields);
                    if (key == null) {
                        continue;
                    }
                    String path = pathFor(name, fields);
                    ScopedMapping mapping = facts.scoped.computeIfAbsent(name, k -> new ScopedMapping(path, response.getIndexName()));
                    if (mapping.firstPath.equals(path) == false && mapping.pathDisagreement == null) {
                        mapping.pathDisagreement = Strings.format(
                            "its nested path is [%s] in [%s] but [%s] in [%s]",
                            mapping.firstPath,
                            mapping.firstIndex,
                            path,
                            response.getIndexName()
                        );
                    }
                    mapping.caps.add(key);
                }
            }
            Set<String> paths = new HashSet<>();
            facts.scoped.values().forEach(mapping -> paths.add(mapping.firstPath));
            for (var response : responses) {
                for (var fc : response.get().values()) {
                    if (fc.type().equals("nested") == false && paths.contains(fc.name())) {
                        facts.otherMappings.putIfAbsent(fc.name(), new OtherMapping(fc.type(), response.getIndexName()));
                    }
                }
            }
            return facts;
        }
    }

    private record ResolvedField(
        String fullName,
        DataType type,
        Kind kind,
        String path,
        @Nullable String conflict,
        @Nullable EsField leaf
    ) {}

    private static List<ResolvedField> resolve(MappingFacts facts, LeafMerger merger) {
        List<ResolvedField> resolved = new ArrayList<>(facts.scoped.size());
        for (var entry : facts.scoped.entrySet()) {
            String fullName = entry.getKey();
            ScopedMapping mapping = entry.getValue();
            String conflict = conflict(mapping, facts);
            boolean anyCollection = mapping.caps.stream().anyMatch(fc -> fc.type().equals("nested"));
            if (anyCollection) {
                boolean allCollection = mapping.caps.stream().allMatch(fc -> fc.type().equals("nested"));
                if (allCollection) {
                    resolved.add(new ResolvedField(fullName, DataType.SOURCE, Kind.COLLECTION, mapping.firstPath, conflict, null));
                } else {
                    EsField unsupported = new UnsupportedEsField(fullName.substring(fullName.lastIndexOf('.') + 1), List.of("nested"));
                    resolved.add(new ResolvedField(fullName, DataType.UNSUPPORTED, Kind.VALUE, mapping.firstPath, conflict, unsupported));
                }
                continue;
            }
            String name = fullName.substring(fullName.lastIndexOf('.') + 1);
            EsField leaf = merger.merge(name, fullName, mapping.caps);
            DataType type = leaf.getDataType();
            if (type == DataType.OBJECT) {
                resolved.add(new ResolvedField(fullName, type, Kind.OBJECT, mapping.firstPath, conflict, null));
            } else {
                resolved.add(new ResolvedField(fullName, type, Kind.VALUE, mapping.firstPath, conflict, leaf));
            }
        }
        return resolved;
    }

    /**
     * A field whose nested path differs between indices is reported by that difference. Otherwise, a path that another index maps
     * as an object, or anything else, has no child documents there to read.
     */
    @Nullable
    private static String conflict(ScopedMapping mapping, MappingFacts facts) {
        if (mapping.pathDisagreement != null) {
            return mapping.pathDisagreement;
        }
        OtherMapping other = facts.otherMappings.get(mapping.firstPath);
        if (other == null) {
            return null;
        }
        return Strings.format(
            "[%s] is [nested] in [%s] but [%s] in [%s]",
            mapping.firstPath,
            facts.scoped.get(mapping.firstPath).firstIndex,
            other.type(),
            other.index()
        );
    }

    private static EsField tree(List<ResolvedField> fields) {
        Map<String, EsField> roots = new TreeMap<>();
        for (ResolvedField field : fields) {
            Map<String, EsField> parent = roots;
            String[] parts = field.fullName().split("\\.");
            for (int i = 0; i < parts.length - 1; i++) {
                EsField object = parent.computeIfAbsent(
                    parts[i],
                    k -> new EsField(k, DataType.OBJECT, new TreeMap<>(), false, TimeSeriesFieldType.NONE)
                );
                parent = object.getProperties();
            }
            String name = parts[parts.length - 1];
            Map<String, EsField> properties = parent.containsKey(name) ? parent.get(name).getProperties() : new TreeMap<>();
            parent.put(
                name,
                new NestedScopeField(name, field.type(), properties, field.path(), field.kind(), field.conflict(), field.leaf())
            );
        }
        return new EsField(NAMESPACE, DataType.OBJECT, roots, false, TimeSeriesFieldType.NONE);
    }
}
