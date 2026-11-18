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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

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
     * <p>
     * Indices with the same mapping hash have the same fields, so only the first of them by name is read. The work is linear in
     * the number of fields of the distinct mappings, however many nested paths they hold.
     * <p>
     * The schema is built in three steps. {@link MappingFacts} records what the indices map, without judging it. {@code resolve}
     * decides each field's type, kind and conflict from those facts. {@code tree} lays the decided fields out as the analyzer reads
     * them. {@code typeOf} is how ordinary field resolution turns one index's capabilities into a type, so a nested leaf is typed
     * the same way an ordinary one is.
     */
    @Nullable
    public static EsField namespace(List<FieldCapabilitiesIndexResponse> responses, Function<IndexFieldCapabilities, DataType> typeOf) {
        MappingFacts facts = MappingFacts.collect(distinctMappings(responses));
        if (facts.scoped.isEmpty()) {
            return null;
        }
        return tree(resolve(facts, typeOf));
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

        static MappingFacts collect(List<FieldCapabilitiesIndexResponse> responses) {
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

    private record ResolvedField(String fullName, DataType type, Kind kind, String path, @Nullable String conflict) {}

    private static List<ResolvedField> resolve(MappingFacts facts, Function<IndexFieldCapabilities, DataType> typeOf) {
        List<ResolvedField> resolved = new ArrayList<>(facts.scoped.size());
        for (var entry : facts.scoped.entrySet()) {
            ScopedMapping mapping = entry.getValue();
            boolean collection = mapping.caps.stream().allMatch(fc -> fc.type().equals("nested"));
            DataType type = collection ? DataType.SOURCE : leafType(mapping.caps, typeOf);
            Kind kind = collection ? Kind.COLLECTION : type == DataType.OBJECT ? Kind.OBJECT : Kind.VALUE;
            resolved.add(new ResolvedField(entry.getKey(), type, kind, mapping.firstPath, conflict(mapping, facts)));
        }
        return resolved;
    }

    /**
     * Small numerics widen because a nested child is read as the wider column. Indices that disagree on the widened type leave
     * the field {@link DataType#UNSUPPORTED}, as nothing here can cast it.
     */
    private static DataType leafType(List<IndexFieldCapabilities> caps, Function<IndexFieldCapabilities, DataType> typeOf) {
        DataType first = typeOf.apply(caps.getFirst()).widenSmallNumeric();
        for (IndexFieldCapabilities fc : caps) {
            if (typeOf.apply(fc).widenSmallNumeric() != first) {
                return DataType.UNSUPPORTED;
            }
        }
        return first;
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
            parent.put(name, new NestedScopeField(name, field.type(), properties, field.path(), field.kind(), field.conflict()));
        }
        return new EsField(NAMESPACE, DataType.OBJECT, roots, false, TimeSeriesFieldType.NONE);
    }
}
