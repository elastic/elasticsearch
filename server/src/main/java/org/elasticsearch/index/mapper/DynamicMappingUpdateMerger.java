/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Combines the dynamic mapping updates of consecutive documents into a single update.
 * <p>
 * Each update is computed against the mappings of the index, unaware of the updates before it. The combined update gives the
 * same mappings as applying the accepted updates one after the other, which requires that an update:
 * <ul>
 *   <li>defines the objects, fields and runtime fields that a previous update defines in exactly the same way,</li>
 *   <li>doesn't define an object or a field where a runtime field exists, or a runtime field where an object or a field
 *   exists, since a runtime field prevents the field of the same path from being mapped dynamically,</li>
 *   <li>keeps the number of added fields within the total fields limit.</li>
 * </ul>
 * The first update that is rejected closes the merger, so that the accepted updates are the ones of consecutive documents.
 * <p>
 * The field count is conservative: an existing object that an update repeats as the parent of a new field counts as a new field.
 */
public final class DynamicMappingUpdateMerger {

    private static final MergeReason REASON = MergeReason.MAPPING_UPDATE;
    private static final String PROPERTIES = "properties";
    private static final String RUNTIME = "runtime";

    private final MappingParser mappingParser;
    private final Predicate<String> isMappedField;
    private final CompressedXContent first;
    // the accepted updates that define something that the previous ones don't
    private final List<CompressedXContent> accepted = new ArrayList<>();
    private final NewFieldsBudget budget;
    private final ParseFieldLimits limits;
    // the source of the first update and of the accepted ones
    private Map<String, Object> definitions;
    // the paths of the objects and fields, and the names of the runtime fields, of the definitions
    private final Set<String> fieldPaths = new HashSet<>();
    private final Set<String> runtimeFieldNames = new HashSet<>();
    // null once a merge failed, which leaves the builder partially modified
    private MappingBuilder builder;
    private boolean closed;

    /**
     * @param isMappedField whether a path resolves to a field in the mappings of the index
     * @param first the update of the first document, always part of the combined update
     * @param remainingFields the number of fields that the mappings of the index can still take
     * @param totalFieldsLimit the total fields limit of the index
     */
    DynamicMappingUpdateMerger(
        MappingParser mappingParser,
        Predicate<String> isMappedField,
        CompressedXContent first,
        long remainingFields,
        long totalFieldsLimit
    ) {
        this.mappingParser = mappingParser;
        this.isMappedField = isMappedField;
        this.first = first;
        this.budget = NewFieldsBudget.throwing(remainingFields, totalFieldsLimit);
        this.limits = ParseFieldLimits.withBudget(budget);
        try {
            definitions = definitions(first);
            collectFieldPaths(definitions, "", fieldPaths);
            runtimeFieldNames.addAll(children(definitions, RUNTIME).keySet());
            if (fieldPaths.stream().anyMatch(isMappedField)) {
                closed = true;
            }
            MappingBuilder firstBuilder = parse(first);
            // merging into an empty builder counts the fields of the first update against the budget
            builder = firstBuilder.withoutMappers();
            builder.merge(firstBuilder, REASON, limits);
        } catch (Exception e) {
            builder = null;
            closed = true;
        }
    }

    /**
     * Returns true if an update that adds a field can still be accepted.
     */
    public boolean hasCapacity() {
        return closed == false && budget.hasCapacityFor(1);
    }

    /**
     * Adds the update of the next document.
     *
     * @return {@code false} if the update is rejected, in which case no further update is accepted
     */
    public boolean add(CompressedXContent update) {
        if (closed) {
            return false;
        }
        try {
            Map<String, Object> updateDefinitions = definitions(update);
            Set<String> updateFieldPaths = new HashSet<>();
            collectFieldPaths(updateDefinitions, "", updateFieldPaths);
            Set<String> updateRuntimeFieldNames = children(updateDefinitions, RUNTIME).keySet();
            if (identicalWhereOverlapping(definitions, updateDefinitions) == false
                || updateFieldPaths.stream().anyMatch(isMappedField)
                || Collections.disjoint(runtimeFieldNames, updateFieldPaths) == false
                || Collections.disjoint(fieldPaths, updateRuntimeFieldNames) == false) {
                closed = true;
                return false;
            }
            if (definesNothingNew(definitions, updateDefinitions)) {
                return true;
            }
            builder.merge(parse(update), REASON, limits);
            addMissing(definitions, updateDefinitions);
            fieldPaths.addAll(updateFieldPaths);
            runtimeFieldNames.addAll(updateRuntimeFieldNames);
        } catch (Exception e) {
            builder = null;
            closed = true;
            return false;
        }
        accepted.add(update);
        return true;
    }

    /**
     * Returns the combination of the first update and all the accepted ones.
     */
    public CompressedXContent merged() {
        if (accepted.isEmpty()) {
            return first;
        }
        if (builder == null) {
            builder = parse(first);
            for (CompressedXContent update : accepted) {
                builder.merge(parse(update), REASON, ParseFieldLimits.UNLIMITED);
            }
        }
        return builder.build(REASON).toCompressedXContent();
    }

    private MappingBuilder parse(CompressedXContent update) {
        return mappingParser.parseToBuilder(MapperService.SINGLE_MAPPING_NAME, REASON, update);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> definitions(CompressedXContent update) {
        Map<String, Object> source = MappingParser.convertToMap(update);
        if (source.size() == 1 && source.get(MapperService.SINGLE_MAPPING_NAME) instanceof Map<?, ?> root) {
            return (Map<String, Object>) root;
        }
        return source;
    }

    /**
     * Compares the definition of the root, of an object or of a field in two updates. They must have the same parameters, and
     * the same definition for the fields and runtime fields that both define.
     */
    private static boolean identicalWhereOverlapping(Map<String, Object> existing, Map<String, Object> incoming) {
        if (ownParameters(existing) != ownParameters(incoming)) {
            return false;
        }
        for (Map.Entry<String, Object> parameter : incoming.entrySet()) {
            String name = parameter.getKey();
            if (name.equals(PROPERTIES) || name.equals(RUNTIME)) {
                continue;
            }
            if (existing.containsKey(name) == false || Objects.equals(existing.get(name), parameter.getValue()) == false) {
                return false;
            }
        }
        for (Map.Entry<String, Object> runtimeField : children(incoming, RUNTIME).entrySet()) {
            Object existingRuntimeField = children(existing, RUNTIME).get(runtimeField.getKey());
            if (existingRuntimeField != null && existingRuntimeField.equals(runtimeField.getValue()) == false) {
                return false;
            }
        }
        Map<String, Object> existingFields = children(existing, PROPERTIES);
        for (Map.Entry<String, Object> field : children(incoming, PROPERTIES).entrySet()) {
            Object existingField = existingFields.get(field.getKey());
            if (existingField != null && identicalWhereOverlapping(asMap(existingField), asMap(field.getValue())) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the existing definitions have all the fields and runtime fields of the incoming update.
     */
    private static boolean definesNothingNew(Map<String, Object> existing, Map<String, Object> incoming) {
        if (children(existing, RUNTIME).keySet().containsAll(children(incoming, RUNTIME).keySet()) == false) {
            return false;
        }
        Map<String, Object> existingFields = children(existing, PROPERTIES);
        for (Map.Entry<String, Object> field : children(incoming, PROPERTIES).entrySet()) {
            Object existingField = existingFields.get(field.getKey());
            if (existingField == null || definesNothingNew(asMap(existingField), asMap(field.getValue())) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds the fields and runtime fields that only the incoming update defines.
     */
    private static void addMissing(Map<String, Object> existing, Map<String, Object> incoming) {
        Map<String, Object> incomingRuntimeFields = children(incoming, RUNTIME);
        if (incomingRuntimeFields.isEmpty() == false) {
            Map<String, Object> runtimeFields = asMap(existing.computeIfAbsent(RUNTIME, k -> new HashMap<>()));
            incomingRuntimeFields.forEach(runtimeFields::putIfAbsent);
        }
        Map<String, Object> incomingFields = children(incoming, PROPERTIES);
        if (incomingFields.isEmpty() == false) {
            Map<String, Object> fields = asMap(existing.computeIfAbsent(PROPERTIES, k -> new HashMap<>()));
            for (Map.Entry<String, Object> field : incomingFields.entrySet()) {
                Object existingField = fields.putIfAbsent(field.getKey(), field.getValue());
                if (existingField != null) {
                    addMissing(asMap(existingField), asMap(field.getValue()));
                }
            }
        }
    }

    private static void collectFieldPaths(Map<String, Object> definition, String prefix, Set<String> paths) {
        for (Map.Entry<String, Object> field : children(definition, PROPERTIES).entrySet()) {
            String path = prefix + field.getKey();
            paths.add(path);
            collectFieldPaths(asMap(field.getValue()), path + ".", paths);
        }
    }

    private static int ownParameters(Map<String, Object> definition) {
        return definition.size() - (definition.containsKey(PROPERTIES) ? 1 : 0) - (definition.containsKey(RUNTIME) ? 1 : 0);
    }

    private static Map<String, Object> children(Map<String, Object> definition, String name) {
        return definition.get(name) instanceof Map<?, ?> ? asMap(definition.get(name)) : Map.of();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object definition) {
        return (Map<String, Object>) definition;
    }
}
