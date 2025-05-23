/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.regex.Regex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
final class FieldTypeLookup {
    private final Map<String, MappedFieldType> fullNameToFieldType;
    private final Map<String, String> fullSubfieldNameToParentPath;
    private final Map<String, DynamicFieldType> dynamicFieldTypes;

    /**
     * A map from field name to all fields whose content has been copied into it
     * through copy_to. A field only be present in the map if some other field
     * has listed it as a target of copy_to.
     *
     * For convenience, the set of copied fields includes the field itself.
     */
    private final Map<String, Set<String>> fieldToCopiedFields;
    /**
     * Fields that are destinations of copy_to meaning fields that
     * contain values copied from other fields.
     */
    private final Set<String> copyToDestinationFields;

    private final int maxParentPathDots;

    FieldTypeLookup(Collection<FieldMapper> fieldMappers, Collection<FieldAliasMapper> fieldAliasMappers) {
        this(fieldMappers, fieldAliasMappers, List.of(), List.of());
    }

    FieldTypeLookup(
        Collection<FieldMapper> fieldMappers,
        Collection<FieldAliasMapper> fieldAliasMappers,
        Collection<PassThroughObjectMapper> passThroughMappers,
        Collection<RuntimeField> runtimeFields
    ) {

        final Map<String, MappedFieldType> fullNameToFieldType = new HashMap<>();
        final Map<String, String> fullSubfieldNameToParentPath = new HashMap<>();
        final Map<String, DynamicFieldType> dynamicFieldTypes = new HashMap<>();
        final Map<String, Set<String>> fieldToCopiedFields = new HashMap<>();
        final Set<String> copiedFields = new HashSet<>();
        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.fullPath();
            MappedFieldType fieldType = fieldMapper.fieldType();
            fullNameToFieldType.put(fieldType.name(), fieldType);
            fieldMapper.sourcePathUsedBy().forEachRemaining(mapper -> fullSubfieldNameToParentPath.put(mapper.fullPath(), fieldName));
            if (fieldType instanceof DynamicFieldType) {
                dynamicFieldTypes.put(fieldType.name(), (DynamicFieldType) fieldType);
            }
            for (String targetField : fieldMapper.copyTo().copyToFields()) {
                copiedFields.add(targetField);

                Set<String> sourcePath = fieldToCopiedFields.get(targetField);
                if (sourcePath == null) {
                    Set<String> fieldCopiedFields = new HashSet<>();
                    fieldCopiedFields.add(targetField);
                    fieldToCopiedFields.put(targetField, fieldCopiedFields);
                }
                fieldToCopiedFields.get(targetField).add(fieldName);
            }
        }

        int maxParentPathDots = 0;
        for (String dynamicRoot : dynamicFieldTypes.keySet()) {
            maxParentPathDots = Math.max(maxParentPathDots, dotCount(dynamicRoot));
        }
        this.maxParentPathDots = maxParentPathDots;

        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String aliasName = fieldAliasMapper.fullPath();
            String path = fieldAliasMapper.targetPath();
            MappedFieldType fieldType = fullNameToFieldType.get(path);
            if (fieldType == null) {
                continue;
            }
            fullNameToFieldType.put(aliasName, fieldType);
            if (fieldType instanceof DynamicFieldType) {
                dynamicFieldTypes.put(aliasName, (DynamicFieldType) fieldType);
            }
        }

        // Pass-though subfields can be referenced without the prefix corresponding to the
        // PassThroughObjectMapper name. This is achieved by adding a second reference to their
        // MappedFieldType using the remaining suffix.
        Map<String, PassThroughObjectMapper> passThroughFieldAliases = new HashMap<>();
        for (PassThroughObjectMapper passThroughMapper : passThroughMappers) {
            for (Mapper subfield : passThroughMapper.mappers.values()) {
                if (subfield instanceof FieldMapper fieldMapper) {
                    String name = fieldMapper.leafName();
                    // Check for conflict between PassThroughObjectMapper subfields.
                    PassThroughObjectMapper conflict = passThroughFieldAliases.put(name, passThroughMapper);
                    if (conflict != null) {
                        if (conflict.priority() > passThroughMapper.priority()) {
                            // Keep the conflicting field if it has higher priority.
                            passThroughFieldAliases.put(name, conflict);
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, PassThroughObjectMapper> entry : passThroughFieldAliases.entrySet()) {
            String name = entry.getKey();
            if (fullNameToFieldType.containsKey(name)) {
                // There's an existing field or alias for the same field.
                continue;
            }
            Mapper mapper = entry.getValue().getMapper(name);
            if (mapper instanceof FieldMapper fieldMapper) {
                MappedFieldType fieldType = fieldMapper.fieldType();
                fullNameToFieldType.put(name, fieldType);
                if (fieldType instanceof DynamicFieldType) {
                    dynamicFieldTypes.put(name, (DynamicFieldType) fieldType);
                }
            }
        }

        for (MappedFieldType fieldType : RuntimeField.collectFieldTypes(runtimeFields).values()) {
            // this will override concrete fields with runtime fields that have the same name
            fullNameToFieldType.put(fieldType.name(), fieldType);
        }
        // make all fields into compact+fast immutable maps
        this.fullNameToFieldType = Map.copyOf(fullNameToFieldType);
        this.fullSubfieldNameToParentPath = Map.copyOf(fullSubfieldNameToParentPath);
        this.dynamicFieldTypes = Map.copyOf(dynamicFieldTypes);
        // make values into more compact immutable sets to save memory
        fieldToCopiedFields.entrySet().forEach(e -> e.setValue(Set.copyOf(e.getValue())));
        this.fieldToCopiedFields = Map.copyOf(fieldToCopiedFields);
        this.copyToDestinationFields = Set.copyOf(copiedFields);
    }

    public static int dotCount(String path) {
        int dotCount = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '.') {
                dotCount++;
            }
        }
        return dotCount;
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    MappedFieldType get(String field) {
        MappedFieldType fieldType = fullNameToFieldType.get(field);
        if (fieldType != null) {
            return fieldType;
        }
        return getDynamicField(field);
    }

    // for testing
    int getMaxParentPathDots() {
        return maxParentPathDots;
    }

    // Check if the given field corresponds to a dynamic key mapper of the
    // form 'path_to_field.path_to_key'. If so, returns a field type that
    // can be used to perform searches on this field. Otherwise returns null.
    private MappedFieldType getDynamicField(String field) {
        if (dynamicFieldTypes.isEmpty()) {
            // no parent fields defined
            return null;
        }
        int dotIndex = -1;
        int fieldDepth = -1;

        while (true) {
            if (++fieldDepth > maxParentPathDots) {
                return null;
            }

            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                return null;
            }

            String parentField = field.substring(0, dotIndex);
            DynamicFieldType dft = dynamicFieldTypes.get(parentField);
            if (dft != null && Objects.equals(field, parentField) == false) {
                String key = field.substring(dotIndex + 1);
                return dft.getChildFieldType(key);
            }
        }
    }

    /**
     * Returns a set of field names that match a regex-like pattern
     *
     * All field names in the returned set are guaranteed to resolve to a field
     */
    Set<String> getMatchingFieldNames(String pattern) {
        if (Regex.isMatchAllPattern(pattern)) {
            return fullNameToFieldType.keySet();
        }
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return get(pattern) == null ? Collections.emptySet() : Collections.singleton(pattern);
        }
        return fullNameToFieldType.keySet()
            .stream()
            .filter(field -> Regex.simpleMatch(pattern, field))
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Given a concrete field name, return its paths in the _source.
     *
     * For most fields, the source path is the same as the field itself. However
     * there are cases where a field's values are found elsewhere in the _source:
     *   - For a multi-field, the source path is the parent field.
     *   - One field's content could have been copied to another through copy_to.
     *
     * @param field The field for which to look up the _source path. Note that the field
     *              should be a concrete field and *not* an alias.
     * @return A set of paths in the _source that contain the field's values.
     */
    Set<String> sourcePaths(String field) {
        if (fullNameToFieldType.isEmpty()) {
            return Set.of();
        }

        // If the field is dynamically generated then return its full path
        MappedFieldType fieldType = getDynamicField(field);
        if (fieldType != null) {
            return Set.of(field);
        }

        String resolvedField = field;
        if (fullSubfieldNameToParentPath.containsKey(field)) {
            resolvedField = fullSubfieldNameToParentPath.get(field);
        }

        return fieldToCopiedFields.containsKey(resolvedField) ? fieldToCopiedFields.get(resolvedField) : Set.of(resolvedField);
    }

    /**
     * If field is a leaf multi-field return the path to the parent field. Otherwise, return null.
     */
    public String parentField(String field) {
        return fullSubfieldNameToParentPath.get(field);
    }

    public Set<String> getCopyToDestinationFields() {
        return copyToDestinationFields;
    }
}
