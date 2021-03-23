/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.regex.Regex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
final class FieldTypeLookup {
    private final Map<String, MappedFieldType> fullNameToFieldType = new HashMap<>();

    /**
     * A map from field name to all fields whose content has been copied into it
     * through copy_to. A field only be present in the map if some other field
     * has listed it as a target of copy_to.
     *
     * For convenience, the set of copied fields includes the field itself.
     */
    private final Map<String, Set<String>> fieldToCopiedFields = new HashMap<>();
    private final DynamicKeyFieldTypeLookup dynamicKeyLookup;

    FieldTypeLookup(
        Collection<FieldMapper> fieldMappers,
        Collection<FieldAliasMapper> fieldAliasMappers,
        Collection<RuntimeFieldType> runtimeFieldTypes
    ) {
        Map<String, DynamicKeyFieldMapper> dynamicKeyMappers = new HashMap<>();

        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.name();
            MappedFieldType fieldType = fieldMapper.fieldType();
            fullNameToFieldType.put(fieldType.name(), fieldType);
            if (fieldMapper instanceof DynamicKeyFieldMapper) {
                dynamicKeyMappers.put(fieldName, (DynamicKeyFieldMapper) fieldMapper);
            }

            for (String targetField : fieldMapper.copyTo().copyToFields()) {
                Set<String> sourcePath = fieldToCopiedFields.get(targetField);
                if (sourcePath == null) {
                    Set<String> copiedFields = new HashSet<>();
                    copiedFields.add(targetField);
                    fieldToCopiedFields.put(targetField, copiedFields);
                }
                fieldToCopiedFields.get(targetField).add(fieldName);
            }
        }

        final Map<String, String> aliasToConcreteName = new HashMap<>();
        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String aliasName = fieldAliasMapper.name();
            String path = fieldAliasMapper.path();
            aliasToConcreteName.put(aliasName, path);
            fullNameToFieldType.put(aliasName, fullNameToFieldType.get(path));
        }

        for (RuntimeFieldType runtimeField : runtimeFieldTypes) {
            MappedFieldType runtimeFieldType = runtimeField.asMappedFieldType();
            //this will override concrete fields with runtime fields that have the same name
            fullNameToFieldType.put(runtimeFieldType.name(), runtimeFieldType);
        }

        this.dynamicKeyLookup = new DynamicKeyFieldTypeLookup(dynamicKeyMappers, aliasToConcreteName);
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    MappedFieldType get(String field) {
        MappedFieldType fieldType = fullNameToFieldType.get(field);
        if (fieldType != null) {
            return fieldType;
        }

        // If the mapping contains fields that support dynamic sub-key lookup, check
        // if this could correspond to a keyed field of the form 'path_to_field.path_to_key'.
        return dynamicKeyLookup.get(field);
    }

    /**
     * Returns all the mapped field types.
     */
    Collection<MappedFieldType> get() {
        return fullNameToFieldType.values();
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return Collections.singleton(pattern);
        }
        Set<String> fields = new HashSet<>();
        for (String field : fullNameToFieldType.keySet()) {
            if (Regex.simpleMatch(pattern, field)) {
                fields.add(field);
            }
        }
        return fields;
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
        String resolvedField = field;
        int lastDotIndex = field.lastIndexOf('.');
        if (lastDotIndex > 0) {
            String parentField = field.substring(0, lastDotIndex);
            if (fullNameToFieldType.containsKey(parentField)) {
                resolvedField = parentField;
            }
        }

        return fieldToCopiedFields.containsKey(resolvedField)
            ? fieldToCopiedFields.get(resolvedField)
            : Set.of(resolvedField);
    }

    /**
     * Returns an {@link Iterable} over all the distinct field types matching the provided predicate.
     * When a field alias is present, {@link #get(String)} returns the same {@link MappedFieldType} no matter if it's  looked up
     * providing the field name or the alias name. In this case the {@link Iterable} returned by this method will contain only one
     * instance of the field type. Note that filtering by name is not reliable as it does not take into account field aliases.
     */
    Iterable<MappedFieldType> filter(Predicate<MappedFieldType> predicate) {
        return () -> Stream.concat(fullNameToFieldType.values().stream(), dynamicKeyLookup.fieldTypes())
            .distinct().filter(predicate).iterator();
    }
}
