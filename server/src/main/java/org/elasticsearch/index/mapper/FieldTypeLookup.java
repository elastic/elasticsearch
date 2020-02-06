/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.regex.Regex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
class FieldTypeLookup implements Iterable<MappedFieldType> {

    final CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType;
    private final CopyOnWriteHashMap<String, String> aliasToConcreteName;
    private final CopyOnWriteHashMap<String, Set<String>> fieldToSourcePath;
    private final DynamicKeyFieldTypeLookup dynamicKeyLookup;


    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        aliasToConcreteName = new CopyOnWriteHashMap<>();
        fieldToSourcePath = new CopyOnWriteHashMap<>();
        dynamicKeyLookup = new DynamicKeyFieldTypeLookup();
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType,
                            CopyOnWriteHashMap<String, String> aliasToConcreteName,
                            CopyOnWriteHashMap<String, Set<String>> fieldToSourcePath,
                            DynamicKeyFieldTypeLookup dynamicKeyLookup) {
        this.fullNameToFieldType = fullNameToFieldType;
        this.aliasToConcreteName = aliasToConcreteName;
        this.fieldToSourcePath = fieldToSourcePath;
        this.dynamicKeyLookup = dynamicKeyLookup;
    }

    /**
     * Return a new instance that contains the union of this instance and the field types
     * from the provided mappers. If a field already exists, its field type will be updated
     * to use the new type from the given field mapper. Similarly if an alias already
     * exists, it will be updated to reference the field type from the new mapper.
     */
    public FieldTypeLookup copyAndAddAll(Collection<FieldMapper> fieldMappers,
                                         Collection<FieldAliasMapper> fieldAliasMappers) {

        CopyOnWriteHashMap<String, MappedFieldType> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, String> aliases = this.aliasToConcreteName;
        CopyOnWriteHashMap<String, Set<String>> sourcePaths = this.fieldToSourcePath;
        Map<String, DynamicKeyFieldMapper> dynamicKeyMappers = new HashMap<>();

        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.name();
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            if (Objects.equals(fieldType, fullNameFieldType) == false) {
                fullName = fullName.copyAndPut(fieldType.name(), fieldType);
            }

            if (fieldMapper instanceof DynamicKeyFieldMapper) {
                dynamicKeyMappers.put(fieldName, (DynamicKeyFieldMapper) fieldMapper);
            }

            for (String targetField : fieldMapper.copyTo().copyToFields()) {
                Set<String> sourcePath = sourcePaths.get(targetField);
                if (sourcePath == null) {
                    sourcePaths = sourcePaths.copyAndPut(targetField, Collections.singleton(fieldName));
                } else if (sourcePath.contains(fieldName) == false) {
                    Set<String> newSourcePath = new HashSet<>(sourcePath);
                    newSourcePath.add(fieldName);
                    sourcePaths = sourcePaths.copyAndPut(targetField, Collections.unmodifiableSet(newSourcePath));
                }
            }
        }

        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String aliasName = fieldAliasMapper.name();
            String path = fieldAliasMapper.path();

            String existingPath = aliases.get(aliasName);
            if (Objects.equals(path, existingPath) == false) {
                aliases = aliases.copyAndPut(aliasName, path);
            }
        }

        DynamicKeyFieldTypeLookup newDynamicKeyLookup = this.dynamicKeyLookup.copyAndAddAll(dynamicKeyMappers, aliases);
        return new FieldTypeLookup(fullName, aliases, sourcePaths, newDynamicKeyLookup);
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    public MappedFieldType get(String field) {
        String concreteField = aliasToConcreteName.getOrDefault(field, field);
        MappedFieldType fieldType = fullNameToFieldType.get(concreteField);
        if (fieldType != null) {
            return fieldType;
        }

        // If the mapping contains fields that support dynamic sub-key lookup, check
        // if this could correspond to a keyed field of the form 'path_to_field.path_to_key'.
        return dynamicKeyLookup.get(field);
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = new HashSet<>();
        for (MappedFieldType fieldType : this) {
            if (Regex.simpleMatch(pattern, fieldType.name())) {
                fields.add(fieldType.name());
            }
        }
        for (String aliasName : aliasToConcreteName.keySet()) {
            if (Regex.simpleMatch(pattern, aliasName)) {
                fields.add(aliasName);
            }
        }
        return fields;
    }

    public Set<String> sourcePath(String field) {
        String targetField = aliasToConcreteName.get(field);
        if (targetField != null) {
            return Collections.singleton(targetField);
        }

        String parentField = getParentField(field);
        if (parentField != null) {
            return Collections.singleton(parentField);
        }

        return fieldToSourcePath.getOrDefault(field, new HashSet<>());
    }

    private String getParentField(String field) {
        int lastDotIndex = field.lastIndexOf('.');
        if (lastDotIndex < 0) {
            return null;
        }

        String parentField = field.substring(0, lastDotIndex);
        return fullNameToFieldType.containsKey(parentField) ? parentField : null;
    }

    @Override
    public Iterator<MappedFieldType> iterator() {
        Iterator<MappedFieldType> concreteFieldTypes = fullNameToFieldType.values().iterator();
        Iterator<MappedFieldType> keyedFieldTypes = dynamicKeyLookup.fieldTypes();
        return Iterators.concat(concreteFieldTypes, keyedFieldTypes);
    }
}
