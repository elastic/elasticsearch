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

    private final CopyOnWriteHashMap<String, DynamicKeyFieldMapper> dynamicKeyMappers;

    /**
     * The maximum field depth of any mapper that implements {@link DynamicKeyFieldMapper}.
     * Allows us stop searching for a 'dynamic key' mapper as soon as we've passed the maximum
     * possible field depth.
     */
    private final int maxDynamicKeyDepth;

    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        aliasToConcreteName = new CopyOnWriteHashMap<>();
        dynamicKeyMappers = new CopyOnWriteHashMap<>();
        maxDynamicKeyDepth = 0;
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType,
                            CopyOnWriteHashMap<String, String> aliasToConcreteName,
                            CopyOnWriteHashMap<String, DynamicKeyFieldMapper> dynamicKeyMappers,
                            int maxDynamicKeyDepth) {
        this.fullNameToFieldType = fullNameToFieldType;
        this.aliasToConcreteName = aliasToConcreteName;
        this.dynamicKeyMappers = dynamicKeyMappers;
        this.maxDynamicKeyDepth = maxDynamicKeyDepth;
    }

    /**
     * Return a new instance that contains the union of this instance and the field types
     * from the provided mappers. If a field already exists, its field type will be updated
     * to use the new type from the given field mapper. Similarly if an alias already
     * exists, it will be updated to reference the field type from the new mapper.
     */
    public FieldTypeLookup copyAndAddAll(String type,
                                         Collection<FieldMapper> fieldMappers,
                                         Collection<FieldAliasMapper> fieldAliasMappers) {
        Objects.requireNonNull(type, "type must not be null");
        if (MapperService.DEFAULT_MAPPING.equals(type)) {
            throw new IllegalArgumentException("Default mappings should not be added to the lookup");
        }

        CopyOnWriteHashMap<String, MappedFieldType> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, String> aliases = this.aliasToConcreteName;
        CopyOnWriteHashMap<String, DynamicKeyFieldMapper> dynamicKeyMappers = this.dynamicKeyMappers;

        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.name();
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            if (Objects.equals(fieldType, fullNameFieldType) == false) {
                fullName = fullName.copyAndPut(fieldType.name(), fieldType);
            }

            if (fieldMapper instanceof DynamicKeyFieldMapper) {
                DynamicKeyFieldMapper dynamicKeyMapper = (DynamicKeyFieldMapper) fieldMapper;
                dynamicKeyMappers = dynamicKeyMappers.copyAndPut(fieldName, dynamicKeyMapper);
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

        int maxDynamicKeyDepth = getMaxDynamicKeyDepth(aliases, dynamicKeyMappers);

        return new FieldTypeLookup(fullName, aliases, dynamicKeyMappers, maxDynamicKeyDepth);
    }

    private static int getMaxDynamicKeyDepth(CopyOnWriteHashMap<String, String> aliases,
                                             CopyOnWriteHashMap<String, DynamicKeyFieldMapper> dynamicKeyMappers) {
        int maxFieldDepth = 0;
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            String aliasName = entry.getKey();
            String path = entry.getValue();
            if (dynamicKeyMappers.containsKey(path)) {
                maxFieldDepth = Math.max(maxFieldDepth, fieldDepth(aliasName));
            }
        }

        for (String fieldName : dynamicKeyMappers.keySet()) {
            if (dynamicKeyMappers.containsKey(fieldName)) {
                maxFieldDepth = Math.max(maxFieldDepth, fieldDepth(fieldName));
            }
        }

        return maxFieldDepth;
    }

    /**
     * Computes the total depth of this field by counting the number of parent fields
     * in its path. As an example, the field 'parent1.parent2.field' has depth 3.
     */
    private static int fieldDepth(String field) {
        int numDots = 0;
        int dotIndex = -1;
        while (true) {
            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                break;
            }
            numDots++;
        }
        return numDots + 1;
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
        return !dynamicKeyMappers.isEmpty() ? getKeyedFieldType(field) : null;
    }

    /**
     * Check if the given field corresponds to a dynamic lookup mapper of the
     * form 'path_to_field.path_to_key'. If so, returns a field type that
     * can be used to perform searches on this field.
     */
    private MappedFieldType getKeyedFieldType(String field) {
        int dotIndex = -1;
        int fieldDepth = 0;

        while (true) {
            if (++fieldDepth > maxDynamicKeyDepth) {
                return null;
            }

            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                return null;
            }

            String parentField = field.substring(0, dotIndex);
            String concreteField = aliasToConcreteName.getOrDefault(parentField, parentField);
            DynamicKeyFieldMapper mapper = dynamicKeyMappers.get(concreteField);

            if (mapper != null) {
                String key = field.substring(dotIndex + 1);
                return mapper.keyedFieldType(key);
            }
        }
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

    @Override
    public Iterator<MappedFieldType> iterator() {
        Iterator<MappedFieldType> concreteFieldTypes = fullNameToFieldType.values().iterator();

        if (dynamicKeyMappers.isEmpty()) {
            return concreteFieldTypes;
        } else {
            Iterator<MappedFieldType> keyedFieldTypes = dynamicKeyMappers.values().stream()
                .<MappedFieldType>map(mapper -> mapper.keyedFieldType(""))
                .iterator();
            return Iterators.concat(concreteFieldTypes, keyedFieldTypes);
        }
    }

    // Visible for testing.
    int maxKeyedLookupDepth() {
        return maxDynamicKeyDepth;
    }
}
