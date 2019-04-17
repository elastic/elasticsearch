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

    private final CopyOnWriteHashMap<String, JsonFieldMapper> fullNameToJsonMapper;
    private final int maxJsonFieldDepth;

    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        aliasToConcreteName = new CopyOnWriteHashMap<>();
        fullNameToJsonMapper = new CopyOnWriteHashMap<>();
        maxJsonFieldDepth = 0;
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType,
                            CopyOnWriteHashMap<String, String> aliasToConcreteName,
                            CopyOnWriteHashMap<String, JsonFieldMapper> fullNameToJsonMapper,
                            int maxJsonFieldDepth) {
        this.fullNameToFieldType = fullNameToFieldType;
        this.aliasToConcreteName = aliasToConcreteName;
        this.fullNameToJsonMapper = fullNameToJsonMapper;
        this.maxJsonFieldDepth = maxJsonFieldDepth;
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
        CopyOnWriteHashMap<String, JsonFieldMapper> jsonMappers = this.fullNameToJsonMapper;

        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.name();
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            if (Objects.equals(fieldType, fullNameFieldType) == false) {
                fullName = fullName.copyAndPut(fieldType.name(), fieldType);
            }

            if (fieldMapper instanceof JsonFieldMapper) {
                jsonMappers = fullNameToJsonMapper.copyAndPut(fieldName, (JsonFieldMapper) fieldMapper);
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

        int maxFieldDepth = getMaxJsonFieldDepth(aliases, jsonMappers);

        return new FieldTypeLookup(fullName, aliases, jsonMappers, maxFieldDepth);
    }

    private static int getMaxJsonFieldDepth(CopyOnWriteHashMap<String, String> aliases,
                                            CopyOnWriteHashMap<String, JsonFieldMapper> jsonMappers) {
        int maxFieldDepth = 0;
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            String aliasName = entry.getKey();
            String path = entry.getValue();
            if (jsonMappers.containsKey(path)) {
                maxFieldDepth = Math.max(maxFieldDepth, fieldDepth(aliasName));
            }
        }

        for (String fieldName : jsonMappers.keySet()) {
            if (jsonMappers.containsKey(fieldName)) {
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
        for (int i = 0; i < field.length(); ++i) {
            if (field.charAt(i) == '.') {
                numDots++;
            }
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

        // If the mapping contains JSON fields, check if this could correspond
        // to a keyed JSON field of the form 'path_to_json_field.path_to_key'.
        return !fullNameToJsonMapper.isEmpty() ? getKeyedJsonField(field) : null;
    }

    /**
     * Check if the given field corresponds to a keyed JSON field of the form
     * 'path_to_json_field.path_to_key'. If so, returns a field type that can
     * be used to perform searches on this field.
     */
    private MappedFieldType getKeyedJsonField(String field) {
        int dotIndex = -1;
        int fieldDepth = 0;

        while (true) {
            if (++fieldDepth > maxJsonFieldDepth) {
                return null;
            }

            dotIndex = field.indexOf('.', dotIndex + 1);
            if (dotIndex < 0) {
                return null;
            }

            String parentField = field.substring(0, dotIndex);
            String concreteField = aliasToConcreteName.getOrDefault(parentField, parentField);
            JsonFieldMapper mapper = fullNameToJsonMapper.get(concreteField);

            if (mapper != null) {
                String key = field.substring(dotIndex + 1);
                return mapper.keyedFieldType(key);
            }
        }
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    public Collection<String> simpleMatchToFullName(String pattern) {
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

        if (fullNameToJsonMapper.isEmpty()) {
            return concreteFieldTypes;
        } else {
            Iterator<MappedFieldType> keyedJsonFieldTypes = fullNameToJsonMapper.values().stream()
                .<MappedFieldType>map(mapper -> mapper.keyedFieldType(""))
                .iterator();
            return Iterators.concat(concreteFieldTypes, keyedJsonFieldTypes);
        }
    }

    // Visible for testing.
    int maxJsonFieldDepth() {
        return maxJsonFieldDepth;
    }
}
