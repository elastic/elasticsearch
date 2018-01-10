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
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
class FieldTypeLookup implements Iterable<MappedFieldType> {

    /** Full field name to field type */
    final CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType;

    /** Full field name to types containing a mapping for this full name. */
    final CopyOnWriteHashMap<String, Set<String>> fullNameToTypes;

    /** Create a new empty instance. */
    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        fullNameToTypes = new CopyOnWriteHashMap<>();
    }

    private FieldTypeLookup(
            CopyOnWriteHashMap<String, MappedFieldType> fullName,
            CopyOnWriteHashMap<String, Set<String>> fullNameToTypes) {
        this.fullNameToFieldType = fullName;
        this.fullNameToTypes = fullNameToTypes;
    }

    private static CopyOnWriteHashMap<String, Set<String>> addType(CopyOnWriteHashMap<String, Set<String>> map, String key, String type) {
        Set<String> types = map.get(key);
        if (types == null) {
            return map.copyAndPut(key, Collections.singleton(type));
        } else if (types.contains(type)) {
            // noting to do
            return map;
        } else {
            Set<String> newTypes = new HashSet<>(types.size() + 1);
            newTypes.addAll(types);
            newTypes.add(type);
            assert newTypes.size() == types.size() + 1;
            newTypes = Collections.unmodifiableSet(newTypes);
            return map.copyAndPut(key, newTypes);
        }
    }

    /**
     * Return a new instance that contains the union of this instance and the field types
     * from the provided fields. If a field already exists, the field type will be updated
     * to use the new mappers field type.
     */
    public FieldTypeLookup copyAndAddAll(String type, Collection<FieldMapper> fieldMappers, boolean updateAllTypes) {
        Objects.requireNonNull(type, "type must not be null");
        if (MapperService.DEFAULT_MAPPING.equals(type)) {
            throw new IllegalArgumentException("Default mappings should not be added to the lookup");
        }

        CopyOnWriteHashMap<String, MappedFieldType> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, Set<String>> fullNameToTypes = this.fullNameToTypes;

        for (FieldMapper fieldMapper : fieldMappers) {
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            // is the update even legal?
            checkCompatibility(type, fieldMapper, updateAllTypes);

            if (fieldType.equals(fullNameFieldType) == false) {
                fullName = fullName.copyAndPut(fieldType.name(), fieldMapper.fieldType());
            }

            fullNameToTypes = addType(fullNameToTypes, fieldType.name(), type);
        }
        return new FieldTypeLookup(fullName, fullNameToTypes);
    }

    private static boolean beStrict(String type, Set<String> types, boolean updateAllTypes) {
        assert types.size() >= 1;
        if (updateAllTypes) {
            return false;
        } else if (types.size() == 1 && types.contains(type)) {
            // we are implicitly updating all types
            return false;
        } else {
            return true;
        }
    }

    /**
     * Checks if the given field type is compatible with an existing field type.
     * An IllegalArgumentException is thrown in case of incompatibility.
     * If updateAllTypes is true, only basic compatibility is checked.
     */
    private void checkCompatibility(String type, FieldMapper fieldMapper, boolean updateAllTypes) {
        MappedFieldType fieldType = fullNameToFieldType.get(fieldMapper.fieldType().name());
        if (fieldType != null) {
            List<String> conflicts = new ArrayList<>();
            final Set<String> types = fullNameToTypes.get(fieldMapper.fieldType().name());
            boolean strict = beStrict(type, types, updateAllTypes);
            fieldType.checkCompatibility(fieldMapper.fieldType(), conflicts, strict);
            if (conflicts.isEmpty() == false) {
                throw new IllegalArgumentException("Mapper for [" + fieldMapper.fieldType().name() + "] conflicts with existing mapping in other types:\n" + conflicts.toString());
            }
        }
    }

    /** Returns the field for the given field */
    public MappedFieldType get(String field) {
        return fullNameToFieldType.get(field);
    }

    /** Get the set of types that have a mapping for the given field. */
    public Set<String> getTypes(String field) {
        Set<String> types = fullNameToTypes.get(field);
        if (types == null) {
            types = Collections.emptySet();
        }
        return types;
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
        return fields;
    }

    @Override
    public Iterator<MappedFieldType> iterator() {
        return fullNameToFieldType.values().iterator();
    }
}
