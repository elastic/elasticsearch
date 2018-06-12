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

    /** Create a new empty instance. */
    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldType> fullName) {
        this.fullNameToFieldType = fullName;
    }

    /**
     * Return a new instance that contains the union of this instance and the field types
     * from the provided fields. If a field already exists, the field type will be updated
     * to use the new mappers field type.
     */
    public FieldTypeLookup copyAndAddAll(String type, Collection<FieldMapper> fieldMappers) {
        Objects.requireNonNull(type, "type must not be null");
        if (MapperService.DEFAULT_MAPPING.equals(type)) {
            throw new IllegalArgumentException("Default mappings should not be added to the lookup");
        }

        CopyOnWriteHashMap<String, MappedFieldType> fullName = this.fullNameToFieldType;

        for (FieldMapper fieldMapper : fieldMappers) {
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            if (fullNameFieldType == null) {
                // introduction of a new field
                fullName = fullName.copyAndPut(fieldType.name(), fieldMapper.fieldType());
            } else {
                // modification of an existing field
                checkCompatibility(fullNameFieldType, fieldType);
                if (fieldType.equals(fullNameFieldType) == false) {
                    fullName = fullName.copyAndPut(fieldType.name(), fieldMapper.fieldType());
                }
            }
        }
        return new FieldTypeLookup(fullName);
    }

    /**
     * Checks if the given field type is compatible with an existing field type.
     * An IllegalArgumentException is thrown in case of incompatibility.
     */
    private void checkCompatibility(MappedFieldType existingFieldType, MappedFieldType newFieldType) {
        List<String> conflicts = new ArrayList<>();
        existingFieldType.checkCompatibility(newFieldType, conflicts);
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Mapper for [" + newFieldType.name() + "] conflicts with existing mapping:\n" + conflicts.toString());
        }
    }

    /** Returns the field for the given field */
    public MappedFieldType get(String field) {
        return fullNameToFieldType.get(field);
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
