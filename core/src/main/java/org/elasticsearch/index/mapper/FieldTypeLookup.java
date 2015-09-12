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
import java.util.Set;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
class FieldTypeLookup implements Iterable<MappedFieldType> {

    /** Full field name to field type */
    private final CopyOnWriteHashMap<String, MappedFieldTypeReference> fullNameToFieldType;

    /** Index field name to field type */
    private final CopyOnWriteHashMap<String, MappedFieldTypeReference> indexNameToFieldType;

    /** Create a new empty instance. */
    public FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        indexNameToFieldType = new CopyOnWriteHashMap<>();
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldTypeReference> fullName, CopyOnWriteHashMap<String, MappedFieldTypeReference> indexName) {
        fullNameToFieldType = fullName;
        indexNameToFieldType = indexName;
    }

    /**
     * Return a new instance that contains the union of this instance and the field types
     * from the provided fields. If a field already exists, the field type will be updated
     * to use the new mappers field type.
     */
    public FieldTypeLookup copyAndAddAll(Collection<FieldMapper> newFieldMappers) {
        CopyOnWriteHashMap<String, MappedFieldTypeReference> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, MappedFieldTypeReference> indexName = this.indexNameToFieldType;

        for (FieldMapper fieldMapper : newFieldMappers) {
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldTypeReference fullNameRef = fullName.get(fieldType.names().fullName());
            MappedFieldTypeReference indexNameRef = indexName.get(fieldType.names().indexName());
            if (fullNameRef == null && indexNameRef == null) {
                // new field, just use the ref from this field mapper
                fullName = fullName.copyAndPut(fieldType.names().fullName(), fieldMapper.fieldTypeReference());
                indexName = indexName.copyAndPut(fieldType.names().indexName(), fieldMapper.fieldTypeReference());
            } else if (fullNameRef == null) {
                // this index name already exists, so copy over the reference
                fullName = fullName.copyAndPut(fieldType.names().fullName(), indexNameRef);
                indexNameRef.set(fieldMapper.fieldType()); // field type is updated, since modifiable settings may have changed
                fieldMapper.setFieldTypeReference(indexNameRef);
            } else if (indexNameRef == null) {
                // this full name already exists, so copy over the reference
                indexName = indexName.copyAndPut(fieldType.names().indexName(), fullNameRef);
                fullNameRef.set(fieldMapper.fieldType()); // field type is updated, since modifiable settings may have changed
                fieldMapper.setFieldTypeReference(fullNameRef);
            } else if (fullNameRef == indexNameRef) {
                // the field already exists, so replace the reference in this mapper with the pre-existing one
                fullNameRef.set(fieldMapper.fieldType()); // field type is updated, since modifiable settings may have changed
                fieldMapper.setFieldTypeReference(fullNameRef);
            } else {
                // this new field bridges between two existing field names (a full and index name), which we cannot support
                throw new IllegalStateException("insane mappings found. field " + fieldType.names().fullName() + " maps across types to field " + fieldType.names().indexName());
            }
        }
        return new FieldTypeLookup(fullName, indexName);
    }

    /**
     * Checks if the given mappers' field types are compatible with existing field types.
     * If any are not compatible, an IllegalArgumentException is thrown.
     * If updateAllTypes is true, only basic compatibility is checked.
     */
    public void checkCompatibility(Collection<FieldMapper> newFieldMappers, boolean updateAllTypes) {
        for (FieldMapper fieldMapper : newFieldMappers) {
            MappedFieldTypeReference ref = fullNameToFieldType.get(fieldMapper.fieldType().names().fullName());
            if (ref != null) {
                List<String> conflicts = new ArrayList<>();
                ref.get().checkTypeName(fieldMapper.fieldType(), conflicts);
                if (conflicts.isEmpty()) { // only check compat if they are the same type
                    boolean strict = updateAllTypes == false;
                    ref.get().checkCompatibility(fieldMapper.fieldType(), conflicts, strict);
                }
                if (conflicts.isEmpty() == false) {
                    throw new IllegalArgumentException("Mapper for [" + fieldMapper.fieldType().names().fullName() + "] conflicts with existing mapping in other types:\n" + conflicts.toString());
                }
            }

            // field type for the index name must be compatible too
            MappedFieldTypeReference indexNameRef = indexNameToFieldType.get(fieldMapper.fieldType().names().indexName());
            if (indexNameRef != null) {
                List<String> conflicts = new ArrayList<>();
                indexNameRef.get().checkTypeName(fieldMapper.fieldType(), conflicts);
                if (conflicts.isEmpty()) { // only check compat if they are the same type
                    boolean strict = updateAllTypes == false;
                    indexNameRef.get().checkCompatibility(fieldMapper.fieldType(), conflicts, strict);
                }
                if (conflicts.isEmpty() == false) {
                    throw new IllegalArgumentException("Mapper for [" + fieldMapper.fieldType().names().fullName() + "] conflicts with mapping with the same index name in other types" + conflicts.toString());
                }
            }
        }
    }

    /** Returns the field for the given field */
    public MappedFieldType get(String field) {
        MappedFieldTypeReference ref = fullNameToFieldType.get(field);
        if (ref == null) return null;
        return ref.get();
    }

    /** Returns the field type for the given index name */
    public MappedFieldType getByIndexName(String field) {
        MappedFieldTypeReference ref = indexNameToFieldType.get(field);
        if (ref == null) return null;
        return ref.get();
    }

    /**
     * Returns a list of the index names of a simple match regex like pattern against full name and index name.
     */
    public Collection<String> simpleMatchToIndexNames(String pattern) {
        Set<String> fields = new HashSet<>();
        for (MappedFieldType fieldType : this) {
            if (Regex.simpleMatch(pattern, fieldType.names().fullName())) {
                fields.add(fieldType.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldType.names().indexName())) {
                fields.add(fieldType.names().indexName());
            }
        }
        return fields;
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    public Collection<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = new HashSet<>();
        for (MappedFieldType fieldType : this) {
            if (Regex.simpleMatch(pattern, fieldType.names().fullName())) {
                fields.add(fieldType.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldType.names().indexName())) {
                fields.add(fieldType.names().fullName());
            }
        }
        return fields;
    }

    public Iterator<MappedFieldType> iterator() {
        return fullNameToFieldType.values().stream().map((p) -> p.get()).iterator();
    }
}
