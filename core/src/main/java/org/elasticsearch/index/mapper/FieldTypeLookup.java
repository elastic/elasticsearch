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
    private final CopyOnWriteHashMap<String, MappedFieldTypeReference> fullNameToFieldType;

    /** Full field name to types containing a mapping for this full name. */
    private final CopyOnWriteHashMap<String, Set<String>> fullNameToTypes;

    /** Index field name to field type */
    private final CopyOnWriteHashMap<String, MappedFieldTypeReference> indexNameToFieldType;

    /** Index field name to types containing a mapping for this index name. */
    private final CopyOnWriteHashMap<String, Set<String>> indexNameToTypes;

    /** Create a new empty instance. */
    public FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        fullNameToTypes = new CopyOnWriteHashMap<>();
        indexNameToFieldType = new CopyOnWriteHashMap<>();
        indexNameToTypes = new CopyOnWriteHashMap<>();
    }

    private FieldTypeLookup(
            CopyOnWriteHashMap<String, MappedFieldTypeReference> fullName,
            CopyOnWriteHashMap<String, Set<String>> fullNameToTypes,
            CopyOnWriteHashMap<String, MappedFieldTypeReference> indexName,
            CopyOnWriteHashMap<String, Set<String>> indexNameToTypes) {
        this.fullNameToFieldType = fullName;
        this.fullNameToTypes = fullNameToTypes;
        this.indexNameToFieldType = indexName;
        this.indexNameToTypes = indexNameToTypes;
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
    public FieldTypeLookup copyAndAddAll(String type, Collection<FieldMapper> newFieldMappers) {
        Objects.requireNonNull(type, "type must not be null");
        if (MapperService.DEFAULT_MAPPING.equals(type)) {
            throw new IllegalArgumentException("Default mappings should not be added to the lookup");
        }
        CopyOnWriteHashMap<String, MappedFieldTypeReference> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, Set<String>> fullNameToTypes = this.fullNameToTypes;
        CopyOnWriteHashMap<String, MappedFieldTypeReference> indexName = this.indexNameToFieldType;
        CopyOnWriteHashMap<String, Set<String>> indexNameToTypes = this.indexNameToTypes;

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

            fullNameToTypes = addType(fullNameToTypes, fieldType.names().fullName(), type);
            indexNameToTypes = addType(indexNameToTypes, fieldType.names().indexName(), type);
        }
        return new FieldTypeLookup(fullName, fullNameToTypes, indexName, indexNameToTypes);
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
     * Checks if the given mappers' field types are compatible with existing field types.
     * If any are not compatible, an IllegalArgumentException is thrown.
     * If updateAllTypes is true, only basic compatibility is checked.
     */
    public void checkCompatibility(String type, Collection<FieldMapper> fieldMappers, boolean updateAllTypes) {
        for (FieldMapper fieldMapper : fieldMappers) {
            MappedFieldTypeReference ref = fullNameToFieldType.get(fieldMapper.fieldType().names().fullName());
            if (ref != null) {
                List<String> conflicts = new ArrayList<>();
                final Set<String> types = fullNameToTypes.get(fieldMapper.fieldType().names().fullName());
                boolean strict = beStrict(type, types, updateAllTypes);
                ref.get().checkCompatibility(fieldMapper.fieldType(), conflicts, strict);
                if (conflicts.isEmpty() == false) {
                    throw new IllegalArgumentException("Mapper for [" + fieldMapper.fieldType().names().fullName() + "] conflicts with existing mapping in other types:\n" + conflicts.toString());
                }
            }

            // field type for the index name must be compatible too
            MappedFieldTypeReference indexNameRef = indexNameToFieldType.get(fieldMapper.fieldType().names().indexName());
            if (indexNameRef != null) {
                List<String> conflicts = new ArrayList<>();
                final Set<String> types = indexNameToTypes.get(fieldMapper.fieldType().names().indexName());
                boolean strict = beStrict(type, types, updateAllTypes);
                indexNameRef.get().checkCompatibility(fieldMapper.fieldType(), conflicts, strict);
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

    /** Get the set of types that have a mapping for the given field. */
    public Set<String> getTypes(String field) {
        Set<String> types = fullNameToTypes.get(field);
        if (types == null) {
            types = Collections.emptySet();
        }
        return types;
    }

    /** Returns the field type for the given index name */
    public MappedFieldType getByIndexName(String field) {
        MappedFieldTypeReference ref = indexNameToFieldType.get(field);
        if (ref == null) return null;
        return ref.get();
    }

    /** Get the set of types that have a mapping for the given field. */
    public Set<String> getTypesByIndexName(String field) {
        Set<String> types = indexNameToTypes.get(field);
        if (types == null) {
            types = Collections.emptySet();
        }
        return types;
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
