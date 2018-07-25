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

    /**
     * A mapping from concrete field name to field type.
     **/
    final CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType;
    private final CopyOnWriteHashMap<String, String> aliasToConcreteName;

    /**
     * Full field name to types containing a mapping for this full name. Note that
     * this map contains aliases as well as concrete fields.
     **/
    private final CopyOnWriteHashMap<String, Set<String>> fullNameToTypes;

    FieldTypeLookup() {
        fullNameToFieldType = new CopyOnWriteHashMap<>();
        aliasToConcreteName = new CopyOnWriteHashMap<>();
        fullNameToTypes = new CopyOnWriteHashMap<>();
    }

    private FieldTypeLookup(CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType,
                            CopyOnWriteHashMap<String, String> aliasToConcreteName,
                            CopyOnWriteHashMap<String, Set<String>> fullNameToTypes) {
        this.fullNameToFieldType = fullNameToFieldType;
        this.aliasToConcreteName = aliasToConcreteName;
        this.fullNameToTypes = fullNameToTypes;
    }

    private static CopyOnWriteHashMap<String, Set<String>> addType(CopyOnWriteHashMap<String, Set<String>> map,
                                                                   String key,
                                                                   String type) {
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
     * from the provided mappers. If a field already exists, its field type will be updated
     * to use the new type from the given field mapper. Similarly if an alias already
     * exists, it will be updated to reference the field type from the new mapper.
     */
    public FieldTypeLookup copyAndAddAll(String type,
                                         Collection<FieldMapper> fieldMappers,
                                         Collection<FieldAliasMapper> fieldAliasMappers,
                                         boolean updateAllTypes) {
        Objects.requireNonNull(type, "type must not be null");
        if (MapperService.DEFAULT_MAPPING.equals(type)) {
            throw new IllegalArgumentException("Default mappings should not be added to the lookup");
        }

        CopyOnWriteHashMap<String, MappedFieldType> fullName = this.fullNameToFieldType;
        CopyOnWriteHashMap<String, String> aliases = this.aliasToConcreteName;
        CopyOnWriteHashMap<String, Set<String>> fullNameToTypes = this.fullNameToTypes;

        for (FieldMapper fieldMapper : fieldMappers) {
            MappedFieldType fieldType = fieldMapper.fieldType();
            MappedFieldType fullNameFieldType = fullName.get(fieldType.name());

            if (!Objects.equals(fieldType, fullNameFieldType)) {
                validateField(type, fullNameFieldType, fieldType, aliases, updateAllTypes);
                fullName = fullName.copyAndPut(fieldType.name(), fieldType);
            }
            fullNameToTypes = addType(fullNameToTypes, fieldType.name(), type);
        }

        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String aliasName = fieldAliasMapper.name();
            String path = fieldAliasMapper.path();

            validateAlias(aliasName, path, aliases, fullName);
            aliases = aliases.copyAndPut(aliasName, path);
        }

        return new FieldTypeLookup(fullName, aliases, fullNameToTypes);
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
    private void validateField(String type,
                               MappedFieldType existingFieldType,
                               MappedFieldType newFieldType,
                               CopyOnWriteHashMap<String, String> aliasToConcreteName,
                               boolean updateAllTypes) {
        String fieldName = newFieldType.name();
        if (aliasToConcreteName.containsKey(fieldName)) {
            throw new IllegalArgumentException("The name for field [" + fieldName + "] has already" +
                " been used to define a field alias.");
        }

        if (existingFieldType != null) {
            List<String> conflicts = new ArrayList<>();
            final Set<String> types = fullNameToTypes.get(newFieldType.name());
            boolean strict = beStrict(type, types, updateAllTypes);
            existingFieldType.checkCompatibility(newFieldType, conflicts, strict);
            if (conflicts.isEmpty() == false) {
                throw new IllegalArgumentException("Mapper for [" + fieldName +
                    "] conflicts with existing mapping in other types:\n" + conflicts.toString());
            }
        }
    }

    /**
     * Checks that the new field alias is valid.
     *
     * Note that this method assumes that new concrete fields have already been processed, so that it
     * can verify that an alias refers to an existing concrete field.
     */
    private void validateAlias(String aliasName,
                               String path,
                               CopyOnWriteHashMap<String, String> aliasToConcreteName,
                               CopyOnWriteHashMap<String, MappedFieldType> fullNameToFieldType) {
        if (fullNameToFieldType.containsKey(aliasName)) {
            throw new IllegalArgumentException("The name for field alias [" + aliasName + "] has already" +
                " been used to define a concrete field.");
        }

        if (path.equals(aliasName)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to itself.");
        }

        if (aliasToConcreteName.containsKey(path)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to another alias.");
        }

        if (!fullNameToFieldType.containsKey(path)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias must refer to an existing field in the mappings.");
        }
    }

    /** Returns the field for the given field */
    public MappedFieldType get(String field) {
        String concreteField = aliasToConcreteName.getOrDefault(field, field);
        return fullNameToFieldType.get(concreteField);
    }

    /**
     * Get the set of types that have a mapping for the given field.
     * Note: this method is only visible for testing.
     **/
    protected Set<String> getTypes(String field) {
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
        for (String aliasName : aliasToConcreteName.keySet()) {
            if (Regex.simpleMatch(pattern, aliasName)) {
                fields.add(aliasName);
            }
        }
        return fields;
    }

    @Override
    public Iterator<MappedFieldType> iterator() {
        return fullNameToFieldType.values().iterator();
    }
}
