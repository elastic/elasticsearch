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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A utility class that helps validate certain aspects of a mappings update.
 */
class MapperMergeValidator {

    /**
     * Validates the new mapping addition, checking whether duplicate entries are present and if the
     * provided fields are compatible with the mappings that are already defined.
     *
     * @param objectMappers The newly added object mappers.
     * @param fieldMappers The newly added field mappers.
     * @param fieldAliasMappers The newly added field alias mappers.
     */
    public static void validateNewMappers(Collection<ObjectMapper> objectMappers,
                                          Collection<FieldMapper> fieldMappers,
                                          Collection<FieldAliasMapper> fieldAliasMappers) {
        Set<String> objectFullNames = new HashSet<>();
        for (ObjectMapper objectMapper : objectMappers) {
            String fullPath = objectMapper.fullPath();
            if (objectFullNames.add(fullPath) == false) {
                throw new IllegalArgumentException("Object mapper [" + fullPath + "] is defined twice.");
            }
        }

        Set<String> fieldNames = new HashSet<>();
        for (FieldMapper fieldMapper : fieldMappers) {
            String name = fieldMapper.name();
            if (objectFullNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field.");
            } else if (fieldNames.add(name) == false) {
                throw new IllegalArgumentException("Field [" + name + "] is defined twice.");
            }
        }

        Set<String> fieldAliasNames = new HashSet<>();
        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String name = fieldAliasMapper.name();
            if (objectFullNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field.");
            } else if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an alias and a concrete field.");
            } else if (fieldAliasNames.add(name) == false) {
                throw new IllegalArgumentException("Field [" + name + "] is defined twice.");
            }

            validateFieldAliasMapper(name, fieldAliasMapper.path(), fieldNames, fieldAliasNames);
        }
    }

    /**
     * Checks that the new field alias is valid.
     *
     * Note that this method assumes that new concrete fields have already been processed, so that it
     * can verify that an alias refers to an existing concrete field.
     */
    private static void validateFieldAliasMapper(String aliasName,
                                                 String path,
                                                 Set<String> fieldMappers,
                                                 Set<String> fieldAliasMappers) {
        if (path.equals(aliasName)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to itself.");
        }

        if (fieldAliasMappers.contains(path)) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias cannot refer to another alias.");
        }

        if (fieldMappers.contains(path) == false) {
            throw new IllegalArgumentException("Invalid [path] value [" + path + "] for field alias [" +
                aliasName + "]: an alias must refer to an existing field in the mappings.");
        }
    }
    /**
     * Verifies that each field reference, e.g. the value of copy_to or the target
     * of a field alias, corresponds to a valid part of the mapping.
     *
     * @param fieldMappers The newly added field mappers.
     * @param fieldAliasMappers The newly added field alias mappers.
     * @param fullPathObjectMappers All object mappers, indexed by their full path.
     * @param fieldTypes All field and field alias mappers, collected into a lookup structure.
     */
    public static void validateFieldReferences(List<FieldMapper> fieldMappers,
                                               List<FieldAliasMapper> fieldAliasMappers,
                                               Map<String, ObjectMapper> fullPathObjectMappers,
                                               FieldTypeLookup fieldTypes) {
        validateCopyTo(fieldMappers, fullPathObjectMappers, fieldTypes);
        validateFieldAliasTargets(fieldAliasMappers, fullPathObjectMappers);
    }

    private static void validateCopyTo(List<FieldMapper> fieldMappers,
                                       Map<String, ObjectMapper> fullPathObjectMappers,
                                       FieldTypeLookup fieldTypes) {
        for (FieldMapper mapper : fieldMappers) {
            if (mapper.copyTo() != null && mapper.copyTo().copyToFields().isEmpty() == false) {
                String sourceParent = parentObject(mapper.name());
                if (sourceParent != null && fieldTypes.get(sourceParent) != null) {
                    throw new IllegalArgumentException("[copy_to] may not be used to copy from a multi-field: [" + mapper.name() + "]");
                }

                final String sourceScope = getNestedScope(mapper.name(), fullPathObjectMappers);
                for (String copyTo : mapper.copyTo().copyToFields()) {
                    String copyToParent = parentObject(copyTo);
                    if (copyToParent != null && fieldTypes.get(copyToParent) != null) {
                        throw new IllegalArgumentException("[copy_to] may not be used to copy to a multi-field: [" + copyTo + "]");
                    }

                    if (fullPathObjectMappers.containsKey(copyTo)) {
                        throw new IllegalArgumentException("Cannot copy to field [" + copyTo + "] since it is mapped as an object");
                    }

                    final String targetScope = getNestedScope(copyTo, fullPathObjectMappers);
                    checkNestedScopeCompatibility(sourceScope, targetScope);
                }
            }
        }
    }

    private static void validateFieldAliasTargets(List<FieldAliasMapper> fieldAliasMappers,
                                                  Map<String, ObjectMapper> fullPathObjectMappers) {
        for (FieldAliasMapper mapper : fieldAliasMappers) {
            String aliasName = mapper.name();
            String path = mapper.path();

            String aliasScope = getNestedScope(aliasName, fullPathObjectMappers);
            String pathScope = getNestedScope(path, fullPathObjectMappers);

            if (!Objects.equals(aliasScope, pathScope)) {
                StringBuilder message = new StringBuilder("Invalid [path] value [" + path + "] for field alias [" +
                    aliasName + "]: an alias must have the same nested scope as its target. ");
                message.append(aliasScope == null
                    ? "The alias is not nested"
                    : "The alias's nested scope is [" + aliasScope + "]");
                message.append(", but ");
                message.append(pathScope == null
                    ? "the target is not nested."
                    : "the target's nested scope is [" + pathScope + "].");
                throw new IllegalArgumentException(message.toString());
            }
        }
    }

    private static String getNestedScope(String path, Map<String, ObjectMapper> fullPathObjectMappers) {
        for (String parentPath = parentObject(path); parentPath != null; parentPath = parentObject(parentPath)) {
            ObjectMapper objectMapper = fullPathObjectMappers.get(parentPath);
            if (objectMapper != null && objectMapper.nested().isNested()) {
                return parentPath;
            }
        }
        return null;
    }

    private static void checkNestedScopeCompatibility(String source, String target) {
        boolean targetIsParentOfSource;
        if (source == null || target == null) {
            targetIsParentOfSource = target == null;
        } else {
            targetIsParentOfSource = source.equals(target) || source.startsWith(target + ".");
        }
        if (targetIsParentOfSource == false) {
            throw new IllegalArgumentException(
                "Illegal combination of [copy_to] and [nested] mappings: [copy_to] may only copy data to the current nested " +
                    "document or any of its parents, however one [copy_to] directive is trying to copy data from nested object [" +
                    source + "] to [" + target + "]");
        }
    }

    private static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }
}
