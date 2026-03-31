/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implemented by mapper types that expose sub-fields as root-level aliases (passthrough behavior).
 * Both {@link PassThroughObjectMapper} and passthrough-enabled {@code FlattenedFieldMapper} implement
 * this interface, allowing {@link FieldTypeLookup} and {@link MappingLookup} to resolve passthrough
 * aliases in a unified, priority-based way.
 */
public sealed interface PassThroughFieldSource permits PassThroughObjectMapper, FlattenedFieldMapper {

    /**
     * The non-negative priority used to resolve conflicts when multiple passthrough sources expose
     * sub-fields with the same leaf name at the root level. Higher priority wins.
     */
    int priority();

    /**
     * The fully-qualified path of this field/mapper in the index mapping.
     */
    String fullPath();

    /**
     * The set of sub-field mappers whose {@link FieldMapper#leafName()} should be registered as
     * root-level aliases. Returns an empty collection when passthrough is disabled or there are no
     * eligible sub-fields.
     */
    Collection<FieldMapper> passThroughSubFields();

    static Map<String, FieldMapper> resolveConflictingPriorities(Collection<PassThroughFieldSource> passThroughSources) {
        // Passthrough sub-fields can be referenced without the prefix of the passthrough source.
        // Use priority to resolve conflicts when multiple sources expose the same leaf name.
        Map<String, Integer> passThroughPriorities = new HashMap<>();
        Map<String, FieldMapper> passThroughAliases = new HashMap<>();

        for (PassThroughFieldSource source : passThroughSources) {
            for (FieldMapper subField : source.passThroughSubFields()) {
                String name = subField.leafName();
                Integer existingPriority = passThroughPriorities.get(name);
                if (existingPriority == null || source.priority() > existingPriority) {
                    passThroughAliases.put(name, subField);
                    passThroughPriorities.put(name, source.priority());
                }
            }
        }
        return passThroughAliases;
    }
}
