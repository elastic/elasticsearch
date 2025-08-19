/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;

/**
 * In serverless, prohibits mappings that start with _project, including subfields such as _project.foo.
 * The _project "namespace" in serverless is reserved in order to allow users to include project metadata tags
 * (e.g., _project._region or _project._csp, etc.) which are useful in cross-project search and ES|QL queries.
 */
public class ServerlessRootObjectMapperNamespaceValidator implements RootObjectMapperNamespaceValidator {
    private static final String SERVERLESS_RESERVED_NAMESPACE = "_project";

    // MP TODO: we can also pass in a MappingLookup - would that help here?
    @Override
    public void validateNamespace(ObjectMapper.Subobjects subobjects, Mapper mapper) {   // TODO: stop passing in Mapper and pass in String
                                                                                         // fieldName
        if (mapper.leafName().equals(SERVERLESS_RESERVED_NAMESPACE)) {
            System.err.println("XX: 1A: " + mapper.leafName());
            System.err.println("XX: 1B: " + mapper.fullPath());
            System.err.println("XX: 1C: " + mapper.typeName());
            System.err.println("XX: 1D: " + mapper);
            throw new IllegalArgumentException(generateErrorMessage());
        } else if (subobjects != ObjectMapper.Subobjects.ENABLED) {
            System.err.println("YYY: 2A: " + mapper.leafName());
            System.err.println("YYY: 2B: " + mapper.fullPath());
            System.err.println("YYY: 2C: " + mapper.typeName());
            System.err.println("YYY: 2D: " + mapper);
            // leafName here will be something like _project.myfield, rather than just _project
            if (mapper.leafName().startsWith(SERVERLESS_RESERVED_NAMESPACE + ".")) {
                throw new IllegalArgumentException(generateErrorMessage(mapper.leafName()));
            }
        }
    }

    private String generateErrorMessage(String fieldName) {
        return Strings.format(
            "Mapping rejected%s. No mappings of [%s] are allowed in order to avoid conflicts with project metadata tags in serverless",
            fieldName == null ? "" : ": [" + fieldName + "]",
            SERVERLESS_RESERVED_NAMESPACE
        );
    }

    private String generateErrorMessage() {
        return generateErrorMessage(null);
    }
}
