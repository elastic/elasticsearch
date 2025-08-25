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
import org.elasticsearch.core.Nullable;

/**
 * In serverless, prohibits mappings that start with _project, including subfields such as _project.foo.
 * The _project "namespace" in serverless is reserved in order to allow users to include project metadata tags
 * (e.g., _project._region or _project._csp, etc.) which are useful in cross-project search and ES|QL queries.
 */
public class ServerlessRootObjectMapperNamespaceValidator implements RootObjectMapperNamespaceValidator {
    private static final String SERVERLESS_RESERVED_NAMESPACE = "_project";

    /**
     * Throws an error if a top level field with {@code SERVERLESS_RESERVED_NAMESPACE} is found.
     * If subobjects = false, then it also checks for field names starting with "_project."
     * @param subobjects if null, it will be interpreted as subobjects != ObjectMapper.Subobjects.ENABLED
     * @param name field name to evaluation
     */
    @Override
    public void validateNamespace(@Nullable ObjectMapper.Subobjects subobjects, String name) {
        if (name.equals(SERVERLESS_RESERVED_NAMESPACE)) {
            throw new IllegalArgumentException(generateErrorMessage());
        } else if (subobjects != ObjectMapper.Subobjects.ENABLED) {
            // name here will be something like _project.my_field, rather than just _project
            if (name.startsWith(SERVERLESS_RESERVED_NAMESPACE + ".")) {
                throw new IllegalArgumentException(generateErrorMessage(name));
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
