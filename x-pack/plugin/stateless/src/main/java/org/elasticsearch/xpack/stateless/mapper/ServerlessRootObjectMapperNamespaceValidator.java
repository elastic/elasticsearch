/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapperNamespaceValidator;

/**
 * In serverless, prohibits mappings that start with _project, including subfields such as _project.foo.
 * The _project "namespace" in serverless is reserved in order to allow users to include project metadata tags
 * (e.g., _project._region or _project._csp, etc.) which are useful in cross-project search and ES|QL queries.
 */
public class ServerlessRootObjectMapperNamespaceValidator implements RootObjectMapperNamespaceValidator {
    private static final String SERVERLESS_RESERVED_NAMESPACE = "_project";

    /**
     * Throws an error if a top level field with {@code SERVERLESS_RESERVED_NAMESPACE} is found.
     * If subobjects = false (or is null), then it also checks for field names starting with "_project."
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
