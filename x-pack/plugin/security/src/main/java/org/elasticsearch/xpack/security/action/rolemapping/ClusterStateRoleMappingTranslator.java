/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ReservedRoleMappingXContentNameFieldHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterStateRoleMappingTranslator {
    private static final Logger logger = LogManager.getLogger(ClusterStateRoleMappingTranslator.class);

    static final String READ_ONLY_ROLE_MAPPING_SUFFIX = " (read only)";
    static final String READ_ONLY_METADATA_FLAG = "_read_only";

    static ExpressionRoleMapping translate(ExpressionRoleMapping mapping) {
        Map<String, Object> metadata = new HashMap<>(mapping.getMetadata());
        if (metadata.put(READ_ONLY_METADATA_FLAG, true) != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Please rename this field in your role mapping configuration.",
                READ_ONLY_METADATA_FLAG
            );
        }
        ReservedRoleMappingXContentNameFieldHelper.removeNameFromMetadata(metadata);
        return new ExpressionRoleMapping(
            withReservedReadOnlySuffix(mapping.getName()),
            mapping.getExpression(),
            mapping.getRoles(),
            mapping.getRoleTemplates(),
            metadata,
            mapping.isEnabled()
        );
    }

    public static boolean hasReservedReadOnlySuffix(String name) {
        return name.endsWith(READ_ONLY_ROLE_MAPPING_SUFFIX);
    }

    public static String withReservedReadOnlySuffix(String name) {
        return name + READ_ONLY_ROLE_MAPPING_SUFFIX;
    }

    public static Set<String> removeReservedReadOnlySuffix(Set<String> names) {
        return names.stream().map(ClusterStateRoleMappingTranslator::removeReservedReadOnlySuffix).collect(Collectors.toSet());
    }

    public static String removeReservedReadOnlySuffix(String name) {
        if (name.endsWith(READ_ONLY_ROLE_MAPPING_SUFFIX)) {
            return name.substring(0, name.length() - READ_ONLY_ROLE_MAPPING_SUFFIX.length());
        }
        return name;
    }
}
