/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public final class ReservedRoleMappingXContentNameFieldHelper {
    private static final Logger logger = LogManager.getLogger(ReservedRoleMappingXContentNameFieldHelper.class);

    public static final String METADATA_NAME_FIELD = "_es_reserved_role_mapping_name";
    public static final String FALLBACK_NAME = "name_not_available_after_deserialization";

    private ReservedRoleMappingXContentNameFieldHelper() {}

    public static ExpressionRoleMapping copyWithNameInMetadata(ExpressionRoleMapping roleMapping) {
        // Use tree map to get deterministic order, and ensure we have a mutable map to work with
        Map<String, Object> metadata = new TreeMap<>(roleMapping.getMetadata());
        Object previousValue = metadata.put(METADATA_NAME_FIELD, roleMapping.getName());
        if (previousValue != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Please rename this field in your role mapping configuration.",
                METADATA_NAME_FIELD
            );
        }
        return new ExpressionRoleMapping(
            roleMapping.getName(),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            metadata,
            roleMapping.isEnabled()
        );
    }

    public static ExpressionRoleMapping parseWithNameFromMetadata(XContentParser parser) throws IOException {
        ExpressionRoleMapping roleMapping = ExpressionRoleMapping.parse(FALLBACK_NAME, parser);
        return new ExpressionRoleMapping(
            getNameFromMetadata(roleMapping),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            roleMapping.getMetadata(),
            roleMapping.isEnabled()
        );
    }

    private static String getNameFromMetadata(ExpressionRoleMapping roleMapping) {
        Map<String, Object> metadata = roleMapping.getMetadata();
        if (metadata.containsKey(METADATA_NAME_FIELD) && metadata.get(METADATA_NAME_FIELD) instanceof String name) {
            return name;
        }

        logger.error(
            "Role mapping metadata is missing a required internal system field [{}]. "
                + "This may result in inconsistent Role Mappings API behavior.",
            METADATA_NAME_FIELD
        );
        assert false : "role mapping metadata should contain string field [" + METADATA_NAME_FIELD + "]";
        return FALLBACK_NAME;
    }
}
