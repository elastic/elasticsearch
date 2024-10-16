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
import java.util.HashMap;
import java.util.Map;

public class ClusterStateRoleMappingXContentTranslator {

    private static final Logger logger = LogManager.getLogger(ClusterStateRoleMappingXContentTranslator.class);

    public static final String NAME_FIELD = "_es_reserved_name";
    public static final String FALLBACK_NAME = "name_not_available_after_deserialization";

    public static ExpressionRoleMapping copyWithNameInMetadata(ExpressionRoleMapping roleMapping) {
        Map<String, Object> metadata = new HashMap<>(roleMapping.getMetadata());
        Object previousValue = metadata.put(NAME_FIELD, roleMapping.getName());
        if (previousValue != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten. Please rename this field in your role mapping configuration.",
                NAME_FIELD
            );
        }
        return new ExpressionRoleMapping(
            roleMapping.getName(),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            Map.copyOf(metadata),
            roleMapping.isEnabled()
        );
    }

    public static ExpressionRoleMapping fromXContent(XContentParser parser) throws IOException {
        ExpressionRoleMapping roleMapping = ExpressionRoleMapping.parse(FALLBACK_NAME, parser);
        String name = getNameFromMetadata(roleMapping);
        return new ExpressionRoleMapping(
            name,
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            roleMapping.getMetadata(),
            roleMapping.isEnabled()
        );
    }

    private static String getNameFromMetadata(ExpressionRoleMapping roleMapping) {
        Map<String, Object> metadata = roleMapping.getMetadata();
        if (metadata.containsKey(NAME_FIELD) && metadata.get(NAME_FIELD) instanceof String name) {
            return name;
        }

        logger.error("role mapping metadata should contain reserved string field [" + NAME_FIELD + "]");
        assert false : "role mapping metadata should contain reserved string field [" + NAME_FIELD + "]";
        return FALLBACK_NAME;
    }
}
