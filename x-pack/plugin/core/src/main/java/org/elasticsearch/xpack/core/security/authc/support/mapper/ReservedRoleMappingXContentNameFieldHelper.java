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

public final class ReservedRoleMappingXContentNameFieldHelper {
    private static final Logger logger = LogManager.getLogger(ReservedRoleMappingXContentNameFieldHelper.class);

    public static final String METADATA_NAME_FIELD = "_es_reserved_role_mapping_name";
    public static final String FALLBACK_NAME = "name_not_available_after_deserialization";

    private ReservedRoleMappingXContentNameFieldHelper() {}

    public static ExpressionRoleMapping copyWithNameInMetadata(ExpressionRoleMapping roleMapping) {
        // metadata name field already present, nothing to do
        if (roleMapping.getMetadata().containsKey(METADATA_NAME_FIELD)) {
            return roleMapping;
        }
        // xcontent should already give us back a hashmap but make sure we have one, so we can modify it
        // can't use Maps.copyWith... since these create maps that don't support `null` values in map entries
        Map<String, Object> metadata = toHashMap(roleMapping.getMetadata());
        metadata.put(METADATA_NAME_FIELD, roleMapping.getName());
        return new ExpressionRoleMapping(
            roleMapping.getName(),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            metadata,
            roleMapping.isEnabled()
        );
    }

    public static ExpressionRoleMapping removeNameFromMetadata(ExpressionRoleMapping roleMapping, boolean logError) {
        if (roleMapping.getMetadata().containsKey(METADATA_NAME_FIELD) == false) {
            return roleMapping;
        }
        // xcontent should already give us back a hashmap but make sure we have one, so we can modify it
        // can't use Maps.copyWith... since these create maps that don't support `null` values in map entries
        Map<String, Object> metadata = toHashMap(roleMapping.getMetadata());
        metadata.remove(METADATA_NAME_FIELD);
        if (logError) {
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
        } else {
            // This is valid the first time we recover from cluster-state: the old format metadata won't have a name stored in metadata yet
            return FALLBACK_NAME;
        }
    }

    private static <K, V> HashMap<K, V> toHashMap(Map<K, V> map) {
        if (map instanceof HashMap) {
            return (HashMap<K, V>) map;
        } else {
            return new HashMap<>(map);
        }
    }
}
