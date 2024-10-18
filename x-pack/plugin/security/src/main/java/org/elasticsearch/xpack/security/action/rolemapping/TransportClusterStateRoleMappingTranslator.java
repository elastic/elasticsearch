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

/**
 * Translator class for ensuring unique API names and marking cluster-state role mappings as read-only.
 * Role mappings retrieved from cluster-state are surfaced through both the transport and REST layers,
 * along with native role mappings. Unlike native role mappings, cluster-state role mappings are
 * read-only and cannot be modified via APIs. It is possible for cluster-state and native role mappings
 * to have overlapping names.
 *
 * <p>
 * This class handles the following responsibilities to ensure correct processing of cluster-state role mappings:
 * </p>
 *
 * <ol>
 *   <li>Appends a reserved suffix to cluster-state role mapping names to avoid conflicts with native role mappings.</li>
 *   <li>Marks the metadata of cluster-state role mappings with a reserved read-only flag.</li>
 *   <li>Removes internal metadata flags used in processing (see {@link ReservedRoleMappingXContentNameFieldHelper}).</li>
 * </ol>
 */
public final class TransportClusterStateRoleMappingTranslator {
    private static final Logger logger = LogManager.getLogger(TransportClusterStateRoleMappingTranslator.class);

    static final String READ_ONLY_ROLE_MAPPING_SUFFIX = "-read-only-operator-config";
    static final String READ_ONLY_ROLE_MAPPING_METADATA_FLAG = "_read_only";

    private TransportClusterStateRoleMappingTranslator() {}

    static ExpressionRoleMapping translate(ExpressionRoleMapping mapping) {
        Map<String, Object> metadata = new HashMap<>(mapping.getMetadata());
        if (metadata.put(READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true) != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Please rename this field in your role mapping configuration.",
                READ_ONLY_ROLE_MAPPING_METADATA_FLAG
            );
        }
        ReservedRoleMappingXContentNameFieldHelper.removeNameFromMetadata(metadata);
        return new ExpressionRoleMapping(
            addReadOnlySuffix(mapping.getName()),
            mapping.getExpression(),
            mapping.getRoles(),
            mapping.getRoleTemplates(),
            metadata,
            mapping.isEnabled()
        );
    }

    public static boolean hasReadOnlySuffix(String name) {
        return name.endsWith(READ_ONLY_ROLE_MAPPING_SUFFIX);
    }

    public static void validateNoReadOnlySuffix(String name) {
        if (hasReadOnlySuffix(name)) {
            throw new IllegalArgumentException(
                "Invalid mapping name [" + name + "]. [" + READ_ONLY_ROLE_MAPPING_SUFFIX + "] is not an allowed suffix"
            );
        }
    }

    public static String addReadOnlySuffix(String name) {
        return name + READ_ONLY_ROLE_MAPPING_SUFFIX;
    }

    public static Set<String> removeReadOnlySuffixIfPresent(Set<String> names) {
        return names.stream().map(TransportClusterStateRoleMappingTranslator::removeReadOnlySuffixIfPresent).collect(Collectors.toSet());
    }

    public static String removeReadOnlySuffixIfPresent(String name) {
        return name.endsWith(READ_ONLY_ROLE_MAPPING_SUFFIX)
            ? name.substring(0, name.length() - READ_ONLY_ROLE_MAPPING_SUFFIX.length())
            : name;
    }
}
