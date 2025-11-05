/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Collection;
import java.util.List;

/**
 * Plugin that manages Kibana-related system indices in Elasticsearch.
 * <p>
 * This plugin registers and manages system index descriptors for Kibana configuration,
 * reporting, Onechat, workflows, and APM functionality. These system indices are protected
 * and can only be modified by Kibana products.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Plugin is automatically loaded by Elasticsearch
 * // System indices are automatically registered:
 * // - .kibana_* (Kibana saved objects)
 * // - .reporting-* (Reporting data)
 * // - .chat-* (Onechat data)
 * // - .workflows-* (Workflows data)
 * // - .apm-agent-configuration* (APM agent config)
 * // - .apm-custom-link* (APM custom links)
 * }</pre>
 */
public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    private static final List<String> KIBANA_PRODUCT_ORIGIN = List.of("kibana");

    public static final SystemIndexDescriptor KIBANA_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".kibana_*")
        .setDescription("Kibana saved objects system index")
        .setAliasName(".kibana")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    public static final SystemIndexDescriptor REPORTING_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".reporting-*")
        .setDescription("system index for reporting")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor ONECHAT_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".chat-*")
        .setDescription("Onechat system index")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    public static final SystemIndexDescriptor APM_AGENT_CONFIG_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".apm-agent-configuration*")
        .setDescription("system index for APM agent configuration")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor APM_CUSTOM_LINK_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".apm-custom-link*")
        .setDescription("system index for APM custom links")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor WORKFLOWS_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".workflows-*")
        .setDescription("Workflows system index")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    /**
     * Returns the collection of system index descriptors managed by this plugin.
     * <p>
     * This method provides descriptors for all Kibana-related system indices including
     * saved objects, reporting data, Onechat, workflows, and APM configuration. These
     * indices are protected as external unmanaged system indices that can only be
     * accessed by Kibana products.
     * </p>
     *
     * @param settings the Elasticsearch settings (unused in this implementation)
     * @return an immutable collection of system index descriptors for Kibana-related indices
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called by Elasticsearch during plugin initialization
     * Collection<SystemIndexDescriptor> descriptors = plugin.getSystemIndexDescriptors(settings);
     * for (SystemIndexDescriptor descriptor : descriptors) {
     *     String pattern = descriptor.getIndexPattern();
     *     String description = descriptor.getDescription();
     * }
     * }</pre>
     */
    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            KIBANA_INDEX_DESCRIPTOR,
            REPORTING_INDEX_DESCRIPTOR,
            ONECHAT_INDEX_DESCRIPTOR,
            WORKFLOWS_INDEX_DESCRIPTOR,
            APM_AGENT_CONFIG_INDEX_DESCRIPTOR,
            APM_CUSTOM_LINK_INDEX_DESCRIPTOR
        );
    }

    /**
     * Returns the feature name for this plugin.
     * <p>
     * The feature name identifies this plugin in Elasticsearch's feature registry
     * and is used for licensing and feature tracking purposes.
     * </p>
     *
     * @return the string "kibana" identifying this feature
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called by Elasticsearch feature registry
     * String featureName = plugin.getFeatureName();
     * // Returns: "kibana"
     * }</pre>
     */
    @Override
    public String getFeatureName() {
        return "kibana";
    }

    /**
     * Returns a human-readable description of this plugin's feature.
     * <p>
     * This description is used in Elasticsearch's feature registry to provide
     * information about the functionality provided by this plugin.
     * </p>
     *
     * @return a description of the Kibana feature functionality
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called by Elasticsearch feature registry
     * String description = plugin.getFeatureDescription();
     * // Returns: "Manages Kibana configuration and reports"
     * }</pre>
     */
    @Override
    public String getFeatureDescription() {
        return "Manages Kibana configuration and reports";
    }
}
