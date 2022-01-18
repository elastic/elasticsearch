/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Collection;
import java.util.List;

public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    private static final List<String> KIBANA_PRODUCT_ORIGIN = List.of("kibana");

    public static final SystemIndexDescriptor KIBANA_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".kibana_*")
        .setDescription("Kibana saved objects system index")
        .setAliasName(".kibana")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor REPORTING_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".reporting-*")
        .setDescription("system index for reporting")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
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

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            KIBANA_INDEX_DESCRIPTOR,
            REPORTING_INDEX_DESCRIPTOR,
            APM_AGENT_CONFIG_INDEX_DESCRIPTOR,
            APM_CUSTOM_LINK_INDEX_DESCRIPTOR
        );
    }

    @Override
    public String getFeatureName() {
        return "kibana";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages Kibana configuration and reports";
    }
}
