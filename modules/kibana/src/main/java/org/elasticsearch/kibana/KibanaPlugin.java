/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.indices.ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS;

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

    public static final SystemDataStreamDescriptor KIBANA_REPORTING_DS_DESCRIPTOR;

    private static String loadTemplateSource() throws IOException {
        try (InputStream is = KibanaPlugin.class.getResourceAsStream("/kibana-reporting-template.json")) {
            if (is == null) {
                throw new IOException(
                    "Kibana reporting template [/kibana-reporting-template.json] not found in kibana template resources."
                );
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static {
        try {
            final String source = loadTemplateSource();
            KIBANA_REPORTING_DS_DESCRIPTOR = new SystemDataStreamDescriptor(
                ".kibana-reporting",
                "system data stream for reporting",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(".kibana-reporting"))
                    .priority(200L)
                    .version(15L)
                    .allowAutoCreate(true)
                    .deprecated(false)
                    .ignoreMissingComponentTemplates(List.of("kibana-reporting@custom"))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(true, false))
                    .metadata(Map.of("managed", "true", "description", "default kibana reporting template installed by elasticsearch"))
                    .componentTemplates(List.of("kibana-reporting@settings", "kibana-reporting@custom"))
                    .template(
                        Template.builder()
                            .mappings(CompressedXContent.fromJSON(source))
                            .lifecycle(DataStreamLifecycle.dataLifecycleBuilder().enabled(true))
                    )
                    .build(),
                Map.of(
                    "kibana-reporting@settings",
                    new ComponentTemplate(
                        Template.builder()
                            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.auto_expand_replicas", "0-1"))
                            .build(),
                        null,
                        null
                    )
                ),
                KIBANA_PRODUCT_ORIGIN,
                KIBANA_PRODUCT_ORIGIN.getFirst(),
                DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new RuntimeException("unable to read kibana reporting template JSON", e);
        }
    }

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

    @Override
    public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
        return List.of(KIBANA_REPORTING_DS_DESCRIPTOR);
    }

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

    @Override
    public String getFeatureName() {
        return "kibana";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages Kibana configuration and reports";
    }
}
