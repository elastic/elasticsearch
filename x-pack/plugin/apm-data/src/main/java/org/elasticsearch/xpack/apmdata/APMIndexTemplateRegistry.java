/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.apmdata.ResourceUtils.APM_TEMPLATE_VERSION_VARIABLE;
import static org.elasticsearch.xpack.apmdata.ResourceUtils.loadResource;
import static org.elasticsearch.xpack.apmdata.ResourceUtils.loadVersionedResourceUTF8;

/**
 * Creates all index templates and ingest pipelines that are required for using Elastic APM.
 */
public class APMIndexTemplateRegistry extends IndexTemplateRegistry {
    private final int version;

    private final Map<String, ComponentTemplate> componentTemplates;
    private final Map<String, ComposableIndexTemplate> composableIndexTemplates;
    private final List<IngestPipelineConfig> ingestPipelines;
    private final boolean enabled;

    @SuppressWarnings("unchecked")
    public APMIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);

        try {
            final Map<String, Object> apmResources = XContentHelper.convertToMap(
                YamlXContent.yamlXContent,
                loadResource("/resources.yaml"),
                false
            );
            version = (((Number) apmResources.get("version")).intValue());
            final List<Object> componentTemplateNames = (List<Object>) apmResources.get("component-templates");
            final List<Object> indexTemplateNames = (List<Object>) apmResources.get("index-templates");
            final List<Object> ingestPipelineConfigs = (List<Object>) apmResources.get("ingest-pipelines");

            componentTemplates = componentTemplateNames.stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadComponentTemplate(name, version)));
            composableIndexTemplates = indexTemplateNames.stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadIndexTemplate(name, version)));
            ingestPipelines = ingestPipelineConfigs.stream().map(o -> (Map<String, Map<String, Object>>) o).map(map -> {
                Map.Entry<String, Map<String, Object>> pipelineConfig = map.entrySet().iterator().next();
                return loadIngestPipeline(pipelineConfig.getKey(), version, (List<String>) pipelineConfig.getValue().get("dependencies"));
            }).collect(Collectors.toList());

            enabled = XPackSettings.APM_DATA_ENABLED.get(nodeSettings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getVersion() {
        return version;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.APM_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        if (enabled) {
            return componentTemplates;
        } else {
            return Map.of();
        }
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (enabled) {
            return composableIndexTemplates;
        } else {
            return Map.of();
        }
    }

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        if (enabled) {
            return ingestPipelines;
        } else {
            return Collections.emptyList();
        }
    }

    private static ComponentTemplate loadComponentTemplate(String name, int version) {
        try {
            final byte[] content = loadVersionedResourceUTF8("/component-templates/" + name + ".yaml", version);
            return ComponentTemplate.parse(YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content));
        } catch (Exception e) {
            throw new RuntimeException("failed to load APM component template: " + name, e);
        }
    }

    private static ComposableIndexTemplate loadIndexTemplate(String name, int version) {
        try {
            final byte[] content = loadVersionedResourceUTF8("/index-templates/" + name + ".yaml", version);
            return ComposableIndexTemplate.parse(YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content));
        } catch (Exception e) {
            throw new RuntimeException("failed to load APM index template: " + name, e);
        }
    }

    private static IngestPipelineConfig loadIngestPipeline(String name, int version, @Nullable List<String> dependencies) {
        if (dependencies == null) {
            dependencies = Collections.emptyList();
        }
        return new YamlIngestPipelineConfig(
            name,
            "/ingest-pipelines/" + name + ".yaml",
            version,
            APM_TEMPLATE_VERSION_VARIABLE,
            dependencies
        );
    }

    @Override
    protected boolean applyRolloverAfterTemplateV2Upgrade() {
        return true;
    }
}
