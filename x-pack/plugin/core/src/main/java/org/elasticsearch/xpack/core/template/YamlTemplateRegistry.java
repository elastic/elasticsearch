/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.template.ResourceUtils.loadResource;
import static org.elasticsearch.xpack.core.template.ResourceUtils.loadVersionedResourceUTF8;

/**
 * Creates index templates and ingest pipelines based on YAML files from resources.
 */
public abstract class YamlTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(YamlTemplateRegistry.class);
    // this node feature is a redefinition of {@link DataStreamFeatures#DATA_STREAM_LIFECYCLE} and it's meant to avoid adding a
    // dependency to the data-streams module just for this
    public static final NodeFeature DATA_STREAM_LIFECYCLE = new NodeFeature("data_stream.lifecycle");
    private final int version;

    private final Map<String, ComponentTemplate> componentTemplates;
    private final Map<String, ComposableIndexTemplate> composableIndexTemplates;
    private final List<IngestPipelineConfig> ingestPipelines;
    private final FeatureService featureService;
    private volatile boolean enabled;

    @SuppressWarnings({ "unchecked", "this-escape" })
    public YamlTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        FeatureService featureService
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);

        try {
            final Map<String, Object> resources = XContentHelper.convertToMap(
                YamlXContent.yamlXContent,
                loadResource(this.getClass(), "/resources.yaml"),
                false
            );
            version = (((Number) resources.get("version")).intValue());

            final List<Object> componentTemplateNames = (List<Object>) resources.get("component-templates");
            final List<Object> indexTemplateNames = (List<Object>) resources.get("index-templates");
            final List<Object> ingestPipelineConfigs = (List<Object>) resources.get("ingest-pipelines");

            componentTemplates = Optional.ofNullable(componentTemplateNames)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadComponentTemplate(name, version)));
            composableIndexTemplates = Optional.ofNullable(indexTemplateNames)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadIndexTemplate(name, version)));
            ingestPipelines = Optional.ofNullable(ingestPipelineConfigs)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (Map<String, Map<String, Object>>) o)
                .map(map -> {
                    Map.Entry<String, Map<String, Object>> pipelineConfig = map.entrySet().iterator().next();
                    return loadIngestPipeline(
                        pipelineConfig.getKey(),
                        version,
                        (List<String>) pipelineConfig.getValue().get("dependencies")
                    );
                })
                .collect(Collectors.toList());
            this.featureService = featureService;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getVersion() {
        return version;
    }

    /***
     *
     * @return A friendly, human readable name of the index template regisry
     */
    public abstract String getName();

    public void setEnabled(boolean enabled) {
        logger.info("{} index template registry is {}", getName(), enabled ? "enabled" : "disabled");
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    protected boolean isClusterReady(ClusterChangedEvent event) {
        // Ensure current version of the components are installed only after versions that support data stream lifecycle
        // due to the use of the feature in all the `@lifecycle` component templates
        return featureService.clusterHasFeature(event.state(), DATA_STREAM_LIFECYCLE);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    public Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        if (enabled) {
            return componentTemplates;
        } else {
            return Map.of();
        }
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        if (enabled) {
            return composableIndexTemplates;
        } else {
            return Map.of();
        }
    }

    @Override
    public List<IngestPipelineConfig> getIngestPipelines() {
        if (enabled) {
            return ingestPipelines;
        } else {
            return Collections.emptyList();
        }
    }

    protected abstract String getVersionProperty();

    private ComponentTemplate loadComponentTemplate(String name, int version) {
        try {
            final byte[] content = loadVersionedResourceUTF8(
                this.getClass(),
                "/component-templates/" + name + ".yaml",
                version,
                getVersionProperty()
            );
            try (var parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content)) {
                return ComponentTemplate.parse(parser);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to load " + getName() + " Ingest plugin's component template: " + name, e);
        }
    }

    private ComposableIndexTemplate loadIndexTemplate(String name, int version) {
        try {
            final byte[] content = loadVersionedResourceUTF8(
                this.getClass(),
                "/index-templates/" + name + ".yaml",
                version,
                getVersionProperty()
            );
            try (var parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, content)) {
                return ComposableIndexTemplate.parse(parser);
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to load " + getName() + " Ingest plugin's index template: " + name, e);
        }
    }

    private IngestPipelineConfig loadIngestPipeline(String name, int version, @Nullable List<String> dependencies) {
        if (dependencies == null) {
            dependencies = Collections.emptyList();
        }
        return new YamlIngestPipelineConfig(
            name,
            "/ingest-pipelines/" + name + ".yaml",
            version,
            getVersionProperty(),
            dependencies,
            this.getClass()
        );
    }

    @Override
    protected boolean applyRolloverAfterTemplateV2Upgrade() {
        return true;
    }
}
