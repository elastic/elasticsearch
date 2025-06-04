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
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.template.ResourceUtils.loadResource;
import static org.elasticsearch.xpack.core.template.ResourceUtils.loadVersionedResourceUTF8;

/**
 * Creates index templates and ingest pipelines based on YAML files defined in resources.yaml.
 */
public abstract class YamlTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(YamlTemplateRegistry.class);
    private final int version;

    private final Map<String, ComponentTemplate> componentTemplates;
    private final Map<String, ComposableIndexTemplate> composableIndexTemplates;
    private final List<IngestPipelineConfig> ingestPipelines;
    private final List<LifecyclePolicy> lifecyclePolicies;
    private volatile boolean enabled;

    public YamlTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        this(nodeSettings, clusterService, threadPool, client, xContentRegistry, ignored -> true, projectResolver);
    }

    @SuppressWarnings({ "unchecked", "this-escape" })
    public YamlTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        Predicate<String> templateFilter,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
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
            final List<Object> lifecyclePolicyConfigs = (List<Object>) resources.get("lifecycle-policies");

            componentTemplates = Optional.ofNullable(componentTemplateNames)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (String) o)
                .filter(templateFilter)
                .collect(Collectors.toMap(name -> name, name -> loadComponentTemplate(name, version)));
            composableIndexTemplates = Optional.ofNullable(indexTemplateNames)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (String) o)
                .filter(templateFilter)
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
            lifecyclePolicies = Optional.ofNullable(lifecyclePolicyConfigs)
                .orElse(Collections.emptyList())
                .stream()
                .map(o -> (String) o)
                .filter(templateFilter)
                .map(this::loadLifecyclePolicy)
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    public int getVersion() {
        return version;
    }

    /***
     *
     * @return A friendly, human-readable name of the index template registry
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

    @Override
    public List<LifecyclePolicy> getLifecyclePolicies() {
        if (enabled) {
            return lifecyclePolicies;
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
            throw new ElasticsearchException("failed to load " + getName() + " Ingest plugin's component template: " + name, e);
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
            throw new ElasticsearchException("failed to load " + getName() + " Ingest plugin's index template: " + name, e);
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

    // IndexTemplateRegistry ensures that ILM lifecycle policies are not loaded
    // when in DSL only mode.
    private LifecyclePolicy loadLifecyclePolicy(String name) {
        try {
            var rawPolicy = loadResource(this.getClass(), "/lifecycle-policies/" + name + ".yaml");
            return LifecyclePolicyUtils.parsePolicy(rawPolicy, name, LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY, XContentType.YAML);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    protected boolean applyRolloverAfterTemplateV2Update() {
        return true;
    }
}
