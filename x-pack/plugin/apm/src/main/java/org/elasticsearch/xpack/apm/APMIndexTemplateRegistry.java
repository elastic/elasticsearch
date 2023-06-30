/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates all index templates and ingest pipelines that are required for using Elastic APM.
 */
public class APMIndexTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(APMIndexTemplateRegistry.class);

    // TODO(axw) when the template version is updated, data streams using the old
    // version should be rolled over after upgrading. Ideally this would be done
    // automatically: https://github.com/elastic/elasticsearch/issues/96521

    public static final String APM_TEMPLATE_VERSION_VARIABLE = "xpack.apm.template.version";

    private final Map<String, ComponentTemplate> componentTemplates;
    private final Map<String, ComposableIndexTemplate> composableIndexTemplates;
    private final List<IngestPipelineConfig> ingestPipelines;

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
            final int version = ((Number) apmResources.get("version")).intValue();
            final List<Object> componentTemplateNames = (List<Object>) apmResources.get("component-templates");
            final List<Object> indexTemplateNames = (List<Object>) apmResources.get("index-templates");
            final List<Object> ingestPipelineNames = (List<Object>) apmResources.get("ingest-pipelines");

            componentTemplates = componentTemplateNames.stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadComponentTemplate(name, version)));
            composableIndexTemplates = indexTemplateNames.stream()
                .map(o -> (String) o)
                .collect(Collectors.toMap(name -> name, name -> loadIndexTemplate(name, version)));
            ingestPipelines = ingestPipelineNames.stream()
                .map(o -> (String) o)
                .map(name -> loadIngestPipeline(name, version))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        return componentTemplates;
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return composableIndexTemplates;
    }

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return ingestPipelines;
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

    private static IngestPipelineConfig loadIngestPipeline(String name, int version) {
        return new IngestPipelineConfig(
            name,
            version,
            Collections.emptyList(),
            () -> new PutPipelineRequest(
                name,
                new BytesArray(loadVersionedResourceUTF8("/ingest-pipelines/" + name + ".yaml", version)),
                XContentType.YAML
            )
        );
    }

    private static byte[] loadVersionedResourceUTF8(String name, int version) {
        try {
            String content = loadResource(name);
            content = TemplateUtils.replaceVariable(content, APM_TEMPLATE_VERSION_VARIABLE, String.valueOf(version));
            return content.getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String loadResource(String name) throws IOException {
        InputStream is = APMIndexTemplateRegistry.class.getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Resource [" + name + "] not found in classpath.");
        }
        return Streams.readFully(is).utf8ToString();
    }
}
