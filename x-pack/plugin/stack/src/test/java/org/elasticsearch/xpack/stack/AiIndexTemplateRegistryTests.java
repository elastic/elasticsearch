/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_DS_PATTERN;
import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_DS_SETTINGS_COMPONENT_NAME;
import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_DS_TEMPLATE_NAME;
import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_IDX_PATTERN;
import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_IDX_TEMPLATE_NAME;
import static org.elasticsearch.xpack.stack.AiIndexTemplateRegistry.AI_INDEX_MAPPINGS_COMPONENT_NAME;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AiIndexTemplateRegistryTests extends ESTestCase {

    private ThreadPool threadPool;
    private AiIndexTemplateRegistry registry;

    private AiIndexTemplateRegistry createRegistry(Settings settings) {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        return new AiIndexTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        );
    }

    @After
    public void stopPool() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    public void testDisabledDoesNotAddTemplates() {
        Settings settings = Settings.builder().put(AiIndexTemplateRegistry.AI_INDEX_REGISTRY_ENABLED.getKey(), false).build();
        registry = createRegistry(settings);
        assertThat(registry.getComponentTemplateConfigs(), anEmptyMap());
        assertThat(registry.getComposableTemplateConfigs(), anEmptyMap());
    }

    public void testEnabledAddsAllComponentAndComposableTemplates() {
        registry = createRegistry(Settings.EMPTY);
        assertThat(
            registry.getComponentTemplateConfigs().keySet(),
            containsInAnyOrder(AI_INDEX_MAPPINGS_COMPONENT_NAME, AI_INDEX_DS_SETTINGS_COMPONENT_NAME)
        );
        assertThat(
            registry.getComposableTemplateConfigs().keySet(),
            containsInAnyOrder(AI_INDEX_IDX_TEMPLATE_NAME, AI_INDEX_DS_TEMPLATE_NAME)
        );
    }

    public void testSharedMappingsComponentDefinesSemanticFields() throws IOException {
        registry = createRegistry(Settings.EMPTY);
        ComponentTemplate mappings = registry.getComponentTemplateConfigs().get(AI_INDEX_MAPPINGS_COMPONENT_NAME);
        assertThat(mappings, notNullValue());

        Map<String, Object> properties = mappingProperties(mappings);
        assertThat(propertyType(properties, "@timestamp"), equalTo("date"));
        assertThat(propertyType(properties, "type"), equalTo("keyword"));
        assertThat(propertyType(properties, "title"), equalTo("text"));
        assertThat(propertyType(properties, "attributes"), equalTo("flattened"));

        // The text fields expose a semantic_text sub-field for hybrid retrieval.
        for (String field : new String[] { "title", "description", "content" }) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldDef = (Map<String, Object>) properties.get(field);
            @SuppressWarnings("unchecked")
            Map<String, Object> subFields = (Map<String, Object>) fieldDef.get("fields");
            @SuppressWarnings("unchecked")
            Map<String, Object> semantic = (Map<String, Object>) subFields.get("semantic");
            assertThat("field [" + field + "]", semantic.get("type"), equalTo("semantic_text"));
        }
    }

    public void testDataStreamSettingsComponent() {
        registry = createRegistry(Settings.EMPTY);
        ComponentTemplate dsSettings = registry.getComponentTemplateConfigs().get(AI_INDEX_DS_SETTINGS_COMPONENT_NAME);
        assertThat(dsSettings, notNullValue());
        Settings settings = dsSettings.template().settings();
        assertThat(settings.get("index.mode"), equalTo("columnar"));
        assertThat(settings.get("index.sort.field"), equalTo("@timestamp"));
    }

    public void testStandardIndexTemplateComposition() {
        registry = createRegistry(Settings.EMPTY);
        ComposableIndexTemplate template = registry.getComposableTemplateConfigs().get(AI_INDEX_IDX_TEMPLATE_NAME);
        assertThat(template, notNullValue());
        assertThat(template.indexPatterns(), contains(AI_INDEX_IDX_PATTERN));
        // The optional ai-index@custom escape hatch is composed last so user overrides win.
        assertThat(template.composedOf(), contains(AI_INDEX_MAPPINGS_COMPONENT_NAME, "ai-index@custom"));
        assertThat(template.getIgnoreMissingComponentTemplates(), contains("ai-index@custom"));
        // A standard index template, not a data stream.
        assertThat(template.getDataStreamTemplate(), nullValue());
    }

    public void testDataStreamTemplateComposition() {
        registry = createRegistry(Settings.EMPTY);
        ComposableIndexTemplate template = registry.getComposableTemplateConfigs().get(AI_INDEX_DS_TEMPLATE_NAME);
        assertThat(template, notNullValue());
        assertThat(template.indexPatterns(), contains(AI_INDEX_DS_PATTERN));
        assertThat(
            template.composedOf(),
            containsInAnyOrder(AI_INDEX_MAPPINGS_COMPONENT_NAME, AI_INDEX_DS_SETTINGS_COMPONENT_NAME, "ai-index@custom")
        );
        assertThat(template.getIgnoreMissingComponentTemplates(), contains("ai-index@custom"));
        assertThat(template.getDataStreamTemplate(), notNullValue());
    }

    public void testRegistryIsUpToDate() throws Exception {
        assumeFalse("This test relies on text files checksum, which is inconsistent between Windows and Linux", Constants.WINDOWS);
        CRC32 crc32 = new CRC32();
        for (IndexTemplateConfig config : AiIndexTemplateRegistry.componentTemplateConfigs()) {
            crc32.update(loadTemplate(config.getFileName()));
        }
        for (IndexTemplateConfig config : AiIndexTemplateRegistry.composableTemplateConfigs()) {
            crc32.update(loadTemplate(config.getFileName()));
        }
        String computedChecksum = Long.toHexString(crc32.getValue());
        assertEquals(
            "The AiIndexTemplateRegistry.REGISTRY_VERSION must be incremented when templates are changed. "
                + "Please update REGISTRY_VERSION to "
                + (AiIndexTemplateRegistry.REGISTRY_VERSION + 1)
                + " and update COMPUTED_CHECKSUM to \""
                + computedChecksum
                + "\"",
            AiIndexTemplateRegistry.COMPUTED_CHECKSUM,
            computedChecksum
        );
    }

    private byte[] loadTemplate(String name) throws IOException {
        try (InputStream is = AiIndexTemplateRegistry.class.getResourceAsStream(name)) {
            if (is == null) {
                throw new IOException("Template [" + name + "] not found");
            }
            return is.readAllBytes();
        }
    }

    private static Map<String, Object> mappingProperties(ComponentTemplate template) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, template.template().mappings().string())
        ) {
            Map<String, Object> mappings = parser.map();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            return properties;
        }
    }

    private static String propertyType(Map<String, Object> properties, String field) {
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldDef = (Map<String, Object>) properties.get(field);
        return (String) fieldDef.get("type");
    }
}
