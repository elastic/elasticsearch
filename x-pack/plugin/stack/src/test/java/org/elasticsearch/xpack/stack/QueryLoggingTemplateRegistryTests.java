/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
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
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.not;

public class QueryLoggingTemplateRegistryTests extends ESTestCase {

    private ThreadPool threadPool;

    @After
    public void stopPool() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    public void testDisabledDoesNotAddTemplates() {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        Settings settings = Settings.builder().put(QueryLoggingTemplateRegistry.QUERY_LOGGING_REGISTRY_ENABLED.getKey(), false).build();
        QueryLoggingTemplateRegistry registry = new QueryLoggingTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        );
        assertThat(registry.getComposableTemplateConfigs(), anEmptyMap());
        assertThat(registry.getComponentTemplateConfigs(), anEmptyMap());
    }

    public void testEnabledAddsTemplates() {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        QueryLoggingTemplateRegistry registry = new QueryLoggingTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        );
        assertThat(registry.getComposableTemplateConfigs(), not(anEmptyMap()));
        assertThat(registry.getComponentTemplateConfigs(), not(anEmptyMap()));
    }

    public void testMappingsComponentTemplateHasValidDynamicTemplates() throws IOException {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        QueryLoggingTemplateRegistry registry = new QueryLoggingTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        );
        ComponentTemplate mappingsTemplate = registry.getComponentTemplateConfigs()
            .get(QueryLoggingTemplateRegistry.QUERY_LOGGING_MAPPINGS_NAME);
        assertNotNull(mappingsTemplate);
        CompressedXContent mappings = mappingsTemplate.template().mappings();
        assertNotNull(mappings);

        Set<String> validMatchMappingTypes = Set.of("object", "string", "long", "double", "boolean", "date", "binary");
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, mappings.string())) {
            Map<String, Object> mappingsMap = parser.map();
            @SuppressWarnings("unchecked")
            List<Object> dynamicTemplates = (List<Object>) mappingsMap.get("dynamic_templates");
            if (dynamicTemplates != null) {
                for (int i = 0; i < dynamicTemplates.size(); i++) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> entry = (Map<String, Object>) dynamicTemplates.get(i);
                    assertEquals(
                        "dynamic_templates[" + i + "] must have exactly one key (its name), but had: " + entry.keySet(),
                        1,
                        entry.size()
                    );
                    @SuppressWarnings("unchecked")
                    Map<String, Object> templateDef = (Map<String, Object>) entry.values().iterator().next();
                    Object matchMappingType = templateDef.get("match_mapping_type");
                    if (matchMappingType instanceof String s) {
                        assertTrue(
                            "dynamic_templates["
                                + i
                                + "].match_mapping_type ["
                                + s
                                + "] is not a valid type; valid: "
                                + validMatchMappingTypes,
                            validMatchMappingTypes.contains(s)
                        );
                    } else if (matchMappingType instanceof List<?> list) {
                        for (Object t : list) {
                            assertTrue(
                                "dynamic_templates["
                                    + i
                                    + "].match_mapping_type ["
                                    + t
                                    + "] is not a valid type; valid: "
                                    + validMatchMappingTypes,
                                validMatchMappingTypes.contains(t)
                            );
                        }
                    }
                }
            }
        }
    }
}
