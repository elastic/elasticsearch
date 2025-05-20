/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSimulateIndexTemplateActionTests extends ESTestCase {

    public void testSettingsProviderIsOverridden() throws Exception {
        String matchingTemplate = "test_template";
        String indexName = "test_index_name";
        CompressedXContent expectedMockMapping = new CompressedXContent(Map.of("key", "value"));

        boolean isDslOnlyMode = false;
        ProjectMetadata simulatedProject = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(
                Map.of(
                    matchingTemplate,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("test_index*"))
                        .template(new Template(Settings.builder().put("test-setting", 1).put("test-setting-2", 2).build(), null, null))
                        .build()
                )
            )
            .build();

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.withTempIndexService(any(IndexMetadata.class), any())).thenReturn(List.of()) // First call is mocked to return
                                                                                                         // aliases
            .thenReturn(expectedMockMapping); // Second call is mocked to return the merged mappings

        // This is not actually called in this test
        SystemIndices systemIndices = mock(SystemIndices.class);

        // Create a setting provider that sets the test-setting to 0
        Set<IndexSettingProvider> indexSettingsProviders = Set.of(new IndexSettingProvider() {
            @Override
            public Settings getAdditionalIndexSettings(
                String indexName,
                String dataStreamName,
                IndexMode templateIndexMode,
                ProjectMetadata projectMetadata,
                Instant resolvedAt,
                Settings allSettings,
                List<CompressedXContent> combinedTemplateMappings
            ) {
                return Settings.builder().put("test-setting", 0).build();
            }
        }, new IndexSettingProvider() {
            @Override
            public Settings getAdditionalIndexSettings(
                String indexName,
                String dataStreamName,
                IndexMode templateIndexMode,
                ProjectMetadata projectMetadata,
                Instant resolvedAt,
                Settings indexTemplateAndCreateRequestSettings,
                List<CompressedXContent> combinedTemplateMappings
            ) {
                return Settings.builder().put("test-setting-2", 10).build();
            }

            @Override
            public boolean overrulesTemplateAndRequestSettings() {
                return true;
            }
        });

        Template resolvedTemplate = TransportSimulateIndexTemplateAction.resolveTemplate(
            matchingTemplate,
            indexName,
            simulatedProject,
            isDslOnlyMode,
            xContentRegistry(),
            indicesService,
            systemIndices,
            indexSettingsProviders
        );

        assertThat(resolvedTemplate.settings().getAsInt("test-setting", -1), is(1));
        assertThat(resolvedTemplate.settings().getAsInt("test-setting-2", -1), is(10));
    }
}
