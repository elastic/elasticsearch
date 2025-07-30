/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class TemplateDeprecationCheckerTests extends ESTestCase {

    private final TemplateDeprecationChecker checker = new TemplateDeprecationChecker();

    public void testCheckSourceModeInComponentTemplates() throws IOException {
        Template template = Template.builder().mappings(CompressedXContent.fromJSON("""
            { "_doc": { "_source": { "mode": "stored"} } }""")).build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        Template template2 = Template.builder().mappings(CompressedXContent.fromJSON("""
            { "_doc": { "_source": { "enabled": false} } }""")).build();
        ComponentTemplate componentTemplate2 = new ComponentTemplate(template2, 1L, new HashMap<>());

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .componentTemplates(
                        Map.of("my-template-1", componentTemplate, "my-template-2", componentTemplate, "my-template-3", componentTemplate2)
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            SourceFieldMapper.DEPRECATION_WARNING_TITLE,
            "https://ela.st/migrate-source-mode",
            SourceFieldMapper.DEPRECATION_WARNING,
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckLegacyTiersInComponentTemplates() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();
        ComponentTemplate componentTemplate2 = new ComponentTemplate(template2, 1L, new HashMap<>());

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .componentTemplates(
                        Map.of("my-template-1", componentTemplate, "my-template-2", componentTemplate, "my-template-3", componentTemplate2)
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your component templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckLegacyTierSettings() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .indexTemplates(
                        Map.of(
                            "my-template-1",
                            ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-template-2",
                            ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-template-3",
                            ComposableIndexTemplate.builder().template(template2).indexPatterns(List.of(randomAlphaOfLength(10))).build()
                        )
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your index templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testComponentAndComposableTemplateWithSameName() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Template template = Template.builder().settings(Settings.builder().put(setting, "hot").build()).build();

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", randomAlphaOfLength(10)).build())
            .build();

        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .componentTemplates(Map.of("my-template-1", componentTemplate))
                    .indexTemplates(
                        Map.of(
                            "my-template-1",
                            ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-template-2",
                            ComposableIndexTemplate.builder().template(template).indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-template-3",
                            ComposableIndexTemplate.builder().template(template2).indexPatterns(List.of(randomAlphaOfLength(10))).build()
                        )
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expectedIndexTemplateIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your index templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        final DeprecationIssue expectedComponentTemplateIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your component templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expectedIndexTemplateIssue));
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expectedComponentTemplateIssue));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expectedIndexTemplateIssue));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }
}
