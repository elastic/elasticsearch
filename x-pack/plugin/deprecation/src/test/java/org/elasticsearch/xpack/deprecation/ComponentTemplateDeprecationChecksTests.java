/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
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

public class ComponentTemplateDeprecationChecksTests extends ESTestCase {

    private final ComponentTemplateDeprecationChecks componentTemplateDeprecationChecks = new ComponentTemplateDeprecationChecks();

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

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = componentTemplateDeprecationChecks.check(clusterState, null);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            SourceFieldMapper.DEPRECATION_WARNING,
            "https://github.com/elastic/elasticsearch/pull/117172",
            SourceFieldMapper.DEPRECATION_WARNING,
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }

    public void testCheckLegacyTiersInComponentTemplates() throws IOException {
        Template template = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require.data", "hot").build())
            .build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());

        Template template2 = Template.builder()
            .settings(Settings.builder().put("index.routing.allocation.require." + randomAlphaOfLength(10), "hot").build())
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

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = componentTemplateDeprecationChecks.check(clusterState, null);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting 'index.routing.allocation.require.data' is not recommended",
            "https://ela.st/migrate-to-tiers",
            "One or more of your component templates is configured with 'index.routing.allocation.require.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.get("my-template-2"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-3"), is(false));
    }
}
