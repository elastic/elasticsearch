/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class ComposableIndexTemplateDeprecationCheckerTests extends ESTestCase {

    private final ComposableIndexTemplateDeprecationChecker checker = new ComposableIndexTemplateDeprecationChecker();

    public void testLegacyTierSettings() {
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

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState, null);
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
}
