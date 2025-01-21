/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class LegacyIndexTemplateDeprecationCheckerTests extends ESTestCase {

    private final LegacyIndexTemplateDeprecationChecker checker = new LegacyIndexTemplateDeprecationChecker();

    public void testLegacyTierSettings() {
        String setting = "index.routing.allocation." + randomFrom("include", "require", "exclude") + ".data";
        Settings deprecatedSettings = Settings.builder().put(setting, "hot").build();
        Settings nonDeprecatedSettings = Settings.builder().put(setting, "non-deprecated").build();

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder("my-template-1")
                            .patterns(List.of(randomAlphaOfLength(10)))
                            .settings(deprecatedSettings)
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder("my-template-2")
                            .patterns(List.of(randomAlphaOfLength(10)))
                            .settings(nonDeprecatedSettings)
                            .build()
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState, null);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your legacy index templates is configured with 'index.routing.allocation.*.data' settings."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            DeprecationIssue.createMetaMapForRemovableSettings(List.of(setting))
        );
        assertThat(issuesByComponentTemplate.get("my-template-1"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("my-template-2"), is(false));
    }
}
