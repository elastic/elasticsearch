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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class LegacyTiersDeprecationChecksTests extends ESTestCase {

    public void testLegacyTierRoutingTemplatesV1() {
        Settings.Builder settings = settings(IndexVersion.current());
        String dataValue = randomAlphanumericOfLength(10);
        settings.put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", dataValue);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .templates(
                        Map.of(
                            "my-legacy-routing-1",
                            IndexTemplateMetadata.builder("my-legacy-routing-1")
                                .patterns(List.of(randomAlphaOfLength(10)))
                                .settings(settings)
                                .build(),
                            "my-legacy-template",
                            IndexTemplateMetadata.builder("my-legacy-template").patterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-legacy-routing-2",
                            IndexTemplateMetadata.builder("my-legacy-routing-2")
                                .patterns(List.of(randomAlphaOfLength(10)))
                                .settings(settings)
                                .build()
                        )
                    )
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(clusterState));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "legacy index templates [my-legacy-routing-1,my-legacy-routing-2] have configured"
                        + " 'index.routing.allocation.require.data'. This setting is not recommended to be used for setting tiers.",
                    "https://ela.st/es-deprecation-7-node-attr-data-setting",
                    "One or more of your legacy index templates is configured with 'index.routing.allocation.require.data' settings."
                        + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines. Data tiers are"
                        + " a recommended replacement for tiered architecture clusters.",
                    false,
                    null
                )
            )
        );
    }

    public void testLegacyTierRoutingTemplatesV2() {
        Settings.Builder settings = settings(IndexVersion.current());
        String dataValue = randomAlphanumericOfLength(10);
        settings.put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", dataValue);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .indexTemplates(
                        Map.of(
                            "my-legacy-routing-2",
                            ComposableIndexTemplate.builder()
                                .indexPatterns(List.of(randomAlphaOfLength(10)))
                                .template(Template.builder().settings(settings).build())
                                .build(),
                            "my-template",
                            ComposableIndexTemplate.builder().indexPatterns(List.of(randomAlphaOfLength(10))).build(),
                            "my-legacy-routing-1",
                            ComposableIndexTemplate.builder()
                                .indexPatterns(List.of(randomAlphaOfLength(10)))
                                .template(Template.builder().settings(settings))
                                .build()
                        )
                    )
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(clusterState));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "index templates [my-legacy-routing-1,my-legacy-routing-2] have configured"
                        + " 'index.routing.allocation.require.data'. This setting is not recommended to be used for setting tiers.",
                    "https://ela.st/es-deprecation-7-node-attr-data-setting",
                    "One or more of your index templates is configured with 'index.routing.allocation.require.data' settings."
                        + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines. Data tiers are"
                        + " a recommended replacement for tiered architecture clusters.",
                    false,
                    null
                )
            )
        );
    }

    public void testLegacyTierRoutingComponentTemplates() {
        Settings.Builder settings = settings(IndexVersion.current());
        String dataValue = randomAlphanumericOfLength(10);
        settings.put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", dataValue);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .componentTemplates(
                        Map.of(
                            "my-legacy-routing-1",
                            new ComponentTemplate(Template.builder().settings(settings).build(), null, null, null),
                            "my-template",
                            new ComponentTemplate(Template.builder().build(), null, null, null),
                            "my-legacy-routing-2",
                            new ComponentTemplate(Template.builder().settings(settings).build(), null, null, null)
                        )
                    )
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(clusterState));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "component templates [my-legacy-routing-1,my-legacy-routing-2] have configured"
                        + " 'index.routing.allocation.require.data'. This setting is not recommended to be used for setting tiers.",
                    "https://ela.st/es-deprecation-7-node-attr-data-setting",
                    "One or more of your component templates is configured with 'index.routing.allocation.require.data' settings."
                        + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines. Data tiers are"
                        + " a recommended replacement for tiered architecture clusters.",
                    false,
                    null
                )
            )
        );
    }

    public void testLegacyTierRoutingIlmPolicy() {
        Settings.Builder settings = settings(IndexVersion.current());
        String dataValue = randomAlphanumericOfLength(10);
        settings.put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", dataValue);

        String phase = randomAlphaOfLength(10);
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(
            Map.of(
                "policy",
                new LifecyclePolicyMetadata(
                    new LifecyclePolicy(
                        "policy",
                        Map.of(
                            phase,
                            new Phase(
                                phase,
                                TimeValue.ONE_MINUTE,
                                Map.of(AllocateAction.NAME, new AllocateAction(null, null, null, null, Map.of("data", "hot")))
                            )
                        )
                    ),
                    null,
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            ),
            OperationMode.RUNNING
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata))
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(clusterState));
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "ILM policies [policy] have an Allocate action configured that sets 'require.data'."
                        + " This setting is not recommended to be used for setting tiers.",
                    "https://ela.st/es-deprecation-7-node-attr-data-setting",
                    "One or more of your  ILM policies has an Allocate action configured that sets 'require.data'."
                        + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                        + " Data tiers are a recommended replacement for tiered architecture clusters.",
                    false,
                    null
                )
            )
        );
    }

    public void testLegacyTierRouting() {
        Settings.Builder settings = settings(IndexVersion.current());
        String dataValue = randomAlphanumericOfLength(10);
        settings.put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", dataValue);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            INDEX_SETTINGS_CHECKS,
            c -> c.apply(indexMetadata, ClusterState.EMPTY_STATE)
        );
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "index [test] has configured 'index.routing.allocation.require.data: "
                        + dataValue
                        + "'. This setting is not recommended to be used for setting tiers.",
                    "https://ela.st/es-deprecation-7-node-attr-data-setting",
                    "One or more of your indices is configured with 'index.routing.allocation.require.data' setting. This is"
                        + " typically used to create a hot/warm or tiered architecture, based on legacy guidelines. Data tiers are"
                        + " a recommended replacement for tiered architecture clusters.",
                    false,
                    null
                )
            )
        );
    }

    public void testTruncatingResourceNames() {
        assertThat(LegacyTiersDeprecationChecks.truncatedListOfNames(new ArrayList<>()), equalTo("[]"));
        assertThat(LegacyTiersDeprecationChecks.truncatedListOfNames(new ArrayList<>(List.of("1", "2", "3"))), equalTo("[1,2,3]"));
        assertThat(
            LegacyTiersDeprecationChecks.truncatedListOfNames(new ArrayList<>(IntStream.range(1, 11).mapToObj(Integer::toString).toList())),
            equalTo("[1,10,2,3,4,5,6,7,8,9]")
        );
        assertThat(
            LegacyTiersDeprecationChecks.truncatedListOfNames(new ArrayList<>(IntStream.range(1, 16).mapToObj(Integer::toString).toList())),
            equalTo("[1,10,11,12,13,14,15,2,3,4,...]")
        );
    }
}
