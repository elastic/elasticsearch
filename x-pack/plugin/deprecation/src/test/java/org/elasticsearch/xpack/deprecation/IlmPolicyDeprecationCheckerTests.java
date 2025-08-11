/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class IlmPolicyDeprecationCheckerTests extends ESTestCase {

    private final IlmPolicyDeprecationChecker checker = new IlmPolicyDeprecationChecker();

    public void testLegacyTierSettings() {

        LifecyclePolicy deprecatedTiersPolicy = new LifecyclePolicy(
            TimeseriesLifecycleType.INSTANCE,
            "deprecated-tiers",
            Map.of(
                "warm",
                new Phase(
                    "warm",
                    TimeValue.ONE_MINUTE,
                    Map.of(AllocateAction.NAME, new AllocateAction(null, null, Map.of("data", "hot"), null, null))
                )
            ),
            Map.of(),
            randomOptionalBoolean()
        );
        LifecyclePolicy otherAttributePolicy = new LifecyclePolicy(
            TimeseriesLifecycleType.INSTANCE,
            "other-attribute",
            Map.of(
                "warm",
                new Phase(
                    "warm",
                    TimeValue.ONE_MINUTE,
                    Map.of(AllocateAction.NAME, new AllocateAction(null, null, Map.of("other", "hot"), null, null))
                )
            ),
            Map.of(),
            randomOptionalBoolean()
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(
                            Map.of(
                                "deprecated-tiers",
                                new LifecyclePolicyMetadata(
                                    deprecatedTiersPolicy,
                                    Map.of(),
                                    randomNonNegativeLong(),
                                    randomNonNegativeLong()
                                ),
                                "other-attribute",
                                new LifecyclePolicyMetadata(
                                    otherAttributePolicy,
                                    Map.of(),
                                    randomNonNegativeLong(),
                                    randomNonNegativeLong()
                                )
                            ),
                            OperationMode.RUNNING
                        )
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring tiers via filtered allocation is not recommended.",
            "https://ela.st/migrate-to-tiers",
            "One or more of your ILM policies is configuring tiers via the 'data' node attribute."
                + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                + " Data tiers are a recommended replacement for tiered architecture clusters.",
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("deprecated-tiers"), hasItem(expected));
        assertThat(issuesByComponentTemplate.containsKey("other-attribute"), is(false));
    }

    public void testFrozenAction() {

        LifecyclePolicy deprecatedTiersPolicy = new LifecyclePolicy(
            TimeseriesLifecycleType.INSTANCE,
            "deprecated-action",
            Map.of("cold", new Phase("cold", TimeValue.ONE_MINUTE, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE))),
            Map.of(),
            randomOptionalBoolean()
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(
                            Map.of(
                                "deprecated-action",
                                new LifecyclePolicyMetadata(
                                    deprecatedTiersPolicy,
                                    Map.of(),
                                    randomNonNegativeLong(),
                                    randomNonNegativeLong()
                                )
                            ),
                            OperationMode.RUNNING
                        )
                    )
            )
            .build();

        Map<String, List<DeprecationIssue>> issuesByComponentTemplate = checker.check(clusterState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "ILM policy [deprecated-action] contains the action 'freeze' that is deprecated and will be removed in a future version.",
            "https://ela.st/es-deprecation-7-frozen-index",
            "This action is already a noop so it can be safely removed, because frozen indices no longer offer any advantages."
                + " Consider cold or frozen tiers in place of frozen indices.",
            false,
            null
        );
        assertThat(issuesByComponentTemplate.get("deprecated-action"), hasItem(expected));
    }
}
