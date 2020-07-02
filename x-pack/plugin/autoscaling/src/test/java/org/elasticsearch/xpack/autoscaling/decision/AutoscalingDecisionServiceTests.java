/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AutoscalingDecisionServiceTests extends AutoscalingTestCase {
    public void testAlwaysDecision() {
        AutoscalingDecisionService service = new AutoscalingDecisionService(Set.of(new AlwaysAutoscalingDeciderService()));
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> randomAlphaOfLength(10))
            .collect(Collectors.toSet());
        SortedMap<String, AutoscalingDeciderConfiguration> deciders = new TreeMap<>(
            Map.of(AlwaysAutoscalingDeciderConfiguration.NAME, new AlwaysAutoscalingDeciderConfiguration())
        );
        SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>(
            policyNames.stream()
                .map(s -> Tuple.tuple(s, new AutoscalingPolicyMetadata(new AutoscalingPolicy(s, deciders))))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(AutoscalingMetadata.NAME, new AutoscalingMetadata(policies)))
            .build();
        SortedMap<String, AutoscalingDecisions> decisions = service.decide(state);
        SortedMap<String, AutoscalingDecisions> expected = new TreeMap<>(
            policyNames.stream()
                .map(
                    s -> Tuple.tuple(
                        s,
                        new AutoscalingDecisions(
                            List.of(
                                new AutoscalingDecision(
                                    AlwaysAutoscalingDeciderConfiguration.NAME,
                                    AutoscalingDecisionType.SCALE_UP,
                                    "always"
                                )
                            )
                        )
                    )
                )
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );
        assertThat(decisions, Matchers.equalTo(expected));
    }
}
