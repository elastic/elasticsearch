/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionServiceTests extends AutoscalingTestCase {
    public void testMultiplePoliciesFixedDecision() {
        AutoscalingDecisionService service = new AutoscalingDecisionService(Set.of(new FixedAutoscalingDeciderService()));
        Set<String> policyNames = IntStream.range(0, randomIntBetween(1, 10))
            .mapToObj(i -> "test_ " + randomAlphaOfLength(10))
            .collect(Collectors.toSet());

        SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>(
            policyNames.stream()
                .map(s -> Tuple.tuple(s, new AutoscalingPolicyMetadata(new AutoscalingPolicy(s, randomFixedDeciders()))))
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(AutoscalingMetadata.NAME, new AutoscalingMetadata(policies)))
            .build();
        SortedMap<String, AutoscalingDecisions> decisions = service.decide(state, new ClusterInfo() {
        });
        assertThat(decisions.keySet(), equalTo(policyNames));
        for (Map.Entry<String, AutoscalingDecisions> entry : decisions.entrySet()) {
            AutoscalingDecisions decision = entry.getValue();
            assertThat(decision.tier(), equalTo(entry.getKey()));
            SortedMap<String, AutoscalingDeciderConfiguration> deciders = policies.get(decision.tier()).policy().deciders();
            assertThat(deciders.size(), equalTo(1));
            FixedAutoscalingDeciderConfiguration configuration = (FixedAutoscalingDeciderConfiguration) deciders.values().iterator().next();
            AutoscalingCapacity requiredCapacity = calculateFixedDecisionCapacity(configuration);
            assertThat(decision.requiredCapacity(), equalTo(requiredCapacity));
            assertThat(decision.decisions().size(), equalTo(1));
            AutoscalingDecision deciderDecision = decision.decisions().get(deciders.firstKey());
            assertNotNull(deciderDecision);
            assertThat(deciderDecision.requiredCapacity(), equalTo(requiredCapacity));
            ByteSizeValue storage = configuration.storage();
            ByteSizeValue memory = configuration.memory();
            int nodes = configuration.nodes();
            assertThat(deciderDecision.reason(), equalTo(new FixedAutoscalingDeciderService.FixedReason(storage, memory, nodes)));
            assertThat(
                deciderDecision.reason().summary(),
                equalTo("fixed storage [" + storage + "] memory [" + memory + "] nodes [" + nodes + "]")
            );

            // there is no nodes in any tier.
            assertThat(decision.currentCapacity(), equalTo(AutoscalingCapacity.ZERO));
        }
    }

    private SortedMap<String, AutoscalingDeciderConfiguration> randomFixedDeciders() {
        return new TreeMap<>(
            Map.of(
                FixedAutoscalingDeciderConfiguration.NAME,
                new FixedAutoscalingDeciderConfiguration(
                    randomNullableByteSizeValue(),
                    randomNullableByteSizeValue(),
                    randomIntBetween(1, 10)
                )
            )
        );
    }

    private AutoscalingCapacity calculateFixedDecisionCapacity(FixedAutoscalingDeciderConfiguration configuration) {
        ByteSizeValue totalStorage = configuration.storage() != null
            ? new ByteSizeValue(configuration.storage().getBytes() * configuration.nodes())
            : null;
        ByteSizeValue totalMemory = configuration.memory() != null
            ? new ByteSizeValue(configuration.memory().getBytes() * configuration.nodes())
            : null;

        if (totalStorage == null && totalMemory == null) {
            return null;
        } else {
            return new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(totalStorage, totalMemory),
                new AutoscalingCapacity.AutoscalingResources(configuration.storage(), configuration.memory())
            );
        }
    }

    public void testContext() {
        String tier = randomAlphaOfLength(5);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterInfo info = ClusterInfo.EMPTY;
        AutoscalingDecisionService.DecisionAutoscalingDeciderContext context =
            new AutoscalingDecisionService.DecisionAutoscalingDeciderContext(tier, state, info);

        assertSame(state, context.state());
        // there is no nodes in any tier.
        assertThat(context.currentCapacity(), equalTo(AutoscalingCapacity.ZERO));

        tier = "data";
        state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(new DiscoveryNode("nodeId", buildNewFakeTransportAddress(), Version.CURRENT)))
            .build();
        context = new AutoscalingDecisionService.DecisionAutoscalingDeciderContext(tier, state, info);

        assertNull(context.currentCapacity());

        ImmutableOpenMap.Builder<String, DiskUsage> leastUsages = ImmutableOpenMap.<String, DiskUsage>builder();
        ImmutableOpenMap.Builder<String, DiskUsage> mostUsages = ImmutableOpenMap.<String, DiskUsage>builder();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        long sumTotal = 0;
        long maxTotal = 0;
        for (int i = 0; i < randomIntBetween(1, 5); ++i) {
            String nodeId = "nodeId" + i;
            nodes.add(new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Version.CURRENT));

            long total = randomLongBetween(1, 1L << 40);
            long total1 = randomBoolean() ? total : randomLongBetween(0, total);
            long total2 = total1 != total ? total : randomLongBetween(0, total);
            leastUsages.fPut(nodeId, new DiskUsage(nodeId, null, null, total1, randomLongBetween(0, total)));
            mostUsages.fPut(nodeId, new DiskUsage(nodeId, null, null, total2, randomLongBetween(0, total)));
            sumTotal += total;
            maxTotal = Math.max(total, maxTotal);
        }
        state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        info = new ClusterInfo(leastUsages.build(), mostUsages.build(), null, null, null);
        context = new AutoscalingDecisionService.DecisionAutoscalingDeciderContext(tier, state, info);

        AutoscalingCapacity capacity = context.currentCapacity();
        assertThat(capacity.node().storage(), equalTo(new ByteSizeValue(maxTotal)));
        assertThat(capacity.tier().storage(), equalTo(new ByteSizeValue(sumTotal)));
        // todo: fix these once we know memory of all node on master.
        assertThat(capacity.node().memory(), equalTo(ByteSizeValue.ZERO));
        assertThat(capacity.tier().memory(), equalTo(ByteSizeValue.ZERO));
    }
}
