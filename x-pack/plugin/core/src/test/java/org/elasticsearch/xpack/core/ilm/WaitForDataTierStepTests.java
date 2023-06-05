/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class WaitForDataTierStepTests extends AbstractStepTestCase<WaitForDataTierStep> {

    @Override
    protected WaitForDataTierStep createRandomInstance() {
        return new WaitForDataTierStep(randomStepKey(), randomStepKey(), randomAlphaOfLength(5));
    }

    @Override
    protected WaitForDataTierStep mutateInstance(WaitForDataTierStep instance) {
        switch (between(0, 2)) {
            case 0:
                return new WaitForDataTierStep(
                    randomValueOtherThan(instance.getKey(), AbstractStepTestCase::randomStepKey),
                    instance.getNextStepKey(),
                    instance.tierPreference()
                );
            case 1:
                return new WaitForDataTierStep(
                    instance.getKey(),
                    randomValueOtherThan(instance.getNextStepKey(), AbstractStepTestCase::randomStepKey),
                    instance.tierPreference()
                );
            case 2:
                return new WaitForDataTierStep(
                    instance.getKey(),
                    instance.getNextStepKey(),
                    randomValueOtherThan(instance.tierPreference(), () -> randomAlphaOfLength(5))
                );
        }
        throw new AssertionError();
    }

    @Override
    protected WaitForDataTierStep copyInstance(WaitForDataTierStep instance) {
        return new WaitForDataTierStep(instance.getKey(), instance.getNextStepKey(), instance.tierPreference());
    }

    public void testConditionMet() {
        String notIncludedTier = randomFrom(DataTier.ALL_DATA_TIERS);
        List<String> otherTiers = DataTier.ALL_DATA_TIERS.stream()
            .filter(tier -> notIncludedTier.equals(tier) == false)
            .collect(Collectors.toList());
        List<String> includedTiers = randomSubsetOf(between(1, otherTiers.size()), otherTiers);
        String tierPreference = String.join(",", includedTiers);
        WaitForDataTierStep step = new WaitForDataTierStep(randomStepKey(), randomStepKey(), tierPreference);

        verify(step, ClusterState.EMPTY_STATE, false, "no nodes for tiers [" + tierPreference + "] available");
        verify(step, state(List.of(notIncludedTier)), false, "no nodes for tiers [" + tierPreference + "] available");
        verify(step, state(includedTiers), true, null);
        verify(step, state(List.of(DiscoveryNodeRole.DATA_ROLE.roleName())), true, null);
    }

    private void verify(WaitForDataTierStep step, ClusterState state, boolean complete, String message) {
        ClusterStateWaitStep.Result result = step.isConditionMet(null, state);
        assertThat(result.isComplete(), is(complete));
        if (message != null) {
            assertThat(Strings.toString(result.getInfomationContext()), containsString(message));
        } else {
            assertThat(result.getInfomationContext(), is(nullValue()));
        }
    }

    private ClusterState state(Collection<String> roles) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        IntStream.range(0, between(1, 5))
            .mapToObj(
                i -> TestDiscoveryNode.create(
                    "node_" + i,
                    UUIDs.randomBase64UUID(),
                    buildNewFakeTransportAddress(),
                    Map.of(),
                    randomSubsetOf(between(1, roles.size()), roles).stream()
                        .map(DiscoveryNodeRole::getRoleFromRoleName)
                        .collect(Collectors.toSet())
                )
            )
            .forEach(builder::add);
        return ClusterState.builder(ClusterName.DEFAULT).nodes(builder).build();
    }
}
