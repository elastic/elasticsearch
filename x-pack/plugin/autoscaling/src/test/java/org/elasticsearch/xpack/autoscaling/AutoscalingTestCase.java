/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AlwaysAutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisions;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AutoscalingTestCase extends ESTestCase {

    public static AutoscalingDecision randomAutoscalingDecision() {
        return randomAutoscalingDecisionOfType(randomFrom(AutoscalingDecisionType.values()));
    }

    public static AutoscalingDecision randomAutoscalingDecisionOfType(final AutoscalingDecisionType type) {
        return new AutoscalingDecision(randomAlphaOfLength(8), type, randomAlphaOfLength(8));
    }

    public static AutoscalingDecisions randomAutoscalingDecisions() {
        final int numberOfDecisions = 1 + randomIntBetween(1, 8);
        final List<AutoscalingDecision> decisions = new ArrayList<>(numberOfDecisions);
        for (int i = 0; i < numberOfDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecisionType.SCALE_DOWN));
        }
        final int numberOfDownDecisions = randomIntBetween(0, 8);
        final int numberOfNoDecisions = randomIntBetween(0, 8);
        final int numberOfUpDecisions = randomIntBetween(numberOfDownDecisions + numberOfNoDecisions == 0 ? 1 : 0, 8);
        return randomAutoscalingDecisions(numberOfDownDecisions, numberOfNoDecisions, numberOfUpDecisions);
    }

    public static AutoscalingDecisions randomAutoscalingDecisions(
        final int numberOfDownDecisions,
        final int numberOfNoDecisions,
        final int numberOfUpDecisions
    ) {
        final List<AutoscalingDecision> decisions = new ArrayList<>(numberOfDownDecisions + numberOfNoDecisions + numberOfUpDecisions);
        for (int i = 0; i < numberOfDownDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecisionType.SCALE_DOWN));
        }
        for (int i = 0; i < numberOfNoDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecisionType.NO_SCALE));
        }
        for (int i = 0; i < numberOfUpDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecisionType.SCALE_UP));
        }
        Randomness.shuffle(decisions);
        return new AutoscalingDecisions(decisions);
    }

    public static SortedMap<String, AutoscalingDeciderConfiguration> randomAutoscalingDeciders() {
        return new TreeMap<>(
            List.of(new AlwaysAutoscalingDeciderConfiguration())
                .stream()
                .collect(Collectors.toMap(AutoscalingDeciderConfiguration::name, Function.identity()))
        );
    }

    public static AutoscalingPolicy randomAutoscalingPolicy() {
        return randomAutoscalingPolicyOfName(randomAlphaOfLength(8));
    }

    public static AutoscalingPolicy randomAutoscalingPolicyOfName(final String name) {
        return new AutoscalingPolicy(name, randomAutoscalingDeciders());
    }

    public static AutoscalingPolicy mutateAutoscalingPolicy(final AutoscalingPolicy instance) {
        final SortedMap<String, AutoscalingDeciderConfiguration> deciders;
        if (randomBoolean()) {
            // if the policy name did not change, or randomly, use a mutated set of deciders
            deciders = mutateAutoscalingDeciders(instance.deciders());
        } else {
            deciders = instance.deciders();
        }
        return new AutoscalingPolicy(randomValueOtherThan(instance.name(), () -> randomAlphaOfLength(8)), deciders);
    }

    public static SortedMap<String, AutoscalingDeciderConfiguration> mutateAutoscalingDeciders(
        final SortedMap<String, AutoscalingDeciderConfiguration> deciders
    ) {
        if (deciders.size() == 0) {
            return randomAutoscalingDeciders();
        } else {
            // use a proper subset of the deciders
            return new TreeMap<>(
                randomSubsetOf(randomIntBetween(0, deciders.size() - 1), deciders.entrySet()).stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }
    }

    public static AutoscalingMetadata randomAutoscalingMetadata() {
        return randomAutoscalingMetadataOfPolicyCount(randomIntBetween(0, 8));
    }

    public static AutoscalingMetadata randomAutoscalingMetadataOfPolicyCount(final int numberOfPolicies) {
        final SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < numberOfPolicies; i++) {
            final AutoscalingPolicy policy = randomAutoscalingPolicy();
            final AutoscalingPolicyMetadata policyMetadata = new AutoscalingPolicyMetadata(policy);
            policies.put(policy.name(), policyMetadata);
        }
        return new AutoscalingMetadata(policies);
    }

    public static NamedWriteableRegistry getAutoscalingNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new Autoscaling(Settings.EMPTY).getNamedWriteables());
    }

    public static NamedXContentRegistry getAutoscalingXContentRegistry() {
        return new NamedXContentRegistry(new Autoscaling(Settings.EMPTY).getNamedXContent());
    }

}
