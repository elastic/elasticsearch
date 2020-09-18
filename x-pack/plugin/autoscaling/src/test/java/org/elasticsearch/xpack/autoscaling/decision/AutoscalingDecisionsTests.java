/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionsTests extends AutoscalingTestCase {

    public void testAutoscalingDecisionsRejectsEmptyDecisions() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AutoscalingDecisions(
                randomAlphaOfLength(10),
                new AutoscalingCapacity(randomAutoscalingResources(), randomAutoscalingResources()),
                new TreeMap<>()
            )
        );
        assertThat(e.getMessage(), equalTo("decisions can not be empty"));
    }

    public void testRequiredCapacity() {
        AutoscalingCapacity single = randomBoolean() ? randomAutoscalingCapacity() : null;
        verifyRequiredCapacity(single, single);
        // any undecided decider nulls out any decision making
        verifyRequiredCapacity(null, single, null);
        verifyRequiredCapacity(null, null, single);

        boolean node = randomBoolean();
        boolean storage = randomBoolean();
        boolean memory = randomBoolean() || storage == false;

        AutoscalingCapacity large = randomCapacity(node, storage, memory, 1000, 2000);

        List<AutoscalingCapacity> autoscalingCapacities = new ArrayList<>();
        autoscalingCapacities.add(large);
        IntStream.range(0, 10).mapToObj(i -> randomCapacity(node, storage, memory, 0, 1000)).forEach(autoscalingCapacities::add);

        Randomness.shuffle(autoscalingCapacities);
        verifyRequiredCapacity(large, autoscalingCapacities.toArray(AutoscalingCapacity[]::new));

        AutoscalingCapacity largerStorage = randomCapacity(node, true, false, 2000, 3000);
        verifySingleMetricLarger(node, largerStorage, large, autoscalingCapacities, largerStorage);

        AutoscalingCapacity largerMemory = randomCapacity(node, false, true, 2000, 3000);
        verifySingleMetricLarger(node, large, largerMemory, autoscalingCapacities, largerMemory);
    }

    private void verifySingleMetricLarger(
        boolean node,
        AutoscalingCapacity expectedStorage,
        AutoscalingCapacity expectedMemory,
        List<AutoscalingCapacity> other,
        AutoscalingCapacity larger
    ) {
        List<AutoscalingCapacity> autoscalingCapacities = new ArrayList<>(other);
        autoscalingCapacities.add(larger);
        Randomness.shuffle(autoscalingCapacities);
        AutoscalingCapacity.Builder expectedBuilder = AutoscalingCapacity.builder()
            .tier(expectedStorage.tier().storage(), expectedMemory.tier().memory());
        if (node) {
            expectedBuilder.node(expectedStorage.node().storage(), expectedMemory.node().memory());
        }
        verifyRequiredCapacity(expectedBuilder.build(), autoscalingCapacities.toArray(AutoscalingCapacity[]::new));
    }

    private void verifyRequiredCapacity(AutoscalingCapacity expected, AutoscalingCapacity... capacities) {
        AtomicInteger uniqueGenerator = new AtomicInteger();
        SortedMap<String, AutoscalingDecision> decisions = Arrays.stream(capacities)
            .map(AutoscalingDecisionsTests::randomAutoscalingDecisionWithCapacity)
            .collect(
                Collectors.toMap(
                    k -> randomAlphaOfLength(10) + "-" + uniqueGenerator.incrementAndGet(),
                    Function.identity(),
                    (a, b) -> { throw new UnsupportedOperationException(); },
                    TreeMap::new
                )
            );
        assertThat(
            new AutoscalingDecisions(randomAlphaOfLength(10), randomAutoscalingCapacity(), decisions).requiredCapacity(),
            equalTo(expected)
        );
    }

    private AutoscalingCapacity randomCapacity(boolean node, boolean storage, boolean memory, int lower, int upper) {
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder();
        builder.tier(storage ? randomLongBetween(lower, upper) : null, memory ? randomLongBetween(lower, upper) : null);
        if (node) {
            builder.node(storage ? randomLongBetween(lower, upper) : null, memory ? randomLongBetween(lower, upper) : null);
        }
        return builder.build();
    }
}
