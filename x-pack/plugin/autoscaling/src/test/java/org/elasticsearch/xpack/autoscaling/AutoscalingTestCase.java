/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public abstract class AutoscalingTestCase extends ESTestCase {

    static AutoscalingDecision randomAutoscalingDecision() {
        return randomAutoscalingDecisionOfType(randomFrom(AutoscalingDecisionType.values()));
    }

    static AutoscalingDecision randomAutoscalingDecisionOfType(final AutoscalingDecisionType type) {
        return new AutoscalingDecision(randomAlphaOfLength(8), type, randomAlphaOfLength(8));
    }

    static AutoscalingDecisions randomAutoscalingDecisions() {
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

    static AutoscalingDecisions randomAutoscalingDecisions(
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

}
