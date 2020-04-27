/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionsTests extends AutoscalingTestCase {

    public void testAutoscalingDecisionsRejectsEmptyDecisions() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new AutoscalingDecisions(List.of()));
        assertThat(e.getMessage(), equalTo("decisions can not be empty"));
    }

    public void testAutoscalingDecisionsTypeDown() {
        final AutoscalingDecisions decisions = randomAutoscalingDecisions(randomIntBetween(1, 8), 0, 0);
        assertThat(decisions.type(), equalTo(AutoscalingDecisionType.SCALE_DOWN));
    }

    public void testAutoscalingDecisionsTypeNo() {
        final AutoscalingDecisions decision = randomAutoscalingDecisions(randomIntBetween(0, 8), randomIntBetween(1, 8), 0);
        assertThat(decision.type(), equalTo(AutoscalingDecisionType.NO_SCALE));
    }

    public void testAutoscalingDecisionsTypeUp() {
        final AutoscalingDecisions decision = randomAutoscalingDecisions(0, 0, randomIntBetween(1, 8));
        assertThat(decision.type(), equalTo(AutoscalingDecisionType.SCALE_UP));
    }

}
