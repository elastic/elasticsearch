/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionTests extends AutoscalingTestCase {

    public void testAutoscalingDecisionType() {
        final AutoscalingDecisionType type = randomFrom(AutoscalingDecisionType.values());
        final AutoscalingDecision decision = randomAutoscalingDecisionOfType(type);
        assertThat(decision.type(), equalTo(type));
    }

    public void testAutoscalingDecisionTypeSerialization() throws IOException {
        final AutoscalingDecisionType before = randomFrom(AutoscalingDecisionType.values());
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);
        final AutoscalingDecisionType after = AutoscalingDecisionType.readFrom(out.bytes().streamInput());
        assertThat(after, equalTo(before));
    }

}
