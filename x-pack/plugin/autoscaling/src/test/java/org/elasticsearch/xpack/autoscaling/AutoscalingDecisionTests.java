/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingDecision.SingleAutoscalingDecision;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionTests extends AutoscalingTestCase {

    public void testSingleAutoscalingDecisionType() {
        final AutoscalingDecision.Type type = randomFrom(AutoscalingDecision.Type.values());
        final SingleAutoscalingDecision decision = randomAutoscalingDecisionOfType(type);
        assertThat(decision.type(), equalTo(type));
    }

    public void testMultipleAutoscalingDecisionTypeDown() {
        final int numberOfDecisions = 1 + randomIntBetween(1, 8);
        final List<SingleAutoscalingDecision> decisions = new ArrayList<>(numberOfDecisions);
        for (int i = 0; i < numberOfDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecision.Type.SCALE_DOWN));
        }
        final AutoscalingDecision.MultipleAutoscalingDecision decision = new AutoscalingDecision.MultipleAutoscalingDecision(decisions);
        assertThat(decision.type(), equalTo(AutoscalingDecision.Type.SCALE_DOWN));
    }

    public void testMultipleAutoscalingDecisionTypeNo() {
        final int numberOfDownDecisions = randomIntBetween(0, 8);
        final int numberOfNoDecisions = randomIntBetween(1, 8);
        final List<SingleAutoscalingDecision> decisions = new ArrayList<>(numberOfDownDecisions + numberOfNoDecisions);
        for (int i = 0; i < numberOfDownDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecision.Type.SCALE_DOWN));
        }
        for (int i = 0; i < numberOfNoDecisions; i++) {
            decisions.add(randomAutoscalingDecisionOfType(AutoscalingDecision.Type.NO_SCALE));
        }
        Randomness.shuffle(decisions);
        final AutoscalingDecision.MultipleAutoscalingDecision decision = new AutoscalingDecision.MultipleAutoscalingDecision(decisions);
        assertThat(decision.type(), equalTo(AutoscalingDecision.Type.NO_SCALE));
    }

    public void testMultipleAutoscalingDecisionTypeUp() {
        final int numberOfDecisions = randomIntBetween(1, 8);
        final List<SingleAutoscalingDecision> decisions = new ArrayList<>(numberOfDecisions);
        for (int i = 0; i < numberOfDecisions; i++) {
            decisions.add(randomAutoscalingDecision());
        }
        final SingleAutoscalingDecision up = randomAutoscalingDecisionOfType(AutoscalingDecision.Type.SCALE_UP);
        decisions.add(up);
        Randomness.shuffle(decisions);
        final AutoscalingDecision.MultipleAutoscalingDecision decision = new AutoscalingDecision.MultipleAutoscalingDecision(decisions);
        assertThat(decision.type(), equalTo(AutoscalingDecision.Type.SCALE_UP));
    }

    public void testSingleAutoscalingDecisionSerialiation() throws IOException {
        final AutoscalingDecision before = randomAutoscalingDecision();
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);
        final AutoscalingDecision after = new SingleAutoscalingDecision(out.bytes().streamInput());
        assertThat(after, equalTo(before));
    }

    public void testMultipleAutoscalingDecisionSerialization() throws IOException {
        final int numberOfDecisions = randomIntBetween(1, 8);
        final List<SingleAutoscalingDecision> decisions = new ArrayList<>(numberOfDecisions);
        for (int i = 0; i < numberOfDecisions; i++) {
            decisions.add(randomAutoscalingDecision());
        }
        final AutoscalingDecision.MultipleAutoscalingDecision before = new AutoscalingDecision.MultipleAutoscalingDecision(decisions);
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);
        final AutoscalingDecision.MultipleAutoscalingDecision after = new AutoscalingDecision.MultipleAutoscalingDecision(
            out.bytes().streamInput()
        );
        assertThat(after, equalTo(before));
    }

    public void testAutoscalingDecisionTypeSerialization() throws IOException {
        final AutoscalingDecision.Type before = randomFrom(AutoscalingDecision.Type.values());
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);
        final AutoscalingDecision.Type after = AutoscalingDecision.Type.readFrom(out.bytes().streamInput());
        assertThat(after, equalTo(before));
    }

    public void testInvalidAutoscalingDecisionTypeSerialiation() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final Set<Byte> values = Arrays.stream(AutoscalingDecision.Type.values())
            .map(AutoscalingDecision.Type::id)
            .collect(Collectors.toSet());
        final byte value = randomValueOtherThanMany(values::contains, ESTestCase::randomByte);
        out.writeByte(value);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AutoscalingDecision.Type.readFrom(out.bytes().streamInput())
        );
        assertThat(e.getMessage(), equalTo("unexpected value [" + value + "] for autoscaling decision type"));
    }

}
