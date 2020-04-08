/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDecisionTypeWireSerializingTests extends AbstractWireSerializingTestCase<AutoscalingDecisionType> {

    @Override
    protected Writeable.Reader<AutoscalingDecisionType> instanceReader() {
        return AutoscalingDecisionType::readFrom;
    }

    @Override
    protected AutoscalingDecisionType createTestInstance() {
        return randomFrom(AutoscalingDecisionType.values());
    }

    @Override
    protected void assertEqualInstances(final AutoscalingDecisionType expectedInstance, final AutoscalingDecisionType newInstance) {
        assertSame(expectedInstance, newInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    public void testInvalidAutoscalingDecisionTypeSerialization() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final Set<Byte> values = Arrays.stream(AutoscalingDecisionType.values())
            .map(AutoscalingDecisionType::id)
            .collect(Collectors.toSet());
        final byte value = randomValueOtherThanMany(values::contains, ESTestCase::randomByte);
        out.writeByte(value);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AutoscalingDecisionType.readFrom(out.bytes().streamInput())
        );
        assertThat(e.getMessage(), equalTo("unexpected value [" + value + "] for autoscaling decision type"));
    }

}
