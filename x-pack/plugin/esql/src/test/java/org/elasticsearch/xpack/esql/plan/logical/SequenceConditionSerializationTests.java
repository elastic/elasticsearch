/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import static org.hamcrest.Matchers.containsString;

/** Pins condition scheduling provenance and protected identities through the registered logical wire format. */
public class SequenceConditionSerializationTests extends AbstractLogicalPlanSerializationTests<SequenceCondition> {
    @Override
    protected SequenceCondition createTestInstance() {
        return SequenceConditionTests.randomSequenceCondition();
    }

    @Override
    protected SequenceCondition mutateInstance(SequenceCondition instance) {
        return new SequenceCondition(
            instance.source(),
            instance.child(),
            instance.ownStage(),
            Literal.TRUE,
            instance.referencedStages(),
            instance.inputs()
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testConditionBindingsAndOriginalStageSetRoundTrip() throws Exception {
        SequenceCondition original = createTestInstance();
        SequenceCondition restored = copyInstance(original);
        assertEquals(original, restored);
        assertEquals(original.ownStage(), restored.ownStage());
        assertEquals(original.referencedStages(), restored.referencedStages());
        assertEquals(original.inputs().getFirst().source().id(), restored.inputs().getFirst().source().id());
        assertEquals(original.inputs().getFirst().reference().id(), restored.inputs().getFirst().reference().id());
        assertNotEquals(restored.inputs().getFirst().source().id(), restored.inputs().getFirst().reference().id());
        assertEquals(original.output(), restored.output());
    }

    public void testOlderPeerIsRejectedBeforeAnyConditionBytes() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_LATEST_VALID_LANGUAGE));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE latest_valid pairing is not supported")
            );
            assertEquals(0, out.size());
        }
    }
}
