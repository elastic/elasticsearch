/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;

/** Keeps the command's stage scopes and generated output IDs intact on the wire. */
public class SequenceSerializationTests extends AbstractLogicalPlanSerializationTests<Sequence> {
    @Override
    protected Sequence createTestInstance() {
        List<LogicalPlan> stages = new ArrayList<>();
        List<Attribute> keys = randomBoolean() ? List.of(randomReferenceAttribute(false)) : List.of();
        for (int i = 0; i < between(2, 6); i++) {
            stages.add(new SequenceStage(randomSource(), randomChild(0), keys));
        }
        return new Sequence(randomSource(), stages, randomLongBetween(1, Long.MAX_VALUE));
    }

    @Override
    protected Sequence mutateInstance(Sequence instance) {
        return new Sequence(
            instance.source(),
            instance.children(),
            randomValueOtherThan(instance.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE))
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testRejectUnsupportedPeerBeforeWriting() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_STAGE_COLUMNS));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE is not supported")
            );
            assertEquals(0, out.size());
        }
    }

    public void testResolvedWideSchemaRoundTrip() throws Exception {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0; stage < 3; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var child = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, child, List.of(key)));
        }
        Sequence sequence = new Sequence(Source.EMPTY, stages, 30_000_000_000L).resolveSchema();
        Sequence restored = copyInstance(sequence);
        assertTrue(restored.resolved());
        assertEquals(sequence, restored);
        assertEquals(
            List.of("key", "stage1.payload", "stage2.payload", "stage3.payload"),
            restored.output().stream().map(Attribute::name).toList()
        );
        assertEquals(sequence.output().stream().map(Attribute::id).toList(), restored.output().stream().map(Attribute::id).toList());
        assertEquals(
            sequence.sourceAttributes().stream().map(Attribute::id).toList(),
            restored.sourceAttributes().stream().map(Attribute::id).toList()
        );
        assertEquals(List.of(0, 0, 1, 2), restored.sourceStages());
        assertNotEquals(sequence.sourceAttributes().get(1).id(), sequence.output().get(1).id());
    }

    /** The resolved schema remains traversable and retains stage bindings after NodeInfo reconstruction. */
    public void testResolvedSchemaSupportsExpressionAndPropertyTransforms() {
        Sequence sequence = resolvedSequence();
        assertSame(sequence, sequence.transformExpressionsOnly(expression -> expression));
        Sequence changed = (Sequence) sequence.transformPropertiesOnly(Long.class, span -> span + 1);
        assertEquals(sequence.maxSpanNanos() + 1, changed.maxSpanNanos());
        assertEquals(sequence.output(), changed.output());
        assertEquals(sequence.sourceAttributes(), changed.sourceAttributes());
        assertEquals(sequence.sourceStages(), changed.sourceStages());
    }

    private static Sequence resolvedSequence() {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0; stage < 2; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var input = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, input, List.of(key)));
        }
        return new Sequence(Source.EMPTY, stages, 30_000_000_000L).resolveSchema();
    }
}
