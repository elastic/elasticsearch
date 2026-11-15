/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

/** Supplies valid stages and aligned output bindings to the generic node-contract tests. */
public class SequenceTests extends AbstractNodeTestCase<Sequence, LogicalPlan> {
    /** Supplies the generic node generator with coherent stage outputs. */
    public static Sequence randomSequence() {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0, count = between(2, 5); stage < count; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var input = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, input, List.of(key)));
        }
        Sequence.Pairing pairing = randomFrom(Sequence.Pairing.values());
        Integer maxCandidates = pairing == Sequence.Pairing.LATEST_VALID && randomBoolean() ? between(1, 1024) : null;
        Sequence sequence = new Sequence(Source.EMPTY, stages, randomLongBetween(1, Long.MAX_VALUE), pairing, maxCandidates)
            .resolveSchema();
        return sequence;
    }

    @Override
    protected Sequence randomInstance() {
        return randomSequence();
    }

    @Override
    protected Sequence mutate(Sequence instance) {
        return withSpan(instance, randomValueOtherThan(instance.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE)));
    }

    @Override
    protected Sequence copy(Sequence instance) {
        return withSpan(instance, instance.maxSpanNanos());
    }

    private static Sequence withSpan(Sequence instance, long span) {
        return new Sequence(
            instance.source(),
            instance.children(),
            span,
            instance.sourceAttributes(),
            instance.sourceStages(),
            instance.output(),
            instance.expressionsResolved(),
            instance.pairing(),
            instance.maxCandidates()
        );
    }

    @Override
    public void testTransform() {
        Sequence node = randomSequence();
        long span = randomValueOtherThan(node.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE));
        assertEquals(withSpan(node, span), node.transformPropertiesOnly(Long.class, value -> span));
    }

    @Override
    public void testReplaceChildren() {
        Sequence node = randomSequence();
        var children = new ArrayList<>(node.children());
        SequenceStage first = (SequenceStage) children.getFirst();
        children.set(0, first.replaceChild(new Filter(Source.EMPTY, first.child(), Literal.TRUE)));
        assertEquals(
            new Sequence(
                node.source(),
                children,
                node.maxSpanNanos(),
                node.sourceAttributes(),
                node.sourceStages(),
                node.output(),
                node.expressionsResolved(),
                node.pairing(),
                node.maxCandidates()
            ),
            node.replaceChildren(children)
        );
    }

    public void testUnspecifiedCandidateBoundSurvivesNodeReconstruction() {
        Sequence base = randomSequence();
        for (Sequence.Pairing pairing : Sequence.Pairing.values()) {
            var node = new Sequence(
                base.source(),
                base.children(),
                1,
                base.sourceAttributes(),
                base.sourceStages(),
                base.output(),
                true,
                pairing,
                null
            );
            assertNull(node.maxCandidates());
            assertSame(node, node.transformExpressionsOnly(expression -> expression));
            Sequence changed = (Sequence) node.transformPropertiesOnly(Long.class, span -> span + 1);
            assertEquals(2, changed.maxSpanNanos());
            assertEquals(pairing, changed.pairing());
            assertNull(changed.maxCandidates());
            assertEquals(node.output(), changed.output());
            assertNull(node.replaceChildren(node.children()).maxCandidates());
        }
        expectThrows(
            IllegalArgumentException.class,
            () -> new Sequence(base.source(), base.children(), 1, Sequence.Pairing.LATEST_VALID, 0)
        );
    }

    /** A limit constrains an incoming link, so the first member cannot have one. */
    public void testFirstMemberCannotHaveIncomingLimit() {
        Sequence sequence = randomSequence();
        var stages = new ArrayList<>(sequence.children());
        SequenceStage first = (SequenceStage) stages.getFirst();
        stages.set(0, new SequenceStage(first.source(), first.child(), first.keys(), 1));
        expectThrows(IllegalArgumentException.class, () -> sequence.replaceChildren(stages));
    }
}
