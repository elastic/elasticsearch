/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

/** Exercises the real registered node with coherent WHERE-site bindings rather than excluding it from node discovery. */
public class SequenceConditionTests extends AbstractNodeTestCase<SequenceCondition, LogicalPlan> {
    /** Supplies reflection-driven node tests with valid distinct scalar and source identities. */
    public static SequenceCondition randomSequenceCondition() {
        Attribute source = new ReferenceAttribute(Source.EMPTY, null, "flag", DataType.BOOLEAN);
        Attribute reference = new ReferenceAttribute(Source.EMPTY, null, "flag", DataType.BOOLEAN, Nullability.FALSE, new NameId(), true);
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(source), EmptyLocalSupplier.EMPTY);
        int ownStage = between(2, 4);
        int earlier = between(0, ownStage - 1);
        return new SequenceCondition(
            Source.EMPTY,
            child,
            ownStage,
            reference,
            List.of(earlier),
            List.of(new SequenceCondition.Input(reference, earlier, source))
        );
    }

    @Override
    protected SequenceCondition randomInstance() {
        return randomSequenceCondition();
    }

    /** Changes the expression or one piece of scheduling/binding state for equality and wire-contract tests. */
    public static SequenceCondition mutateSequenceCondition(SequenceCondition instance) {
        int ownStage = instance.ownStage();
        var condition = instance.condition();
        List<Integer> referenced = instance.referencedStages();
        List<SequenceCondition.Input> inputs = instance.inputs();
        switch (randomInt(3)) {
            case 0 -> condition = Literal.TRUE;
            case 1 -> ownStage++;
            case 2 -> {
                int additional = referenced.contains(0) ? 1 : 0;
                referenced = java.util.stream.Stream.concat(referenced.stream(), java.util.stream.Stream.of(additional)).sorted().toList();
            }
            case 3 -> {
                SequenceCondition.Input input = inputs.getFirst();
                Attribute source = new ReferenceAttribute(Source.EMPTY, null, "different_flag", DataType.BOOLEAN);
                inputs = List.of(new SequenceCondition.Input(input.reference(), input.stage(), source));
            }
            default -> throw new AssertionError("unexpected mutation");
        }
        return new SequenceCondition(instance.source(), instance.child(), ownStage, condition, referenced, inputs);
    }

    @Override
    protected SequenceCondition mutate(SequenceCondition instance) {
        return mutateSequenceCondition(instance);
    }

    @Override
    protected SequenceCondition copy(SequenceCondition instance) {
        return new SequenceCondition(
            instance.source(),
            instance.child(),
            instance.ownStage(),
            instance.condition(),
            instance.referencedStages(),
            instance.inputs()
        );
    }

    @Override
    public void testTransform() {
        SequenceCondition node = randomSequenceCondition();
        SequenceCondition changed = (SequenceCondition) node.transformExpressionsOnly(expression -> Literal.TRUE);
        assertEquals(
            new SequenceCondition(node.source(), node.child(), node.ownStage(), Literal.TRUE, node.referencedStages(), node.inputs()),
            changed
        );
        assertEquals(node.referencedStages(), changed.referencedStages());
        assertEquals(node.inputs(), changed.inputs());
        assertTrue(changed.activeInputs().isEmpty());
        assertTrue(changed.references().isEmpty());
        assertSame(node, node.transformExpressionsOnly(expression -> expression));
    }

    @Override
    public void testReplaceChildren() {
        SequenceCondition node = randomSequenceCondition();
        LogicalPlan child = new Filter(Source.EMPTY, node.child(), Literal.TRUE);
        assertEquals(
            new SequenceCondition(node.source(), child, node.ownStage(), node.condition(), node.referencedStages(), node.inputs()),
            node.replaceChild(child)
        );
        assertEquals(node.child().output(), node.output());
    }

    public void testReferencedStagesMustBeSortedDistinctAndEarlier() {
        SequenceCondition node = randomSequenceCondition();
        for (List<Integer> invalid : List.of(List.<Integer>of(), List.of(-1), List.of(1, 0), List.of(0, 0), List.of(node.ownStage()))) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new SequenceCondition(node.source(), node.child(), node.ownStage(), node.condition(), invalid, List.of())
            );
        }
        expectThrows(
            IllegalArgumentException.class,
            () -> new SequenceCondition(node.source(), node.child(), 0, node.condition(), List.of(0), List.of())
        );
    }

    public void testOwnReferencesExcludeEarlierInputsAndDeadLeaves() {
        Attribute source = new ReferenceAttribute(Source.EMPTY, null, "same_name", DataType.BOOLEAN);
        Attribute own = new ReferenceAttribute(Source.EMPTY, null, "same_name", DataType.BOOLEAN, Nullability.FALSE, new NameId(), true);
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(source), EmptyLocalSupplier.EMPTY);
        var node = new SequenceCondition(Source.EMPTY, child, 1, own, List.of(0), List.of(new SequenceCondition.Input(own, 1, source)));
        assertTrue(node.references().contains(source));
        assertFalse(node.references().contains(own));
        assertTrue(randomSequenceCondition().references().isEmpty());
        expectThrows(IllegalArgumentException.class, () -> new SequenceCondition.Input(source, 0, source));
    }
}
