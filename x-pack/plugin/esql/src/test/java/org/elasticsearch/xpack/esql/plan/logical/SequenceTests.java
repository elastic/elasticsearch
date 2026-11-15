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
    public void testSingleEngineVersionFollowsPrivateCheckpoint() {
        assertTrue(Sequence.ESQL_SEQUENCE_R5.supports(Sequence.ESQL_SEQUENCE_LATEST_VALID_LANGUAGE));
        assertFalse(Sequence.ESQL_SEQUENCE_LATEST_VALID_LANGUAGE.supports(Sequence.ESQL_SEQUENCE_R5));
    }

    /** Supplies the generic node generator with coherent stage outputs. */
    public static Sequence randomSequence() {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0, count = between(2, 5); stage < count; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var input = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, input, List.of(key)));
        }
        return new Sequence(Source.EMPTY, stages, randomLongBetween(1, Long.MAX_VALUE)).resolveSchema();
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
            instance.expressionsResolved()
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
                node.expressionsResolved()
            ),
            node.replaceChildren(children)
        );
    }

    public void testSchemaSurvivesNodeReconstruction() {
        Sequence node = randomSequence();
        assertSame(node, node.transformExpressionsOnly(expression -> expression));
        Sequence changed = (Sequence) node.transformPropertiesOnly(Long.class, span -> span == Long.MAX_VALUE ? span - 1 : span + 1);
        assertNotEquals(node.maxSpanNanos(), changed.maxSpanNanos());
        assertEquals(node.output(), changed.output());
        assertEquals(node.sourceAttributes(), changed.sourceAttributes());
        assertEquals(node.sourceStages(), changed.sourceStages());
        assertEquals(node, node.replaceChildren(node.children()));
    }

    /** Each preserved schema component affects identity, even when its list lengths remain valid. */
    public void testSchemaFieldsParticipateInEquality() {
        List<Sequence> variants = schemaFieldVariants();
        Sequence baseline = variants.get(0);
        assertNotEquals("source attributes", baseline, variants.get(1));
        assertNotEquals("source stages", baseline, variants.get(2));
        assertNotEquals("output attributes", baseline, variants.get(3));
        assertNotEquals("schema resolved", variants.get(4), variants.get(5));
    }

    /** Builds distinct, aligned bindings with a shared source attribute valid in either stage. */
    static List<Sequence> schemaFieldVariants() {
        Attribute shared = new ReferenceAttribute(Source.EMPTY, null, "shared", DataType.KEYWORD);
        Attribute alternate = new ReferenceAttribute(Source.EMPTY, null, "alternate", DataType.KEYWORD);
        Attribute output = new ReferenceAttribute(Source.EMPTY, null, "output", DataType.KEYWORD);
        Attribute alternateOutput = new ReferenceAttribute(Source.EMPTY, null, "alternate_output", DataType.KEYWORD);
        var input = new LocalRelation(Source.EMPTY, List.of(shared, alternate), EmptyLocalSupplier.EMPTY);
        List<LogicalPlan> stages = List.of(
            new SequenceStage(Source.EMPTY, input, List.of()),
            new SequenceStage(Source.EMPTY, input, List.of())
        );
        Sequence baseline = new Sequence(Source.EMPTY, stages, 1, List.of(shared), List.of(0), List.of(output), true);
        Sequence changedSource = new Sequence(Source.EMPTY, stages, 1, List.of(alternate), List.of(0), List.of(output), true);
        Sequence changedStage = new Sequence(Source.EMPTY, stages, 1, List.of(shared), List.of(1), List.of(output), true);
        Sequence changedOutput = new Sequence(Source.EMPTY, stages, 1, List.of(shared), List.of(0), List.of(alternateOutput), true);
        var emptyInput = new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        List<LogicalPlan> emptyStages = List.of(
            new SequenceStage(Source.EMPTY, emptyInput, List.of()),
            new SequenceStage(Source.EMPTY, emptyInput, List.of())
        );
        Sequence unresolved = new Sequence(Source.EMPTY, emptyStages, 1);
        Sequence resolved = unresolved.resolveSchema();
        assertTrue(resolved.expressionsResolved());
        return List.of(baseline, changedSource, changedStage, changedOutput, unresolved, resolved);
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
