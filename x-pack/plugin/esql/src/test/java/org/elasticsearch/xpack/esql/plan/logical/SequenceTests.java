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
        Sequence sequence = new Sequence(Source.EMPTY, stages, randomLongBetween(1, Long.MAX_VALUE)).resolveSchema();
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
}
