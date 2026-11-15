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

import java.util.List;

/** Pins incoming-link metadata across node reconstruction and supplies coherent stages to generic node discovery. */
public class SequenceStageTests extends AbstractNodeTestCase<SequenceStage, LogicalPlan> {
    /** Generic node generation must respect the optional nonnegative limit instead of feeding an arbitrary signed long. */
    public static SequenceStage randomSequenceStage() {
        Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
        Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
        var input = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
        return new SequenceStage(Source.EMPTY, input, List.of(key), randomBoolean() ? 0 : randomLongBetween(1, Long.MAX_VALUE));
    }

    @Override
    protected SequenceStage randomInstance() {
        return randomSequenceStage();
    }

    @Override
    protected SequenceStage mutate(SequenceStage instance) {
        long gap = randomValueOtherThan(instance.maxGapNanos(), () -> randomLongBetween(0, Long.MAX_VALUE));
        return new SequenceStage(instance.source(), instance.child(), instance.keys(), gap);
    }

    @Override
    protected SequenceStage copy(SequenceStage instance) {
        return new SequenceStage(instance.source(), instance.child(), instance.keys(), instance.maxGapNanos());
    }

    @Override
    public void testTransform() {
        SequenceStage stage = randomSequenceStage();
        long gap = randomValueOtherThan(stage.maxGapNanos(), () -> randomLongBetween(0, Long.MAX_VALUE));
        assertEquals(
            new SequenceStage(stage.source(), stage.child(), stage.keys(), gap),
            stage.transformPropertiesOnly(Long.class, ignored -> gap)
        );
        List<Attribute> keys = List.of(new ReferenceAttribute(Source.EMPTY, null, "other", DataType.LONG));
        assertEquals(
            new SequenceStage(stage.source(), stage.child(), keys, stage.maxGapNanos()),
            stage.transformPropertiesOnly(List.class, ignored -> keys)
        );
    }

    @Override
    public void testReplaceChildren() {
        SequenceStage stage = randomSequenceStage();
        var child = new Filter(Source.EMPTY, stage.child(), Literal.TRUE);
        assertEquals(new SequenceStage(stage.source(), child, stage.keys(), stage.maxGapNanos()), stage.replaceChild(child));
    }

    /** Only the existing zero-default constructor and positive incoming limits are valid. */
    public void testAbsentDefaultAndNegativeLimit() {
        SequenceStage stage = randomSequenceStage();
        assertEquals(0L, new SequenceStage(stage.source(), stage.child(), stage.keys()).maxGapNanos());
        expectThrows(IllegalArgumentException.class, () -> new SequenceStage(stage.source(), stage.child(), stage.keys(), -1));
    }
}
