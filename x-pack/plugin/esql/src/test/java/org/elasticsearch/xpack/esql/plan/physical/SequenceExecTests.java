/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceTests;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;

/** Keeps physical node generation resolved while exercising its logical sequence and request filter. */
public class SequenceExecTests extends AbstractNodeTestCase<SequenceExec, PhysicalPlan> {
    /** Supplies the generic node generator with a resolved coordinator leaf and a valid request filter. */
    public static SequenceExec randomSequenceExec() {
        return new SequenceExec(
            Source.EMPTY,
            SequenceTests.randomSequence(),
            randomBoolean() ? termQuery("tenant", randomAlphaOfLength(5)) : null
        );
    }

    @Override
    protected SequenceExec randomInstance() {
        return randomSequenceExec();
    }

    @Override
    protected SequenceExec mutate(SequenceExec instance) {
        return instance.withFilter(randomValueOtherThan(instance.esFilter(), () -> termQuery("tenant", randomAlphaOfLength(5))));
    }

    @Override
    protected SequenceExec copy(SequenceExec instance) {
        return new SequenceExec(instance.source(), instance.sequence(), instance.esFilter());
    }

    @Override
    public void testTransform() {
        SequenceExec node = randomSequenceExec();
        Sequence sequence = node.sequence();
        Sequence changed = new Sequence(
            sequence.source(),
            sequence.children(),
            randomValueOtherThan(sequence.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE)),
            sequence.sourceAttributes(),
            sequence.sourceStages(),
            sequence.output(),
            sequence.expressionsResolved()
        );
        assertEquals(
            new SequenceExec(node.source(), changed, node.esFilter()),
            node.transformPropertiesOnly(Sequence.class, value -> changed)
        );
    }

    @Override
    public void testReplaceChildren() {
        SequenceExec node = randomSequenceExec();
        assertThat(
            expectThrows(UnsupportedOperationException.class, () -> node.replaceChildren(List.of())).getMessage(),
            containsString("doesn't have any children to replace")
        );
    }
}
