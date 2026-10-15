/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;

/** Checks that the coordinator source retains stage scopes and wide bindings across transport. */
public class SequenceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<SequenceExec> {
    @Override
    protected SequenceExec createTestInstance() {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0, count = between(2, 5); stage < count; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute value = new ReferenceAttribute(Source.EMPTY, null, "value", DataType.LONG);
            var input = new LocalRelation(Source.EMPTY, List.of(key, value), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, input, List.of(key)));
        }
        Sequence sequence = new Sequence(Source.EMPTY, stages, randomLongBetween(1, Long.MAX_VALUE)).resolveSchema();
        return new SequenceExec(Source.EMPTY, sequence);
    }

    @Override
    protected SequenceExec mutateInstance(SequenceExec instance) {
        Sequence sequence = instance.sequence();
        var changed = new Sequence(
            Source.EMPTY,
            sequence.children(),
            randomValueOtherThan(sequence.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE)),
            sequence.sourceAttributes(),
            sequence.sourceStages(),
            sequence.output(),
            true
        );
        return new SequenceExec(Source.EMPTY, changed);
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

    public void testResolvedOutputAndBindingsSurviveTransport() throws Exception {
        var original = createTestInstance();
        var restored = copyInstance(original);
        assertEquals(original.output(), restored.output());
        assertEquals(original.sequence().sourceStages(), restored.sequence().sourceStages());
        assertEquals(original.sequence().sourceAttributes(), restored.sequence().sourceAttributes());
        assertEquals(original.sequence().children(), restored.sequence().children());
        assertTrue(restored.children().isEmpty());
        assertTrue(restored.references().isEmpty());
    }

    public void testRequestFilterSurvivesTransportAndKeepsOutputIdentity() throws Exception {
        var original = createTestInstance();
        var filtered = original.withFilter(termQuery("tenant", "visible"));
        var restored = copyInstance(filtered);
        assertEquals(filtered.esFilter(), restored.esFilter());
        assertEquals(original.output(), restored.output());
        assertEquals(original.sequence(), restored.sequence());
        assertNotEquals(original, filtered);
    }

}
