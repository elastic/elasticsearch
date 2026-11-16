/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Internal display labels must neither reject a user's key nor determine a column's stable identity. */
public class SequenceSchemaTests extends ESTestCase {
    /** A legal dotted key must not collide with a compiler-generated label for an unrelated payload. */
    public void testPayloadNamesReserveUserKeyNames() {
        Attribute key = attribute("stage1.label");
        Attribute label = attribute("label");
        Sequence sequence = sequence(key, List.of(key, label), List.of(key, label)).resolveSchema();
        assertEquals(
            List.of("stage1.label", "stage1.label_1", "stage2.label"),
            sequence.output().subList(0, 3).stream().map(Attribute::name).toList()
        );
        assertEquals(sequence.output().size(), new HashSet<>(sequence.output().stream().map(Attribute::name).toList()).size());
        assertSame(sequence, sequence.resolveSchema());
    }

    /** Display changes preserve source bindings, and reusing a source ID in another stage does not merge the outputs. */
    public void testOutputIdsFollowStageAndSourceAcrossDisplayRename() {
        Attribute key = attribute("key");
        Attribute label = attribute("label");
        Sequence sequence = sequence(key, List.of(key, label), List.of(key, label)).resolveSchema();
        List<LogicalPlan> stages = new ArrayList<>(sequence.children());
        stages.set(0, stage(key, List.of(key, label.withName("renamed"))));
        Sequence refreshed = sequence.replaceChildren(stages).resolveSchema();
        assertEquals(sequence.output().get(1).id(), refreshed.output().get(1).id());
        assertEquals(sequence.output().get(2).id(), refreshed.output().get(2).id());
        assertNotEquals(refreshed.output().get(1).id(), refreshed.output().get(2).id());
        assertEquals("stage1.renamed", refreshed.output().get(1).name());
        assertSame(refreshed, refreshed.resolveSchema());
    }

    /** A quoted user key may also occupy the first fallback, so allocation must reserve all keys before payloads. */
    public void testCollisionSuffixReservesEveryLegalKey() {
        Attribute key = attribute("stage1.label");
        Attribute secondKey = attribute("stage1.label_1");
        Attribute label = attribute("label");
        List<Attribute> row = List.of(key, secondKey, label);
        SequenceStage stage = new SequenceStage(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, row, EmptyLocalSupplier.EMPTY),
            List.of(key, secondKey)
        );
        Sequence sequence = new Sequence(Source.EMPTY, List.of(stage, stage), 10_000_000_000L).resolveSchema();
        assertEquals("stage1.label_2", sequence.output().get(2).name());
        assertSame(sequence, sequence.resolveSchema());
    }

    private static Attribute attribute(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static SequenceStage stage(Attribute key, List<Attribute> output) {
        return new SequenceStage(Source.EMPTY, new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY), List.of(key));
    }

    private static Sequence sequence(Attribute key, List<Attribute> first, List<Attribute> second) {
        return new Sequence(Source.EMPTY, List.of(stage(key, first), stage(key, second)), 10_000_000_000L);
    }
}
