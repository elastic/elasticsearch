/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonMerge;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonRemove;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;

/**
 * Exercises lowering directly, including combinations still restricted by the public PromQL verifier.
 * The input is a real plan with materialized label attributes; no scan or evaluator mocks are needed.
 */
public class TranslationContextTests extends ESTestCase {
    public void testWithoutOverWithoutReadsThePreviousResult() {
        var input = input(attr("current_record"));
        TranslationContext context = context(input, TransportVersion.current());
        var first = context.bind(input, sub(input.shape(), of("instance")), Source.EMPTY);
        JsonRemove firstEdit = (JsonRemove) ((Eval) first.plan()).fields().getFirst().child();
        assertEquals(input.packedLabels(), firstEdit.children().getFirst());
        assertNull(first.label("instance"));
        assertSame(input.label("region"), first.label("region"));

        var second = context.bind(first, sub(first.shape(), of("region")), Source.EMPTY);
        JsonRemove secondEdit = (JsonRemove) ((Eval) second.plan()).fields().getFirst().child();
        assertEquals(first.packedLabels(), secondEdit.children().getFirst());
        assertNotEquals(input.packedLabels(), second.packedLabels());
        assertTrue(second.labels().isEmpty());
        assertEquals(1, second.attributes().size());
    }

    public void testSourceProjectionReplacesRatherThanAddsAVariant() {
        var input = input(new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of()));
        var context = context(input, TransportVersion.current());
        var first = context.bind(input, sub(input.shape(), of("instance")), Source.EMPTY);
        var second = context.bind(first, sub(first.shape(), of("region")), Source.EMPTY);
        assertEquals(Set.of("instance", "region"), ((TimeSeriesMetadataAttribute) second.packedLabels()).excludedFields());
        assertEquals(1, second.plan().output().stream().filter(TimeSeriesMetadataAttribute.class::isInstance).count());
        assertFalse(second.plan().outputSet().contains(input.packedLabels()));
        assertFalse(second.plan().outputSet().contains(first.packedLabels()));
        assertFalse(second.plan() instanceof Eval);
    }

    public void testMissingNamedBindingIsAbsentEvenWithAPackedRecord() {
        var input = input(attr("current_record"));
        var result = context(input, TransportVersion.current()).bind(input, of("missing_projection"), Source.EMPTY);
        assertEquals(new Literal(Source.EMPTY, null, DataType.KEYWORD), ((Eval) result.plan()).fields().getFirst().child());
        assertNull(result.packedLabels());
        assertNotNull(result.label("missing_projection"));
    }

    public void testDroppedNamedBindingCannotBeReadBackFromTheRecord() {
        var input = input(attr("current_record"));
        var context = context(input, TransportVersion.current());
        var dropped = context.bind(input, sub(input.shape(), of("region")), Source.EMPTY);
        var result = context.bind(dropped, of("region"), Source.EMPTY);
        assertEquals(new Literal(Source.EMPTY, null, DataType.KEYWORD), ((Eval) result.plan()).fields().getFirst().child());
        assertNotEquals(input.label("region"), result.label("region"));
    }

    public void testRelabelUpdatesBothBindings() {
        var input = input(attr("current_record"));
        var destination = new Alias(Source.EMPTY, "instance", Literal.keyword(Source.EMPTY, "new"));
        var result = context(input, TransportVersion.current()).replaceLabel(input, "instance", destination);
        assertEquals(destination.toAttribute(), result.label("instance"));
        assertNotEquals(input.packedLabels(), result.packedLabels());
        Eval record = result.plan().collect(Eval.class).getFirst();
        Case choice = (Case) record.fields().getFirst().child();
        assertTrue(choice.anyMatch(e -> e instanceof JsonRemove));
        assertTrue(choice.anyMatch(e -> e instanceof JsonMerge));
        assertTrue(choice.references().contains(input.packedLabels()));
    }

    public void testNamedOnlyProjectionNeedsNoJsonEdits() {
        var input = input(null);
        var result = context(input, TransportVersion.current()).bind(input, of("region"), Source.EMPTY);
        assertSame(input.plan(), result.plan());
        assertSame(input.label("region"), result.label("region"));
        assertNull(result.packedLabels());
    }

    public void testComputedRecordCanBeEditedOnCoordinatorWithOlderDataNodes() {
        var input = input(attr("current_record"));
        var context = context(input, TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_PROMQL_LABEL_RECORD));
        var result = context.bind(input, sub(input.shape(), of("region")), Source.EMPTY);
        assertTrue(((Eval) result.plan()).fields().getFirst().child() instanceof JsonRemove);
    }

    public void testMissingBindingDoesNotRetainTheUnprojectedSourceRecord() {
        var input = input(new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of()));
        var selected = TranslationConstraint.union(sub(input.shape(), of("instance")), of("new_projection"));
        var result = context(input, TransportVersion.current()).bind(input, selected, Source.EMPTY);
        assertEquals(new Literal(Source.EMPTY, null, DataType.KEYWORD), ((Eval) result.plan()).fields().getFirst().child());
        assertFalse(result.plan().outputSet().contains(input.packedLabels()));
    }

    public void testDottedDerivedLabelUsesItsNamedBinding() {
        var input = input(attr("current_record"));
        var context = context(input, TransportVersion.current());
        var destination = new Alias(Source.EMPTY, "service.name", Literal.keyword(Source.EMPTY, "derived"));
        var derived = context.replaceLabel(input, "service.name", destination);
        var result = context.bind(derived, of("service.name"), Source.EMPTY);
        assertSame(derived.plan(), result.plan());
        assertEquals(destination.toAttribute(), result.label("service.name"));
    }

    private static TranslationResult input(Attribute record) {
        Attribute region = attr("region");
        Attribute instance = attr("instance");
        var columns = new java.util.ArrayList<>(List.of(region, instance));
        if (record != null) columns.add(record);
        var plan = new LocalRelation(Source.EMPTY, columns, EmptyLocalSupplier.EMPTY);
        return new TranslationResult(
            plan,
            Map.of("region", region, "instance", instance),
            record,
            Literal.NULL,
            attr("step"),
            null,
            TranslationResult.Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    private static TranslationContext context(TranslationResult input, TransportVersion version) {
        var cmd = new PromqlCommand(
            Source.EMPTY,
            input.plan(),
            input.plan(),
            Literal.NULL,
            Literal.NULL,
            Literal.NULL,
            Literal.NULL,
            Literal.NULL,
            "value",
            input.step()
        );
        return new TranslationContext(cmd, EsqlTestUtils.analyzer().minimumTransportVersion(version).buildContext());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }
}
