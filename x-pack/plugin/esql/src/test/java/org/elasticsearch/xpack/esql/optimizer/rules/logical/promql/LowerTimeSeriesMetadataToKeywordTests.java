/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeries;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/**
 * The keyword form of a plan over typed metadata: casts give way to the keyword columns they cast, and what has no keyword
 * form fails.
 */
public class LowerTimeSeriesMetadataToKeywordTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    /** The source's keyword {@code _timeseries}, as a collapse reads it. */
    private final Attribute keyword = new ReferenceAttribute(SOURCE, "_timeseries", DataType.KEYWORD);
    private final LogicalPlan source = new LocalRelation(SOURCE, List.of(keyword), EmptyLocalSupplier.EMPTY);

    /** {@code plan} with a reader of every column it outputs. */
    private static LogicalPlan read(LogicalPlan plan) {
        return new Project(SOURCE, plan, plan.output());
    }

    /** A reader of a cast reads the keyword column it casts; the cast goes. */
    public void testCastReadsItsKeywordColumn() {
        Alias typed = new Alias(SOURCE, "_timeseries", new ToTimeSeries(SOURCE, keyword));
        LogicalPlan plan = new Project(SOURCE, new Eval(SOURCE, source, List.of(typed)), List.of(typed.toAttribute()));

        Project lowered = as(LowerTimeSeriesMetadataToKeyword.lower(plan), Project.class);
        assertSame(source, lowered.child());
        assertEquals(List.of(keyword), lowered.projections());
    }

    /** Typed metadata read through further columns reads keyword there too. */
    public void testReadersOfTypedColumnsGoKeyword() {
        Alias typed = new Alias(SOURCE, "_timeseries", new ToTimeSeries(SOURCE, keyword));
        Alias renamed = new Alias(SOURCE, "series", typed.toAttribute());
        LogicalPlan plan = read(new Eval(SOURCE, new Eval(SOURCE, source, List.of(typed)), List.of(renamed)));

        Project lowered = as(LowerTimeSeriesMetadataToKeyword.lower(plan), Project.class);
        Eval eval = as(lowered.child(), Eval.class);
        assertSame(source, eval.child());
        assertEquals(keyword, eval.fields().getFirst().child());
        assertEquals(DataType.KEYWORD, lowered.output().getLast().dataType());
    }

    /** The metadata of a series without labels is null as keyword metadata. */
    public void testEmptyMetadataIsNull() {
        Alias empty = new Alias(SOURCE, "$empty", new ToTimeSeries(SOURCE, Literal.keyword(SOURCE, "{}")));
        Project lowered = as(LowerTimeSeriesMetadataToKeyword.lower(read(new Eval(SOURCE, source, List.of(empty)))), Project.class);
        assertEquals(new Literal(SOURCE, null, DataType.KEYWORD), as(lowered.child(), Eval.class).fields().getFirst().child());
    }

    /** Rendering typed metadata as keyword is the keyword column itself. */
    public void testRenderingIsTheKeywordColumn() {
        Alias typed = new Alias(SOURCE, "_timeseries", new ToTimeSeries(SOURCE, keyword));
        Alias rendered = new Alias(SOURCE, "_timeseries", new ToString(SOURCE, typed.toAttribute(), TEST_CFG));
        LogicalPlan plan = new Project(
            SOURCE,
            new Eval(SOURCE, new Eval(SOURCE, source, List.of(typed)), List.of(rendered)),
            List.of(rendered.toAttribute())
        );

        Project lowered = as(LowerTimeSeriesMetadataToKeyword.lower(plan), Project.class);
        assertSame(source, lowered.child());
        assertEquals(List.of(keyword), lowered.projections());
    }

    /** Leaving labels out of metadata, or reading one out of it, has no keyword form. */
    public void testDerivingMetadataFails() {
        Alias typed = new Alias(SOURCE, "_timeseries", new ToTimeSeries(SOURCE, keyword));
        Source unsetSource = new Source(1, 1, "without (pod)");
        Alias unset = new Alias(
            SOURCE,
            "_timeseries$pod",
            new TimeSeriesUnset(unsetSource, typed.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")))
        );
        Alias get = new Alias(SOURCE, "pod", new TimeSeriesGet(unsetSource, typed.toAttribute(), Literal.keyword(SOURCE, "pod")));
        for (Alias derived : List.of(unset, get)) {
            LogicalPlan plan = read(new Eval(SOURCE, new Eval(SOURCE, source, List.of(typed)), List.of(derived)));
            var e = expectThrows(VerificationException.class, () -> LowerTimeSeriesMetadataToKeyword.lower(plan));
            assertThat(
                e.getMessage(),
                containsString("leaving labels out of _timeseries requires typed time-series metadata [without (pod)]")
            );
        }
    }
}
