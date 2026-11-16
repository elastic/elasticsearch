/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.IntoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.planner.PlannerUtils.SimplePlanReduction.NO_REDUCTION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class IntoMapperTests extends ESTestCase {
    private static final Source SRC = Source.EMPTY;

    public void testIntoEsRelationMapsToWriteUnderSum() {
        Into into = intoEsRelation();
        PhysicalPlan physical = new Mapper().map(new Versioned<>(into, TransportVersion.current()));

        IntoExec sum = as(physical, IntoExec.class);
        assertThat(sum.mode(), equalTo(IntoExec.Mode.SUM));
        assertThat(sum.destination(), equalTo("dst"));

        ExchangeExec exchange = as(sum.child(), ExchangeExec.class);

        FragmentExec fragment = as(exchange.child(), FragmentExec.class);
        assertThat(fragment.fragment(), sameInstance(into));
    }

    public void testLocalMapperTurnsFragmentIntoWriteExec() {
        Into into = intoEsRelation();
        PhysicalPlan writeExec = LocalMapper.INSTANCE.map(into);

        IntoExec write = as(writeExec, IntoExec.class);
        assertThat(write.mode(), equalTo(IntoExec.Mode.WRITE));
        assertThat(write.destination(), equalTo("dst"));
        assertThat(write.sourceAttributes(), equalTo(into.child().output()));
    }

    public void testReductionPlanReturnsNoReductionForIntoExec() {
        Into into = intoEsRelation();
        PhysicalPlan physical = new Mapper().map(new Versioned<>(into, TransportVersion.current()));

        assertThat(PlannerUtils.reductionPlan(physical), sameInstance(NO_REDUCTION));
    }

    public void testProjectAwayColumnsPreservesReductionPlan() {
        Into into = intoEsRelation();
        PhysicalPlan physical = new Mapper().map(new Versioned<>(into, TransportVersion.current()));
        PhysicalPlan optimized = new ProjectAwayColumns().apply(physical);

        // IntoExec(SUM) requires exactly Into.output() = [total, created]; ProjectAwayColumns adds no projection.
        IntoExec sum = as(optimized, IntoExec.class);
        assertThat(sum.mode(), equalTo(IntoExec.Mode.SUM));
        as(as(sum.child(), ExchangeExec.class).child(), FragmentExec.class);
        assertThat(PlannerUtils.reductionPlan(optimized), sameInstance(NO_REDUCTION));
    }

    private static Into intoEsRelation() {
        List<Attribute> attrs = List.of(
            new FieldAttribute(SRC, "k", new EsField("k", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        EsRelation relation = new EsRelation(SRC, "src", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), attrs);
        return new Into(SRC, relation, "dst");
    }
}
