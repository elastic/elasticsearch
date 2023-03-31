/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PlanNamedTypesTests extends ESTestCase {

    // List of known serializable plan nodes - this should be kept up to date or retrieved
    // programmatically. Excludes LocalSourceExec
    static final List<Class<? extends PhysicalPlan>> PHYSICAL_PLAN_NODE_CLS = List.of(
        AggregateExec.class,
        DissectExec.class,
        EsQueryExec.class,
        EsSourceExec.class,
        EvalExec.class,
        ExchangeExec.class,
        FieldExtractExec.class,
        FilterExec.class,
        LimitExec.class,
        OrderExec.class,
        ProjectExec.class,
        RowExec.class,
        ShowExec.class,
        TopNExec.class
    );

    // Tests that all physical plan nodes have a suitably named serialization entry.
    public void testPhysicalPlanEntries() {
        var expected = PHYSICAL_PLAN_NODE_CLS.stream().map(Class::getSimpleName).toList();
        var actual = PlanNamedTypes.namedTypeEntries()
            .stream()
            .filter(e -> e.categoryClass().isAssignableFrom(PhysicalPlan.class))
            .map(PlanNameRegistry.Entry::name)
            .toList();
        assertThat(actual, equalTo(expected));
    }

    // Tests that all names are unique - there should be a good reason if this is not the case.
    public void testUniqueNames() {
        var actual = PlanNamedTypes.namedTypeEntries().stream().map(PlanNameRegistry.Entry::name).distinct().toList();
        assertThat(actual.size(), equalTo(PlanNamedTypes.namedTypeEntries().size()));
    }
}
