/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/** PromQL set operators translated over a label container. */
public class PromqlPlanVectorBinarySetV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanVectorBinarySetV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** {@code or} unions its branches and keeps, per step, the first branch's series for each label set. */
    public void testOrUnionsTheBranches() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h network.bytes_in or network.cost", false);

        UnionAll union = plan.collect(UnionAll.class).getFirst();
        assertThat(union.children(), hasSize(2));
        TopNBy first = plan.collect(TopNBy.class).getFirst();
        // the label pushdown projects the union's label columns apart
        assertThat(as(first.child(), Project.class).child(), equalTo(union));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** A chain of {@code or} unions all its branches at once. */
    public void testOrChainUnionsEveryBranch() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h network.bytes_in or network.cost or network.total_bytes_in", false);
        assertThat(plan.collect(UnionAll.class).getFirst().children(), hasSize(3));
    }

    /** The translated set operators plan through the optimizer. */
    public void testSetOperatorsOptimize() {
        for (String query : List.of(
            "network.bytes_in or network.cost",
            "network.bytes_in or network.cost or network.total_bytes_in",
            "sum by (cluster) (network.bytes_in) or sum by (cluster) (network.cost)"
        )) {
            LogicalPlan plan = planPromql("PROMQL index=k8s step=1h " + query, true);
            assertTrue(query, plan.resolved());
        }
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
