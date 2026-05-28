/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class PromqlWithoutMissingLabelReproductionTests extends AbstractPromqlPlanOptimizerTests {

    @Before
    public void assumePromqlWithoutGroupingEnabled() {
        // assumeTrue("PromQL WITHOUT grouping must be enabled", ...);
    }

    public void testWithoutMissingLabelInOutput() {
        // 'non_existent' label is not in k8s index mappings
        String query = "PROMQL index=k8s step=1h result=(sum without (non_existent) (network.cost))";
        LogicalPlan plan = planPromql(query, false, false);

        PromqlCommand cmd = (PromqlCommand) plan;
        List<String> outputNames = cmd.output().stream().map(Attribute::name).toList();

        // The output should contain 'non_existent' as a null-filled column,
        // or at least it should be part of the schema if PromQL semantics say so.
        // Currently, it seems it's missing.
        assertThat(
            "Output should contain the 'without' label even if missing from index",
            outputNames,
            containsInAnyOrder("result", "step", MetadataAttribute.TIMESERIES, "non_existent")
        );
    }

    public void testWithoutExistingAndMissingLabelInOutput() {
        // 'pod' exists, 'non_existent' does not
        String query = "PROMQL index=k8s step=1h result=(sum without (pod, non_existent) (network.cost))";
        LogicalPlan plan = planPromql(query, false, false);

        PromqlCommand cmd = (PromqlCommand) plan;
        List<String> outputNames = cmd.output().stream().map(Attribute::name).toList();

        assertThat(
            "Output should contain both 'pod' and 'non_existent' labels",
            outputNames,
            containsInAnyOrder("result", "step", MetadataAttribute.TIMESERIES, "pod", "non_existent")
        );
    }
}
