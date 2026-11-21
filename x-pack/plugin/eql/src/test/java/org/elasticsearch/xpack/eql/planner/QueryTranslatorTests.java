/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.LocalExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class QueryTranslatorTests extends AbstractQueryTranslatorTestCase {

    public void testLikeExactEqualsNoOptimization() throws Exception {
        PhysicalPlan plan = plan("process where process_name == \"*\" ");
        assertThat(asQuery(plan), containsString("\"term\":{\"process_name\""));
    }

    public void testLikeOptimization() throws Exception {
        PhysicalPlan plan = plan("process where process_name : \"*\" ");
        assertThat(asQuery(plan), containsString("\"exists\":{\"field\":\"process_name\""));
    }

    /**
     * A mandatory key is never null while a missing optional key is always null, so the join can never match.
     * The mandatory key's not-null constraint propagated to the missing optional key must still be translatable.
     */
    public void testMandatoryKeyConstraintOnMissingOptionalKey() {
        PhysicalPlan plan = plan("sequence [process where true] by pid [process where true] by ?missing_field");
        assertThat(plan, instanceOf(LocalExec.class));
    }

    private static String asQuery(PhysicalPlan plan) {
        return plan.toString().replaceAll("\\s+", "");
    }
}
