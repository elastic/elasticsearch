/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;

import static org.hamcrest.Matchers.containsString;

public class QueryTranslatorTests extends AbstractQueryTranslatorTestCase {

    public void testLikeExactEqualsNoOptimization() throws Exception {
        PhysicalPlan plan = plan("process where process_name == \"*\" ");
        assertThat(asQuery(plan), containsString("\"term\":{\"process_name\""));
    }

    public void testLikeOptimization() throws Exception {
        PhysicalPlan plan = plan("process where process_name : \"*\" ");
        assertThat(asQuery(plan), containsString("\"exists\":{\"field\":\"process_name\""));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/63263")
    public void testMatchOptimization() throws Exception {
        PhysicalPlan plan = plan("process where match(process_name, \".*\") ");
        assertThat(asQuery(plan), containsString("\"exists\":{\"field\":\"process_name\""));
    }

    private static String asQuery(PhysicalPlan plan) {
        return plan.toString().replaceAll("\\s+", "");
    }
}
