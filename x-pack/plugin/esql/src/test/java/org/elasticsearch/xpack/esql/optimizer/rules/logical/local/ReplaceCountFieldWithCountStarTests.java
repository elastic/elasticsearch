/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class ReplaceCountFieldWithCountStarTests extends AbstractLocalLogicalPlanOptimizerTests {

    public void testComparisonWithConstant() {
        for (String comparison : List.of(
            "hire_date >= \"2023-10-20\"",
            "hire_date == \"2023-10-20\"",
            "\"2023-10-20\" < hire_date",
            "hire_date >= \"2023-10-20\" and hire_date < \"2023-10-24\"",
            "hire_date >= \"2023-10-20\" and salary > 10"
        )) {
            String query = "from test | where " + comparison + " | stats c = count(hire_date) by x = date_trunc(1 day, hire_date)";
            assertThat(query, count(query).field(), instanceOf(Literal.class));
        }
    }

    public void testFilterBelowOtherCommands() {
        String query = """
            from test
            | where hire_date >= "2023-10-20"
            | eval s = salary * 2
            | where s > 10
            | stats c = count(hire_date) by emp_no
            """;
        assertThat(count(query).field(), instanceOf(Literal.class));
    }

    public void testUngroupedCount() {
        String query = "from test | where hire_date >= \"2023-10-20\" | stats c = count(hire_date)";
        assertThat(count(query).field(), instanceOf(Literal.class));
    }

    /**
     * Without a comparison that every row must pass, the field may be null or multivalued, and its count differs from the
     * count of the rows.
     */
    public void testNotReplaced() {
        for (String query : List.of(
            "from test | stats c = count(hire_date) by emp_no",
            "from test | where salary > 10 | stats c = count(hire_date) by emp_no",
            "from test | where hire_date >= \"2023-10-20\" or salary > 10 | stats c = count(hire_date) by emp_no",
            "from test | where not(hire_date >= \"2023-10-20\") is null | stats c = count(hire_date) by emp_no",
            "from test | where hire_date is not null | stats c = count(hire_date) by emp_no",
            "from test | where hire_date >= \"2023-10-20\" | stats c = count(salary) by emp_no"
        )) {
            assertThat(query, count(query).field(), instanceOf(FieldAttribute.class));
        }
    }

    public void testFilteredCountNotReplaced() {
        String query = "from test | where hire_date >= \"2023-10-20\" | stats c = count(hire_date) where salary > 10 by emp_no";
        Count count = count(query);
        assertThat(count.hasFilter(), equalTo(true));
        assertThat(count.field(), instanceOf(FieldAttribute.class));
    }

    /**
     * The only {@link Count} of the only aggregation of the locally optimized plan.
     */
    private Count count(String query) {
        LogicalPlan localPlan = localPlan(testAnalyzer().coordinatorPlan(query), TEST_CFG, TEST_SEARCH_STATS);
        List<Count> counts = new ArrayList<>();
        localPlan.forEachDown(Aggregate.class, aggregate -> {
            for (NamedExpression aggregateExpression : aggregate.aggregates()) {
                if (Alias.unwrap(aggregateExpression) instanceof Count count) {
                    counts.add(count);
                }
            }
        });
        assertThat(query, counts, hasSize(1));
        return as(counts.getFirst(), Count.class);
    }
}
