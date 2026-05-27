/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.contains;

public class PruneRedundantAggregateGroupingsTests extends AbstractLogicalPlanOptimizerTests {
    private static final String S3_PATH = "s3://bucket/data.parquet";

    public void testPrunesEvalConstantGrouping() {
        var plan = plan("""
            FROM test
            | EVAL const1 = 1
            | STATS c = COUNT(*) BY const1, last_name
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "const1", "last_name"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("const1"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "last_name"));
    }

    public void testPrunesDirectLiteralGrouping() {
        var plan = plan("""
            FROM test
            | STATS c = COUNT(*) BY 1, last_name
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "1", "last_name"));

        var aggregate = rewrittenAggregate(as(project.child(), Eval.class));
        assertThat(Expressions.names(aggregate.groupings()), contains("last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "last_name"));
    }

    public void testDoesNotPruneMultivalueConstantGrouping() {
        var plan = plan("""
            FROM test
            | EVAL mv = [1, 2]
            | STATS c = COUNT(*) BY mv, last_name
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("mv", "last_name"));
    }

    public void testPrunesDerivedExternalGroupings() {
        var plan = externalPlan("""
            EXTERNAL "s3://bucket/data.parquet"
            | EVAL ip_m1 = ClientIP - 1, ip_m2 = ClientIP - 2, ip_m3 = ClientIP - 3
            | STATS c = COUNT(*) BY ClientIP, ip_m1, ip_m2, ip_m3
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_m1", "ip_m2", "ip_m3"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("ip_m1", "ip_m2", "ip_m3"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP"));
    }

    public void testDoesNotPruneDerivedOrdinaryIndexGrouping() {
        var plan = plan("""
            FROM test
            | EVAL emp_m1 = emp_no - 1
            | STATS c = COUNT(*) BY emp_no, emp_m1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("emp_no", "emp_m1"));
    }

    public void testDoesNotPruneIndependentExternalExpression() {
        var plan = externalPlan("""
            EXTERNAL "s3://bucket/data.parquet"
            | EVAL other_m1 = OtherIP - 1
            | STATS c = COUNT(*) BY ClientIP, other_m1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "other_m1"));
    }

    public void testDoesNotPruneNonWhitelistedExternalExpression() {
        var plan = externalPlan("""
            EXTERNAL "s3://bucket/data.parquet"
            | EVAL ip_mul = ClientIP * 2
            | STATS c = COUNT(*) BY ClientIP, ip_mul
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_mul"));
    }

    private LogicalPlan externalPlan(String query) {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        List<Attribute> schema = List.of(
            referenceAttribute("ClientIP", INTEGER),
            referenceAttribute("OtherIP", INTEGER),
            referenceAttribute("URL", KEYWORD)
        );
        return optimize(analyzer().externalSourceResolution(S3_PATH, schema, FileList.UNRESOLVED).query(query));
    }

    private static Project rewrittenProject(LogicalPlan plan) {
        return as(plan, Project.class);
    }

    private static Aggregate rewrittenAggregate(Eval eval) {
        return as(as(eval.child(), Limit.class).child(), Aggregate.class);
    }
}
