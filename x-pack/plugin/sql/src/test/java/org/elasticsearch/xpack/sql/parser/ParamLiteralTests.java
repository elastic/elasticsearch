/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DateUtils.UTC;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class ParamLiteralTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private LogicalPlan parse(String sql, SqlTypedParamValue... parameters) {
        return parser.createStatement(sql, List.of(parameters), UTC);
    }

    public void testMultipleParamLiteralsWithUnresolvedAliases() {
        LogicalPlan logicalPlan = parse("SELECT ?, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 200)
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan.children().get(0)).projections();
        assertThat(projections, everyItem(instanceOf(UnresolvedAlias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?"));
        assertThat(projections.get(1).toString(), startsWith("200 AS ?"));
    }

    public void testMultipleParamLiteralsWithUnresolvedAliasesAndWhereClause() {
        LogicalPlan logicalPlan = parse("SELECT ?, ?, (?) FROM test WHERE 1 < ?",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 200),
            new SqlTypedParamValue("integer", 300)
        );
        Project project = (Project) logicalPlan.children().get(0);
        List<? extends NamedExpression> projections = project.projections();
        assertThat(projections, everyItem(instanceOf(UnresolvedAlias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?"));
        assertThat(projections.get(1).toString(), startsWith("100 AS ?"));
        assertThat(projections.get(2).toString(), startsWith("200 AS ?"));
        assertThat(project.children().get(0), instanceOf(Filter.class));
        Filter filter = (Filter) project.children().get(0);
        assertThat(filter.condition(), instanceOf(LessThan.class));
        LessThan condition = (LessThan) filter.condition();
        assertThat(condition.left(), instanceOf(Literal.class));
        assertThat(condition.right(), instanceOf(Literal.class));
        assertThat(((Literal)condition.right()).value(), equalTo(300));
    }

    public void testParamLiteralsWithUnresolvedAliasesAndMixedTypes() {
        LogicalPlan logicalPlan = parse("SELECT ?, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("text", "100")
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan.children().get(0)).projections();
        assertThat(projections, everyItem(instanceOf(UnresolvedAlias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?"));
        assertThat(projections.get(1).toString(), startsWith("100 AS ?"));
    }

    public void testParamLiteralsWithResolvedAndUnresolvedAliases() {
        LogicalPlan logicalPlan = parse("SELECT ?, ? as x, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 200),
            new SqlTypedParamValue("integer", 300)
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan.children().get(0)).projections();
        assertThat(projections.get(0).toString(), startsWith("100 AS ?"));
        assertThat(projections.get(1).toString(), startsWith("200 AS x#"));;
        assertThat(projections.get(2).toString(), startsWith("300 AS ?"));;
    }

}
