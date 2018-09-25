/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SqlParserTests extends ESTestCase {

    public void testSelectStar() {
        singleProjection(project(parseStatement("SELECT * FROM foo")), UnresolvedStar.class);
    }

    private <T> T singleProjection(Project project, Class<T> type) {
        assertThat(project.projections(), hasSize(1));
        NamedExpression p = project.projections().get(0);
        assertThat(p, instanceOf(type));
        return type.cast(p);
    }

    public void testSelectField() {
        UnresolvedAttribute a = singleProjection(project(parseStatement("SELECT bar FROM foo")), UnresolvedAttribute.class);
        assertEquals("bar", a.name());
    }

    public void testSelectScore() {
        UnresolvedFunction f = singleProjection(project(parseStatement("SELECT SCORE() FROM foo")), UnresolvedFunction.class);
        assertEquals("SCORE", f.functionName());
    }

    public void testSelectRightFunction() {
        UnresolvedFunction f = singleProjection(project(parseStatement("SELECT RIGHT()")), UnresolvedFunction.class);
        assertEquals("RIGHT", f.functionName());
    }

    public void testOrderByField() {
        Order.OrderDirection dir = randomFrom(Order.OrderDirection.values());
        OrderBy ob = orderBy(parseStatement("SELECT * FROM foo ORDER BY bar" + stringForDirection(dir)));
        assertThat(ob.order(), hasSize(1));
        Order o = ob.order().get(0);
        assertEquals(dir, o.direction());
        assertThat(o.child(), instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute a = (UnresolvedAttribute) o.child();
        assertEquals("bar", a.name());
    }

    public void testOrderByScore() {
        Order.OrderDirection dir = randomFrom(Order.OrderDirection.values());
        OrderBy ob = orderBy(parseStatement("SELECT * FROM foo ORDER BY SCORE()" + stringForDirection(dir)));
        assertThat(ob.order(), hasSize(1));
        Order o = ob.order().get(0);
        assertEquals(dir, o.direction());
        assertThat(o.child(), instanceOf(UnresolvedFunction.class));
        UnresolvedFunction f = (UnresolvedFunction) o.child();
        assertEquals("SCORE", f.functionName());
    }

    public void testOrderByTwo() {
        Order.OrderDirection dir0 = randomFrom(Order.OrderDirection.values());
        Order.OrderDirection dir1 = randomFrom(Order.OrderDirection.values());
        OrderBy ob = orderBy(parseStatement(
            "     SELECT *"
            + "     FROM foo"
            + " ORDER BY bar" + stringForDirection(dir0) + ", baz" + stringForDirection(dir1)));
        assertThat(ob.order(), hasSize(2));
        Order o = ob.order().get(0);
        assertEquals(dir0, o.direction());
        assertThat(o.child(), instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute a = (UnresolvedAttribute) o.child();
        assertEquals("bar", a.name());
        o = ob.order().get(1);
        assertEquals(dir1, o.direction());
        assertThat(o.child(), instanceOf(UnresolvedAttribute.class));
        a = (UnresolvedAttribute) o.child();
        assertEquals("baz", a.name());
    }

    public void testStringQuery() {
        LogicalPlan plan =
            parseStatement("SELECT * FROM FOO WHERE " +
                "QUERY('foo', 'default_field=last_name;lenient=true', 'fuzzy_rewrite=scoring_boolean')");

        StringQueryPredicate sqp = (StringQueryPredicate) ((Filter) plan.children().get(0).children().get(0)).condition();
        assertEquals("foo", sqp.query());
        assertEquals(3, sqp.optionMap().size());
        assertThat(sqp.optionMap(), hasEntry("default_field", "last_name"));
        assertThat(sqp.optionMap(), hasEntry("lenient", "true"));
        assertThat(sqp.optionMap(), hasEntry("fuzzy_rewrite", "scoring_boolean"));
    }

    public void testMatchQuery() {
        LogicalPlan plan = parseStatement("SELECT * FROM FOO WHERE " +
                    "MATCH(first_name, 'foo', 'operator=AND;lenient=true', 'fuzzy_rewrite=scoring_boolean')");

        MatchQueryPredicate mqp = (MatchQueryPredicate) ((Filter) plan.children().get(0).children().get(0)).condition();
        assertEquals("foo", mqp.query());
        assertEquals("?first_name", mqp.field().toString());
        assertEquals(3, mqp.optionMap().size());
        assertThat(mqp.optionMap(), hasEntry("operator", "AND"));
        assertThat(mqp.optionMap(), hasEntry("lenient", "true"));
        assertThat(mqp.optionMap(), hasEntry("fuzzy_rewrite", "scoring_boolean"));
    }

    public void testMultiMatchQuery() {
        LogicalPlan plan = parseStatement("SELECT * FROM FOO WHERE " +
                "MATCH('first_name,last_name', 'foo', 'operator=AND;type=best_fields', 'fuzzy_rewrite=scoring_boolean')");

        MultiMatchQueryPredicate mmqp = (MultiMatchQueryPredicate) ((Filter) plan.children().get(0).children().get(0)).condition();
        assertEquals("foo", mmqp.query());
        assertEquals("first_name,last_name", mmqp.fieldString());
        assertEquals(3, mmqp.optionMap().size());
        assertThat(mmqp.optionMap(), hasEntry("operator", "AND"));
        assertThat(mmqp.optionMap(), hasEntry("type", "best_fields"));
        assertThat(mmqp.optionMap(), hasEntry("fuzzy_rewrite", "scoring_boolean"));
    }

    private LogicalPlan parseStatement(String sql) {
        return new SqlParser().createStatement(sql);
    }

    private Project project(LogicalPlan plan) {
        List<Project> sync = new ArrayList<>(1);
        projectRecur(plan, sync);
        assertThat("expected only one SELECT", sync, hasSize(1));
        return sync.get(0);
    }

    private void projectRecur(LogicalPlan plan, List<Project> sync) {
        if (plan instanceof Project) {
            sync.add((Project) plan);
            return;
        }
        for (LogicalPlan child : plan.children()) {
            projectRecur(child, sync);
        }
    }

    /**
     * Find the one and only {@code ORDER BY} in a plan.
     */
    private OrderBy orderBy(LogicalPlan plan) {
        List<LogicalPlan> l = plan.children().stream()
            .filter(c -> c instanceof OrderBy)
            .collect(toList());
        assertThat("expected only one ORDER BY", l, hasSize(1));
        return (OrderBy) l.get(0);
    }

    /**
     * Convert a direction into a string that represents that parses to
     * that direction.
     */
    private String stringForDirection(Order.OrderDirection dir) {
        String dirStr = dir.toString();
        return randomBoolean() && dirStr.equals("ASC") ? "" : " " + dirStr;
    }
}
