/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import com.google.common.base.Joiner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.BooleanExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QueryPrimaryDefaultContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.QueryTermContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StatementContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StatementDefaultContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ValueExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ValueExpressionDefaultContext;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.logical.With;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

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
        assertEquals("SCORE()", f.sourceText());
    }

    public void testSelectCast() {
        Cast f = singleProjection(project(parseStatement("SELECT CAST(POWER(languages, 2) AS DOUBLE) FROM foo")), Cast.class);
        assertEquals("CAST(POWER(languages, 2) AS DOUBLE)", f.sourceText());
    }

    public void testSelectCastOperator() {
        Cast f = singleProjection(project(parseStatement("SELECT POWER(languages, 2)::DOUBLE FROM foo")), Cast.class);
        assertEquals("POWER(languages, 2)::DOUBLE", f.sourceText());
    }

    public void testSelectCastWithSQLOperator() {
        Cast f = singleProjection(project(parseStatement("SELECT CONVERT(POWER(languages, 2), SQL_DOUBLE) FROM foo")), Cast.class);
        assertEquals("CONVERT(POWER(languages, 2), SQL_DOUBLE)", f.sourceText());
    }

    public void testSelectCastToEsType() {
        Cast f = singleProjection(project(parseStatement("SELECT CAST('0.' AS SCALED_FLOAT)")), Cast.class);
        assertEquals("CAST('0.' AS SCALED_FLOAT)", f.sourceText());
    }

    public void testSelectAddWithParanthesis() {
        Add f = singleProjection(project(parseStatement("SELECT (1 +  2)")), Add.class);
        assertEquals("1 +  2", f.sourceText());
    }

    public void testSelectRightFunction() {
        UnresolvedFunction f = singleProjection(project(parseStatement("SELECT RIGHT()")), UnresolvedFunction.class);
        assertEquals("RIGHT()", f.sourceText());
    }

    public void testsSelectNonReservedKeywords() {
        String[] reserved = new String[] {
            "ANALYZE", "ANALYZED", "CATALOGS", "COLUMNS", "CURRENT", "DAY", "DEBUG", "EXECUTABLE", "EXPLAIN",
            "FIRST", "FORMAT", "FULL", "FUNCTIONS", "GRAPHVIZ", "HOUR", "INTERVAL", "LAST", "LIMIT",
            "MAPPED", "MINUTE", "MONTH", "OPTIMIZED", "PARSED", "PHYSICAL", "PLAN", "QUERY", "RLIKE",
            "SCHEMAS", "SECOND", "SHOW", "SYS", "TABLES", "TEXT", "TYPE", "TYPES", "VERIFY", "YEAR"};
        StringJoiner sj = new StringJoiner(",");
        for (String s : reserved) {
            sj.add(s);
        }

        Project project = project(parseStatement("SELECT " + sj.toString() + " FROM foo"));
        assertEquals(reserved.length, project.projections().size());

        for (int i = 0; i < project.projections().size(); i++) {
            NamedExpression ne = project.projections().get(i);
            assertEquals(UnresolvedAttribute.class, ne.getClass());
            assertEquals(reserved[i], ne.name());
        }
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
        OrderBy ob = orderBy(parseStatement("SELECT * FROM foo ORDER BY SCORE( )" + stringForDirection(dir)));
        assertThat(ob.order(), hasSize(1));
        Order o = ob.order().get(0);
        assertEquals(dir, o.direction());
        assertThat(o.child(), instanceOf(UnresolvedFunction.class));
        UnresolvedFunction f = (UnresolvedFunction) o.child();
        assertEquals("SCORE( )", f.sourceText());
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

    public void testLimitToPreventStackOverflowFromLongListOfQuotedIdentifiers() {
        // Create expression in the form of "t"."field","t"."field", ...

        // 200 elements is ok
        new SqlParser().createStatement("SELECT " +
            Joiner.on(",").join(nCopies(200, "\"t\".\"field\"")) + " FROM t");

        // 201 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createStatement("SELECT " +
            Joiner.on(",").join(nCopies(201, "\"t\".\"field\"")) + " FROM t"));
        assertEquals("line 1:2409: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLongListOfUnQuotedIdentifiers() {
        // Create expression in the form of t.field,t.field, ...

        // 250 elements is ok
        new SqlParser().createStatement("SELECT " +
            Joiner.on(",").join(nCopies(200, "t.field")) + " FROM t");

        // 251 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createStatement("SELECT " +
            Joiner.on(",").join(nCopies(201, "t.field")) + " FROM t"));
        assertEquals("line 1:1609: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeUnaryBooleanExpression() {
        // Create expression in the form of NOT(NOT(NOT ... (b) ...)

        // 99 elements is ok
        new SqlParser().createExpression(
            Joiner.on("").join(nCopies(99, "NOT(")).concat("b").concat(Joiner.on("").join(nCopies(99, ")"))));

        // 100 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createExpression(
            Joiner.on("").join(nCopies(100, "NOT(")).concat("b").concat(Joiner.on("").join(nCopies(100, ")")))));
        assertEquals("line 1:402: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeBinaryBooleanExpression() {
        // Create expression in the form of a = b OR a = b OR ... a = b

        // 100 elements is ok
        new SqlParser().createExpression(Joiner.on(" OR ").join(nCopies(100, "a = b")));

        // 101 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () ->
            new SqlParser().createExpression(Joiner.on(" OR ").join(nCopies(101, "a = b"))));
        assertEquals("line 1:902: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeUnaryArithmeticExpression() {
        // Create expression in the form of abs(abs(abs ... (i) ...)

        // 199 elements is ok
        new SqlParser().createExpression(
            Joiner.on("").join(nCopies(199, "abs(")).concat("i").concat(Joiner.on("").join(nCopies(199, ")"))));

        // 200 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createExpression(
            Joiner.on("").join(nCopies(200, "abs(")).concat("i").concat(Joiner.on("").join(nCopies(200, ")")))));
        assertEquals("line 1:802: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeBinaryArithmeticExpression() {
        // Create expression in the form of a + a + a + ... + a

        // 200 elements is ok
        new SqlParser().createExpression(Joiner.on(" + ").join(nCopies(200, "a")));

        // 201 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () ->
            new SqlParser().createExpression(Joiner.on(" + ").join(nCopies(201, "a"))));
        assertEquals("line 1:802: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeSubselectTree() {
        // Test with queries in the form of `SELECT * FROM (SELECT * FROM (... t) ...)

        // 200 elements is ok
        new SqlParser().createStatement(
            Joiner.on(" (").join(nCopies(200, "SELECT * FROM"))
                .concat("t")
                .concat(Joiner.on("").join(nCopies(199, ")"))));

        // 201 elements parser's "circuit breaker" is triggered
        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createStatement(
            Joiner.on(" (").join(nCopies(201, "SELECT * FROM"))
                .concat("t")
                .concat(Joiner.on("").join(nCopies(200, ")")))));
        assertEquals("line 1:3002: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitToPreventStackOverflowFromLargeComplexSubselectTree() {
        // Test with queries in the form of `SELECT true OR true OR .. FROM (SELECT true OR true OR... FROM (... t) ...)

        new SqlParser().createStatement(
            Joiner.on(" (").join(nCopies(20, "SELECT ")).
                concat(Joiner.on(" OR ").join(nCopies(180, "true"))).concat(" FROM")
                .concat("t").concat(Joiner.on("").join(nCopies(19, ")"))));

        ParsingException e = expectThrows(ParsingException.class, () -> new SqlParser().createStatement(
            Joiner.on(" (").join(nCopies(20, "SELECT ")).
                concat(Joiner.on(" OR ").join(nCopies(190, "true"))).concat(" FROM")
                .concat("t").concat(Joiner.on("").join(nCopies(19, ")")))));
        assertEquals("line 1:1628: SQL statement too large; halt parsing to prevent memory errors (stopped at depth 200)",
            e.getMessage());
    }

    public void testLimitStackOverflowForInAndLiteralsIsNotApplied() {
        int noChildren = 10_000;
        LogicalPlan plan = parseStatement("SELECT * FROM t WHERE a IN(" +
            Joiner.on(",").join(nCopies(noChildren, "a + 10")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "-(-a - 10)")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "20")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "-20")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "20.1234")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "-20.4321")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "1.1234E56")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "-1.4321E-65")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "'foo'")) + "," +
            Joiner.on(",").join(nCopies(noChildren, "'bar'")) + ")");

        assertEquals(With.class, plan.getClass());
        assertEquals(Project.class, ((With) plan).child().getClass());
        assertEquals(Filter.class, ((Project) ((With) plan).child()).child().getClass());
        Filter filter = (Filter) ((Project) ((With) plan).child()).child();
        assertEquals(In.class, filter.condition().getClass());
        In in = (In) filter.condition();
        assertEquals("?a", in.value().toString());
        assertEquals(noChildren * 2 + 8, in.list().size());
        assertThat(in.list().get(0).toString(), startsWith("Add[?a,10]#"));
        assertThat(in.list().get(noChildren).toString(), startsWith("Neg[Sub[Neg[?a]#"));
        assertEquals("20", in.list().get(noChildren * 2).toString());
        assertEquals("-20", in.list().get(noChildren * 2 + 1).toString());
        assertEquals("20.1234", in.list().get(noChildren * 2 + 2).toString());
        assertEquals("-20.4321", in.list().get(noChildren * 2 + 3).toString());
        assertEquals("1.1234E56", in.list().get(noChildren * 2 + 4).toString());
        assertEquals("-1.4321E-65", in.list().get(noChildren * 2 + 5).toString());
        assertEquals("'foo'=foo", in.list().get(noChildren * 2 + 6).toString());
        assertEquals("'bar'=bar", in.list().get(noChildren * 2 + 7).toString());
    }

    public void testDecrementOfDepthCounter() {
        SqlParser.CircuitBreakerListener cbl = new SqlParser.CircuitBreakerListener();
        StatementContext sc = new StatementContext();
        QueryTermContext qtc = new QueryTermContext();
        ValueExpressionContext vec = new ValueExpressionContext();
        BooleanExpressionContext bec = new BooleanExpressionContext();

        cbl.enterEveryRule(sc);
        cbl.enterEveryRule(sc);
        cbl.enterEveryRule(qtc);
        cbl.enterEveryRule(qtc);
        cbl.enterEveryRule(qtc);
        cbl.enterEveryRule(vec);
        cbl.enterEveryRule(bec);
        cbl.enterEveryRule(bec);

        cbl.exitEveryRule(new StatementDefaultContext(sc));
        cbl.exitEveryRule(new StatementDefaultContext(sc));
        cbl.exitEveryRule(new QueryPrimaryDefaultContext(qtc));
        cbl.exitEveryRule(new QueryPrimaryDefaultContext(qtc));
        cbl.exitEveryRule(new ValueExpressionDefaultContext(vec));
        cbl.exitEveryRule(new SqlBaseParser.BooleanDefaultContext(bec));

        assertEquals(0, cbl.depthCounts().get(SqlBaseParser.StatementContext.class.getSimpleName()));
        assertEquals(1, cbl.depthCounts().get(SqlBaseParser.QueryTermContext.class.getSimpleName()));
        assertEquals(0, cbl.depthCounts().get(SqlBaseParser.ValueExpressionContext.class.getSimpleName()));
        assertEquals(1, cbl.depthCounts().get(SqlBaseParser.BooleanExpressionContext.class.getSimpleName()));
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
