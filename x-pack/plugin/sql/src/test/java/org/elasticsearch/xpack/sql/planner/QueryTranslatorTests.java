/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AbstractPercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.ql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.ql.querydsl.query.PrefixQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStatsEnclosed;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.UnaryStringFunction;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.QueryFolder.FoldAggregate.GroupingContext;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.QueryTranslation;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByDateHistogram;
import org.elasticsearch.xpack.sql.querydsl.container.MetricAggRef;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.sql.SqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.sql.SqlTestUtils.literal;
import static org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation.E;
import static org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation.PI;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.DATE_FORMAT;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.TIME_FORMAT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class QueryTranslatorTests extends ESTestCase {

    private static class TestContext {
        private final SqlFunctionRegistry sqlFunctionRegistry;
        private final SqlParser parser;
        private final Analyzer analyzer;
        private final Optimizer optimizer;
        private final Planner planner;

        TestContext(String mappingFile) {
            parser = new SqlParser();
            sqlFunctionRegistry = new SqlFunctionRegistry();

            Map<String, EsField> mapping = SqlTypesTests.loadMapping(mappingFile);
            EsIndex test = new EsIndex("test", mapping);
            IndexResolution getIndexResult = IndexResolution.valid(test);
            analyzer = new Analyzer(TEST_CFG, sqlFunctionRegistry, getIndexResult, new Verifier(new Metrics()));
            optimizer = new Optimizer();
            planner = new Planner();
        }

        public LogicalPlan plan(String sql) {
            return plan(sql, DateUtils.UTC);
        }

        public LogicalPlan plan(String sql, ZoneId zoneId) {
            return analyzer.analyze(parser.createStatement(sql, zoneId), true);
        }

        private PhysicalPlan optimizeAndPlan(String sql) {
            return optimizeAndPlan(plan(sql));
        }

        private PhysicalPlan optimizeAndPlan(LogicalPlan plan) {
            return planner.plan(optimizer.optimize(plan),true);
        }

        private LogicalPlan parameterizedSql(String sql, SqlTypedParamValue... params) {
            return analyzer.analyze(parser.createStatement(sql, asList(params), DateUtils.UTC), true);
        }
    }

    private static TestContext defaultTestContext;

    @BeforeClass
    public static void init() {
        defaultTestContext = new TestContext("mapping-multi-field-variation.json");
    }

    private LogicalPlan plan(String sql) {
        return defaultTestContext.plan(sql, DateUtils.UTC);
    }

    private LogicalPlan plan(String sql, ZoneId zoneId) {
        return defaultTestContext.plan(sql, zoneId);
    }

    private PhysicalPlan optimizeAndPlan(String sql) {
        return defaultTestContext.optimizeAndPlan(sql);
    }

    private PhysicalPlan optimizeAndPlan(LogicalPlan plan) {
        return defaultTestContext.optimizeAndPlan(plan);
    }

    private QueryTranslation translate(Expression condition) {
        return QueryTranslator.toQuery(condition, false);
    }

    private QueryTranslation translateWithAggs(Expression condition) {
        return QueryTranslator.toQuery(condition, true);
    }

    private LogicalPlan parameterizedSql(String sql, SqlTypedParamValue... params) {
        return defaultTestContext.parameterizedSql(sql, params);
    }

    @SafeVarargs
    private void assertESQuery(LogicalPlan plan, Matcher<String>... matchers) {
        assertESQuery(optimizeAndPlan(plan));
    }

    @SafeVarargs
    private void assertESQuery(PhysicalPlan plan, Matcher<String>... matchers) {
        assertEquals(EsQueryExec.class, plan.getClass());
        EsQueryExec eqe = (EsQueryExec) plan;
        final String esQuery = eqe.queryContainer().toString().replaceAll("\\s+", "");
        for (Matcher<String> matcher : matchers) {
            assertThat(esQuery, matcher);
        }
    }

    // Miscellaneous
    /////////////////
    public void testAliasAndGroupByResolution() {
        LogicalPlan p = plan("SELECT COUNT(*) AS c FROM test WHERE ABS(int) > 0 GROUP BY int");
        assertTrue(p instanceof Aggregate);
        var pc = ((Aggregate) p).child();
        assertTrue(pc instanceof Filter);
        Expression condition = ((Filter) pc).condition();
        assertEquals("GREATERTHAN", ((GreaterThan) condition).functionName());
        List<Expression> groupings = ((Aggregate) p).groupings();
        assertTrue(groupings.get(0).resolved());
        var agg = ((Aggregate) p).aggregates();
        assertEquals("c", (agg.get(0)).name());
        assertEquals("COUNT", ((Count) ((Alias) agg.get(0)).child()).functionName());
    }
    public void testLiteralWithGroupBy() {
        LogicalPlan p = plan("SELECT 1 as t, 2 FROM test GROUP BY int");
        assertTrue(p instanceof Aggregate);
        List<Expression> groupings = ((Aggregate) p).groupings();
        assertEquals(1, groupings.size());
        assertTrue(groupings.get(0).resolved());
        assertTrue(groupings.get(0) instanceof FieldAttribute);
        var aggs = ((Aggregate) p).aggregates();
        assertEquals(2, aggs.size());
        assertEquals("t", (aggs.get(0)).name());
        assertTrue(((Alias) aggs.get(0)).child() instanceof Literal);
        assertEquals("1", ((Alias) aggs.get(0)).child().toString());
        assertEquals("2", ((Alias) aggs.get(1)).child().toString());
    }

    public void testComparisonAgainstColumns() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > int");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> translate(condition));
        assertEquals("Line 1:43: Comparisons against fields are not (currently) supported; offender [int] in [>]", ex.getMessage());
    }

    public void testMathFunctionHavingClause() {
        MathOperation operation =
                (MathOperation) randomFrom(Stream.of(MathOperation.values()).filter(o -> o != PI && o != E).toArray());

        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING " +
                operation.name() + "(max(int)) > 10");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = translateWithAggs(condition);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(InternalSqlScriptUtils." +
                        operation.name().toLowerCase(Locale.ROOT) +
                        "(InternalQlScriptUtils.nullSafeCastNumeric(params.a0,params.v0)),params.v1))",
                aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=max(int)"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=10}]"));
    }

    public void testHavingWithLiteralImplicitGrouping() {
        PhysicalPlan p = optimizeAndPlan("SELECT 1 FROM test HAVING COUNT(*) > 0");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertTrue("Should be tracking hits", eqe.queryContainer().shouldTrackHits());
        assertEquals(1, eqe.output().size());
        assertThat(eqe.queryContainer().toString().replaceAll("\\s+", ""), containsString("\"size\":0"));
    }

    public void testHavingWithColumnImplicitGrouping() {
        PhysicalPlan p = optimizeAndPlan("SELECT MAX(int) FROM test HAVING COUNT(*) > 0");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertTrue("Should be tracking hits", eqe.queryContainer().shouldTrackHits());
        assertEquals(1, eqe.output().size());
        assertThat(eqe.queryContainer().toString().replaceAll("\\s+", ""), containsString(
                "\"script\":{\"source\":\"InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a0,params.v0))\","
                        + "\"lang\":\"painless\",\"params\":{\"v0\":0}}"));
    }

    public void testScriptsInsideAggregateFunctions() {
        for (FunctionDefinition fd : defaultTestContext.sqlFunctionRegistry.listFunctions()) {
            if (AggregateFunction.class.isAssignableFrom(fd.clazz()) && (MatrixStatsEnclosed.class.isAssignableFrom(fd.clazz()) == false)) {
                String aggFunction = fd.name() + "(ABS((int * 10) / 3) + 1";
                if (fd.clazz() == Percentile.class || fd.clazz() == PercentileRank.class) {
                    aggFunction += ", 50";
                }
                aggFunction += ")";
                PhysicalPlan p = optimizeAndPlan("SELECT " + aggFunction + " FROM test");
                if (fd.clazz() == Count.class) {
                    assertESQuery(
                            p,
                            containsString(
                                    ":{\"script\":{\"source\":\"InternalQlScriptUtils.isNotNull(InternalSqlScriptUtils.add("
                                            + "InternalSqlScriptUtils.abs(InternalSqlScriptUtils.div(InternalSqlScriptUtils.mul("
                                            + "InternalQlScriptUtils.docValue(doc,params.v0),params.v1),params.v2)),params.v3))\","
                                            + "\"lang\":\"painless\",\"params\":{\"v0\":\"int\",\"v1\":10,\"v2\":3,\"v3\":1}}"
                            )
                    );
                } else {
                    assertESQuery(
                            p,
                            containsString(
                                    ":{\"script\":{\"source\":\"InternalSqlScriptUtils.add(InternalSqlScriptUtils.abs("
                                            + "InternalSqlScriptUtils.div(InternalSqlScriptUtils.mul(InternalQlScriptUtils.docValue("
                                            + "doc,params.v0),params.v1),params.v2)),params.v3)\",\"lang\":\"painless\",\"params\":{"
                                            + "\"v0\":\"int\",\"v1\":10,\"v2\":3,\"v3\":1}}"
                            )
                    );
                }
            }
        }
    }

    public void testScriptsInsideAggregateFunctionsWithHaving() {
        for (FunctionDefinition fd : defaultTestContext.sqlFunctionRegistry.listFunctions()) {
            if (AggregateFunction.class.isAssignableFrom(fd.clazz())
                    && (MatrixStatsEnclosed.class.isAssignableFrom(fd.clazz()) == false)
                    // First/Last don't support having: https://github.com/elastic/elasticsearch/issues/37938
                    && (TopHits.class.isAssignableFrom(fd.clazz()) == false)) {
                String aggFunction = fd.name() + "(ABS((int * 10) / 3) + 1";
                if (fd.clazz() == Percentile.class || fd.clazz() == PercentileRank.class) {
                    aggFunction += ", 50";
                }
                aggFunction += ")";
                LogicalPlan p = plan("SELECT " + aggFunction + ", keyword FROM test " + "GROUP BY keyword HAVING " + aggFunction + " > 20");
                assertTrue(p instanceof Filter);
                assertTrue(((Filter) p).child() instanceof Aggregate);
                List<Attribute> outputs = ((Filter) p).child().output();
                assertEquals(2, outputs.size());
                assertEquals(aggFunction, outputs.get(0).qualifiedName());
                assertEquals("test.keyword", outputs.get(1).qualifiedName());

                Expression condition = ((Filter) p).condition();
                assertFalse(condition.foldable());
                QueryTranslation translation = translateWithAggs(condition);
                assertNull(translation.query);
                AggFilter aggFilter = translation.aggFilter;
                String fn = fd.name();
                String typeName = "MAX".equals(fn) || "MIN".equals(fn) ? "INTEGER" : "SUM".equals(fn) ? "LONG" : "";
                String safeCast = "InternalQlScriptUtils.nullSafeCastNumeric(params.a0,params.v0)";
                String param1st = typeName.length() == 0 ? "params.a0" : safeCast;
                String param2nd = typeName.length() == 0 ? "params.v0" : "params.v1";
                assertEquals(
                        "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(" + param1st + "," + param2nd + "))",
                        aggFilter.scriptTemplate().toString()
                );
                String params = "[{a=" + aggFunction + "}," + (typeName.length() == 0 ? "" : " {v=" + typeName + "},") + " {v=20}]";
                assertEquals(params, aggFilter.scriptTemplate().params().toString());
            }
        }
    }

    public void testFoldingWithParamsWithoutIndex() {
        PhysicalPlan p = optimizeAndPlan(parameterizedSql("SELECT ?, ?, ? FROM test",
                new SqlTypedParamValue("integer", 100),
                new SqlTypedParamValue("integer", 100),
                new SqlTypedParamValue("integer", 200)));
        assertThat(p.output(), everyItem(instanceOf(ReferenceAttribute.class)));
        assertThat(p.output().get(0).toString(), startsWith("?{r}#"));
        assertThat(p.output().get(1).toString(), startsWith("?{r}#"));
        assertThat(p.output().get(2).toString(), startsWith("?{r}#"));
        assertNotEquals(p.output().get(1).id(), p.output().get(2).id());
    }

    public void testSameAliasForParamAndField() {
        PhysicalPlan p = optimizeAndPlan(parameterizedSql("SELECT ?, int as \"?\" FROM test",
                new SqlTypedParamValue("integer", 100)));
        assertThat(p.output(), everyItem(instanceOf(ReferenceAttribute.class)));
        assertThat(p.output().get(0).toString(), startsWith("?{r}#"));
        assertThat(p.output().get(1).toString(), startsWith("?{r}#"));
        assertNotEquals(p.output().get(0).id(), p.output().get(1).id());
    }

    public void testSameAliasOnSameField() {
        PhysicalPlan p = optimizeAndPlan(parameterizedSql("SELECT int as \"int\", int as \"int\" FROM test"));
        assertThat(p.output(), everyItem(instanceOf(ReferenceAttribute.class)));
        assertThat(p.output().get(0).toString(), startsWith("int{r}#"));
        assertThat(p.output().get(1).toString(), startsWith("int{r}#"));
    }

    public void testFoldingWithMixedParamsWithoutAlias() {
        PhysicalPlan p = optimizeAndPlan(parameterizedSql("SELECT ?, ? FROM test",
                new SqlTypedParamValue("integer", 100),
                new SqlTypedParamValue("text", "200")));
        assertThat(p.output(), everyItem(instanceOf(ReferenceAttribute.class)));
        assertThat(p.output().get(0).toString(), startsWith("?{r}#"));
        assertThat(p.output().get(1).toString(), startsWith("?{r}#"));
    }

    public void testSameExpressionWithoutAlias() {
        PhysicalPlan physicalPlan = optimizeAndPlan("SELECT 100, 100 FROM test");
        assertEquals(EsQueryExec.class, physicalPlan.getClass());
        EsQueryExec eqe = (EsQueryExec) physicalPlan;
        assertEquals(2, eqe.output().size());
        assertThat(eqe.output().get(0).toString(), startsWith("100{r}#"));
        assertThat(eqe.output().get(1).toString(), startsWith("100{r}#"));
        // these two should be semantically different reference attributes
        assertNotEquals(eqe.output().get(0).id(), eqe.output().get(1).id());
    }

    public void testInOutOfRangeValues() {
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class,
                () -> optimizeAndPlan("SELECT int FROM test WHERE int IN (1, 2, 3, " + Long.MAX_VALUE + ", 5, 6, 7)"));
        assertThat(ex.getMessage(), is("[" + Long.MAX_VALUE + "] out of [integer] range"));
    }

    public void testInInRangeValues() {
        TestContext testContext = new TestContext("mapping-numeric.json");
        PhysicalPlan p = testContext.optimizeAndPlan("SELECT long FROM test WHERE long IN (1, 2, 3, " + Long.MAX_VALUE + ", 5, 6, 7)");
        assertEquals(EsQueryExec.class, p.getClass());
    }

    // Datetime
    ///////////
    public void testTermEqualityForDateWithLiteralDate() {
        ZoneId zoneId = randomZone();
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date = CAST('2019-08-08T12:34:56' AS DATETIME)", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals(asStringInZone("2019-08-08T12:34:56.000", zoneId), rq.upper());
        assertEquals(asStringInZone("2019-08-08T12:34:56.000", zoneId), rq.lower());
        assertTrue(rq.includeLower());
        assertTrue(rq.includeUpper());
        assertEquals(DATE_FORMAT, rq.format());
        assertEquals(zoneId, rq.zoneId());
    }

    private String asStringInZone(String dateString, ZoneId zoneId) {
        return org.elasticsearch.xpack.ql.type.DateUtils.toString(asDateTime(dateString, zoneId));
    }

    public void testTermEqualityForDateWithLiteralTime() {
        ZoneId zoneId = randomZone();
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date = CAST('12:34:56' AS TIME)", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals("12:34:56.000", rq.upper());
        assertEquals("12:34:56.000", rq.lower());
        assertTrue(rq.includeLower());
        assertTrue(rq.includeUpper());
        assertEquals(TIME_FORMAT, rq.format());
        assertEquals(zoneId, rq.zoneId());
    }

    public void testDateRange() {
        ZoneId zoneId = randomZone();
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > 1969-05-13", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals(1951, rq.lower());
        assertEquals(zoneId, rq.zoneId());
    }

    public void testDateRangeLiteral() {
        ZoneId zoneId = randomZone();
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > '1969-05-13'", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals("1969-05-13", rq.lower());
        assertEquals(zoneId, rq.zoneId());
    }

    public void testDateRangeCast() {
        ZoneId zoneId = randomZone();
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > CAST('1969-05-13T12:34:56Z' AS DATETIME)", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals("1969-05-13T12:34:56.000Z", rq.lower());
        assertEquals(zoneId, rq.zoneId());
    }

    public void testDateRangeWithCurrentTimestamp() {
        Integer nanoPrecision = randomPrecision();
        testDateRangeWithCurrentFunctions(
                functionWithPrecision("CURRENT_TIMESTAMP", nanoPrecision),
                DATE_FORMAT,
                nanoPrecision,
                TEST_CFG.now()
        );
        testDateRangeWithCurrentFunctionsAndRangeOptimization(
                functionWithPrecision("CURRENT_TIMESTAMP", nanoPrecision),
                DATE_FORMAT,
                nanoPrecision,
                TEST_CFG.now().minusDays(1L).minusSeconds(1L),
                TEST_CFG.now().plusDays(1L).plusSeconds(1L)
        );
    }

    public void testDateRangeWithCurrentDate() {
        testDateRangeWithCurrentFunctions("CURRENT_DATE()", DATE_FORMAT, null, DateUtils.asDateOnly(TEST_CFG.now()));
        testDateRangeWithCurrentFunctionsAndRangeOptimization(
                "CURRENT_DATE()",
                DATE_FORMAT,
                null,
                DateUtils.asDateOnly(TEST_CFG.now().minusDays(1L)).minusSeconds(1),
                DateUtils.asDateOnly(TEST_CFG.now().plusDays(1L)).plusSeconds(1)
        );
    }

    public void testDateRangeWithToday() {
        testDateRangeWithCurrentFunctions("TODAY()", DATE_FORMAT, null, DateUtils.asDateOnly(TEST_CFG.now()));
        testDateRangeWithCurrentFunctionsAndRangeOptimization(
                "TODAY()",
                DATE_FORMAT,
                null,
                DateUtils.asDateOnly(TEST_CFG.now().minusDays(1L)).minusSeconds(1),
                DateUtils.asDateOnly(TEST_CFG.now().plusDays(1L)).plusSeconds(1)
        );
    }

    public void testDateRangeWithNow() {
        Integer nanoPrecision = randomPrecision();
        testDateRangeWithCurrentFunctions(
                functionWithPrecision("NOW", nanoPrecision),
                DATE_FORMAT,
                nanoPrecision,
                TEST_CFG.now()
        );
        testDateRangeWithCurrentFunctionsAndRangeOptimization(
                functionWithPrecision("NOW", nanoPrecision),
                DATE_FORMAT,
                nanoPrecision,
                TEST_CFG.now().minusDays(1L).minusSeconds(1L),
                TEST_CFG.now().plusDays(1L).plusSeconds(1L)
        );
    }

    public void testDateRangeWithCurrentTime() {
        Integer nanoPrecision = randomPrecision();
        testDateRangeWithCurrentFunctions(
                functionWithPrecision("CURRENT_TIME", nanoPrecision),
                TIME_FORMAT,
                nanoPrecision,
                TEST_CFG.now()
        );
        testDateRangeWithCurrentFunctionsAndRangeOptimization(
                functionWithPrecision("CURRENT_TIME", nanoPrecision),
                TIME_FORMAT,
                nanoPrecision,
                TEST_CFG.now().minusDays(1L).minusSeconds(1L),
                TEST_CFG.now().plusDays(1L).plusSeconds(1L)
        );
    }

    private Integer randomPrecision() {
        return randomFrom(new Integer[] {null, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    }

    private String functionWithPrecision(String function, Integer precision) {
        return function + "(" + (precision == null ? "" : precision.toString()) + ")";
    }

    private void testDateRangeWithCurrentFunctions(String function, String pattern, Integer nanoPrecision, ZonedDateTime now) {
        String operator = randomFrom(">", ">=", "<", "<=", "=", "!=");
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date" + operator + function);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        RangeQuery rq;

        if (operator.equals("!=")) {
            assertTrue(query instanceof NotQuery);
            NotQuery nq = (NotQuery) query;
            assertTrue(nq.child() instanceof RangeQuery);
            rq = (RangeQuery) nq.child();
        } else {
            assertTrue(query instanceof RangeQuery);
            rq = (RangeQuery) query;
        }
        assertEquals("date", rq.field());

        if (operator.contains("<") || operator.equals("=") || operator.equals("!=")) {
            assertEquals(DateFormatter.forPattern(pattern).format(now.withNano(DateUtils.getNanoPrecision(
                    nanoPrecision == null ? null : literal(nanoPrecision), now.getNano()))), rq.upper());
        }
        if (operator.contains(">") || operator.equals("=") || operator.equals("!=")) {
            assertEquals(DateFormatter.forPattern(pattern).format(now.withNano(DateUtils.getNanoPrecision(
                    nanoPrecision == null ? null : literal(nanoPrecision), now.getNano()))), rq.lower());
        }

        assertEquals(operator.equals("=") || operator.equals("!=") || operator.equals("<="), rq.includeUpper());
        assertEquals(operator.equals("=") || operator.equals("!=") || operator.equals(">="), rq.includeLower());
        assertEquals(pattern, rq.format());
    }

    private void testDateRangeWithCurrentFunctionsAndRangeOptimization(
            String function,
            String pattern,
            Integer nanoPrecision,
            ZonedDateTime lowerValue,
            ZonedDateTime upperValue
    ) {
        String lowerOperator = randomFrom("<", "<=");
        String upperOperator = randomFrom(">", ">=");
        // use both date-only interval (1 DAY) and time-only interval (1 second) to cover CURRENT_TIMESTAMP and TODAY scenarios
        String interval = "(INTERVAL 1 DAY + INTERVAL 1 SECOND)";

        PhysicalPlan p = optimizeAndPlan("SELECT some.string FROM test WHERE date" + lowerOperator + function + " + " + interval
                + " AND date " + upperOperator + function + " - " + interval);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(1, eqe.output().size());
        assertEquals("test.some.string", eqe.output().get(0).qualifiedName());
        assertEquals(TEXT, eqe.output().get(0).dataType());

        Query query = eqe.queryContainer().query();
        // the range queries optimization should create a single "range" query with "from" and "to" populated with the values
        // in the two branches of the AND condition
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());

        assertEquals(DateFormatter.forPattern(pattern)
                .format(upperValue.withNano(DateUtils.getNanoPrecision(
                        nanoPrecision == null ? null : literal(nanoPrecision), upperValue.getNano()))), rq.upper());
        assertEquals(DateFormatter.forPattern(pattern)
                .format(lowerValue.withNano(DateUtils.getNanoPrecision(
                        nanoPrecision == null ? null : literal(nanoPrecision), lowerValue.getNano()))), rq.lower());

        assertEquals(lowerOperator.equals("<="), rq.includeUpper());
        assertEquals(upperOperator.equals(">="), rq.includeLower());
        assertEquals(pattern, rq.format());
    }

    public void testDateRangeWithESDateMath() {
        ZoneId zoneId = randomZone();
        String operator = randomFrom(">", ">=", "<", "<=", "=", "!=");
        String dateMath = randomFrom("now", "now/d", "now/h", "now-2h", "now+2h", "now-5d", "now+5d");
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date" + operator + "'" + dateMath + "'", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);
        Query query = translation.query;

        if ("=".equals(operator) || "!=".equals(operator)) {
            TermQuery tq;
            if ("=".equals(operator)) {
                assertTrue(query instanceof TermQuery);
                tq = (TermQuery) query;
            } else {
                assertTrue(query instanceof NotQuery);
                NotQuery nq = (NotQuery) query;
                assertTrue(nq.child() instanceof TermQuery);
                tq = (TermQuery) nq.child();
            }
            assertEquals("date", tq.term());
        } else {
            assertTrue(query instanceof RangeQuery);
            RangeQuery rq = (RangeQuery) query;
            assertEquals("date", rq.field());

            if (operator.contains("<")) {
                assertEquals(dateMath, rq.upper());
            }
            if (operator.contains(">")) {
                assertEquals(dateMath, rq.lower());
            }

            assertEquals("<=".equals(operator), rq.includeUpper());
            assertEquals(">=".equals(operator), rq.includeLower());
            assertNull(rq.format());
            assertEquals(zoneId, rq.zoneId());
        }
    }

    public void testInExpressionWhereClauseDatetime() {
        ZoneId zoneId = randomZone();
        String[] dates = {"2002-02-02T02:02:02.222Z", "2003-03-03T03:03:03.333Z"};
        LogicalPlan p = plan("SELECT * FROM test WHERE date IN ('" + dates[0] + "'::datetime, '" + dates[1] + "'::datetime)", zoneId);
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = translate(condition);

        Query query = translation.query;
        assertTrue(query instanceof BoolQuery);
        BoolQuery bq = (BoolQuery) query;
        assertFalse(bq.isAnd());
        assertTrue(bq.left() instanceof RangeQuery);
        assertTrue(bq.right() instanceof RangeQuery);
        List<Tuple<String, RangeQuery>> tuples = asList(new Tuple<>(dates[0], (RangeQuery)bq.left()),
                new Tuple<>(dates[1], (RangeQuery) bq.right()));

        for (Tuple<String, RangeQuery> t: tuples) {
            String date = t.v1();
            RangeQuery rq = t.v2();

            assertEquals("date", rq.field());
            assertEquals(date, rq.upper().toString());
            assertEquals(date, rq.lower().toString());
            assertEquals(zoneId, rq.zoneId());
            assertTrue(rq.includeLower());
            assertTrue(rq.includeUpper());
            assertEquals(DATE_FORMAT, rq.format());
        }
    }

    public void testChronoFieldBasedDateTimeFunctionsWithMathIntervalAndGroupBy() {
        DateTimeExtractor randomFunction = randomValueOtherThan(DateTimeExtractor.YEAR, () -> randomFrom(DateTimeExtractor.values()));
        PhysicalPlan p = optimizeAndPlan(
                "SELECT "
                        + randomFunction.name()
                        + "(date + INTERVAL 1 YEAR) FROM test GROUP BY " + randomFunction.name() + "(date + INTERVAL 1 YEAR)");
        assertESQuery(
            p,
            containsString(
                "{\"terms\":{\"script\":{\"source\":\"InternalSqlScriptUtils.dateTimeExtract("
                    + "InternalSqlScriptUtils.add(InternalQlScriptUtils.docValue(doc,params.v0),"
                    + "InternalSqlScriptUtils.intervalYearMonth(params.v1,params.v2)),params.v3,params.v4)\","
                    + "\"lang\":\"painless\",\"params\":{\"v0\":\"date\",\"v1\":\"P1Y\",\"v2\":\"INTERVAL_YEAR\","
                    + "\"v3\":\"Z\",\"v4\":\""
                    + randomFunction.name()
                    + "\"}},\"missing_bucket\":true,"
                    + "\"value_type\":\"long\",\"order\":\"asc\"}}}]}}}}"
            )
        );
    }

    public void testDateTimeFunctionsWithMathIntervalAndGroupBy() {
        String[] functions = new String[] {"DAY_NAME", "MONTH_NAME", "DAY_OF_WEEK", "WEEK_OF_YEAR", "QUARTER"};
        String[] scriptMethods = new String[] {"dayName", "monthName", "dayOfWeek", "weekOfYear", "quarter"};
        int pos = randomIntBetween(0, functions.length - 1);
        PhysicalPlan p = optimizeAndPlan(
                "SELECT "
                        + functions[pos]
                        + "(date + INTERVAL 1 YEAR) FROM test GROUP BY " + functions[pos] + "(date + INTERVAL 1 YEAR)");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertThat(eqe.queryContainer().toString().replaceAll("\\s+", ""), containsString(
                "{\"terms\":{\"script\":{\"source\":\"InternalSqlScriptUtils." + scriptMethods[pos]
                        + "(InternalSqlScriptUtils.add(InternalQlScriptUtils.docValue(doc,params.v0),"
                        + "InternalSqlScriptUtils.intervalYearMonth(params.v1,params.v2)),params.v3)\",\"lang\":\"painless\","
                        + "\"params\":{\"v0\":\"date\",\"v1\":\"P1Y\",\"v2\":\"INTERVAL_YEAR\",\"v3\":\"Z\"}},\"missing_bucket\":true,"));
    }

    // Like/RLike/StartsWith
    ////////////////////////
    public void testDifferentLikeAndNotLikePatterns() {
        LogicalPlan p = plan("SELECT keyword k FROM test WHERE k LIKE 'X%' AND k NOT LIKE 'Y%'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);

        Expression condition = ((Filter) p).condition();
        QueryTranslation qt = translate(condition);
        assertEquals(BoolQuery.class, qt.query.getClass());
        BoolQuery bq = ((BoolQuery) qt.query);
        assertTrue(bq.isAnd());
        assertTrue(bq.left() instanceof WildcardQuery);
        assertTrue(bq.right() instanceof NotQuery);

        NotQuery nq = (NotQuery) bq.right();
        assertTrue(nq.child() instanceof WildcardQuery);
        WildcardQuery lqsq = (WildcardQuery) bq.left();
        WildcardQuery rqsq = (WildcardQuery) nq.child();

        assertEquals("X*", lqsq.query());
        assertEquals("keyword", lqsq.field());
        assertEquals("Y*", rqsq.query());
        assertEquals("keyword", rqsq.field());
    }

    public void testRLikePatterns() {
        String[] patterns = new String[] {"(...)+", "abab(ab)?", "(ab){1,2}", "(ab){3}", "aabb|bbaa", "a+b+|b+a+", "aa(cc|bb)",
                "a{4,6}b{4,6}", ".{3}.{3}", "aaa*bbb*", "a+.+", "a.c.e", "[^abc\\-]"};
        for (int i = 0; i < 5; i++) {
            assertDifferentRLikeAndNotRLikePatterns(randomFrom(patterns), randomFrom(patterns));
        }
    }

    private void assertDifferentRLikeAndNotRLikePatterns(String firstPattern, String secondPattern) {
        LogicalPlan p = plan("SELECT keyword k FROM test WHERE k RLIKE '" + firstPattern + "' AND k NOT RLIKE '" + secondPattern + "'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);

        Expression condition = ((Filter) p).condition();
        QueryTranslation qt = translate(condition);
        assertEquals(BoolQuery.class, qt.query.getClass());
        BoolQuery bq = ((BoolQuery) qt.query);
        assertTrue(bq.isAnd());
        assertTrue(bq.left() instanceof RegexQuery);
        assertTrue(bq.right() instanceof NotQuery);

        NotQuery nq = (NotQuery) bq.right();
        assertTrue(nq.child() instanceof RegexQuery);
        RegexQuery lqsq = (RegexQuery) bq.left();
        RegexQuery rqsq = (RegexQuery) nq.child();

        assertEquals(firstPattern, lqsq.regex());
        assertEquals("keyword", lqsq.field());
        assertEquals(secondPattern, rqsq.regex());
        assertEquals("keyword", rqsq.field());
    }

    public void testStartsWithUsesPrefixQuery() {
        LogicalPlan p = plan("SELECT keyword FROM test WHERE STARTS_WITH(keyword, 'x') OR STARTS_WITH(keyword, 'y')");

        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());

        QueryTranslation translation = translate(condition);
        assertTrue(translation.query instanceof BoolQuery);
        BoolQuery bq = (BoolQuery) translation.query;

        assertFalse(bq.isAnd());
        assertTrue(bq.left() instanceof PrefixQuery);
        assertTrue(bq.right() instanceof PrefixQuery);

        PrefixQuery pqr = (PrefixQuery) bq.right();
        assertEquals("keyword", pqr.field());
        assertEquals("y", pqr.query());

        PrefixQuery pql = (PrefixQuery) bq.left();
        assertEquals("keyword", pql.field());
        assertEquals("x", pql.query());
    }

    public void testStartsWithUsesPrefixQueryAndScript() {
        LogicalPlan p = plan("SELECT keyword FROM test WHERE STARTS_WITH(keyword, 'x') AND STARTS_WITH(keyword, 'xy') "
            + "AND STARTS_WITH(LCASE(keyword), 'xyz')");

        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());

        QueryTranslation translation = translate(condition);
        assertTrue(translation.query instanceof BoolQuery);
        BoolQuery bq = (BoolQuery) translation.query;

        assertTrue(bq.isAnd());
        assertTrue(bq.left() instanceof BoolQuery);
        assertTrue(bq.right() instanceof ScriptQuery);

        BoolQuery bbq = (BoolQuery) bq.left();
        assertTrue(bbq.isAnd());
        PrefixQuery pqr = (PrefixQuery) bbq.right();
        assertEquals("keyword", pqr.field());
        assertEquals("xy", pqr.query());

        PrefixQuery pql = (PrefixQuery) bbq.left();
        assertEquals("keyword", pql.field());
        assertEquals("x", pql.query());

        ScriptQuery sq = (ScriptQuery) bq.right();
        assertEquals("InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.startsWith("
                + "InternalSqlScriptUtils.lcase(InternalQlScriptUtils.docValue(doc,params.v0)), "
                + "params.v1, params.v2))",
            sq.script().toString());
        assertEquals("[{v=keyword}, {v=xyz}, {v=false}]", sq.script().params().toString());
    }

    @SuppressWarnings("unchecked")
    public void testTrimWhereClause() {
        Class<? extends UnaryStringFunction> trimFunction = randomFrom(Trim.class, LTrim.class, RTrim.class);
        String trimFunctionName = trimFunction.getSimpleName().toUpperCase(Locale.ROOT);
        LogicalPlan p = plan("SELECT " + trimFunctionName + "(keyword) trimmed FROM test WHERE " + trimFunctionName + "(keyword) = 'foo'");

        assertESQuery(p,
            containsString("InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalSqlScriptUtils." +
                trimFunctionName.toLowerCase(Locale.ROOT) + "(InternalQlScriptUtils.docValue(doc,params.v0)),params.v1))"),
            containsString("\"params\":{\"v0\":\"keyword\",\"v1\":\"foo\"}")
        );
    }

    @SuppressWarnings("unchecked")
    public void testTrimGroupBy() {
        Class<? extends UnaryStringFunction> trimFunction = randomFrom(Trim.class, LTrim.class, RTrim.class);
        String trimFunctionName = trimFunction.getSimpleName().toUpperCase(Locale.ROOT);
        LogicalPlan p = plan("SELECT " + trimFunctionName + "(keyword) trimmed, count(*) FROM test GROUP BY " +
            trimFunctionName + "(keyword)");

        assertEquals(Aggregate.class, p.getClass());
        Aggregate agg = (Aggregate) p;
        assertEquals(1, agg.groupings().size());
        assertEquals(2, agg.aggregates().size());
        assertEquals(trimFunction, agg.groupings().get(0).getClass());
        assertEquals(trimFunction, ((Alias) agg.aggregates().get(0)).child().getClass());
        assertEquals(Count.class,((Alias) agg.aggregates().get(1)).child().getClass());

        UnaryStringFunction trim = (UnaryStringFunction) agg.groupings().get(0);
        assertEquals(1, trim.children().size());

        GroupingContext groupingContext = QueryFolder.FoldAggregate.groupBy(agg.groupings());
        assertNotNull(groupingContext);
        assertESQuery(
           p,
            containsString(
                "InternalSqlScriptUtils." + trimFunctionName.toLowerCase(Locale.ROOT) + "(InternalQlScriptUtils.docValue(doc,params.v0))"
            ),
            containsString("\"params\":{\"v0\"=\"keyword\"}")
        );
    }

    // Histograms
    /////////////
    public void testGroupByDateHistogram() {
        LogicalPlan p = plan("SELECT MAX(int) FROM test GROUP BY HISTOGRAM(int, 1000)");
        assertTrue(p instanceof Aggregate);
        Aggregate a = (Aggregate) p;
        List<Expression> groupings = a.groupings();
        assertEquals(1, groupings.size());
        Expression exp = groupings.get(0);
        assertEquals(Histogram.class, exp.getClass());
        Histogram h = (Histogram) exp;
        assertEquals(1000, h.interval().fold());
        Expression field = h.field();
        assertEquals(FieldAttribute.class, field.getClass());
        assertEquals(INTEGER, field.dataType());
    }

    public void testGroupByHistogram() {
        LogicalPlan p = plan("SELECT MAX(int) FROM test GROUP BY HISTOGRAM(date, INTERVAL 2 YEARS)");
        assertTrue(p instanceof Aggregate);
        Aggregate a = (Aggregate) p;
        List<Expression> groupings = a.groupings();
        assertEquals(1, groupings.size());
        Expression exp = groupings.get(0);
        assertEquals(Histogram.class, exp.getClass());
        Histogram h = (Histogram) exp;
        assertEquals("+2-0", h.interval().fold().toString());
        Expression field = h.field();
        assertEquals(FieldAttribute.class, field.getClass());
        assertEquals(DATETIME, field.dataType());
    }

    public void testGroupByHistogramWithDate() {
        LogicalPlan p = plan("SELECT MAX(int) FROM test GROUP BY HISTOGRAM(CAST(date AS DATE), INTERVAL 2 MONTHS)");
        assertTrue(p instanceof Aggregate);
        Aggregate a = (Aggregate) p;
        List<Expression> groupings = a.groupings();
        assertEquals(1, groupings.size());
        Expression exp = groupings.get(0);
        assertEquals(Histogram.class, exp.getClass());
        Histogram h = (Histogram) exp;
        assertEquals("+0-2", h.interval().fold().toString());
        Expression field = h.field();
        assertEquals(Cast.class, field.getClass());
        assertEquals(DATE, field.dataType());
    }

    public void testGroupByHistogramWithDateAndSmallInterval() {
        PhysicalPlan p = optimizeAndPlan("SELECT MAX(int) FROM test GROUP BY " +
            "HISTOGRAM(CAST(date AS DATE), INTERVAL 5 MINUTES)");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(1, eqe.queryContainer().aggs().groups().size());
        assertEquals(GroupByDateHistogram.class, eqe.queryContainer().aggs().groups().get(0).getClass());
        assertEquals(86400000L, ((GroupByDateHistogram) eqe.queryContainer().aggs().groups().get(0)).fixedInterval());
    }

    public void testGroupByHistogramWithDateTruncateIntervalToDayMultiples() {
        {
            PhysicalPlan p = optimizeAndPlan("SELECT MAX(int) FROM test GROUP BY " +
                "HISTOGRAM(CAST(date AS DATE), INTERVAL '2 3:04' DAY TO MINUTE)");
            assertEquals(EsQueryExec.class, p.getClass());
            EsQueryExec eqe = (EsQueryExec) p;
            assertEquals(1, eqe.queryContainer().aggs().groups().size());
            assertEquals(GroupByDateHistogram.class, eqe.queryContainer().aggs().groups().get(0).getClass());
            assertEquals(172800000L, ((GroupByDateHistogram) eqe.queryContainer().aggs().groups().get(0)).fixedInterval());
        }
        {
            PhysicalPlan p = optimizeAndPlan("SELECT MAX(int) FROM test GROUP BY " +
                "HISTOGRAM(CAST(date AS DATE), INTERVAL 4409 MINUTES)");
            assertEquals(EsQueryExec.class, p.getClass());
            EsQueryExec eqe = (EsQueryExec) p;
            assertEquals(1, eqe.queryContainer().aggs().groups().size());
            assertEquals(GroupByDateHistogram.class, eqe.queryContainer().aggs().groups().get(0).getClass());
            assertEquals(259200000L, ((GroupByDateHistogram) eqe.queryContainer().aggs().groups().get(0)).fixedInterval());
        }
    }

    // Count
    ///////////
    public void testAllCountVariantsWithHavingGenerateCorrectAggregations() {
        PhysicalPlan p = optimizeAndPlan("SELECT AVG(int), COUNT(keyword) ln, COUNT(distinct keyword) dln, COUNT(some.dotted.field) fn,"
                + "COUNT(distinct some.dotted.field) dfn, COUNT(*) ccc FROM test GROUP BY bool "
                + "HAVING dln > 3 AND ln > 32 AND dfn > 1 AND fn > 2 AND ccc > 5 AND AVG(int) > 50000");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(6, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("AVG(int){r}"));
        assertThat(ee.output().get(1).toString(), startsWith("ln{r}"));
        assertThat(ee.output().get(2).toString(), startsWith("dln{r}"));
        assertThat(ee.output().get(3).toString(), startsWith("fn{r}"));
        assertThat(ee.output().get(4).toString(), startsWith("dfn{r}"));
        assertThat(ee.output().get(5).toString(), startsWith("ccc{r}"));

        Collection<AggregationBuilder> subAggs = ee.queryContainer().aggs().asAggBuilder().getSubAggregations();
        assertEquals(5, subAggs.size());
        assertTrue(subAggs.toArray()[0] instanceof AvgAggregationBuilder);
        assertTrue(subAggs.toArray()[1] instanceof FilterAggregationBuilder);
        assertTrue(subAggs.toArray()[2] instanceof CardinalityAggregationBuilder);
        assertTrue(subAggs.toArray()[3] instanceof FilterAggregationBuilder);
        assertTrue(subAggs.toArray()[4] instanceof CardinalityAggregationBuilder);

        AvgAggregationBuilder avgInt = (AvgAggregationBuilder) subAggs.toArray()[0];
        assertEquals("int", avgInt.field());

        FilterAggregationBuilder existsKeyword = (FilterAggregationBuilder) subAggs.toArray()[1];
        assertTrue(existsKeyword.getFilter() instanceof ExistsQueryBuilder);
        assertEquals("keyword", ((ExistsQueryBuilder) existsKeyword.getFilter()).fieldName());

        CardinalityAggregationBuilder cardinalityKeyword = (CardinalityAggregationBuilder) subAggs.toArray()[2];
        assertEquals("keyword", cardinalityKeyword.field());

        FilterAggregationBuilder existsDottedField = (FilterAggregationBuilder) subAggs.toArray()[3];
        assertTrue(existsDottedField.getFilter() instanceof ExistsQueryBuilder);
        assertEquals("some.dotted.field", ((ExistsQueryBuilder) existsDottedField.getFilter()).fieldName());

        CardinalityAggregationBuilder cardinalityDottedField = (CardinalityAggregationBuilder) subAggs.toArray()[4];
        assertEquals("some.dotted.field", cardinalityDottedField.field());

        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
                endsWith("{\"buckets_path\":{"
                        + "\"a0\":\"" + cardinalityKeyword.getName() + "\","
                        + "\"a1\":\"" + existsKeyword.getName() + "._count\","
                        + "\"a2\":\"" + cardinalityDottedField.getName() + "\","
                        + "\"a3\":\"" + existsDottedField.getName() + "._count\","
                        + "\"a4\":\"_count\","
                        + "\"a5\":\"" + avgInt.getName() + "\"},"
                        + "\"script\":{\"source\":\""
                        + "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.and("
                        +   "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.and("
                        +     "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.and("
                        +       "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.and("
                        +         "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.and("
                        +           "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a0,params.v0)),"
                        +           "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a1,params.v1)))),"
                        +         "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a2,params.v2)))),"
                        +       "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a3,params.v3)))),"
                        +     "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a4,params.v4)))),"
                        +   "InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(params.a5,params.v5))))\","
                        + "\"lang\":\"painless\",\"params\":{\"v0\":3,\"v1\":32,\"v2\":1,\"v3\":2,\"v4\":5,\"v5\":50000}},"
                        + "\"gap_policy\":\"skip\"}}}}}"));
    }


    // Stats/Extended Stats
    ///////////////////////
    public void testExtendedStatsAggsStddevAndVar() {
        final Map<String, String> metricToAgg =  Map.of(
                "STDDEV_POP", "std_deviation",
                "STDDEV_SAMP", "std_deviation_sampling",
                "VAR_POP", "variance",
                "VAR_SAMP", "variance_sampling"
        );
        for (String funcName: metricToAgg.keySet()) {
            PhysicalPlan p = optimizeAndPlan("SELECT " + funcName + "(int) FROM test");
            assertEquals(EsQueryExec.class, p.getClass());
            EsQueryExec eqe = (EsQueryExec) p;
            assertEquals(1, eqe.output().size());

            assertEquals(funcName + "(int)", eqe.output().get(0).qualifiedName());
            assertEquals(DOUBLE, eqe.output().get(0).dataType());

            FieldExtraction fe = eqe.queryContainer().fields().get(0).v1();
            assertEquals(MetricAggRef.class, fe.getClass());
            assertEquals(((MetricAggRef) fe).property(), metricToAgg.get(funcName));

            String aggName = eqe.queryContainer().aggs().asAggBuilder().getSubAggregations().iterator().next().getName();
            assertESQuery(
                    p,
                    endsWith("\"aggregations\":{\"" + aggName + "\":{\"extended_stats\":{\"field\":\"int\",\"sigma\":2.0}}}}}}")
            );
        }

    }

    public void testScriptsInsideAggregateFunctionsExtendedStats() {
        for (FunctionDefinition fd : defaultTestContext.sqlFunctionRegistry.listFunctions()) {
            if (ExtendedStatsEnclosed.class.isAssignableFrom(fd.clazz())) {
                String aggFunction = fd.name() + "(ABS((int * 10) / 3) + 1)";
                PhysicalPlan p = optimizeAndPlan("SELECT " + aggFunction + " FROM test");
                assertESQuery(
                        p,
                        containsString(
                                "{\"extended_stats\":{\"script\":{\"source\":\"InternalSqlScriptUtils.add(InternalSqlScriptUtils.abs("
                                        + "InternalSqlScriptUtils.div(InternalSqlScriptUtils.mul(InternalQlScriptUtils.docValue("
                                        + "doc,params.v0),params.v1),params.v2)),params.v3)\",\"lang\":\"painless\",\"params\":{"
                                        + "\"v0\":\"int\",\"v1\":10,\"v2\":3,\"v3\":1}}"
                        )
                );
            }
        }
    }

    @SuppressWarnings({"rawtypes"})
    public void testPercentileMethodParametersSameAsDefault() {
        BiConsumer<String, Function<AbstractPercentilesAggregationBuilder, double[]>> test = (fnName, pctOrValFn) -> {
            final int fieldCount = 5;
            final String sql = ("SELECT " +
                // 0-3: these all should fold into the same aggregation
                "   PERCENTILE(int, 50, 'tdigest', 79.8 + 20.2), " +
                "   PERCENTILE(int, 40 + 10, 'tdigest', null), " +
                "   PERCENTILE(int, 50, 'tdigest'), " +
                "   PERCENTILE(int, 50), " +
                // 4: this has a different method parameter
                // just to make sure we don't fold everything to default
                "   PERCENTILE(int, 50, 'tdigest', 22) "
                + "FROM test").replaceAll("PERCENTILE", fnName);

            List<AbstractPercentilesAggregationBuilder> aggs = percentilesAggsByField(optimizeAndPlan(sql), fieldCount);

            // 0-3
            assertEquals(aggs.get(0), aggs.get(1));
            assertEquals(aggs.get(0), aggs.get(2));
            assertEquals(aggs.get(0), aggs.get(3));
            assertEquals(new PercentilesConfig.TDigest(), aggs.get(0).percentilesConfig());
            assertArrayEquals(new double[] { 50 }, pctOrValFn.apply(aggs.get(0)), 0);

            // 4
            assertEquals(new PercentilesConfig.TDigest(22), aggs.get(4).percentilesConfig());
            assertArrayEquals(new double[] { 50 }, pctOrValFn.apply(aggs.get(4)), 0);
        };

        test.accept("PERCENTILE", p -> ((PercentilesAggregationBuilder)p).percentiles());
        test.accept("PERCENTILE_RANK", p -> ((PercentileRanksAggregationBuilder)p).values());
    }

    @SuppressWarnings({"rawtypes"})
    public void testPercentileOptimization() {
        BiConsumer<String, Function<AbstractPercentilesAggregationBuilder, double[]>> test = (fnName, pctOrValFn) -> {
            final int fieldCount = 5;
            final String sql = ("SELECT " +
                // 0-1: fold into the same aggregation
                "   PERCENTILE(int, 50, 'tdigest'), " +
                "   PERCENTILE(int, 60, 'tdigest'), " +

                // 2-3: fold into one aggregation
                "   PERCENTILE(int, 50, 'hdr'), " +
                "   PERCENTILE(int, 60, 'hdr', 3), " +

                // 4: folds into a separate aggregation
                "   PERCENTILE(int, 60, 'hdr', 4)" +
                "FROM test").replaceAll("PERCENTILE", fnName);

            List<AbstractPercentilesAggregationBuilder> aggs = percentilesAggsByField(optimizeAndPlan(sql), fieldCount);

            // 0-1
            assertEquals(aggs.get(0), aggs.get(1));
            assertEquals(new PercentilesConfig.TDigest(), aggs.get(0).percentilesConfig());
            assertArrayEquals(new double[]{50, 60}, pctOrValFn.apply(aggs.get(0)), 0);

            // 2-3
            assertEquals(aggs.get(2), aggs.get(3));
            assertEquals(new PercentilesConfig.Hdr(), aggs.get(2).percentilesConfig());
            assertArrayEquals(new double[]{50, 60}, pctOrValFn.apply(aggs.get(2)), 0);

            // 4
            assertEquals(new PercentilesConfig.Hdr(4), aggs.get(4).percentilesConfig());
            assertArrayEquals(new double[]{60}, pctOrValFn.apply(aggs.get(4)), 0);
        };

        test.accept("PERCENTILE", p -> ((PercentilesAggregationBuilder)p).percentiles());
        test.accept("PERCENTILE_RANK", p -> ((PercentileRanksAggregationBuilder)p).values());
    }

    // Tests the workaround for the SUM(all zeros) = NULL issue raised in https://github.com/elastic/elasticsearch/issues/45251 and
    // should be removed as soon as root cause https://github.com/elastic/elasticsearch/issues/71582 is fixed and the sum aggregation
    // results can differentiate between SUM(all zeroes) and SUM(all nulls)
    public void testReplaceSumWithStats() {
        List<String> testCases = asList(
            "SELECT keyword, SUM(int) FROM test GROUP BY keyword",
            "SELECT SUM(int) FROM test",
            "SELECT * FROM (SELECT some.string, keyword, int FROM test) PIVOT (SUM(int) FOR keyword IN ('a', 'b'))");
        for (String testCase : testCases) {
            PhysicalPlan physicalPlan = optimizeAndPlan(testCase);
            assertEquals(EsQueryExec.class, physicalPlan.getClass());
            EsQueryExec eqe = (EsQueryExec) physicalPlan;
            assertThat(eqe.queryContainer().toString().replaceAll("\\s+", ""), containsString("{\"stats\":{\"field\":\"int\"}}"));
        }
    }

    @SuppressWarnings({"rawtypes"})
    private static List<AbstractPercentilesAggregationBuilder> percentilesAggsByField(PhysicalPlan p, int fieldCount) {
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        AggregationBuilder aggregationBuilder = ee.queryContainer().aggs().asAggBuilder();
        assertEquals(fieldCount, ee.output().size());
        assertEquals(ReferenceAttribute.class, ee.output().get(0).getClass());
        assertEquals(fieldCount, ee.queryContainer().fields().size());
        assertThat(fieldCount, greaterThanOrEqualTo(ee.queryContainer().aggs().asAggBuilder().getSubAggregations().size()));
        Map<String, AggregationBuilder> aggsByName =
                aggregationBuilder.getSubAggregations().stream().collect(Collectors.toMap(AggregationBuilder::getName, ab -> ab));
        return IntStream.range(0, fieldCount).mapToObj(i -> {
            String percentileAggName = ((MetricAggRef) ee.queryContainer().fields().get(i).v1()).name();
            return (AbstractPercentilesAggregationBuilder) aggsByName.get(percentileAggName);
        }).collect(Collectors.toList());
    }


    // Boolean Conditions
    /////////////////////
    public void testAddMissingEqualsToBoolField() {
        LogicalPlan p = plan("SELECT bool FROM test WHERE bool");
        assertTrue(p instanceof Project);

        p = ((Project) p).child();
        assertTrue(p instanceof Filter);

        Expression condition = ((Filter) p).condition();
        assertTrue(condition instanceof Equals);
        Equals eq = (Equals) condition;

        assertTrue(eq.left() instanceof FieldAttribute);
        assertEquals("bool", ((FieldAttribute) eq.left()).name());

        assertTrue(eq.right() instanceof Literal);
        assertEquals(TRUE, eq.right());
    }

    public void testAddMissingEqualsToNestedBoolField() {
        LogicalPlan p = plan("SELECT bool FROM test " +
            "WHERE int > 1 and (bool or int < 2) or (int = 3 and bool) or (int = 4 and bool = false) or bool");
        LogicalPlan expectedPlan = plan("SELECT bool FROM test " +
            "WHERE int > 1 and (bool = true or int < 2) or (int = 3 and bool = true) or (int = 4 and bool = false) or bool = true");

        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();

        Expression expectedCondition = ((Filter) ((Project) expectedPlan).child()).condition();

        List<Expression> expectedFields = expectedCondition.collect(x -> x instanceof FieldAttribute);
        Set<Expression> expectedBools = expectedFields.stream()
            .filter(x -> ((FieldAttribute) x).name().equals("bool")).collect(Collectors.toSet());
        assertEquals(1, expectedBools.size());
        Set<Expression> expectedInts = expectedFields.stream()
            .filter(x -> ((FieldAttribute) x).name().equals("int")).collect(Collectors.toSet());
        assertEquals(1, expectedInts.size());

        condition = condition
            .transformDown(FieldAttribute.class, x -> x.name().equals("bool") ? (FieldAttribute) expectedBools.toArray()[0] : x)
            .transformDown(FieldAttribute.class, x -> x.name().equals("int") ? (FieldAttribute) expectedInts.toArray()[0] : x);

        assertEquals(expectedCondition, condition);
    }

    // Subqueries
    /////////////////////
    public void testMultiLevelSubqueryWithoutRelation1() {
        PhysicalPlan p = optimizeAndPlan(
                "SELECT int FROM (" +
                "  SELECT int FROM (" +
                "    SELECT 1 AS int" +
                "  ) AS subq1" +
                ") AS subq2");
        assertThat(p, instanceOf(LocalExec.class));
        LocalExec le = (LocalExec) p;
        assertThat(le.executable(), instanceOf(SingletonExecutable.class));
        assertEquals(1, le.executable().output().size());
        assertEquals("int", le.executable().output().get(0).name());
    }

    public void testMultiLevelSubqueryWithoutRelation2() {
        PhysicalPlan p = optimizeAndPlan(
                "SELECT i, string FROM (" +
                "  SELECT * FROM (" +
                "    SELECT int as i, str AS string FROM (" +
                "      SELECT * FROM (" +
                "        SELECT int, s AS str FROM (" +
                "          SELECT 1 AS int, 'foo' AS s" +
                "        ) AS subq1" +
                "      )" +
                "    ) AS subq2" +
                "  ) AS subq3" +
                ")");
        assertThat(p, instanceOf(LocalExec.class));
        LocalExec le = (LocalExec) p;
        assertThat(le.executable(), instanceOf(SingletonExecutable.class));
        assertEquals(2, le.executable().output().size());
        assertEquals("i", le.executable().output().get(0).name());
        assertEquals("string", le.executable().output().get(1).name());
    }
}
