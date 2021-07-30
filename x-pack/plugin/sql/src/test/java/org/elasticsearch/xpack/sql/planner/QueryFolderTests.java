/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public class QueryFolderTests extends ESTestCase {

    private static SqlParser parser;
    private static Analyzer analyzer;
    private static Optimizer optimizer;
    private static Planner planner;

    @BeforeClass
    public static void init() {
        parser = new SqlParser();

        Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-multi-field-variation.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), getIndexResult, new Verifier(new Metrics()));
        optimizer = new Optimizer();
        planner = new Planner();
    }

    @AfterClass
    public static void destroy() {
        parser = null;
        analyzer = null;
    }

    private PhysicalPlan plan(String sql) {
        return planner.plan(optimizer.optimize(analyzer.analyze(parser.createStatement(sql), true)), true);
    }

    public void testFoldingToLocalExecWithProject() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndOrderBy() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndOrderByAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testLocalExecWithPrunedFilterWithFunction() {
        PhysicalPlan p = plan("SELECT E() FROM test WHERE PI() = 5");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){r}#"));
    }

    public void testLocalExecWithPrunedFilterWithFunctionAndAggregation() {
        PhysicalPlan p = plan("SELECT E() FROM test WHERE PI() = 5 GROUP BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){r}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndLimit() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{r}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndOrderBy() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 ORDER BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{r}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndOrderByAndLimit() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{r}#"));
    }

    public void testLocalExecWithoutFromClause() {
        PhysicalPlan p = plan("SELECT E(), 'foo', abs(10)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(3, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("'foo'{r}#"));
        assertThat(ee.output().get(2).toString(), startsWith("abs(10){r}#"));
    }

    public void testLocalExecWithoutFromClauseWithPrunedFilter() {
        PhysicalPlan p = plan("SELECT E() WHERE PI() = 5");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){r}#"));
    }

    public void testLocalExecWithCount() {
        PhysicalPlan p = plan("SELECT COUNT(10), COUNT(DISTINCT 20)" + randomOrderByAndLimit(2));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("COUNT(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("COUNT(DISTINCT 20){r}#"));
    }

    public void testLocalExecWithCountAndWhereFalseFilter() {
        PhysicalPlan p = plan("SELECT COUNT(10), COUNT(DISTINCT 20) WHERE 1 = 2" + randomOrderByAndLimit(2));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("COUNT(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("COUNT(DISTINCT 20){r}#"));
    }

    public void testLocalExecWithCountAndWhereTrueFilter() {
        PhysicalPlan p = plan("SELECT COUNT(10), COUNT(DISTINCT 20) WHERE 1 = 1" + randomOrderByAndLimit(2));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("COUNT(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("COUNT(DISTINCT 20){r}#"));
    }

    public void testLocalExecWithAggs() {
        PhysicalPlan p = plan("SELECT MIN(10), MAX(123), SUM(20), AVG(30)" + randomOrderByAndLimit(4));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(4, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("MIN(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("MAX(123){r}#"));
        assertThat(ee.output().get(2).toString(), startsWith("SUM(20){r}#"));
        assertThat(ee.output().get(3).toString(), startsWith("AVG(30){r}#"));
    }

    public void testLocalExecWithAggsAndWhereFalseFilter() {
        PhysicalPlan p = plan("SELECT MIN(10), MAX(123), SUM(20), AVG(30) WHERE 2 > 3" + randomOrderByAndLimit(4));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(4, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("MIN(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("MAX(123){r}#"));
        assertThat(ee.output().get(2).toString(), startsWith("SUM(20){r}#"));
        assertThat(ee.output().get(3).toString(), startsWith("AVG(30){r}#"));
    }

    public void testLocalExecWithAggsAndWhereTrueFilter() {
        PhysicalPlan p = plan("SELECT MIN(10), MAX(123), SUM(20), AVG(30) WHERE 1 = 1" + randomOrderByAndLimit(4));
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(4, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("MIN(10){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("MAX(123){r}#"));
        assertThat(ee.output().get(2).toString(), startsWith("SUM(20){r}#"));
        assertThat(ee.output().get(3).toString(), startsWith("AVG(30){r}#"));
    }

    public void testFoldingOfIsNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE (keyword IS NOT NULL) IS NULL");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec ee = (LocalExec) p;
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecBooleanAndNull_WhereClause() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int > 10 AND null AND true");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecBooleanAndNull_HavingClause() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) > 10 AND null");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){r}"));
    }

    public void testFoldingBooleanOrNull_WhereClause() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int > 10 OR null OR false");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals("{\"range\":{\"int\":{\"from\":10,\"to\":null,\"include_lower\":false,\"include_upper\":false,\"boost\":1.0}}}",
            ee.queryContainer().query().asBuilder().toString().replaceAll("\\s+", ""));
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingBooleanOrNull_HavingClause() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) > 10 OR null");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertTrue(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", "").contains(
                "\"script\":{\"source\":\"InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(" +
                    "InternalQlScriptUtils.nullSafeCastNumeric(params.a0,params.v0),params.v1))\"," +
                    "\"lang\":\"painless\",\"params\":{\"v0\":\"INTEGER\",\"v1\":10}},"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){r}"));
    }

    public void testFoldingOfIsNotNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE (keyword IS NULL) IS NOT NULL");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithNullFilter() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE null IN (1, 2)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProject_FoldableIn() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int IN (null, null)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProject_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY int LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectWithGroupBy_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test WHERE 1 = 2 GROUP BY keyword ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){r}"));
    }

    public void testFoldingToLocalExecWithProjectWithGroupBy_WithHaving_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING 1 = 2 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){r}"));
    }

    public void testGroupKeyTypes_Boolean() {
        PhysicalPlan p = plan("SELECT count(*), int > 10 AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalQlScriptUtils.gt(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\",\"v1\":10}},\"missing_bucket\":true," +
                "\"value_type\":\"boolean\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testGroupKeyTypes_Integer() {
        PhysicalPlan p = plan("SELECT count(*), int + 10 AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.add(InternalQlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\",\"v1\":10}},\"missing_bucket\":true," +
                "\"value_type\":\"long\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testGroupKeyTypes_Rational() {
        PhysicalPlan p = plan("SELECT count(*), sin(int) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.sin(InternalQlScriptUtils.docValue(doc,params.v0))\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\"}},\"missing_bucket\":true," +
                "\"value_type\":\"double\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testGroupKeyTypes_String() {
        PhysicalPlan p = plan("SELECT count(*), LCASE(keyword) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.lcase(InternalQlScriptUtils.docValue(doc,params.v0))\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"keyword\"}},\"missing_bucket\":true," +
                "\"value_type\":\"string\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testGroupKeyTypes_IP() {
        PhysicalPlan p = plan("SELECT count(*), CAST(keyword AS IP) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{\"source\":\"InternalSqlScriptUtils.cast(" +
                    "InternalQlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                    "\"lang\":\"painless\",\"params\":{\"v0\":\"keyword\",\"v1\":\"IP\"}}," +
                    "\"missing_bucket\":true,\"value_type\":\"ip\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testGroupKeyTypes_DateTime() {
        PhysicalPlan p = plan("SELECT count(*), date + INTERVAL '1-2' YEAR TO MONTH AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.add(InternalQlScriptUtils.docValue(doc,params.v0)," +
                "InternalSqlScriptUtils.intervalYearMonth(params.v1,params.v2))\",\"lang\":\"painless\",\"params\":{" +
                "\"v0\":\"date\",\"v1\":\"P1Y2M\",\"v2\":\"INTERVAL_YEAR_TO_MONTH\"}},\"missing_bucket\":true," +
                "\"value_type\":\"long\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){r}#"));
        assertThat(ee.output().get(1).toString(), startsWith("a{r}"));
    }

    public void testSelectLiteralWithGroupBy() {
        PhysicalPlan p = plan("SELECT 1, MAX(int) FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(2, ee.output().size());
        assertEquals(asList("1", "MAX(int)"), Expressions.names(ee.output()));
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
                containsString("\"max\":{\"field\":\"int\""));

        p = plan("SELECT 1, count(*) FROM test GROUP BY int");
        assertEquals(EsQueryExec.class, p.getClass());
        ee = (EsQueryExec) p;
        assertEquals(2, ee.output().size());
        assertEquals(asList("1", "count(*)"), Expressions.names(ee.output()));
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
                containsString("\"terms\":{\"field\":\"int\""));
    }

    public void testConcatIsNotFoldedForNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE CONCAT(keyword, null) IS NULL");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("test.keyword{f}#"));
    }

    public void testFoldingOfPercentileSecondArgument() {
        PhysicalPlan p = plan("SELECT PERCENTILE(int, 1 + 2) FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertEquals(ReferenceAttribute.class, ee.output().get(0).getClass());
        assertTrue(ee.toString().contains("3.0"));
    }

    public void testFoldingOfPercentileRankSecondArgument() {
        PhysicalPlan p = plan("SELECT PERCENTILE_RANK(int, 1 + 2) FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertEquals(ReferenceAttribute.class, ee.output().get(0).getClass());
        assertTrue(ee.toString().contains("3.0"));
    }

    public void testFoldingOfPivot() {
        PhysicalPlan p = plan("SELECT * FROM (SELECT int, keyword, bool FROM test) PIVOT(AVG(int) FOR keyword IN ('A', 'B'))");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(3, ee.output().size());
        assertEquals(asList("bool", "'A'", "'B'"), Expressions.names(ee.output()));
        String q = ee.toString().replaceAll("\\s+", "");
        assertThat(q, containsString("\"query\":{\"terms\":{\"keyword\":[\"A\",\"B\"]"));
        String a = ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", "");
        assertThat(a, containsString("\"terms\":{\"field\":\"bool\""));
        assertThat(a, containsString("\"terms\":{\"field\":\"keyword\""));
        assertThat(a, containsString("{\"avg\":{\"field\":\"int\"}"));
    }

    public void testPivotHasSameQueryAsGroupBy() {
        final Map<String, String> aggFnsWithMultipleArguments = Map.of(
            "PERCENTILE", "PERCENTILE(int, 0)",
            "PERCENTILE_RANK", "PERCENTILE_RANK(int, 0)"
        );
        List<String> aggregations = new SqlFunctionRegistry().listFunctions()
                .stream()
                .filter(def -> AggregateFunction.class.isAssignableFrom(def.clazz()))
                .map(def -> aggFnsWithMultipleArguments.getOrDefault(def.name(), def.name() + "(int)"))
                .collect(toList());
        for (String aggregationStr : aggregations) {
            PhysicalPlan pivotPlan = plan("SELECT * FROM (SELECT some.dotted.field, bool, keyword, int FROM test) " +
                "PIVOT(" + aggregationStr + " FOR keyword IN ('A', 'B'))");
            PhysicalPlan groupByPlan = plan("SELECT some.dotted.field, bool, keyword, " + aggregationStr + " " +
                "FROM test WHERE keyword IN ('A', 'B') GROUP BY some.dotted.field, bool, keyword");
            assertEquals(EsQueryExec.class, pivotPlan.getClass());
            assertEquals(EsQueryExec.class, groupByPlan.getClass());
            QueryContainer pivotQueryContainer = ((EsQueryExec) pivotPlan).queryContainer();
            QueryContainer groupByQueryContainer = ((EsQueryExec) groupByPlan).queryContainer();
            assertEquals(pivotQueryContainer.query(), groupByQueryContainer.query());
            assertEquals(pivotQueryContainer.aggs(), groupByQueryContainer.aggs());
            assertEquals(pivotPlan.toString(), groupByPlan.toString());
        }
    }

    private static String randomOrderByAndLimit(int noOfSelectArgs) {
        return SqlTestUtils.randomOrderByAndLimit(noOfSelectArgs, random());
    }
}
