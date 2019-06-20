/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.session.SingletonExecutable;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;

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

        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-variation.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(TestUtils.TEST_CFG, new FunctionRegistry(), getIndexResult, new Verifier(new Metrics()));
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
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndOrderBy() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectAndOrderByAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testLocalExecWithPrunedFilterWithFunction() {
        PhysicalPlan p = plan("SELECT E() FROM test WHERE PI() = 5");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){c}#"));
    }

    public void testLocalExecWithPrunedFilterWithFunctionAndAggregation() {
        PhysicalPlan p = plan("SELECT E() FROM test WHERE PI() = 5 GROUP BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){c}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndLimit() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{c}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndOrderBy() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 ORDER BY 1");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{c}#"));
    }

    public void testFoldingToLocalExecWithAggregationAndOrderByAndLimit() {
        PhysicalPlan p = plan("SELECT 'foo' FROM test GROUP BY 1 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("'foo'{c}#"));
    }

    public void testLocalExecWithoutFromClause() {
        PhysicalPlan p = plan("SELECT E(), 'foo', abs(10)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(SingletonExecutable.class, le.executable().getClass());
        SingletonExecutable ee = (SingletonExecutable) le.executable();
        assertEquals(3, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){c}#"));
        assertThat(ee.output().get(1).toString(), startsWith("'foo'{c}#"));
        assertThat(ee.output().get(2).toString(), startsWith("abs(10){c}#"));
    }

    public void testLocalExecWithoutFromClauseWithPrunedFilter() {
        PhysicalPlan p = plan("SELECT E() WHERE PI() = 5");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("E(){c}#"));
    }

    public void testFoldingOfIsNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE (keyword IS NOT NULL) IS NULL");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec ee = (LocalExec) p;
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecBooleanAndNull_WhereClause() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int > 10 AND null AND true");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecBooleanAndNull_HavingClause() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) > 10 AND null");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){a->"));
    }

    public void testFoldingBooleanOrNull_WhereClause() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int > 10 OR null OR false");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals("{\"range\":{\"int\":{\"from\":10,\"to\":null,\"include_lower\":false,\"include_upper\":false,\"boost\":1.0}}}",
            ee.queryContainer().query().asBuilder().toString().replaceAll("\\s+", ""));
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingBooleanOrNull_HavingClause() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) > 10 OR null");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertTrue(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", "").contains(
            "\"script\":{\"source\":\"InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a0,params.v0))\"," +
            "\"lang\":\"painless\",\"params\":{\"v0\":10}},"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){a->"));
    }

    public void testFoldingOfIsNotNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE (keyword IS NULL) IS NOT NULL");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithNullFilter() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE null IN (1, 2)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProject_FoldableIn() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE int IN (null, null)");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProject_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE 1 = 2 ORDER BY int LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingToLocalExecWithProjectWithGroupBy_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test WHERE 1 = 2 GROUP BY keyword ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){a->"));
    }

    public void testFoldingToLocalExecWithProjectWithGroupBy_WithHaving_WithOrderAndLimit() {
        PhysicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING 1 = 2 ORDER BY 1 LIMIT 10");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
        assertThat(ee.output().get(1).toString(), startsWith("max(int){a->"));
    }

    public void testGroupKeyTypes_Boolean() {
        PhysicalPlan p = plan("SELECT count(*), int > 10 AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.gt(InternalSqlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\",\"v1\":10}},\"missing_bucket\":true," +
                "\"value_type\":\"boolean\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testGroupKeyTypes_Integer() {
        PhysicalPlan p = plan("SELECT count(*), int + 10 AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.add(InternalSqlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\",\"v1\":10}},\"missing_bucket\":true," +
                "\"value_type\":\"long\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testGroupKeyTypes_Rational() {
        PhysicalPlan p = plan("SELECT count(*), sin(int) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.sin(InternalSqlScriptUtils.docValue(doc,params.v0))\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"int\"}},\"missing_bucket\":true," +
                "\"value_type\":\"double\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testGroupKeyTypes_String() {
        PhysicalPlan p = plan("SELECT count(*), LCASE(keyword) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.lcase(InternalSqlScriptUtils.docValue(doc,params.v0))\"," +
                "\"lang\":\"painless\",\"params\":{\"v0\":\"keyword\"}},\"missing_bucket\":true," +
                "\"value_type\":\"string\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testGroupKeyTypes_IP() {
        PhysicalPlan p = plan("SELECT count(*), CAST(keyword AS IP) AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{\"source\":\"InternalSqlScriptUtils.cast(" +
                    "InternalSqlScriptUtils.docValue(doc,params.v0),params.v1)\"," +
                    "\"lang\":\"painless\",\"params\":{\"v0\":\"keyword\",\"v1\":\"IP\"}}," +
                    "\"missing_bucket\":true,\"value_type\":\"ip\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testGroupKeyTypes_DateTime() {
        PhysicalPlan p = plan("SELECT count(*), date + INTERVAL '1-2' YEAR TO MONTH AS a FROM test GROUP BY a");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
            endsWith("{\"script\":{" +
                "\"source\":\"InternalSqlScriptUtils.add(InternalSqlScriptUtils.docValue(doc,params.v0)," +
                "InternalSqlScriptUtils.intervalYearMonth(params.v1,params.v2))\",\"lang\":\"painless\",\"params\":{" +
                "\"v0\":\"date\",\"v1\":\"P1Y2M\",\"v2\":\"INTERVAL_YEAR_TO_MONTH\"}},\"missing_bucket\":true," +
                "\"value_type\":\"long\",\"order\":\"asc\"}}}]}}}"));
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("count(*){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("a{s->"));
    }

    public void testConcatIsNotFoldedForNull() {
        PhysicalPlan p = plan("SELECT keyword FROM test WHERE CONCAT(keyword, null) IS NULL");
        assertEquals(LocalExec.class, p.getClass());
        LocalExec le = (LocalExec) p;
        assertEquals(EmptyExecutable.class, le.executable().getClass());
        EmptyExecutable ee = (EmptyExecutable) le.executable();
        assertEquals(1, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("keyword{f}#"));
    }

    public void testFoldingOfPercentileSecondArgument() {
        PhysicalPlan p = plan("SELECT PERCENTILE(int, 1 + 2) FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertEquals(AggregateFunctionAttribute.class, ee.output().get(0).getClass());
        AggregateFunctionAttribute afa = (AggregateFunctionAttribute) ee.output().get(0);
        assertThat(afa.propertyPath(), endsWith("[3.0]"));
    }

    public void testFoldingOfPercentileRankSecondArgument() {
        PhysicalPlan p = plan("SELECT PERCENTILE_RANK(int, 1 + 2) FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(1, ee.output().size());
        assertEquals(AggregateFunctionAttribute.class, ee.output().get(0).getClass());
        AggregateFunctionAttribute afa = (AggregateFunctionAttribute) ee.output().get(0);
        assertThat(afa.propertyPath(), endsWith("[3.0]"));
    }
}
