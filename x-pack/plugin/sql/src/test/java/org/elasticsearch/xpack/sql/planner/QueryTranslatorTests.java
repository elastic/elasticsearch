/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.MappingException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.Aggregate;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.QueryTranslation;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.sql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.sql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.sql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.sql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation.E;
import static org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation.PI;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public class QueryTranslatorTests extends ESTestCase {

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

    private LogicalPlan plan(String sql) {
        return analyzer.analyze(parser.createStatement(sql), true);
    }
    
    private PhysicalPlan optimizeAndPlan(String sql) {
        return  planner.plan(optimizer.optimize(plan(sql)), true);
    }

    public void testTermEqualityAnalyzer() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE some.string = 'value'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof TermQuery);
        TermQuery tq = (TermQuery) query;
        assertEquals("some.string.typical", tq.term());
        assertEquals("value", tq.value());
    }

    public void testTermEqualityAnalyzerAmbiguous() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE some.ambiguous = 'value'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        // the message is checked elsewhere (in FieldAttributeTests)
        expectThrows(MappingException.class, () -> QueryTranslator.toQuery(condition, false));
    }

    public void testTermEqualityNotAnalyzed() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE int = 5");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof TermQuery);
        TermQuery tq = (TermQuery) query;
        assertEquals("int", tq.term());
        assertEquals(5, tq.value());
    }

    public void testComparisonAgainstColumns() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > int");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () -> QueryTranslator.toQuery(condition, false));
        assertEquals("Line 1:43: Comparisons against variables are not (currently) supported; offender [int] in [>]", ex.getMessage());
    }

    public void testDateRange() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > 1969-05-13");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals(1951, rq.lower());
    }

    public void testDateRangeLiteral() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > '1969-05-13'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals("1969-05-13", rq.lower());
    }

    public void testDateRangeCast() {
        LogicalPlan p = plan("SELECT some.string FROM test WHERE date > CAST('1969-05-13T12:34:56Z' AS DATE)");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof RangeQuery);
        RangeQuery rq = (RangeQuery) query;
        assertEquals("date", rq.field());
        assertEquals(DateUtils.of("1969-05-13T12:34:56Z"), rq.lower());
    }
    
    public void testLikeConstructsNotSupported() {
        LogicalPlan p = plan("SELECT LTRIM(keyword) lt FROM test WHERE LTRIM(keyword) LIKE '%a%'");
        assertTrue(p instanceof Project);
        p = ((Project) p).child();
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () -> QueryTranslator.toQuery(condition, false));
        assertEquals("Scalar function (LTRIM(keyword)) not allowed (yet) as arguments for LIKE", ex.getMessage());
    }

    public void testTranslateNotExpression_WhereClause_Painless() {
        LogicalPlan p = plan("SELECT * FROM test WHERE NOT(POSITION('x', keyword) = 0)");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertTrue(translation.query instanceof ScriptQuery);
        ScriptQuery sc = (ScriptQuery) translation.query;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.not(" +
            "InternalSqlScriptUtils.eq(InternalSqlScriptUtils.position(" +
            "params.v0,InternalSqlScriptUtils.docValue(doc,params.v1)),params.v2)))",
            sc.script().toString());
        assertEquals("[{v=x}, {v=keyword}, {v=0}]", sc.script().params().toString());
    }

    public void testTranslateIsNullExpression_WhereClause() {
        LogicalPlan p = plan("SELECT * FROM test WHERE keyword IS NULL");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertTrue(translation.query instanceof NotQuery);
        NotQuery tq = (NotQuery) translation.query;
        assertTrue(tq.child() instanceof ExistsQuery);
        ExistsQuery eq = (ExistsQuery) tq.child();
        assertEquals("{\"exists\":{\"field\":\"keyword\",\"boost\":1.0}}",
            eq.asBuilder().toString().replaceAll("\\s+", ""));
    }

    public void testTranslateIsNullExpression_WhereClause_Painless() {
        LogicalPlan p = plan("SELECT * FROM test WHERE POSITION('x', keyword) IS NULL");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertTrue(translation.query instanceof ScriptQuery);
        ScriptQuery sc = (ScriptQuery) translation.query;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.isNull(" +
            "InternalSqlScriptUtils.position(params.v0,InternalSqlScriptUtils.docValue(doc,params.v1))))",
            sc.script().toString());
        assertEquals("[{v=x}, {v=keyword}]", sc.script().params().toString());
    }

    public void testTranslateIsNotNullExpression_WhereClause() {
        LogicalPlan p = plan("SELECT * FROM test WHERE keyword IS NOT NULL");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertTrue(translation.query instanceof ExistsQuery);
        ExistsQuery eq = (ExistsQuery) translation.query;
        assertEquals("{\"exists\":{\"field\":\"keyword\",\"boost\":1.0}}",
            eq.asBuilder().toString().replaceAll("\\s+", ""));
    }

    public void testTranslateIsNotNullExpression_WhereClause_Painless() {
        LogicalPlan p = plan("SELECT * FROM test WHERE POSITION('x', keyword) IS NOT NULL");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertTrue(translation.query instanceof ScriptQuery);
        ScriptQuery sc = (ScriptQuery) translation.query;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.isNotNull(" +
                "InternalSqlScriptUtils.position(params.v0,InternalSqlScriptUtils.docValue(doc,params.v1))))",
            sc.script().toString());
        assertEquals("[{v=x}, {v=keyword}]", sc.script().params().toString());
    }

    public void testTranslateIsNullExpression_HavingClause_Painless() {
        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) IS NULL");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.isNull(params.a0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
    }

    public void testTranslateIsNotNullExpression_HavingClause_Painless() {
        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) IS NOT NULL");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.isNotNull(params.a0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
    }

    public void testTranslateInExpression_WhereClause() {
        LogicalPlan p = plan("SELECT * FROM test WHERE keyword IN ('foo', 'bar', 'lala', 'foo', concat('la', 'la'))");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof TermsQuery);
        TermsQuery tq = (TermsQuery) query;
        assertEquals("{\"terms\":{\"keyword\":[\"foo\",\"bar\",\"lala\"],\"boost\":1.0}}",
            tq.asBuilder().toString().replaceAll("\\s", ""));
    }

    public void testTranslateInExpression_WhereClauseAndNullHandling() {
        LogicalPlan p = plan("SELECT * FROM test WHERE keyword IN ('foo', null, 'lala', null, 'foo', concat('la', 'la'))");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        Query query = translation.query;
        assertTrue(query instanceof TermsQuery);
        TermsQuery tq = (TermsQuery) query;
        assertEquals("{\"terms\":{\"keyword\":[\"foo\",\"lala\"],\"boost\":1.0}}",
            tq.asBuilder().toString().replaceAll("\\s", ""));
    }

    public void testTranslateInExpressionInvalidValues_WhereClause() {
        LogicalPlan p = plan("SELECT * FROM test WHERE keyword IN ('foo', 'bar', keyword)");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () -> QueryTranslator.toQuery(condition, false));
        assertEquals("Line 1:52: Comparisons against variables are not (currently) supported; " +
                "offender [keyword] in [keyword IN (foo, bar, keyword)]", ex.getMessage());
    }

    public void testTranslateInExpression_WhereClause_Painless() {
        LogicalPlan p = plan("SELECT int FROM test WHERE POWER(int, 2) IN (10, null, 20, 30 - 10)");
        assertTrue(p instanceof Project);
        assertTrue(p.children().get(0) instanceof Filter);
        Expression condition = ((Filter) p.children().get(0)).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, false);
        assertNull(translation.aggFilter);
        assertTrue(translation.query instanceof ScriptQuery);
        ScriptQuery sc = (ScriptQuery) translation.query;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.in(" +
                "InternalSqlScriptUtils.power(InternalSqlScriptUtils.docValue(doc,params.v0),params.v1), params.v2))",
            sc.script().toString());
        assertEquals("[{v=int}, {v=2}, {v=[10.0, null, 20.0]}]", sc.script().params().toString());
    }

    public void testTranslateInExpression_HavingClause_Painless() {
        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) IN (10, 20, 30 - 10)");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.in(params.a0, params.v0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=[10, 20]}]"));
    }

    public void testTranslateInExpression_HavingClause_PainlessOneArg() {
        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) IN (10, 30 - 20)");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.in(params.a0, params.v0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=[10]}]"));

    }

    public void testTranslateInExpression_HavingClause_PainlessAndNullHandling() {
        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING max(int) IN (10, null, 20, 30, null, 30 - 10)");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.in(params.a0, params.v0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=[10, null, 20, 30]}]"));
    }

    public void testTranslateMathFunction_HavingClause_Painless() {
        MathOperation operation =
            (MathOperation) randomFrom(Stream.of(MathOperation.values()).filter(o -> o != PI && o != E).toArray());

        LogicalPlan p = plan("SELECT keyword, max(int) FROM test GROUP BY keyword HAVING " +
            operation.name() + "(max(int)) > 10");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(InternalSqlScriptUtils." +
            operation.name().toLowerCase(Locale.ROOT) + "(params.a0),params.v0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=10}]"));
    }

    public void testGroupByAndHavingWithFunctionOnTopOfAggregation() {
        LogicalPlan p = plan("SELECT keyword, MAX(int) FROM test GROUP BY 1 HAVING ABS(MAX(int)) > 10");
        assertTrue(p instanceof Filter);
        Expression condition = ((Filter) p).condition();
        assertFalse(condition.foldable());
        QueryTranslation translation = QueryTranslator.toQuery(condition, true);
        assertNull(translation.query);
        AggFilter aggFilter = translation.aggFilter;
        assertEquals("InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(InternalSqlScriptUtils.abs" +
                "(params.a0),params.v0))",
            aggFilter.scriptTemplate().toString());
        assertThat(aggFilter.scriptTemplate().params().toString(), startsWith("[{a=MAX(int){a->"));
        assertThat(aggFilter.scriptTemplate().params().toString(), endsWith(", {v=10}]"));
    }

    public void testTranslateCoalesce_GroupBy_Painless() {
        LogicalPlan p = plan("SELECT COALESCE(int, 10) FROM test GROUP BY 1");
        assertTrue(p instanceof Aggregate);
        Expression condition = ((Aggregate) p).groupings().get(0);
        assertFalse(condition.foldable());
        QueryTranslator.GroupingContext groupingContext = QueryTranslator.groupBy(((Aggregate) p).groupings());
        assertNotNull(groupingContext);
        ScriptTemplate scriptTemplate = groupingContext.tail.script();
        assertEquals("InternalSqlScriptUtils.coalesce([InternalSqlScriptUtils.docValue(doc,params.v0),params.v1])",
            scriptTemplate.toString());
        assertEquals("[{v=int}, {v=10}]", scriptTemplate.params().toString());
    }

    public void testTranslateNullIf_GroupBy_Painless() {
        LogicalPlan p = plan("SELECT NULLIF(int, 10) FROM test GROUP BY 1");
        assertTrue(p instanceof Aggregate);
        Expression condition = ((Aggregate) p).groupings().get(0);
        assertFalse(condition.foldable());
        QueryTranslator.GroupingContext groupingContext = QueryTranslator.groupBy(((Aggregate) p).groupings());
        assertNotNull(groupingContext);
        ScriptTemplate scriptTemplate = groupingContext.tail.script();
        assertEquals("InternalSqlScriptUtils.nullif(InternalSqlScriptUtils.docValue(doc,params.v0),params.v1)",
            scriptTemplate.toString());
        assertEquals("[{v=int}, {v=10}]", scriptTemplate.params().toString());
    }

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
        assertEquals(DataType.INTEGER, field.dataType());
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
        assertEquals(DataType.DATE, field.dataType());
    }
    
    public void testCountAndCountDistinctFolding() {
        PhysicalPlan p = optimizeAndPlan("SELECT COUNT(DISTINCT keyword) dkey, COUNT(keyword) key FROM test");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(2, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("dkey{a->"));
        assertThat(ee.output().get(1).toString(), startsWith("key{a->"));
        
        Collection<AggregationBuilder> subAggs = ee.queryContainer().aggs().asAggBuilder().getSubAggregations();
        assertEquals(2, subAggs.size());
        assertTrue(subAggs.toArray()[0] instanceof CardinalityAggregationBuilder);
        assertTrue(subAggs.toArray()[1] instanceof FilterAggregationBuilder);

        CardinalityAggregationBuilder cardinalityKeyword = (CardinalityAggregationBuilder) subAggs.toArray()[0];
        assertEquals("keyword", cardinalityKeyword.field());
        
        FilterAggregationBuilder existsKeyword = (FilterAggregationBuilder) subAggs.toArray()[1];
        assertTrue(existsKeyword.getFilter() instanceof ExistsQueryBuilder);
        assertEquals("keyword", ((ExistsQueryBuilder) existsKeyword.getFilter()).fieldName());
        
        assertThat(ee.queryContainer().aggs().asAggBuilder().toString().replaceAll("\\s+", ""),
                endsWith("{\"filter\":{\"exists\":{\"field\":\"keyword\",\"boost\":1.0}}}}}}"));
    }
    
    public void testAllCountVariantsWithHavingGenerateCorrectAggregations() {
        PhysicalPlan p = optimizeAndPlan("SELECT AVG(int), COUNT(keyword) ln, COUNT(distinct keyword) dln, COUNT(some.dotted.field) fn,"
                + "COUNT(distinct some.dotted.field) dfn, COUNT(*) ccc FROM test GROUP BY bool "
                + "HAVING dln > 3 AND ln > 32 AND dfn > 1 AND fn > 2 AND ccc > 5 AND AVG(int) > 50000");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec ee = (EsQueryExec) p;
        assertEquals(6, ee.output().size());
        assertThat(ee.output().get(0).toString(), startsWith("AVG(int){a->"));
        assertThat(ee.output().get(1).toString(), startsWith("ln{a->"));
        assertThat(ee.output().get(2).toString(), startsWith("dln{a->"));
        assertThat(ee.output().get(3).toString(), startsWith("fn{a->"));
        assertThat(ee.output().get(4).toString(), startsWith("dfn{a->"));
        assertThat(ee.output().get(5).toString(), startsWith("ccc{a->"));
        
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
                        + "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.and("
                        +   "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.and("
                        +     "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.and("
                        +       "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.and("
                        +         "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.and("
                        +           "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a0,params.v0)),"
                        +           "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a1,params.v1)))),"
                        +         "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a2,params.v2)))),"
                        +       "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a3,params.v3)))),"
                        +     "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a4,params.v4)))),"
                        +   "InternalSqlScriptUtils.nullSafeFilter(InternalSqlScriptUtils.gt(params.a5,params.v5))))\","
                        + "\"lang\":\"painless\",\"params\":{\"v0\":3,\"v1\":32,\"v2\":1,\"v3\":2,\"v4\":5,\"v5\":50000}},"
                        + "\"gap_policy\":\"skip\"}}}}}"));
    }
}
