/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.stats.Metrics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.INTRODUCING_ARRAY_TYPES;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.LONG_ARRAY;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FieldAttributeTests extends ESTestCase {

    private SqlParser parser;
    private IndexResolution getIndexResult;
    private FunctionRegistry functionRegistry;
    private Analyzer analyzer;
    private Verifier verifier;

    public FieldAttributeTests() {
        parser = new SqlParser();
        functionRegistry = new SqlFunctionRegistry();
        verifier = new Verifier(new Metrics(), SqlTestUtils.TEST_CFG.version());

        Map<String, EsField> mapping = loadMapping("mapping-multi-field-variation.json");

        EsIndex test = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);
    }

    private LogicalPlan plan(String sql) {
        return analyzer.analyze(parser.createStatement(sql), true);
    }

    private FieldAttribute attribute(String fieldName) {
        // test multiple version of the attribute name
        // to make sure all match the same thing

        // NB: the equality is done on the same since each plan bumps the expression counter

        // unqualified
        FieldAttribute unqualified = parseQueryFor(fieldName);
        // unquoted qualifier
        FieldAttribute unquotedQualifier = parseQueryFor("test." + fieldName);
        assertEquals(unqualified.name(), unquotedQualifier.name());
        assertEquals(unqualified.qualifiedName(), unquotedQualifier.qualifiedName());
        // quoted qualifier
        FieldAttribute quotedQualifier = parseQueryFor("\"test\"." + fieldName);
        assertEquals(unqualified.name(), quotedQualifier.name());
        assertEquals(unqualified.qualifiedName(), quotedQualifier.qualifiedName());

        return randomFrom(unqualified, unquotedQualifier, quotedQualifier);
    }

    private FieldAttribute parseQueryFor(String fieldName) {
        LogicalPlan plan = plan("SELECT " + fieldName + " FROM test");
        assertThat(plan, instanceOf(Project.class));
        Project p = (Project) plan;
        List<? extends NamedExpression> projections = p.projections();
        assertThat(projections, hasSize(1));
        Attribute attribute = projections.get(0).toAttribute();
        assertThat(attribute, instanceOf(FieldAttribute.class));
        return (FieldAttribute) attribute;
    }

    private String error(String fieldName) {
        VerificationException ve = expectThrows(VerificationException.class, () -> plan("SELECT " + fieldName + " FROM test"));
        return ve.getMessage();
    }

    public void testRootField() {
        FieldAttribute attr = attribute("bool");
        assertThat(attr.name(), is("bool"));
        assertThat(attr.dataType(), is(BOOLEAN));
    }

    public void testDottedField() {
        FieldAttribute attr = attribute("some.dotted.field");
        assertThat(attr.path(), is("some.dotted"));
        assertThat(attr.name(), is("some.dotted.field"));
        assertThat(attr.dataType(), is(KEYWORD));
    }

    public void testExactKeyword() {
        FieldAttribute attr = attribute("some.string");
        assertThat(attr.path(), is("some"));
        assertThat(attr.name(), is("some.string"));
        assertThat(attr.dataType(), is(TEXT));
        assertTrue(attr.getExactInfo().hasExact());
        FieldAttribute exact = attr.exactAttribute();
        assertTrue(exact.getExactInfo().hasExact());
        assertThat(exact.name(), is("some.string.typical"));
        assertThat(exact.dataType(), is(KEYWORD));
    }

    public void testAmbiguousExactKeyword() {
        FieldAttribute attr = attribute("some.ambiguous");
        assertThat(attr.path(), is("some"));
        assertThat(attr.name(), is("some.ambiguous"));
        assertThat(attr.dataType(), is(TEXT));
        assertFalse(attr.getExactInfo().hasExact());
        assertThat(attr.getExactInfo().errorMsg(),
            is("Multiple exact keyword candidates available for [ambiguous]; specify which one to use"));
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class, () -> attr.exactAttribute());
        assertThat(e.getMessage(),
                is("Multiple exact keyword candidates available for [ambiguous]; specify which one to use"));
    }

    public void testNormalizedKeyword() {
        FieldAttribute attr = attribute("some.string.normalized");
        assertThat(attr.path(), is("some.string"));
        assertThat(attr.name(), is("some.string.normalized"));
        assertThat(attr.dataType(), is(KEYWORD));
        assertFalse(attr.getExactInfo().hasExact());
    }

    public void testDottedFieldPath() {
        assertThat(error("some"), is("Found 1 problem\nline 1:8: Cannot use field [some] type [object] only its subfields"));
    }

    public void testDottedFieldPathDeeper() {
        assertThat(error("some.dotted"),
                is("Found 1 problem\nline 1:8: Cannot use field [some.dotted] type [object] only its subfields"));
    }

    public void testDottedFieldPathTypo() {
        assertThat(error("some.dotted.fild"),
                is("Found 1 problem\nline 1:8: Unknown column [some.dotted.fild], did you mean [some.dotted.field]?"));
    }

    public void testStarExpansionExcludesObjectAndUnsupportedTypes() {
        LogicalPlan plan = plan("SELECT * FROM test");
        List<? extends NamedExpression> list = ((Project) plan).projections();
        assertThat(list, hasSize(12));
        List<String> names = Expressions.names(list);
        assertThat(names, not(hasItem("some")));
        assertThat(names, not(hasItem("some.dotted")));
        assertThat(names, not(hasItem("unsupported")));
        assertThat(names, hasItems("bool", "text", "keyword", "int"));
    }

    public void testFieldAmbiguity() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-dotted-field.json");

        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        VerificationException ex = expectThrows(VerificationException.class, () -> plan("SELECT test.bar FROM test"));
        assertEquals(
                "Found 1 problem\nline 1:8: Reference [test.bar] is ambiguous (to disambiguate use quotes or qualifiers); "
                        + "matches any of [line 1:22 [\"test\".\"bar\"], line 1:22 [\"test\".\"test.bar\"]]",
                ex.getMessage());

        ex = expectThrows(VerificationException.class, () -> plan("SELECT test.test FROM test"));
        assertEquals(
                "Found 1 problem\nline 1:8: Reference [test.test] is ambiguous (to disambiguate use quotes or qualifiers); "
                        + "matches any of [line 1:23 [\"test\".\"test\"], line 1:23 [\"test\".\"test.test\"]]",
                ex.getMessage());

        LogicalPlan plan = plan("SELECT test.test FROM test AS x");
        assertThat(plan, instanceOf(Project.class));

        plan = plan("SELECT \"test\".test.test FROM test");
        assertThat(plan, instanceOf(Project.class));

        Project p = (Project) plan;
        List<? extends NamedExpression> projections = p.projections();
        assertThat(projections, hasSize(1));
        Attribute attribute = projections.get(0).toAttribute();
        assertThat(attribute, instanceOf(FieldAttribute.class));
        assertThat(attribute.qualifier(), is("test"));
        assertThat(attribute.name(), is("test.test"));
    }

    public void testAggregations() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-basic.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        LogicalPlan plan = plan("SELECT sum(salary) AS s FROM test");
        assertThat(plan, instanceOf(Aggregate.class));

        Aggregate aggregate = (Aggregate) plan;
        assertThat(aggregate.aggregates(), hasSize(1));
        NamedExpression attribute = aggregate.aggregates().get(0);
        assertThat(attribute, instanceOf(Alias.class));
        assertThat(attribute.name(), is("s"));
        assertThat(aggregate.groupings(), hasSize(0));

        plan = plan("SELECT gender AS g, sum(salary) AS s FROM test GROUP BY g");
        assertThat(plan, instanceOf(Aggregate.class));

        aggregate = (Aggregate) plan;
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertThat(aggregates, hasSize(2));
        assertThat(aggregates.get(0), instanceOf(Alias.class));
        assertThat(aggregates.get(1), instanceOf(Alias.class));
        List<String> names = aggregate.aggregates().stream().map(NamedExpression::name).collect(Collectors.toList());
        assertThat(names, contains("g", "s"));

        List<Expression> groupings = aggregate.groupings();
        assertThat(groupings, hasSize(1));
        FieldAttribute grouping = (FieldAttribute) groupings.get(0);
        assertThat(grouping.name(), is("gender"));
    }

    public void testGroupByAmbiguity() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-basic.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        VerificationException ex = expectThrows(VerificationException.class,
            () -> plan("SELECT gender AS g, sum(salary) AS g FROM test GROUP BY g"));
        assertEquals(
            "Found 1 problem\nline 1:57: Reference [g] is ambiguous (to disambiguate use quotes or qualifiers); " +
                "matches any of [line 1:8 [g], line 1:21 [g]]",
            ex.getMessage());

        ex = expectThrows(VerificationException.class,
            () -> plan("SELECT gender AS g, max(salary) AS g, min(salary) AS g FROM test GROUP BY g"));
        assertEquals(
            "Found 1 problem\nline 1:75: Reference [g] is ambiguous (to disambiguate use quotes or qualifiers); " +
                "matches any of [line 1:8 [g], line 1:21 [g], line 1:39 [g]]",
            ex.getMessage());

        ex = expectThrows(VerificationException.class,
            () -> plan("SELECT gender AS g, last_name AS g, sum(salary) AS s FROM test GROUP BY g"));
        assertEquals(
            "Found 1 problem\nline 1:73: Reference [g] is ambiguous (to disambiguate use quotes or qualifiers); " +
                "matches any of [line 1:8 [g], line 1:21 [g]]",
            ex.getMessage());

        ex = expectThrows(VerificationException.class,
            () -> plan("SELECT gender AS g, last_name AS g, min(salary) AS m, max(salary) as m FROM test GROUP BY g, m"));
        assertEquals(
            "Found 2 problems\n" +
                "line 1:91: Reference [g] is ambiguous (to disambiguate use quotes or qualifiers); "
                + "matches any of [line 1:8 [g], line 1:21 [g]]\n" +
                "line 1:94: Reference [m] is ambiguous (to disambiguate use quotes or qualifiers); "
                + "matches any of [line 1:37 [m], line 1:55 [m]]",
            ex.getMessage());
    }

    public void testArrayTypeVersionCompatibility() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-numeric.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);


        SqlVersion preArrayTypes = SqlVersion.fromId(INTRODUCING_ARRAY_TYPES.id - SqlVersion.MINOR_MULTIPLIER);
        SqlVersion postArrayTypes = SqlVersion.fromId(INTRODUCING_ARRAY_TYPES.id + SqlVersion.MINOR_MULTIPLIER);

        String query = "SELECT ARRAY(long) FROM test";

        SqlConfiguration sqlConfig = SqlTestUtils.randomConfiguration(preArrayTypes);
        analyzer = new Analyzer(sqlConfig, functionRegistry, getIndexResult, new Verifier(new Metrics(), sqlConfig.version()));
        VerificationException ex = expectThrows(VerificationException.class, () -> plan(query));
        assertEquals(
            "Found 1 problem\nline 1:8: Cannot use [ARRAY(long)] with type [LONG_ARRAY] unsupported in version [" +
                preArrayTypes + "], upgrade required (to version [" + INTRODUCING_ARRAY_TYPES + "] or higher)",
            ex.getMessage());

        for (SqlVersion v : List.of(INTRODUCING_ARRAY_TYPES, postArrayTypes)) {
            analyzer = new Analyzer(SqlTestUtils.randomConfiguration(v), functionRegistry, getIndexResult, verifier);
            LogicalPlan plan = plan(query);
            assertThat(plan, instanceOf(Project.class));
            Project p = (Project) plan;
            List<? extends NamedExpression> projections = p.projections();
            assertThat(projections, hasSize(1));
            Attribute attribute = projections.get(0).toAttribute();
            assertThat(attribute.dataType(), is(LONG_ARRAY));
            assertThat(attribute.name(), is("ARRAY(long)"));
        }
    }

    public void testUnprojectedArrayTypeVersionCompatibility() {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-numeric.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        SqlVersion preArrayTypes = SqlVersion.fromId(INTRODUCING_ARRAY_TYPES.id - SqlVersion.MINOR_MULTIPLIER);
        SqlConfiguration sqlConfig = SqlTestUtils.randomConfiguration(preArrayTypes);
        analyzer = new Analyzer(sqlConfig, functionRegistry, getIndexResult, new Verifier(new Metrics(), sqlConfig.version()));

        String query = "SELECT l = 1 FROM (SELECT ARRAY(long), long AS l FROM test WHERE l > 10)";

        LogicalPlan plan = plan(query);
        assertThat(plan, instanceOf(Project.class));
        Project p = (Project) plan;
        List<? extends NamedExpression> projections = p.projections();
        assertThat(projections, hasSize(1));
        assertEquals(projections.get(0).dataType(), BOOLEAN);
    }

    public void testFunctionOverNonExistingFieldAsArgumentAndSameAlias() throws Exception {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-basic.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        VerificationException ex = expectThrows(VerificationException.class, () ->
            plan("SELECT sum(missing) AS missing FROM test WHERE missing = 0"));
        assertEquals("Found 1 problem\nline 1:12: Unknown column [missing]", ex.getMessage());
    }

    public void testFunctionWithExpressionOverNonExistingFieldAsArgumentAndSameAlias() throws Exception {
        Map<String, EsField> mapping = TypesTests.loadMapping("mapping-basic.json");
        EsIndex index = new EsIndex("test", mapping);
        getIndexResult = IndexResolution.valid(index);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        VerificationException ex = expectThrows(VerificationException.class, () ->
            plan("SELECT LENGTH(CONCAT(missing, 'x')) + 1 AS missing FROM test WHERE missing = 0"));
        assertEquals("Found 1 problem\nline 1:22: Unknown column [missing]", ex.getMessage());
    }

    public void testExpandStarOnIndexWithoutColumns() {
        EsIndex test = new EsIndex("test", Collections.emptyMap());
        getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(SqlTestUtils.TEST_CFG, functionRegistry, getIndexResult, verifier);

        LogicalPlan plan = plan("SELECT * FROM test");

        assertThat(plan, instanceOf(Project.class));
        assertTrue(((Project) plan).projections().isEmpty());
    }

}
