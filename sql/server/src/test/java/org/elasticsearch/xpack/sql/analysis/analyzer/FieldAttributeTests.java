/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.GetIndexResult;
import org.elasticsearch.xpack.sql.analysis.index.MappingException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.KeywordType;
import org.elasticsearch.xpack.sql.type.TextType;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataTypes.KEYWORD;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FieldAttributeTests extends ESTestCase {

    private SqlParser parser;
    private GetIndexResult getIndexResult;
    private FunctionRegistry functionRegistry;
    private Analyzer analyzer;

    public FieldAttributeTests() {
        parser = new SqlParser(DateTimeZone.UTC);
        functionRegistry = new FunctionRegistry();

        Map<String, DataType> mapping = TypesTests.loadMapping("mapping-multi-field-variation.json");

        EsIndex test = new EsIndex("test", mapping);
        getIndexResult = GetIndexResult.valid(test);
        analyzer = new Analyzer(functionRegistry, getIndexResult, DateTimeZone.UTC);
    }

    private LogicalPlan plan(String sql) {
        return analyzer.analyze(parser.createStatement(sql), true);
    }

    private FieldAttribute attribute(String fieldName) {
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
        assertThat(attr.dataType(), instanceOf(TextType.class));
        assertThat(attr.isInexact(), is(true));
        FieldAttribute exact = attr.exactAttribute();
        assertThat(exact.isInexact(), is(false));
        assertThat(exact.name(), is("some.string.typical"));
        assertThat(exact.dataType(), instanceOf(KeywordType.class));
    }

    public void testAmbiguousExactKeyword() {
        FieldAttribute attr = attribute("some.ambiguous");
        assertThat(attr.path(), is("some"));
        assertThat(attr.name(), is("some.ambiguous"));
        assertThat(attr.dataType(), instanceOf(TextType.class));
        assertThat(attr.isInexact(), is(true));
        MappingException me = expectThrows(MappingException.class, () -> attr.exactAttribute());
        assertThat(me.getMessage(),
                is("Multiple exact keyword candidates [one, two] available for some.ambiguous; specify which one to use"));
    }

    public void testNormalizedKeyword() {
        FieldAttribute attr = attribute("some.string.normalized");
        assertThat(attr.path(), is("some.string"));
        assertThat(attr.name(), is("some.string.normalized"));
        assertThat(attr.dataType(), instanceOf(KeywordType.class));
        assertThat(attr.isInexact(), is(true));
    }

    public void testDottedFieldPath() {
        assertThat(error("some"), is("Found 1 problem(s)\nline 1:8: Cannot use field [some], type [object] only its subfields"));
    }

    public void testDottedFieldPathDeeper() {
        assertThat(error("some.dotted"),
                is("Found 1 problem(s)\nline 1:8: Cannot use field [some.dotted], type [object] only its subfields"));
    }

    public void testDottedFieldPathTypo() {
        assertThat(error("some.dotted.fild"),
                is("Found 1 problem(s)\nline 1:8: Unknown column [some.dotted.fild], did you mean [some.dotted.field]?"));
    }

    public void testStarExpansionExcludesObjectAndUnsupportedTypes() {
        LogicalPlan plan = plan("SELECT * FROM test");
        List<? extends NamedExpression> list = ((Project) plan).projections();
        assertThat(list, hasSize(7));
        List<String> names = Expressions.names(list);
        assertThat(names, not(hasItem("some")));
        assertThat(names, not(hasItem("some.dotted")));
        assertThat(names, not(hasItem("unsupported")));
        assertThat(names, hasItems("bool", "text", "keyword", "int"));
    }
}
