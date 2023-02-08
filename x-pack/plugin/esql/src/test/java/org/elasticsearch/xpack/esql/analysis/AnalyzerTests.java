/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class AnalyzerTests extends ESTestCase {
    public void testIndexResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));
        var plan = analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false));
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);

        assertEquals(new EsRelation(EMPTY, idx, false), project.child());
    }

    public void testFailOnUnresolvedIndex() {
        Analyzer analyzer = newAnalyzer(IndexResolution.invalid("Unknown index [idx]"));

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false))
        );

        assertThat(e.getMessage(), containsString("Unknown index [idx]"));
    }

    public void testIndexWithClusterResolution() {
        EsIndex idx = new EsIndex("cluster:idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, "cluster", "idx"), null, false));
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);

        assertEquals(new EsRelation(EMPTY, idx, false), project.child());
    }

    public void testAttributeResolution() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals(new Alias(EMPTY, "e", new FieldAttribute(EMPTY, "emp_no", idx.mapping().get("emp_no"))), eval.fields().get(0));

        assertEquals(2, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(FieldAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));
    }

    public void testAttributeResolutionOfChainedReferences() {
        Analyzer analyzer = newAnalyzer(loadMapping("mapping-one-field.json", "idx"));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new Eval(
                    EMPTY,
                    new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                    List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
                ),
                List.of(new Alias(EMPTY, "ee", new UnresolvedAttribute(EMPTY, "e")))
            )
        );

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);

        assertEquals(1, eval.fields().size());
        Alias eeField = (Alias) eval.fields().get(0);
        assertEquals("ee", eeField.name());
        assertEquals("e", ((ReferenceAttribute) eeField.child()).name());

        assertEquals(3, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(FieldAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));
        Attribute ee = eval.output().get(2);
        assertEquals("ee", ee.name());
        assertThat(ee, instanceOf(ReferenceAttribute.class));
    }

    public void testRowAttributeResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = newAnalyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new Row(EMPTY, List.of(new Alias(EMPTY, "emp_no", new Literal(EMPTY, 1, DataTypes.INTEGER)))),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals(new Alias(EMPTY, "e", new ReferenceAttribute(EMPTY, "emp_no", DataTypes.INTEGER)), eval.fields().get(0));

        assertEquals(2, eval.output().size());
        Attribute empNo = eval.output().get(0);
        assertEquals("emp_no", empNo.name());
        assertThat(empNo, instanceOf(ReferenceAttribute.class));
        Attribute e = eval.output().get(1);
        assertEquals("e", e.name());
        assertThat(e, instanceOf(ReferenceAttribute.class));

        Row row = (Row) eval.child();
        ReferenceAttribute rowEmpNo = (ReferenceAttribute) row.output().get(0);
        assertEquals(rowEmpNo.id(), empNo.id());
    }

    public void testUnresolvableAttribute() {
        Analyzer analyzer = newAnalyzer(loadMapping("mapping-one-field.json", "idx"));

        VerificationException ve = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(
                new Eval(
                    EMPTY,
                    new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                    List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_nos")))
                )
            )
        );

        assertThat(ve.getMessage(), containsString("Unknown column [emp_nos], did you mean [emp_no]?"));
    }

    public void testProjectBasic() {
        assertProjection("""
            from test
            | project first_name
            """, "first_name");
    }

    public void testProjectBasicPattern() {
        assertProjection("""
            from test
            | project first*name
            """, "first_name");
        assertProjectionTypes("""
            from test
            | project first*name
            """, DataTypes.KEYWORD);
    }

    public void testProjectIncludePattern() {
        assertProjection("""
            from test
            | project *name
            """, "first_name", "last_name");
    }

    public void testProjectIncludeMultiStarPattern() {
        assertProjection("""
            from test
            | project *t*name
            """, "first_name", "last_name");
    }

    public void testProjectStar() {
        assertProjection("""
            from test
            | project *
            """, "_meta_field", "emp_no", "first_name", "languages", "last_name", "salary");
    }

    public void testNoProjection() {
        assertProjection("""
            from test
            """, "_meta_field", "emp_no", "first_name", "languages", "last_name", "salary");
        assertProjectionTypes("""
            from test
            """, DataTypes.KEYWORD, DataTypes.INTEGER, DataTypes.KEYWORD, DataTypes.INTEGER, DataTypes.KEYWORD, DataTypes.INTEGER);
    }

    public void testProjectOrder() {
        assertProjection("""
            from test
            | project first_name, *, last_name
            """, "first_name", "_meta_field", "emp_no", "languages", "salary", "last_name");
    }

    public void testProjectExcludeName() {
        assertProjection("""
            from test
            | project *name, -first_name
            """, "last_name");
    }

    public void testProjectKeepAndExcludeName() {
        assertProjection("""
            from test
            | project last_name, -first_name
            """, "last_name");
    }

    public void testProjectExcludePattern() {
        assertProjection("""
            from test
            | project *, -*_name
            """, "_meta_field", "emp_no", "languages", "salary");
    }

    public void testProjectExcludeNoStarPattern() {
        assertProjection("""
            from test
            | project -*_name
            """, "_meta_field", "emp_no", "languages", "salary");
    }

    public void testProjectOrderPatternWithRest() {
        assertProjection("""
            from test
            | project *name, *, emp_no
            """, "first_name", "last_name", "_meta_field", "languages", "salary", "emp_no");
    }

    public void testProjectExcludePatternAndKeepOthers() {
        assertProjection("""
            from test
            | project -l*, first_name, salary
            """, "first_name", "salary");
    }

    public void testErrorOnNoMatchingPatternInclusion() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | project *nonExisting
            """));
        assertThat(e.getMessage(), containsString("No match found for [*nonExisting]"));
    }

    public void testErrorOnNoMatchingPatternExclusion() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | project -*nonExisting
            """));
        assertThat(e.getMessage(), containsString("No match found for [*nonExisting]"));
    }

    public void testIncludeUnsupportedFieldExplicit() {
        verifyUnsupported("""
            from test
            | project unsupported
            """, "Unknown column [unsupported]");
    }

    public void testIncludeUnsupportedFieldPattern() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | project un*
            """));
        assertThat(e.getMessage(), containsString("No match found for [un*]"));
    }

    public void testExcludeUnsupportedFieldExplicit() {
        verifyUnsupported("""
            from test
            | project -unsupported
            """, "Unknown column [unsupported]");
    }

    public void testExcludeMultipleUnsupportedFieldsExplicitly() {
        verifyUnsupported("""
            from test
            | project -languages, -gender
            """, "Unknown column [languages]");
    }

    public void testExcludePatternUnsupportedFields() {
        assertProjection("""
            from test
            | project -*ala*
            """, "_meta_field", "emp_no", "first_name", "languages", "last_name");
    }

    public void testExcludeUnsupportedPattern() {
        verifyUnsupported("""
            from test
            | project -un*
            """, "No match found for [un*]");
    }

    public void testUnsupportedFieldUsedExplicitly() {
        verifyUnsupported("""
            from test
            | project foo_type
            """, "Unknown column [foo_type]");
    }

    public void testUnsupportedFieldTypes() {
        verifyUnsupported(
            """
                from test
                | project unsigned_long, text, date, date_nanos, unsupported, point, shape, version
                """,
            "Found 6 problems\n"
                + "line 2:11: Unknown column [unsigned_long]\n"
                + "line 2:26: Unknown column [text]\n"
                + "line 2:50: Unknown column [unsupported]\n"
                + "line 2:63: Unknown column [point], did you mean [int]?\n"
                + "line 2:70: Unknown column [shape]\n"
                + "line 2:77: Unknown column [version]"
        );
    }

    public void testUnsupportedDottedFieldUsedExplicitly() {
        verifyUnsupported(
            """
                from test
                | project some.string
                """,
            "Found 1 problem\n"
                + "line 2:11: Unknown column [some.string], did you mean any of [some.string.typical, some.string.normalized]?"
        );
    }

    public void testUnsupportedParentField() {
        verifyUnsupported(
            """
                from test
                | project text, text.keyword
                """,
            "Found 2 problems\n"
                + "line 2:11: Unknown column [text], did you mean [text.raw]?\n"
                + "line 2:17: Unknown column [text.keyword], did you mean any of [text.wildcard, text.raw]?",
            "mapping-multi-field.json"
        );
    }

    public void testUnsupportedParentFieldAndItsSubField() {
        verifyUnsupported(
            """
                from test
                | project text, text.english
                """,
            "Found 2 problems\n"
                + "line 2:11: Unknown column [text], did you mean [text.raw]?\n"
                + "line 2:17: Unknown column [text.english]",
            "mapping-multi-field.json"
        );
    }

    public void testUnsupportedDeepHierarchy() {
        verifyUnsupported(
            """
                from test
                | project x.y.z.w, x.y.z, x.y, x
                """,
            "Found 4 problems\n"
                + "line 2:11: Unknown column [x.y.z.w]\n"
                + "line 2:20: Unknown column [x.y.z]\n"
                + "line 2:27: Unknown column [x.y]\n"
                + "line 2:32: Unknown column [x]",
            "mapping-multi-field-with-nested.json"
        );
    }

    /**
     * Here x.y.z.v is of type "keyword" but its parent is of unsupported type "foobar".
     */
    public void testUnsupportedValidFieldTypeInDeepHierarchy() {
        verifyUnsupported("""
            from test
            | project x.y.z.v
            """, "Found 1 problem\n" + "line 2:11: Unknown column [x.y.z.v]", "mapping-multi-field-with-nested.json");
    }

    public void testUnsupportedValidFieldTypeInNestedParentField() {
        verifyUnsupported("""
            from test
            | project dep.dep_id.keyword
            """, "Found 1 problem\n" + "line 2:11: Unknown column [dep.dep_id.keyword]", "mapping-multi-field-with-nested.json");
    }

    public void testUnsupportedObjectAndNested() {
        verifyUnsupported(
            """
                from test
                | project dep, some
                """,
            "Found 2 problems\n" + "line 2:11: Unknown column [dep]\n" + "line 2:16: Unknown column [some]",
            "mapping-multi-field-with-nested.json"
        );
    }

    public void testSupportedDeepHierarchy() {
        assertProjection("""
            from test
            | project some.dotted.field, some.string.normalized
            """, new StringBuilder("mapping-multi-field-with-nested.json"), "some.dotted.field", "some.string.normalized");
    }

    public void testExcludeSupportedDottedField() {
        assertProjection(
            """
                from test
                | project -some.dotted.field
                """,
            new StringBuilder("mapping-multi-field-variation.json"),
            "bool",
            "date",
            "date_nanos",
            "float",
            "int",
            "keyword",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.string.normalized",
            "some.string.typical"
        );
    }

    public void testImplicitProjectionOfDeeplyComplexMapping() {
        assertProjection(
            "from test",
            new StringBuilder("mapping-multi-field-with-nested.json"),
            "bool",
            "date",
            "date_nanos",
            "int",
            "keyword",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.dotted.field",
            "some.string.normalized",
            "some.string.typical"
        );
    }

    public void testExcludeWildcardDottedField() {
        assertProjection(
            """
                from test
                | project -some.ambiguous.*
                """,
            new StringBuilder("mapping-multi-field-with-nested.json"),
            "bool",
            "date",
            "date_nanos",
            "int",
            "keyword",
            "some.dotted.field",
            "some.string.normalized",
            "some.string.typical"
        );
    }

    public void testExcludeWildcardDottedField2() {
        assertProjection("""
            from test
            | project -some.*
            """, new StringBuilder("mapping-multi-field-with-nested.json"), "bool", "date", "date_nanos", "int", "keyword");
    }

    public void testProjectOrderPatternWithDottedFields() {
        assertProjection(
            """
                from test
                | project *some.string*, *, some.ambiguous.two, keyword
                """,
            new StringBuilder("mapping-multi-field-with-nested.json"),
            "some.string.normalized",
            "some.string.typical",
            "bool",
            "date",
            "date_nanos",
            "int",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.dotted.field",
            "some.ambiguous.two",
            "keyword"
        );
    }

    public void testUnsupportedFieldUsedExplicitly2() {
        verifyUnsupported("""
            from test
            | project keyword, point
            """, "Unknown column [point]");
    }

    public void testCantFilterAfterProjectedAway() {
        verifyUnsupported("""
            from test
            | stats c = avg(float) by int
            | project -int
            | where int > 0
            """, "Unknown column [int]");
    }

    public void testProjectAggGroupsRefs() {
        assertProjection("""
            from test
            | stats c = count(salary) by last_name
            | eval d = c + 1
            | project d, last_name
            """, "d", "last_name");
    }

    public void testExplicitProjectAndLimit() {
        var plan = analyze("""
            from test
            """);
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        as(project.child(), EsRelation.class);
    }

    public void testDateFormatOnInt() {
        verifyUnsupported("""
            from test
            | eval date_format(int)
            """, "first argument of [date_format(int)] must be [datetime], found value [int] type [integer]");
    }

    public void testDateFormatOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_format(float)
            """, "first argument of [date_format(float)] must be [datetime], found value [float] type [double]");
    }

    public void testDateFormatOnText() {
        verifyUnsupported("""
            from test
            | eval date_format(keyword)
            """, "first argument of [date_format(keyword)] must be [datetime], found value [keyword] type [keyword]");
    }

    public void testDateFormatWithNumericFormat() {
        verifyUnsupported("""
            from test
            | eval date_format(date, 1)
            """, "second argument of [date_format(date, 1)] must be [string], found value [1] type [integer]");
    }

    public void testDateFormatWithDateFormat() {
        verifyUnsupported("""
            from test
            | eval date_format(date, date)
            """, "second argument of [date_format(date, date)] must be [string], found value [date] type [datetime]");
    }

    // check field declaration is validated even across duplicated declarations
    public void testAggsWithDuplicatesAndNonExistingFunction() throws Exception {
        verifyUnsupported("""
            row a = 1, b = 2
            | stats x = non_existing(a), x = count(a) by b
            """, "Unknown function [non_existing]");
    }

    // check field declaration is validated even across duplicated declarations
    public void testAggsWithDuplicatesAndNonExistingField() throws Exception {
        verifyUnsupported("""
            row a = 1, b = 2
            | stats x = max(non_existing), x = count(a) by b
            """, "Unknown column [non_existing]");
    }

    // duplicates get merged after stats and do not prevent following commands to blow up
    // due to ambiguity
    public void testAggsWithDuplicates() throws Exception {
        var plan = analyze("""
            row a = 1, b = 2
            | stats x = count(a), x = min(a), x = max(a) by b
            | sort x
            """);

        var limit = as(plan, Limit.class);
        var order = as(limit.child(), OrderBy.class);
        var agg = as(order.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(2));
        assertThat(Expressions.names(aggregates), contains("x", "b"));
        var alias = as(aggregates.get(0), Alias.class);
        var max = as(alias.child(), Max.class);
    }

    // expected stats b by b (grouping overrides the rest of the aggs)
    public void testAggsWithOverridingInputAndGrouping() throws Exception {
        var plan = analyze("""
            row a = 1, b = 2
            | stats b = count(a), b = max(a) by b
            | sort b
            """);
        var limit = as(plan, Limit.class);
        var order = as(limit.child(), OrderBy.class);
        var agg = as(order.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(1));
        assertThat(Expressions.names(aggregates), contains("b"));
    }

    private void verifyUnsupported(String query, String errorMessage) {
        verifyUnsupported(query, errorMessage, "mapping-multi-field-variation.json");
    }

    private void verifyUnsupported(String query, String errorMessage, String mappingFileName) {
        var e = expectThrows(VerificationException.class, () -> analyze(query, mappingFileName));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private void assertProjection(String query, String... names) {
        var plan = analyze(query);
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), contains(names));
    }

    private void assertProjectionTypes(String query, DataType... types) {
        var plan = analyze(query);
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        assertThat(project.projections().stream().map(NamedExpression::dataType).toList(), contains(types));
    }

    private void assertProjection(String query, StringBuilder mapping, String... names) {
        var plan = analyze(query, mapping.toString());
        var limit = as(plan, Limit.class);
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), contains(names));
    }

    private Analyzer newAnalyzer(IndexResolution indexResolution) {
        return new Analyzer(new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), indexResolution), new Verifier());
    }

    private IndexResolution loadMapping(String resource, String indexName) {
        EsIndex test = new EsIndex(indexName, EsqlTestUtils.loadMapping(resource));
        return IndexResolution.valid(test);
    }

    private LogicalPlan analyze(String query) {
        return analyze(query, "mapping-basic.json");
    }

    private LogicalPlan analyze(String query, String mapping) {
        return newAnalyzer(loadMapping(mapping, "test")).analyze(new EsqlParser().createStatement(query));
    }
}
