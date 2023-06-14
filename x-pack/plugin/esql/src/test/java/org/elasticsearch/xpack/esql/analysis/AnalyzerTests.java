/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
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
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

//@TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerTests extends ESTestCase {
    public void testIndexResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));
        var plan = analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false));
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx, false), limit.child());
    }

    public void testFailOnUnresolvedIndex() {
        Analyzer analyzer = analyzer(IndexResolution.invalid("Unknown index [idx]"));

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false))
        );

        assertThat(e.getMessage(), containsString("Unknown index [idx]"));
    }

    public void testIndexWithClusterResolution() {
        EsIndex idx = new EsIndex("cluster:idx", Map.of());
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, "cluster", "idx"), null, false));
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx, false), limit.child());
    }

    public void testAttributeResolution() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "idx"), null, false),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
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
        Analyzer analyzer = analyzer(loadMapping("mapping-one-field.json", "idx"));

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
        var eval = as(limit.child(), Eval.class);

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
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new Row(EMPTY, List.of(new Alias(EMPTY, "emp_no", new Literal(EMPTY, 1, DataTypes.INTEGER)))),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
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
        Analyzer analyzer = analyzer(loadMapping("mapping-one-field.json", "idx"));

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
            """, "_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary");
    }

    public void testNoProjection() {
        assertProjection("""
            from test
            """, "_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary");
        assertProjectionTypes(
            """
                from test
                """,
            DataTypes.KEYWORD,
            DataTypes.INTEGER,
            DataTypes.KEYWORD,
            DataTypes.UNSUPPORTED,
            DataTypes.INTEGER,
            DataTypes.KEYWORD,
            DataTypes.INTEGER
        );
    }

    public void testProjectOrder() {
        assertProjection("""
            from test
            | project first_name, *, last_name
            """, "first_name", "_meta_field", "emp_no", "gender", "languages", "salary", "last_name");
    }

    public void testProjectThenDropName() {
        assertProjection("""
            from test
            | project *name
            | drop first_name
            """, "last_name");
    }

    public void testProjectAfterDropName() {
        assertProjection("""
            from test
            | drop first_name
            | project *name
            """, "last_name");
    }

    public void testProjectKeepAndDropName() {
        assertProjection("""
            from test
            | drop first_name
            | project last_name
            """, "last_name");
    }

    public void testProjectDropPattern() {
        assertProjection("""
            from test
            | project *
            | drop *_name
            """, "_meta_field", "emp_no", "gender", "languages", "salary");
    }

    public void testProjectDropNoStarPattern() {
        assertProjection("""
            from test
            | drop *_name
            """, "_meta_field", "emp_no", "gender", "languages", "salary");
    }

    public void testProjectOrderPatternWithRest() {
        assertProjection("""
            from test
            | project *name, *, emp_no
            """, "first_name", "last_name", "_meta_field", "gender", "languages", "salary", "emp_no");
    }

    public void testProjectDropPatternAndKeepOthers() {
        assertProjection("""
            from test
            | drop l*
            | project first_name, salary
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
            | drop *nonExisting
            """));
        assertThat(e.getMessage(), containsString("No match found for [*nonExisting]"));
    }

    //
    // Unsupported field
    //

    public void testIncludeUnsupportedFieldExplicit() {
        assertProjectionWithMapping("""
            from test
            | project unsupported
            """, "mapping-multi-field-variation.json", "unsupported");
    }

    public void testUnsupportedFieldAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | project unsupported
            | eval x = unsupported
            """, errorMessage);
    }

    public void testUnsupportedFieldEvalAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | project unsupported
            | eval x = unsupported + 1
            """, errorMessage);
    }

    public void testUnsupportedFieldFilterAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | project unsupported
            | where unsupported == null
            """, errorMessage);
    }

    public void testUnsupportedFieldFunctionAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | project unsupported
            | where length(unsupported) > 0
            """, errorMessage);
    }

    public void testUnsupportedFieldSortAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | project unsupported
            | sort unsupported
            """, errorMessage);
    }

    public void testIncludeUnsupportedFieldPattern() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | project un*
            """));
        assertThat(e.getMessage(), containsString("No match found for [un*]"));
    }

    public void testDropUnsupportedFieldExplicit() {
        assertProjectionWithMapping(
            """
                from test
                | drop unsupported
                """,
            "mapping-multi-field-variation.json",
            "bool",
            "date",
            "date_nanos",
            "float",
            "foo_type",
            "int",
            "keyword",
            "point",
            "shape",
            "some.ambiguous",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.dotted.field",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "text",
            "unsigned_long",
            "version"
        );
    }

    public void testDropMultipleUnsupportedFieldsExplicitly() {
        verifyUnsupported("""
            from test
            | drop languages, gender
            """, "Unknown column [languages]");
    }

    public void testDropPatternUnsupportedFields() {
        assertProjection("""
            from test
            | drop *ala*
            """, "_meta_field", "emp_no", "first_name", "gender", "languages", "last_name");
    }

    public void testDropUnsupportedPattern() {
        assertProjectionWithMapping(
            """
                from test
                | drop un*
                """,
            "mapping-multi-field-variation.json",
            "bool",
            "date",
            "date_nanos",
            "float",
            "foo_type",
            "int",
            "keyword",
            "point",
            "shape",
            "some.ambiguous",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.dotted.field",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "text",
            "version"
        );
    }

    public void testRename() {
        assertProjection("""
            from test
            | rename e = emp_no
            | project first_name, e
            """, "first_name", "e");
    }

    public void testChainedRename() {
        assertProjection("""
            from test
            | rename r1 = emp_no, r2 = r1, r3 = r2
            | project first_name, r3
            """, "first_name", "r3");
    }

    public void testChainedRenameReuse() {
        assertProjection("""
            from test
            | rename r1 = emp_no, r2 = r1, r3 = r2, r1 = first_name
            | project r1, r3
            """, "r1", "r3");
    }

    public void testRenameBackAndForth() {
        assertProjection("""
            from test
            | rename r1 = emp_no, emp_no = r1
            | project emp_no
            """, "emp_no");
    }

    public void testRenameReuseAlias() {
        assertProjection("""
            from test
            | rename e = emp_no, e = first_name
            """, "_meta_field", "e", "gender", "languages", "last_name", "salary");
    }

    public void testRenameUnsupportedField() {
        assertProjectionWithMapping("""
            from test
            | rename u = unsupported
            | project int, u, float
            """, "mapping-multi-field-variation.json", "int", "u", "float");
    }

    public void testRenameUnsupportedFieldChained() {
        assertProjectionWithMapping("""
            from test
            | rename u1 = unsupported, u2 = u1
            | project int, u2, float
            """, "mapping-multi-field-variation.json", "int", "u2", "float");
    }

    public void testRenameUnsupportedAndResolved() {
        assertProjectionWithMapping("""
            from test
            | rename u = unsupported, f = float
            | project int, u, f
            """, "mapping-multi-field-variation.json", "int", "u", "f");
    }

    public void testRenameUnsupportedSubFieldAndResolved() {
        assertProjectionWithMapping("""
            from test
            | rename ss = some.string, f = float
            | project int, ss, f
            """, "mapping-multi-field-variation.json", "int", "ss", "f");
    }

    public void testRenameUnsupportedAndUnknown() {
        verifyUnsupported("""
            from test
            | rename t = text, d = doesnotexist
            """, "Found 1 problem\n" + "line 2:24: Unknown column [doesnotexist]");
    }

    public void testRenameResolvedAndUnknown() {
        verifyUnsupported("""
            from test
            | rename i = int, d = doesnotexist
            """, "Found 1 problem\n" + "line 2:23: Unknown column [doesnotexist]");
    }

    public void testUnsupportedFieldUsedExplicitly() {
        assertProjectionWithMapping("""
            from test
            | project foo_type
            """, "mapping-multi-field-variation.json", "foo_type");
    }

    public void testUnsupportedFieldTypes() {
        assertProjectionWithMapping("""
            from test
            | project unsigned_long, date, date_nanos, unsupported, point, version
            """, "mapping-multi-field-variation.json", "unsigned_long", "date", "date_nanos", "unsupported", "point", "version");
    }

    public void testUnsupportedDottedFieldUsedExplicitly() {
        assertProjectionWithMapping("""
            from test
            | project some.string
            """, "mapping-multi-field-variation.json", "some.string");
    }

    public void testUnsupportedParentField() {
        verifyUnsupported(
            """
                from test
                | project text, text.keyword
                """,
            "Found 1 problem\n" + "line 2:17: Unknown column [text.keyword], did you mean any of [text.wildcard, text.raw]?",
            "mapping-multi-field.json"
        );
    }

    public void testUnsupportedParentFieldAndItsSubField() {
        assertProjectionWithMapping("""
            from test
            | project text, text.english
            """, "mapping-multi-field.json", "text", "text.english");
    }

    public void testUnsupportedDeepHierarchy() {
        assertProjectionWithMapping("""
            from test
            | project x.y.z.w, x.y.z, x.y, x
            """, "mapping-multi-field-with-nested.json", "x.y.z.w", "x.y.z", "x.y", "x");
    }

    /**
     * Here x.y.z.v is of type "keyword" but its parent is of unsupported type "foobar".
     */
    public void testUnsupportedValidFieldTypeInDeepHierarchy() {
        assertProjectionWithMapping("""
            from test
            | project x.y.z.v
            """, "mapping-multi-field-with-nested.json", "x.y.z.v");
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

    public void testDropNestedField() {
        verifyUnsupported(
            """
                from test
                | drop dep, dep.dep_id.keyword
                """,
            "Found 2 problems\n" + "line 2:8: Unknown column [dep]\n" + "line 2:13: Unknown column [dep.dep_id.keyword]",
            "mapping-multi-field-with-nested.json"
        );
    }

    public void testDropNestedWildcardField() {
        verifyUnsupported("""
            from test
            | drop dep.*
            """, "Found 1 problem\n" + "line 2:8: No match found for [dep.*]", "mapping-multi-field-with-nested.json");
    }

    public void testSupportedDeepHierarchy() {
        assertProjectionWithMapping("""
            from test
            | project some.dotted.field, some.string.normalized
            """, "mapping-multi-field-with-nested.json", "some.dotted.field", "some.string.normalized");
    }

    public void testDropSupportedDottedField() {
        assertProjectionWithMapping(
            """
                from test
                | drop some.dotted.field
                """,
            "mapping-multi-field-variation.json",
            "bool",
            "date",
            "date_nanos",
            "float",
            "foo_type",
            "int",
            "keyword",
            "point",
            "shape",
            "some.ambiguous",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "text",
            "unsigned_long",
            "unsupported",
            "version"
        );
    }

    public void testImplicitProjectionOfDeeplyComplexMapping() {
        assertProjectionWithMapping(
            "from test",
            "mapping-multi-field-with-nested.json",
            "binary",
            "binary_stored",
            "bool",
            "date",
            "date_nanos",
            "geo_shape",
            "int",
            "keyword",
            "shape",
            "some.ambiguous",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.ambiguous.two",
            "some.dotted.field",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "text",
            "unsigned_long",
            "unsupported",
            "x",
            "x.y",
            "x.y.z",
            "x.y.z.v",
            "x.y.z.w"
        );
    }

    public void testDropWildcardDottedField() {
        assertProjectionWithMapping(
            """
                from test
                | drop some.ambiguous.*
                """,
            "mapping-multi-field-with-nested.json",
            "binary",
            "binary_stored",
            "bool",
            "date",
            "date_nanos",
            "geo_shape",
            "int",
            "keyword",
            "shape",
            "some.ambiguous",
            "some.dotted.field",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "text",
            "unsigned_long",
            "unsupported",
            "x",
            "x.y",
            "x.y.z",
            "x.y.z.v",
            "x.y.z.w"
        );
    }

    public void testDropWildcardDottedField2() {
        assertProjectionWithMapping(
            """
                from test
                | drop some.*
                """,
            "mapping-multi-field-with-nested.json",
            "binary",
            "binary_stored",
            "bool",
            "date",
            "date_nanos",
            "geo_shape",
            "int",
            "keyword",
            "shape",
            "text",
            "unsigned_long",
            "unsupported",
            "x",
            "x.y",
            "x.y.z",
            "x.y.z.v",
            "x.y.z.w"
        );
    }

    public void testProjectOrderPatternWithDottedFields() {
        assertProjectionWithMapping(
            """
                from test
                | project *some.string*, *, some.ambiguous.two, keyword
                """,
            "mapping-multi-field-with-nested.json",
            "some.string",
            "some.string.normalized",
            "some.string.typical",
            "binary",
            "binary_stored",
            "bool",
            "date",
            "date_nanos",
            "geo_shape",
            "int",
            "shape",
            "some.ambiguous",
            "some.ambiguous.normalized",
            "some.ambiguous.one",
            "some.dotted.field",
            "text",
            "unsigned_long",
            "unsupported",
            "x",
            "x.y",
            "x.y.z",
            "x.y.z.v",
            "x.y.z.w",
            "some.ambiguous.two",
            "keyword"
        );
    }

    public void testUnsupportedFieldUsedExplicitly2() {
        assertProjectionWithMapping("""
            from test
            | project keyword, point
            """, "mapping-multi-field-variation.json", "keyword", "point");
    }

    public void testCantFilterAfterDrop() {
        verifyUnsupported("""
            from test
            | stats c = avg(float) by int
            | drop int
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
        as(limit.child(), EsRelation.class);
    }

    private static final String[] COMPARISONS = new String[] { "==", "!=", "<", "<=", ">", ">=" };

    public void testCompareIntToString() {
        for (String comparison : COMPARISONS) {
            var e = expectThrows(VerificationException.class, () -> analyze("""
                from test
                | where emp_no COMPARISON "foo"
                """.replace("COMPARISON", comparison)));
            assertThat(
                e.getMessage(),
                containsString(
                    "first argument of [emp_no COMPARISON \"foo\"] is [numeric] so second argument must also be [numeric] but was [keyword]"
                        .replace("COMPARISON", comparison)
                )
            );
        }
    }

    public void testCompareStringToInt() {
        for (String comparison : COMPARISONS) {
            var e = expectThrows(VerificationException.class, () -> analyze("""
                from test
                | where "foo" COMPARISON emp_no
                """.replace("COMPARISON", comparison)));
            assertThat(
                e.getMessage(),
                containsString(
                    "first argument of [\"foo\" COMPARISON emp_no] is [keyword] so second argument must also be [keyword] but was [integer]"
                        .replace("COMPARISON", comparison)
                )
            );
        }
    }

    public void testCompareDateToString() {
        for (String comparison : COMPARISONS) {
            assertProjectionWithMapping("""
                from test
                | where date COMPARISON "1985-01-01T00:00:00Z"
                | project date
                """.replace("COMPARISON", comparison), "mapping-multi-field-variation.json", "date");
        }
    }

    public void testCompareStringToDate() {
        for (String comparison : COMPARISONS) {
            assertProjectionWithMapping("""
                from test
                | where "1985-01-01T00:00:00Z" COMPARISON date
                | project date
                """.replace("COMPARISON", comparison), "mapping-multi-field-variation.json", "date");
        }
    }

    public void testCompareDateToStringFails() {
        for (String comparison : COMPARISONS) {
            verifyUnsupported("""
                from test
                | where date COMPARISON "not-a-date"
                | project date
                """.replace("COMPARISON", comparison), "Invalid date [not-a-date]", "mapping-multi-field-variation.json");
        }
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

    public void testDateParseOnInt() {
        verifyUnsupported("""
            from test
            | eval date_parse(int, keyword)
            """, "first argument of [date_parse(int, keyword)] must be [string], found value [int] type [integer]");
    }

    public void testDateParseOnDate() {
        verifyUnsupported("""
            from test
            | eval date_parse(date, keyword)
            """, "first argument of [date_parse(date, keyword)] must be [string], found value [date] type [datetime]");
    }

    public void testDateParseOnIntPattern() {
        verifyUnsupported("""
            from test
            | eval date_parse(keyword, int)
            """, "second argument of [date_parse(keyword, int)] must be [string], found value [int] type [integer]");
    }

    public void testDateTruncOnInt() {
        verifyUnsupported("""
            from test
            | eval date_trunc(int, "1M")
            """, "first argument of [date_trunc(int, \"1M\")] must be [datetime], found value [int] type [integer]");
    }

    public void testDateTruncOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_trunc(float, "1M")
            """, "first argument of [date_trunc(float, \"1M\")] must be [datetime], found value [float] type [double]");
    }

    public void testDateTruncOnText() {
        verifyUnsupported("""
            from test
            | eval date_trunc(keyword, "1M")
            """, "first argument of [date_trunc(keyword, \"1M\")] must be [datetime], found value [keyword] type [keyword]");
    }

    public void testDateTruncWithNumericInterval() {
        verifyUnsupported("""
            from test
            | eval date_trunc(date, 1)
            """, "second argument of [date_trunc(date, 1)] must be [dateperiod or timeduration], found value [1] type [integer]");
    }

    public void testDateTruncWithDateInterval() {
        verifyUnsupported("""
            from test
            | eval date_trunc(date, date)
            """, "second argument of [date_trunc(date, date)] must be [dateperiod or timeduration], found value [date] type [datetime]");
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

    public void testAggsWithoutAgg() throws Exception {
        var plan = analyze("""
            row a = 1, b = 2
            | stats by a
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(1));
        assertThat(Expressions.names(aggregates), contains("a"));
        assertThat(Expressions.names(agg.groupings()), contains("a"));
        assertEquals(agg.groupings(), agg.aggregates());
    }

    public void testAggsWithoutAggAndFollowingCommand() throws Exception {
        var plan = analyze("""
            row a = 1, b = 2
            | stats by a
            | sort a
            """);

        var limit = as(plan, Limit.class);
        var order = as(limit.child(), OrderBy.class);
        var agg = as(order.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(1));
        assertThat(Expressions.names(aggregates), contains("a"));
        assertThat(Expressions.names(agg.groupings()), contains("a"));
        assertEquals(agg.groupings(), agg.aggregates());
    }

    public void testUnsupportedFieldsInStats() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";

        verifyUnsupported("""
            from test
            | stats max(point)
            """, errorMsg);
        verifyUnsupported("""
            from test
            | stats max(int) by point
            """, errorMsg);
        verifyUnsupported("""
            from test
            | stats max(int) by bool, point
            """, errorMsg);
    }

    public void testUnsupportedFieldsInEval() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";

        verifyUnsupported("""
            from test
            | eval x = point
            """, errorMsg);
        verifyUnsupported("""
            from test
            | eval foo = 1, x = point
            """, errorMsg);
        verifyUnsupported("""
            from test
            | eval x = 1 + point
            """, errorMsg);
    }

    public void testUnsupportedFieldsInWhere() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";

        verifyUnsupported("""
            from test
            | where point == "[1.0, 1.0]"
            """, errorMsg);
        verifyUnsupported("""
            from test
            | where int > 2 and point == "[1.0, 1.0]"
            """, errorMsg);
    }

    public void testUnsupportedFieldsInSort() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";

        verifyUnsupported("""
            from test
            | sort point
            """, errorMsg);
        verifyUnsupported("""
            from test
            | sort int, point
            """, errorMsg);
    }

    public void testUnsupportedFieldsInDissect() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";
        verifyUnsupported("""
            from test
            | dissect point \"%{foo}\"
            """, errorMsg);
    }

    public void testUnsupportedFieldsInGrok() {
        var errorMsg = "Cannot use field [point] with unsupported type [geo_point]";
        verifyUnsupported("""
            from test
            | grok point \"%{WORD:foo}\"
            """, errorMsg);
    }

    public void testRegexOnInt() {
        for (String op : new String[] { "like", "rlike" }) {
            var e = expectThrows(VerificationException.class, () -> analyze("""
                from test
                | where emp_no COMPARISON "foo"
                """.replace("COMPARISON", op)));
            assertThat(
                e.getMessage(),
                containsString(
                    "argument of [emp_no COMPARISON \"foo\"] must be [string], found value [emp_no] type [integer]".replace(
                        "COMPARISON",
                        op
                    )
                )
            );
        }
    }

    public void testUnsupportedTypesWithToString() {
        // DATE_PERIOD and TIME_DURATION types have been added, but not really patched through the engine; i.e. supported.
        final String supportedTypes = "boolean, datetime, double, integer, ip, keyword, long or version";
        verifyUnsupported(
            "row period = 1 year | eval to_string(period)",
            "line 1:28: argument of [to_string(period)] must be [" + supportedTypes + "], found value [period] type [date_period]"
        );
        verifyUnsupported(
            "row duration = 1 hour | eval to_string(duration)",
            "line 1:30: argument of [to_string(duration)] must be [" + supportedTypes + "], found value [duration] type [time_duration]"
        );
        verifyUnsupported("from test | eval to_string(point)", "line 1:28: Cannot use field [point] with unsupported type [geo_point]");
    }

    public void testNonExistingEnrichPolicy() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich foo on bar
            """));
        assertThat(e.getMessage(), containsString("unresolved enrich policy [foo]"));
    }

    public void testNonExistingEnrichPolicyWithSimilarName() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich language on bar
            """));
        assertThat(e.getMessage(), containsString("unresolved enrich policy [language], did you mean [languages]"));
    }

    public void testEnrichPolicyWrongMatchField() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich languages on bar
            """));
        assertThat(e.getMessage(), containsString("Unknown column [bar]"));
    }

    public void testEnrichWrongMatchFieldType() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich languages on languages
            | project first_name, language_name, id
            """));
        assertThat(
            e.getMessage(),
            containsString("Unsupported type [INTEGER]  for enrich matching field [languages]; only KEYWORD allowed")
        );
    }

    public void testValidEnrich() {
        assertProjection("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x
            | project first_name, language_name
            """, "first_name", "language_name");
    }

    public void testEnrichExcludesPolicyKey() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x
            | project first_name, language_name, id
            """));
        assertThat(e.getMessage(), containsString("Unknown column [id]"));
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
        assertThat(Expressions.names(limit.output()), contains(names));
    }

    private void assertProjectionTypes(String query, DataType... types) {
        var plan = analyze(query);
        var limit = as(plan, Limit.class);
        assertThat(limit.output().stream().map(NamedExpression::dataType).toList(), contains(types));
    }

    private void assertProjectionWithMapping(String query, String mapping, String... names) {
        var plan = analyze(query, mapping.toString());
        var limit = as(plan, Limit.class);
        assertThat(Expressions.names(limit.output()), contains(names));
    }
}
