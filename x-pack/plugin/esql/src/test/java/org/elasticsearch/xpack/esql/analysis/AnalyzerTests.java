/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
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
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

//@TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerTests extends ESTestCase {

    private static final EsqlUnresolvedRelation UNRESOLVED_RELATION = new EsqlUnresolvedRelation(
        EMPTY,
        new TableIdentifier(EMPTY, null, "idx"),
        List.of()
    );

    private static final int MAX_LIMIT = EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY);
    private static final int DEFAULT_LIMIT = EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY);

    public void testIndexResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));
        var plan = analyzer.analyze(UNRESOLVED_RELATION);
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx, NO_FIELDS), limit.child());
    }

    public void testFailOnUnresolvedIndex() {
        Analyzer analyzer = analyzer(IndexResolution.invalid("Unknown index [idx]"));

        VerificationException e = expectThrows(VerificationException.class, () -> analyzer.analyze(UNRESOLVED_RELATION));

        assertThat(e.getMessage(), containsString("Unknown index [idx]"));
    }

    public void testIndexWithClusterResolution() {
        EsIndex idx = new EsIndex("cluster:idx", Map.of());
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(UNRESOLVED_RELATION);
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx, NO_FIELDS), limit.child());
    }

    public void testAttributeResolution() {
        EsIndex idx = new EsIndex("idx", TypesTests.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(EMPTY, UNRESOLVED_RELATION, List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no"))))
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
                new Eval(EMPTY, UNRESOLVED_RELATION, List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))),
                List.of(new Alias(EMPTY, "ee", new UnresolvedAttribute(EMPTY, "e")))
            )
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);

        assertEquals(1, eval.fields().size());
        Alias eeField = eval.fields().get(0);
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
                new Eval(EMPTY, UNRESOLVED_RELATION, List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_nos"))))
            )
        );

        assertThat(ve.getMessage(), containsString("Unknown column [emp_nos], did you mean [emp_no]?"));
    }

    public void testProjectBasic() {
        assertProjection("""
            from test
            | keep first_name
            """, "first_name");
    }

    public void testProjectBasicPattern() {
        assertProjection("""
            from test
            | keep first*name
            """, "first_name");
        assertProjectionTypes("""
            from test
            | keep first*name
            """, DataTypes.KEYWORD);
    }

    public void testProjectIncludePattern() {
        assertProjection("""
            from test
            | keep *name
            """, "first_name", "last_name");
    }

    public void testProjectIncludeMultiStarPattern() {
        assertProjection("""
            from test
            | keep *t*name
            """, "first_name", "last_name");
    }

    public void testProjectStar() {
        assertProjection("""
            from test
            | keep *
            """, "_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary");
    }

    public void testNoProjection() {
        assertProjection("""
            from test
            """, "_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary");
        assertProjectionTypes(
            """
                from test
                """,
            DataTypes.KEYWORD,
            DataTypes.INTEGER,
            DataTypes.KEYWORD,
            DataTypes.TEXT,
            DataTypes.TEXT,
            DataTypes.KEYWORD,
            DataTypes.INTEGER,
            DataTypes.KEYWORD,
            DataTypes.LONG,
            DataTypes.INTEGER
        );
    }

    public void testProjectOrder() {
        assertProjection("""
            from test
            | keep first_name, *, last_name
            """, "first_name", "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "last_name");
    }

    public void testProjectThenDropName() {
        assertProjection("""
            from test
            | keep *name
            | drop first_name
            """, "last_name");
    }

    public void testProjectAfterDropName() {
        assertProjection("""
            from test
            | drop first_name
            | keep *name
            """, "last_name");
    }

    public void testProjectKeepAndDropName() {
        assertProjection("""
            from test
            | drop first_name
            | keep last_name
            """, "last_name");
    }

    public void testProjectDropPattern() {
        assertProjection("""
            from test
            | keep *
            | drop *_name
            """, "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary");
    }

    public void testProjectDropNoStarPattern() {
        assertProjection("""
            from test
            | drop *_name
            """, "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary");
    }

    public void testProjectOrderPatternWithRest() {
        assertProjection("""
            from test
            | keep *name, *, emp_no
            """, "first_name", "last_name", "_meta_field", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "emp_no");
    }

    public void testProjectDropPatternAndKeepOthers() {
        assertProjection("""
            from test
            | drop l*
            | keep first_name, salary
            """, "first_name", "salary");
    }

    public void testErrorOnNoMatchingPatternInclusion() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | keep *nonExisting
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
            | keep unsupported
            """, "mapping-multi-field-variation.json", "unsupported");
    }

    public void testUnsupportedFieldAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | keep unsupported
            | eval x = unsupported
            """, errorMessage);
    }

    public void testUnsupportedFieldEvalAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | keep unsupported
            | eval x = unsupported + 1
            """, errorMessage);
    }

    public void testUnsupportedFieldFilterAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | keep unsupported
            | where unsupported == null
            """, errorMessage);
    }

    public void testUnsupportedFieldFunctionAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | keep unsupported
            | where length(unsupported) > 0
            """, errorMessage);
    }

    public void testUnsupportedFieldSortAfterProject() {
        var errorMessage = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | keep unsupported
            | sort unsupported
            """, errorMessage);
    }

    public void testIncludeUnsupportedFieldPattern() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | keep un*
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
            """, "_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx");
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
            | rename emp_no as e
            | keep first_name, e
            """, "first_name", "e");
    }

    public void testChainedRename() {
        assertProjection("""
            from test
            | rename emp_no as r1, r1 as r2, r2 as r3
            | keep first_name, r3
            """, "first_name", "r3");
    }

    public void testChainedRenameReuse() {
        assertProjection("""
            from test
            | rename emp_no as r1, r1 as r2, r2 as r3, first_name as r1
            | keep r1, r3
            """, "r1", "r3");
    }

    public void testRenameBackAndForth() {
        assertProjection("""
            from test
            | rename emp_no as r1, r1 as emp_no
            | keep emp_no
            """, "emp_no");
    }

    public void testRenameReuseAlias() {
        assertProjection("""
            from test
            | rename emp_no as e, first_name as e
            """, "_meta_field", "e", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary");
    }

    public void testRenameUnsupportedField() {
        assertProjectionWithMapping("""
            from test
            | rename unsupported as u
            | keep int, u, float
            """, "mapping-multi-field-variation.json", "int", "u", "float");
    }

    public void testRenameUnsupportedFieldChained() {
        assertProjectionWithMapping("""
            from test
            | rename unsupported as u1, u1 as u2
            | keep int, u2, float
            """, "mapping-multi-field-variation.json", "int", "u2", "float");
    }

    public void testRenameUnsupportedAndResolved() {
        assertProjectionWithMapping("""
            from test
            | rename unsupported as u, float as f
            | keep int, u, f
            """, "mapping-multi-field-variation.json", "int", "u", "f");
    }

    public void testRenameUnsupportedSubFieldAndResolved() {
        assertProjectionWithMapping("""
            from test
            | rename some.string as ss, float as f
            | keep int, ss, f
            """, "mapping-multi-field-variation.json", "int", "ss", "f");
    }

    public void testRenameUnsupportedAndUnknown() {
        verifyUnsupported("""
            from test
            | rename text as t, doesnotexist as d
            """, "Found 1 problem\n" + "line 2:21: Unknown column [doesnotexist]");
    }

    public void testRenameResolvedAndUnknown() {
        verifyUnsupported("""
            from test
            | rename int as i, doesnotexist as d
            """, "Found 1 problem\n" + "line 2:20: Unknown column [doesnotexist]");
    }

    public void testUnsupportedFieldUsedExplicitly() {
        assertProjectionWithMapping("""
            from test
            | keep foo_type
            """, "mapping-multi-field-variation.json", "foo_type");
    }

    public void testUnsupportedFieldTypes() {
        assertProjectionWithMapping("""
            from test
            | keep unsigned_long, date, date_nanos, unsupported, point, version
            """, "mapping-multi-field-variation.json", "unsigned_long", "date", "date_nanos", "unsupported", "point", "version");
    }

    public void testUnsupportedDottedFieldUsedExplicitly() {
        assertProjectionWithMapping("""
            from test
            | keep some.string
            """, "mapping-multi-field-variation.json", "some.string");
    }

    public void testUnsupportedParentField() {
        verifyUnsupported(
            """
                from test
                | keep text, text.keyword
                """,
            "Found 1 problem\n" + "line 2:14: Unknown column [text.keyword], did you mean any of [text.wildcard, text.raw]?",
            "mapping-multi-field.json"
        );
    }

    public void testUnsupportedParentFieldAndItsSubField() {
        assertProjectionWithMapping("""
            from test
            | keep text, text.english
            """, "mapping-multi-field.json", "text", "text.english");
    }

    public void testUnsupportedDeepHierarchy() {
        assertProjectionWithMapping("""
            from test
            | keep x.y.z.w, x.y.z, x.y, x
            """, "mapping-multi-field-with-nested.json", "x.y.z.w", "x.y.z", "x.y", "x");
    }

    /**
     * Here x.y.z.v is of type "keyword" but its parent is of unsupported type "foobar".
     */
    public void testUnsupportedValidFieldTypeInDeepHierarchy() {
        assertProjectionWithMapping("""
            from test
            | keep x.y.z.v
            """, "mapping-multi-field-with-nested.json", "x.y.z.v");
    }

    public void testUnsupportedValidFieldTypeInNestedParentField() {
        verifyUnsupported("""
            from test
            | keep dep.dep_id.keyword
            """, "Found 1 problem\n" + "line 2:8: Unknown column [dep.dep_id.keyword]", "mapping-multi-field-with-nested.json");
    }

    public void testUnsupportedObjectAndNested() {
        verifyUnsupported(
            """
                from test
                | keep dep, some
                """,
            "Found 2 problems\n" + "line 2:8: Unknown column [dep]\n" + "line 2:13: Unknown column [some]",
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
            | keep some.dotted.field, some.string.normalized
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
                | keep *some.string*, *, some.ambiguous.two, keyword
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
            | keep keyword, point
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
            | keep d, last_name
            """, "d", "last_name");
    }

    public void testImplicitLimit() {
        var plan = analyze("""
            from test
            """);
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(DEFAULT_LIMIT));
        as(limit.child(), EsRelation.class);
    }

    public void testImplicitMaxLimitAfterLimit() {
        for (int i = -1; i <= 1; i++) {
            var plan = analyze("from test | limit " + (MAX_LIMIT + i));
            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(), equalTo(MAX_LIMIT));
            limit = as(limit.child(), Limit.class);
            as(limit.child(), EsRelation.class);
        }
    }

    /*
    Limit[10000[INTEGER]]
    \_Filter[s{r}#3 > 0[INTEGER]]
      \_Eval[[salary{f}#10 * 10[INTEGER] AS s]]
        \_Limit[10000[INTEGER]]
          \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testImplicitMaxLimitAfterLimitAndNonLimit() {
        for (int i = -1; i <= 1; i++) {
            var plan = analyze("from test | limit " + (MAX_LIMIT + i) + " | eval s = salary * 10 | where s > 0");
            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(), equalTo(MAX_LIMIT));
            var filter = as(limit.child(), Filter.class);
            var eval = as(filter.child(), Eval.class);
            limit = as(eval.child(), Limit.class);
            as(limit.child(), EsRelation.class);
        }
    }

    public void testImplicitDefaultLimitAfterLimitAndBreaker() {
        for (var breaker : List.of("stats c = count(salary) by last_name", "sort salary")) {
            var plan = analyze("from test | limit 100000 | " + breaker);
            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(), equalTo(MAX_LIMIT));
        }
    }

    public void testImplicitDefaultLimitAfterBreakerAndNonBreakers() {
        for (var breaker : List.of("stats c = count(salary) by last_name", "eval c = salary | sort c")) {
            var plan = analyze("from test | " + breaker + " | eval cc = c * 10 | where cc > 0");
            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(), equalTo(DEFAULT_LIMIT));
        }
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
                | keep date
                """.replace("COMPARISON", comparison), "mapping-multi-field-variation.json", "date");
        }
    }

    public void testCompareStringToDate() {
        for (String comparison : COMPARISONS) {
            assertProjectionWithMapping("""
                from test
                | where "1985-01-01T00:00:00Z" COMPARISON date
                | keep date
                """.replace("COMPARISON", comparison), "mapping-multi-field-variation.json", "date");
        }
    }

    public void testCompareDateToStringFails() {
        for (String comparison : COMPARISONS) {
            verifyUnsupported("""
                from test
                | where date COMPARISON "not-a-date"
                | keep date
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
            | eval date_format(1, date)
            """, "first argument of [date_format(1, date)] must be [string], found value [1] type [integer]");
    }

    public void testDateFormatWithDateFormat() {
        verifyUnsupported("""
            from test
            | eval date_format(date, date)
            """, "first argument of [date_format(date, date)] must be [string], found value [date] type [datetime]");
    }

    public void testDateParseOnInt() {
        verifyUnsupported("""
            from test
            | eval date_parse(keyword, int)
            """, "second argument of [date_parse(keyword, int)] must be [string], found value [int] type [integer]");
    }

    public void testDateParseOnDate() {
        verifyUnsupported("""
            from test
            | eval date_parse(keyword, date)
            """, "second argument of [date_parse(keyword, date)] must be [string], found value [date] type [datetime]");
    }

    public void testDateParseOnIntPattern() {
        verifyUnsupported("""
            from test
            | eval date_parse(int, keyword)
            """, "first argument of [date_parse(int, keyword)] must be [string], found value [int] type [integer]");
    }

    public void testDateTruncOnInt() {
        verifyUnsupported("""
            from test
            | eval date_trunc("1M", int)
            """, "first argument of [date_trunc(\"1M\", int)] must be [datetime], found value [int] type [integer]");
    }

    public void testDateTruncOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_trunc("1M", float)
            """, "first argument of [date_trunc(\"1M\", float)] must be [datetime], found value [float] type [double]");
    }

    public void testDateTruncOnText() {
        verifyUnsupported("""
            from test
            | eval date_trunc("1M", keyword)
            """, "first argument of [date_trunc(\"1M\", keyword)] must be [datetime], found value [keyword] type [keyword]");
    }

    public void testDateTruncWithNumericInterval() {
        verifyUnsupported("""
            from test
            | eval date_trunc(1, date)
            """, "second argument of [date_trunc(1, date)] must be [dateperiod or timeduration], found value [1] type [integer]");
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

    public void testEmptyEsRelationOnLimitZeroWithCount() throws IOException {
        var query = """
            from test*
            | stats count=count(*)
            | sort count desc
            | limit 0""";
        var plan = analyzeWithEmptyFieldCapsResponse(query);
        var limit = as(plan, Limit.class);
        limit = as(limit.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(0));
        var orderBy = as(limit.child(), OrderBy.class);
        var agg = as(orderBy.child(), Aggregate.class);
        assertEmptyEsRelation(agg.child());
    }

    public void testEmptyEsRelationOnConstantEvalAndKeep() throws IOException {
        var query = """
            from test*
            | eval c = 1
            | keep c
            | limit 2""";
        var plan = analyzeWithEmptyFieldCapsResponse(query);
        var limit = as(plan, Limit.class);
        limit = as(limit.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(2));
        var project = as(limit.child(), EsqlProject.class);
        var eval = as(project.child(), Eval.class);
        assertEmptyEsRelation(eval.child());
    }

    public void testEmptyEsRelationOnConstantEvalAndStats() throws IOException {
        var query = """
            from test*
            | limit 10
            | eval x = 1
            | stats c = count(x)""";
        var plan = analyzeWithEmptyFieldCapsResponse(query);
        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var eval = as(agg.child(), Eval.class);
        limit = as(eval.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(10));
        assertEmptyEsRelation(limit.child());
    }

    public void testEmptyEsRelationOnCountStar() throws IOException {
        var query = """
            from test*
            | stats c = count(*)""";
        var plan = analyzeWithEmptyFieldCapsResponse(query);
        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertEmptyEsRelation(agg.child());
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
        final String supportedTypes = "boolean, datetime, double, integer, ip, keyword, long, text, unsigned_long or version";
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

    public void testNonExistingEnrichNoMatchField() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich foo
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

    public void testEnrichPolicyMatchFieldName() {
        verifyUnsupported("from test | enrich languages on bar", "Unknown column [bar]");
        verifyUnsupported("from test | enrich languages on keywords", "Unknown column [keywords], did you mean [keyword]?");
        verifyUnsupported("from test | enrich languages on keyword with foo", "Enrich field [foo] not found in enrich policy [languages]");
        verifyUnsupported(
            "from test | enrich languages on keyword with language_namez",
            "Enrich field [language_namez] not found in enrich policy [languages], did you mean [language_name]"
        );
        verifyUnsupported(
            "from test | enrich languages on keyword with x = language_namez",
            "Enrich field [language_namez] not found in enrich policy [languages], did you mean [language_name]"
        );
    }

    public void testEnrichWrongMatchFieldType() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | enrich languages on languages
            | keep first_name, language_name, id
            """));
        assertThat(
            e.getMessage(),
            containsString("Unsupported type [INTEGER] for enrich matching field [languages]; only KEYWORD allowed")
        );
    }

    public void testValidEnrich() {
        assertProjection("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x
            | keep first_name, language_name
            """, "first_name", "language_name");

        assertProjection("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x with language_name
            | keep first_name, language_name
            """, "first_name", "language_name");

        assertProjection("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x with y = language_name
            | keep first_name, y
            """, "first_name", "y");
    }

    public void testEnrichExcludesPolicyKey() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | eval x = to_string(languages)
            | enrich languages on x
            | keep first_name, language_name, id
            """));
        assertThat(e.getMessage(), containsString("Unknown column [id]"));
    }

    public void testChainedEvalFieldsUse() {
        var query = "from test | eval x0 = pow(salary, 1), x1 = pow(x0, 2), x2 = pow(x1, 3)";
        int additionalEvals = randomIntBetween(0, 5);
        for (int i = 0, j = 3; i < additionalEvals; i++, j++) {
            query += ", x" + j + " = pow(x" + (j - 1) + ", " + i + ")";
        }
        assertProjection(query + " | keep x*", IntStream.range(0, additionalEvals + 3).mapToObj(v -> "x" + v).toArray(String[]::new));
    }

    public void testMissingAttributeException_InChainedEval() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | eval x1 = concat(first_name, "."), x2 = concat(x1, last_name), x3 = concat(x2, x1), x4 = concat(x3, x5)
            | keep x*
            """));
        assertThat(e.getMessage(), containsString("Unknown column [x5], did you mean any of [x1, x2, x3]?"));
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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static LogicalPlan analyzeWithEmptyFieldCapsResponse(String query) throws IOException {
        IndexResolution resolution = IndexResolver.mergedMappings(
            EsqlDataTypeRegistry.INSTANCE,
            "test*",
            readFieldCapsResponse("empty_field_caps_response.json"),
            EsqlSession::specificValidity,
            IndexResolver.PRESERVE_PROPERTIES,
            IndexResolver.INDEX_METADATA_FIELD
        );
        var analyzer = analyzer(resolution, TEST_VERIFIER, configuration(query));
        return analyze(query, analyzer);
    }

    private static FieldCapabilitiesResponse readFieldCapsResponse(String resourceName) throws IOException {
        InputStream stream = AnalyzerTests.class.getResourceAsStream("/" + resourceName);
        BytesReference ref = Streams.readFully(stream);
        XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, ref, XContentType.JSON);
        return FieldCapabilitiesResponse.fromXContent(parser);
    }

    private void assertEmptyEsRelation(LogicalPlan plan) {
        assertThat(plan, instanceOf(EsRelation.class));
        EsRelation esRelation = (EsRelation) plan;
        assertThat(esRelation.output(), equalTo(NO_FIELDS));
        assertTrue(esRelation.index().mapping().isEmpty());
    }
}
