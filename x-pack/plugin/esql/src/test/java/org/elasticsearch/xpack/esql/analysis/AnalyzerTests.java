/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.tsdbIndexResolution;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
public class AnalyzerTests extends ESTestCase {

    private static final UnresolvedRelation UNRESOLVED_RELATION = new UnresolvedRelation(
        EMPTY,
        new TableIdentifier(EMPTY, null, "idx"),
        false,
        List.of(),
        IndexMode.STANDARD,
        null
    );

    private static final int MAX_LIMIT = EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY);
    private static final int DEFAULT_LIMIT = EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY);

    public void testIndexResolution() {
        EsIndex idx = new EsIndex("idx", Map.of());
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));
        var plan = analyzer.analyze(UNRESOLVED_RELATION);
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx, NO_FIELDS, IndexMode.STANDARD), limit.child());
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

        assertEquals(new EsRelation(EMPTY, idx, NO_FIELDS, IndexMode.STANDARD), limit.child());
    }

    public void testAttributeResolution() {
        EsIndex idx = new EsIndex("idx", LoadMapping.loadMapping("mapping-one-field.json"));
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
                new Row(EMPTY, List.of(new Alias(EMPTY, "emp_no", new Literal(EMPTY, 1, DataType.INTEGER)))),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertEquals(new Alias(EMPTY, "e", new ReferenceAttribute(EMPTY, "emp_no", DataType.INTEGER)), eval.fields().get(0));

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
            """, DataType.KEYWORD);
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

    public void testEscapedStar() {
        assertProjection("""
            from test
            | eval a = 1, `a*` = 2
            | keep `a*`
            """, "a*");
    }

    public void testEscapeStarPlusPattern() {
        assertProjection("""
            row a = 0, `a*` = 1, `ab*` = 2, `ab*cd` = 3, `abc*de` = 4
            | keep `a*`*, abc*
            """, "a*", "abc*de");
    }

    public void testBacktickPlusPattern() {
        assertProjection("""
            row a = 0, `a``` = 1, `a``b*` = 2, `ab*cd` = 3, `abc*de` = 4
            | keep a*, a````b`*`
            """, "a", "a`", "ab*cd", "abc*de", "a`b*");
    }

    public void testRenameBacktickPlusPattern() {
        assertProjection("""
            row a = 0, `a*` = 1, `ab*` = 2, `ab*cd` = 3, `abc*de` = 4
            | rename `ab*` as `xx*`
            """, "a", "a*", "xx*", "ab*cd", "abc*de");
    }

    public void testNoProjection() {
        assertProjection("""
            from test
            """, "_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary");
        assertProjectionTypes(
            """
                from test
                """,
            DataType.KEYWORD,
            DataType.INTEGER,
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.TEXT,
            DataType.KEYWORD,
            DataType.INTEGER,
            DataType.KEYWORD,
            DataType.LONG,
            DataType.INTEGER
        );
    }

    public void testDuplicateProjections() {
        assertProjection("""
            from test
            | keep first_name, first_name
            """, "first_name");
        assertProjection("""
            from test
            | keep first_name, first_name, last_name, first_name
            """, "last_name", "first_name");
    }

    public void testProjectWildcard() {
        assertProjection("""
            from test
            | keep first_name, *, last_name
            """, "first_name", "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "last_name");
        assertProjection("""
            from test
            | keep first_name, last_name, *
            """, "first_name", "last_name", "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary");
        assertProjection("""
            from test
            | keep *, first_name, last_name
            """, "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "first_name", "last_name");

        var e = expectThrows(ParsingException.class, () -> analyze("""
            from test
            | keep *, first_name, last_name, *
            """));
        assertThat(e.getMessage(), containsString("Cannot specify [*] more than once"));

    }

    public void testProjectMixedWildcard() {
        assertProjection("""
            from test
            | keep *name, first*
            """, "last_name", "first_name");
        assertProjection("""
            from test
            | keep first_name, *name, first*
            """, "first_name", "last_name");
        assertProjection("""
            from test
            | keep *ob*, first_name, *name, first*
            """, "job", "job.raw", "first_name", "last_name");
        assertProjection("""
            from test
            | keep first_name, *, *name
            """, "first_name", "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "last_name");
        assertProjection("""
            from test
            | keep first*, *, last_name, first_name
            """, "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "last_name", "first_name");
        assertProjection("""
            from test
            | keep first*, *, last_name, fir*
            """, "_meta_field", "emp_no", "gender", "job", "job.raw", "languages", "long_noidx", "salary", "last_name", "first_name");
        assertProjection("""
            from test
            | keep *, job*
            """, "_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "long_noidx", "salary", "job", "job.raw");
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
        assertThat(e.getMessage(), containsString("No matches found for pattern [*nonExisting]"));
    }

    public void testErrorOnNoMatchingPatternExclusion() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | drop *nonExisting
            """));
        assertThat(e.getMessage(), containsString("No matches found for pattern [*nonExisting]"));
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
        assertThat(e.getMessage(), containsString("No matches found for pattern [un*]"));
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
            "ip",
            "keyword",
            "long",
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
            "ip",
            "keyword",
            "long",
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
            """, "Found 1 problem\n" + "line 2:8: No matches found for pattern [dep.*]", "mapping-multi-field-with-nested.json");
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
            "ip",
            "keyword",
            "long",
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
            verifyUnsupported(
                """
                    from test
                    | where date COMPARISON "not-a-date"
                    | keep date
                    """.replace("COMPARISON", comparison),
                "Cannot convert string [not-a-date] to [DATETIME]",
                "mapping-multi-field-variation.json"
            );
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
            | eval date_trunc(1 month, int)
            """, "second argument of [date_trunc(1 month, int)] must be [datetime], found value [int] type [integer]");
    }

    public void testDateTruncOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_trunc(1 month, float)
            """, "second argument of [date_trunc(1 month, float)] must be [datetime], found value [float] type [double]");
    }

    public void testDateTruncOnText() {
        verifyUnsupported("""
            from test
            | eval date_trunc(1 month, keyword)
            """, "second argument of [date_trunc(1 month, keyword)] must be [datetime], found value [keyword] type [keyword]");
    }

    public void testDateTruncWithNumericInterval() {
        verifyUnsupported("""
            from test
            | eval date_trunc(1, date)
            """, "first argument of [date_trunc(1, date)] must be [dateperiod or timeduration], found value [1] type [integer]");
    }

    public void testDateTruncWithDateInterval() {
        verifyUnsupported("""
            from test
            | eval date_trunc(date, date)
            """, "first argument of [date_trunc(date, date)] must be [dateperiod or timeduration], found value [date] type [datetime]");
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
        var output = agg.output();
        assertThat(output, hasSize(2));
        assertThat(Expressions.names(output), contains("x", "b"));
        var alias = as(aggregates.get(0), Alias.class);
        var count = as(alias.child(), Count.class);
        alias = as(aggregates.get(1), Alias.class);
        var min = as(alias.child(), Min.class);
        alias = as(aggregates.get(2), Alias.class);
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
        var output = agg.output();
        assertThat(output, hasSize(1));
        assertThat(Expressions.names(output), contains("b"));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_EsqlAggregate[[emp_no{f}#9 + languages{f}#12 AS emp_no + languages],[MIN(emp_no{f}#9 + languages{f}#12) AS min(emp_no + langu
     * ages), emp_no + languages{r}#7]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testAggsOverGroupingKey() throws Exception {
        var plan = analyze("""
            from test
            | stats min(emp_no + languages) by emp_no + languages
            """);
        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var output = agg.output();
        assertThat(output, hasSize(2));
        var aggs = agg.aggregates();
        var min = as(Alias.unwrap(aggs.get(0)), Min.class);
        assertThat(min.arguments(), hasSize(1));
        var group = Alias.unwrap(agg.groupings().get(0));
        assertEquals(min.arguments().get(0), group);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_EsqlAggregate[[emp_no{f}#9 + languages{f}#12 AS a],[MIN(a{r}#7) AS min(a), a{r}#7]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testAggsOverGroupingKeyWithAlias() throws Exception {
        var plan = analyze("""
            from test
            | stats min(a) by a = emp_no + languages
            """);
        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var output = agg.output();
        assertThat(output, hasSize(2));
        var aggs = agg.aggregates();
        var min = as(Alias.unwrap(aggs.get(0)), Min.class);
        assertThat(min.arguments(), hasSize(1));
        assertEquals(Expressions.attribute(min.arguments().get(0)), Expressions.attribute(agg.groupings().get(0)));
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
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | stats max(unsupported)
            """, errorMsg);
        verifyUnsupported("""
            from test
            | stats max(int) by unsupported
            """, errorMsg);
        verifyUnsupported("""
            from test
            | stats max(int) by bool, unsupported
            """, errorMsg);
    }

    public void testUnsupportedFieldsInEval() {
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | eval x = unsupported
            """, errorMsg);
        verifyUnsupported("""
            from test
            | eval foo = 1, x = unsupported
            """, errorMsg);
        verifyUnsupported("""
            from test
            | eval x = 1 + unsupported
            """, errorMsg);
    }

    public void testUnsupportedFieldsInWhere() {
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | where unsupported == "[1.0, 1.0]"
            """, errorMsg);
        verifyUnsupported("""
            from test
            | where int > 2 and unsupported == "[1.0, 1.0]"
            """, errorMsg);
    }

    public void testUnsupportedFieldsInSort() {
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";

        verifyUnsupported("""
            from test
            | sort unsupported
            """, errorMsg);
        verifyUnsupported("""
            from test
            | sort int, unsupported
            """, errorMsg);
    }

    public void testUnsupportedFieldsInDissect() {
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";
        verifyUnsupported("""
            from test
            | dissect unsupported \"%{foo}\"
            """, errorMsg);
    }

    public void testUnsupportedFieldsInGrok() {
        var errorMsg = "Cannot use field [unsupported] with unsupported type [ip_range]";
        verifyUnsupported("""
            from test
            | grok unsupported \"%{WORD:foo}\"
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
        final String supportedTypes = "boolean or cartesian_point or cartesian_shape or date_nanos or datetime "
            + "or geo_point or geo_shape or ip or numeric or string or version";
        verifyUnsupported(
            "row period = 1 year | eval to_string(period)",
            "line 1:28: argument of [to_string(period)] must be [" + supportedTypes + "], found value [period] type [date_period]"
        );
        verifyUnsupported(
            "row duration = 1 hour | eval to_string(duration)",
            "line 1:30: argument of [to_string(duration)] must be [" + supportedTypes + "], found value [duration] type [time_duration]"
        );
        verifyUnsupported(
            "from test | eval to_string(unsupported)",
            "line 1:28: Cannot use field [unsupported] with unsupported type [ip_range]"
        );
    }

    public void testEnrichPolicyWithError() {
        IndexResolution testIndex = loadMapping("mapping-basic.json", "test");
        IndexResolution languageIndex = loadMapping("mapping-languages.json", "languages");
        EnrichResolution enrichResolution = new EnrichResolution();
        Map<String, String> enrichIndices = Map.of("", "languages");
        enrichResolution.addResolvedPolicy(
            "languages",
            Enrich.Mode.COORDINATOR,
            new ResolvedEnrichPolicy(
                "language_code",
                "match",
                List.of("language_code", "language_name"),
                enrichIndices,
                languageIndex.get().mapping()
            )
        );
        enrichResolution.addError("languages", Enrich.Mode.REMOTE, "error-1");
        enrichResolution.addError("languages", Enrich.Mode.ANY, "error-2");
        enrichResolution.addError("foo", Enrich.Mode.ANY, "foo-error-101");

        AnalyzerContext context = new AnalyzerContext(configuration("from test"), new EsqlFunctionRegistry(), testIndex, enrichResolution);
        Analyzer analyzer = new Analyzer(context, TEST_VERIFIER);
        {
            LogicalPlan plan = analyze("from test | EVAL x = to_string(languages) | ENRICH _coordinator:languages ON x", analyzer);
            List<Enrich> resolved = new ArrayList<>();
            plan.forEachDown(Enrich.class, resolved::add);
            assertThat(resolved, hasSize(1));
        }
        var e = expectThrows(
            VerificationException.class,
            () -> analyze("from test | EVAL x = to_string(languages) | ENRICH _any:languages ON x", analyzer)
        );
        assertThat(e.getMessage(), containsString("error-2"));
        e = expectThrows(
            VerificationException.class,
            () -> analyze("from test | EVAL x = to_string(languages) | ENRICH languages ON xs", analyzer)
        );
        assertThat(e.getMessage(), containsString("error-2"));
        e = expectThrows(
            VerificationException.class,
            () -> analyze("from test | EVAL x = to_string(languages) | ENRICH _remote:languages ON x", analyzer)
        );
        assertThat(e.getMessage(), containsString("error-1"));

        e = expectThrows(VerificationException.class, () -> analyze("from test | ENRICH foo", analyzer));
        assertThat(e.getMessage(), containsString("foo-error-101"));
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
            | eval x = to_boolean(languages)
            | enrich languages on x
            | keep first_name, language_name, id
            """));
        assertThat(e.getMessage(), containsString("Unsupported type [boolean] for enrich matching field [x]; only [keyword, "));

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM airports
            | EVAL x = to_string(city_location)
            | ENRICH city_boundaries ON x
            | KEEP abbrev, airport, region
            """, "airports", "mapping-airports.json"));
        assertThat(e.getMessage(), containsString("Unsupported type [keyword] for enrich matching field [x]; only [geo_point, "));
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

        assertProjection(analyze("""
            FROM sample_data
            | ENRICH client_cidr ON client_ip WITH env
            | KEEP client_ip, env
            """, "sample_data", "mapping-sample_data.json"), "client_ip", "env");

        assertProjection(analyze("""
            FROM employees
            | WHERE birth_date > "1960-01-01"
            | EVAL birth_year = DATE_EXTRACT("year", birth_date)
            | EVAL age = 2022 - birth_year
            | ENRICH ages_policy ON age WITH age_group = description
            | KEEP first_name, last_name, age, age_group
            """, "employees", "mapping-default.json"), "first_name", "last_name", "age", "age_group");

        assertProjection(analyze("""
            FROM employees
            | ENRICH heights_policy ON height WITH height_group = description
            | KEEP first_name, last_name, height, height_group
            """, "employees", "mapping-default.json"), "first_name", "last_name", "height", "height_group");

        assertProjection(analyze("""
            FROM employees
            | ENRICH decades_policy ON birth_date WITH birth_decade = decade, birth_description = description
            | ENRICH decades_policy ON hire_date WITH hire_decade = decade
            | KEEP first_name, last_name, birth_decade, hire_decade, birth_description
            """, "employees", "mapping-default.json"), "first_name", "last_name", "birth_decade", "hire_decade", "birth_description");

        assertProjection(analyze("""
            FROM airports
            | WHERE abbrev == "CPH"
            | ENRICH city_boundaries ON city_location WITH airport, region
            | KEEP abbrev, airport, region
            """, "airports", "mapping-airports.json"), "abbrev", "airport", "region");
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

    public void testEnrichFieldsIncludeMatchField() {
        String query = """
            FROM test
            | EVAL x = to_string(languages)
            | ENRICH languages ON x
            | KEEP language_name, language_code
            """;
        IndexResolution testIndex = loadMapping("mapping-basic.json", "test");
        IndexResolution languageIndex = loadMapping("mapping-languages.json", "languages");
        EnrichResolution enrichResolution = new EnrichResolution();
        Map<String, String> enrichIndices = Map.of("", "languages");
        enrichResolution.addResolvedPolicy(
            "languages",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "language_code",
                "match",
                List.of("language_code", "language_name"),
                enrichIndices,
                languageIndex.get().mapping()
            )
        );
        AnalyzerContext context = new AnalyzerContext(configuration(query), new EsqlFunctionRegistry(), testIndex, enrichResolution);
        Analyzer analyzer = new Analyzer(context, TEST_VERIFIER);
        LogicalPlan plan = analyze(query, analyzer);
        var limit = as(plan, Limit.class);
        assertThat(Expressions.names(limit.output()), contains("language_name", "language_code"));
    }

    public void testChainedEvalFieldsUse() {
        var query = "from test | eval x0 = pow(salary, 1), x1 = pow(x0, 2), x2 = pow(x1, 3)";
        int additionalEvals = randomIntBetween(0, 5);
        for (int i = 0, j = 3; i < additionalEvals; i++, j++) {
            query += ", x" + j + " = pow(x" + (j - 1) + ", " + i + ")";
        }
        assertProjection(query + " | keep x*", IntStream.range(0, additionalEvals + 3).mapToObj(v -> "x" + v).toArray(String[]::new));
    }

    public void testCounterTypes() {
        var query = "FROM test | KEEP network.* | LIMIT 10";
        Analyzer analyzer = analyzer(tsdbIndexResolution());
        LogicalPlan plan = analyze(query, analyzer);
        var limit = as(plan, Limit.class);
        var attributes = limit.output().stream().collect(Collectors.toMap(NamedExpression::name, a -> a));
        assertThat(
            attributes.keySet(),
            equalTo(Set.of("network.connections", "network.bytes_in", "network.bytes_out", "network.message_in"))
        );
        assertThat(attributes.get("network.connections").dataType(), equalTo(DataType.LONG));
        assertThat(attributes.get("network.bytes_in").dataType(), equalTo(DataType.COUNTER_LONG));
        assertThat(attributes.get("network.bytes_out").dataType(), equalTo(DataType.COUNTER_LONG));
        assertThat(attributes.get("network.message_in").dataType(), equalTo(DataType.COUNTER_DOUBLE));
    }

    public void testMissingAttributeException_InChainedEval() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | eval x1 = concat(first_name, "."), x2 = concat(x1, last_name), x3 = concat(x2, x1), x4 = concat(x3, x5)
            | keep x*
            """));
        assertThat(e.getMessage(), containsString("Unknown column [x5], did you mean any of [x1, x2, x3]?"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103599")
    public void testInsensitiveEqualsWrongType() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | where first_name =~ 12
            """));
        assertThat(
            e.getMessage(),
            containsString("second argument of [first_name =~ 12] must be [string], found value [12] type [integer]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | where first_name =~ languages
            """));
        assertThat(
            e.getMessage(),
            containsString("second argument of [first_name =~ languages] must be [string], found value [languages] type [integer]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | where languages =~ "foo"
            """));
        assertThat(
            e.getMessage(),
            containsString("first argument of [languages =~ \"foo\"] must be [string], found value [languages] type [integer]")
        );
    }

    public void testUnresolvedMvExpand() {
        var e = expectThrows(VerificationException.class, () -> analyze("row foo = 1 | mv_expand bar"));
        assertThat(e.getMessage(), containsString("Unknown column [bar]"));
    }

    public void testRegularStats() {
        var plan = analyze("""
            from tests
            | stats by salary
            """);

        var limit = as(plan, Limit.class);
    }

    public void testLiteralInAggregateNoGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats 1
            """));

        assertThat(e.getMessage(), containsString("expected an aggregate function but found [1]"));
    }

    public void testLiteralBehindEvalInAggregateNoGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |eval x = 1
            |stats x
            """));

        assertThat(e.getMessage(), containsString("column [x] must appear in the STATS BY clause or be used in an aggregate function"));
    }

    public void testLiteralsInAggregateNoGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats 1 + 2
            """));

        assertThat(e.getMessage(), containsString("expected an aggregate function but found [1 + 2]"));
    }

    public void testLiteralsBehindEvalInAggregateNoGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |eval x = 1 + 2
            |stats x
            """));

        assertThat(e.getMessage(), containsString("column [x] must appear in the STATS BY clause or be used in an aggregate function"));
    }

    public void testFoldableInAggregateWithGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats 1 + 2 by languages
            """));

        assertThat(e.getMessage(), containsString("expected an aggregate function but found [1 + 2]"));
    }

    public void testLiteralsInAggregateWithGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats "a" by languages
            """));

        assertThat(e.getMessage(), containsString("expected an aggregate function but found [\"a\"]"));
    }

    public void testFoldableBehindEvalInAggregateWithGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |eval x = 1 + 2
            |stats x by languages
            """));

        assertThat(e.getMessage(), containsString("column [x] must appear in the STATS BY clause or be used in an aggregate function"));
    }

    public void testFoldableInGrouping() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats x by 1
            """));

        assertThat(e.getMessage(), containsString("Unknown column [x]"));
    }

    public void testScalarFunctionsInStats() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
            |stats salary % 3 by languages
            """));

        assertThat(
            e.getMessage(),
            containsString("column [salary] must appear in the STATS BY clause or be used in an aggregate function")
        );
    }

    public void testDeferredGroupingInStats() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test
             |eval x = first_name
             |stats x by first_name
            """));

        assertThat(e.getMessage(), containsString("column [x] must appear in the STATS BY clause or be used in an aggregate function"));
    }

    public void testUnsupportedTypesInStats() {
        verifyUnsupported("""
              row x = to_unsigned_long(\"10\")
              | stats  avg(x), count_distinct(x), max(x), median(x), median_absolute_deviation(x), min(x), percentile(x, 10), sum(x)
            """, """
            Found 8 problems
            line 2:12: argument of [avg(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:20: argument of [count_distinct(x)] must be [any exact type except unsigned_long, _source, or counter types],\
             found value [x] type [unsigned_long]
            line 2:39: argument of [max(x)] must be [representable except unsigned_long and spatial types],\
             found value [x] type [unsigned_long]
            line 2:47: argument of [median(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:58: argument of [median_absolute_deviation(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:88: argument of [min(x)] must be [representable except unsigned_long and spatial types],\
             found value [x] type [unsigned_long]
            line 2:96: first argument of [percentile(x, 10)] must be [numeric except unsigned_long],\
             found value [x] type [unsigned_long]
            line 2:115: argument of [sum(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]""");

        verifyUnsupported("""
            row x = to_version("1.2")
            | stats  avg(x), median(x), median_absolute_deviation(x), percentile(x, 10), sum(x)
            """, """
            Found 5 problems
            line 2:10: argument of [avg(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:18: argument of [median(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:29: argument of [median_absolute_deviation(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:59: first argument of [percentile(x, 10)] must be [numeric except unsigned_long], found value [x] type [version]
            line 2:78: argument of [sum(x)] must be [numeric except unsigned_long or counter types], found value [x] type [version]""");
    }

    public void testInOnText() {
        assertProjectionWithMapping("""
            from a_index
            | eval text in (\"a\", \"b\", \"c\")
            | keep text
            """, "mapping-multi-field-variation.json", "text");

        assertProjectionWithMapping("""
            from a_index
            | eval text in (\"a\", \"b\", \"c\", text)
            | keep text
            """, "mapping-multi-field-variation.json", "text");

        assertProjectionWithMapping("""
            from a_index
            | eval text not in (\"a\", \"b\", \"c\")
            | keep text
            """, "mapping-multi-field-variation.json", "text");

        assertProjectionWithMapping("""
            from a_index
            | eval text not in (\"a\", \"b\", \"c\", text)
            | keep text
            """, "mapping-multi-field-variation.json", "text");
    }

    public void testMvAppendValidation() {
        String[][] fields = {
            { "bool", "boolean" },
            { "int", "integer" },
            { "unsigned_long", "unsigned_long" },
            { "float", "double" },
            { "text", "text" },
            { "keyword", "keyword" },
            { "date", "datetime" },
            { "point", "geo_point" },
            { "shape", "geo_shape" },
            { "long", "long" },
            { "ip", "ip" },
            { "version", "version" } };

        Supplier<Integer> supplier = () -> randomInt(fields.length - 1);
        int first = supplier.get();
        int second = randomValueOtherThan(first, supplier);

        String signature = "mv_append(" + fields[first][0] + ", " + fields[second][0] + ")";
        verifyUnsupported(
            " from test | eval " + signature,
            "second argument of ["
                + signature
                + "] must be ["
                + fields[first][1]
                + "], found value ["
                + fields[second][0]
                + "] type ["
                + fields[second][1]
                + "]"
        );
    }

    public void testLookup() {
        String query = """
              FROM test
            | RENAME languages AS int
            | LOOKUP int_number_names ON int
            """;
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        LogicalPlan plan = analyze(query);
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(1000));

        var lookup = as(limit.child(), Lookup.class);
        assertThat(lookup.tableName().fold(), equalTo("int_number_names"));
        assertMap(lookup.matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(
            lookup.localRelation().output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("int{f}")).item(startsWith("name{f}"))
        );

        var project = as(lookup.child(), EsqlProject.class);
        assertThat(project.projections().stream().map(Object::toString).toList(), hasItem(matchesRegex("languages\\{f}#\\d+ AS int#\\d+")));

        var esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.index().name(), equalTo("test"));

        // Lookup's output looks sensible too
        assertMap(
            lookup.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                // TODO prune unused columns down through the join
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                /*
                 * Int is a reference here because we renamed it in project.
                 * If we hadn't it'd be a field and that'd be fine.
                 */
                .item(containsString("int{r}"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
                /*
                 * It's important that name is returned as a *reference* here
                 * instead of a field. If it were a field we'd use SearchStats
                 * on it and discover that it doesn't exist in the index. It doesn't!
                 * We don't expect it to. It exists only in the lookup table.
                 */
                .item(containsString("name{r}"))
        );
    }

    public void testLookupMissingField() {
        String query = """
              FROM test
            | LOOKUP int_number_names ON garbage
            """;
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 2:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("Unknown column in lookup target [garbage]"));
    }

    public void testLookupMissingTable() {
        String query = """
              FROM test
            | LOOKUP garbage ON a
            """;
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 2:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("Unknown table [garbage]"));
    }

    public void testLookupMatchTypeWrong() {
        String query = """
              FROM test
            | RENAME last_name AS int
            | LOOKUP int_number_names ON int
            """;
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("column type mismatch, table column was [integer] and original column was [keyword]"));
    }

    public void testImplicitCasting() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = concat("2024", "-04", "-01") + 1 day
            """));

        assertThat(
            e.getMessage(),
            containsString("first argument of [concat(\"2024\", \"-04\", \"-01\") + 1 day] must be [datetime or numeric]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = to_string(null) - 1 day
            """));

        assertThat(e.getMessage(), containsString("first argument of [to_string(null) - 1 day] must be [datetime or numeric]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = concat("2024", "-04", "-01") + "1 day"
            """));

        assertThat(
            e.getMessage(),
            containsString("first argument of [concat(\"2024\", \"-04\", \"-01\") + \"1 day\"] must be [datetime or numeric]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = 1 year - "2024-01-01" + 1 day
            """));

        assertThat(
            e.getMessage(),
            containsString(
                "arguments are in unsupported order: cannot subtract a [DATETIME] value [\"2024-01-01\"] "
                    + "from a [DATE_PERIOD] amount [1 year]"
            )
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = "2024-01-01" - 1 day - "2023-12-31"
            """));

        assertThat(e.getMessage(), containsString("[-] has arguments with incompatible types [datetime] and [datetime]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = "2024-01-01" - 1 day + "2023-12-31"
            """));

        assertThat(e.getMessage(), containsString("[+] has arguments with incompatible types [datetime] and [datetime]"));
    }

    public void testRateRequiresCounterTypes() {
        assumeTrue("rate requires snapshot builds", Build.current().isSnapshot());
        Analyzer analyzer = analyzer(tsdbIndexResolution());
        var query = "METRICS test avg(rate(network.connections))";
        VerificationException error = expectThrows(VerificationException.class, () -> analyze(query, analyzer));
        assertThat(
            error.getMessage(),
            containsString(
                "first argument of [rate(network.connections)] must be"
                    + " [counter_long, counter_integer or counter_double], found value [network.connections] type [long]"
            )
        );
    }

    public void testCoalesceWithMixedNumericTypes() {
        LogicalPlan plan = analyze("""
            from test
            | eval x = coalesce(salary_change, null, 0), y = coalesce(languages, null, 0), z = coalesce(languages.long, null, 0)
            , w = coalesce(salary_change, null, 0::long)
            | keep x, y, z, w
            """, "mapping-default.json");
        var limit = as(plan, Limit.class);
        var esqlProject = as(limit.child(), EsqlProject.class);
        List<?> projections = esqlProject.projections();
        var projection = as(projections.get(0), ReferenceAttribute.class);
        assertEquals(projection.name(), "x");
        assertEquals(projection.dataType(), DataType.DOUBLE);
        projection = as(projections.get(1), ReferenceAttribute.class);
        assertEquals(projection.name(), "y");
        assertEquals(projection.dataType(), DataType.INTEGER);
        projection = as(projections.get(2), ReferenceAttribute.class);
        assertEquals(projection.name(), "z");
        assertEquals(projection.dataType(), DataType.LONG);
        projection = as(projections.get(3), ReferenceAttribute.class);
        assertEquals(projection.name(), "w");
        assertEquals(projection.dataType(), DataType.DOUBLE);
        assertThat(limit.limit().fold(), equalTo(1000));
    }

    private void verifyUnsupported(String query, String errorMessage) {
        verifyUnsupported(query, errorMessage, "mapping-multi-field-variation.json");
    }

    private void verifyUnsupported(String query, String errorMessage, String mappingFileName) {
        var e = expectThrows(VerificationException.class, () -> analyze(query, mappingFileName));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private void assertProjection(String query, String... names) {
        assertProjection(analyze(query), names);
    }

    private void assertProjection(LogicalPlan plan, String... names) {
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
        List<FieldCapabilitiesIndexResponse> idxResponses = List.of(new FieldCapabilitiesIndexResponse("idx", "idx", Map.of(), true));
        FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(idxResponses, List.of());
        IndexResolution resolution = new IndexResolver(null).mergedMappings("test*", caps);
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

    @Override
    protected IndexAnalyzers createDefaultIndexAnalyzers() {
        return super.createDefaultIndexAnalyzers();
    }
}
