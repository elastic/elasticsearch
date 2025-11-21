/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.inference.CompletionFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.function.vector.Magnitude;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.fuse.FuseScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getAttributeByName;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.TEXT_EMBEDDING_INFERENCE_ID;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzerDefaultMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexWithDateDateNanosUnionType;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.randomInferenceIdOtherThan;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.tsdbIndexResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.unresolvedRelation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql.analysis:TRACE", reason = "debug")
/**
 * Parses a plan, builds an AST for it, runs logical analysis.
 * So if we don't error out in the process, analysis was successful
 * Use this class if you want to test analysis phase
 * and especially if you expect to get a VerificationException during analysis
 */
public class AnalyzerTests extends ESTestCase {

    private static final UnresolvedRelation UNRESOLVED_RELATION = unresolvedRelation("idx");
    private static final int MAX_LIMIT = AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY);
    private static final int DEFAULT_LIMIT = AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY);
    private static final int DEFAULT_TIMESERIES_LIMIT = AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(
        Settings.EMPTY
    );

    public void testIndexResolution() {
        EsIndex idx = EsIndexGenerator.esIndex("idx");
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));
        var plan = analyzer.analyze(UNRESOLVED_RELATION);
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx.name(), IndexMode.STANDARD, idx.indexNameWithModes(), NO_FIELDS), limit.child());
    }

    public void testFailOnUnresolvedIndex() {
        Analyzer analyzer = analyzer(Map.of(new IndexPattern(Source.EMPTY, "idx"), IndexResolution.invalid("Unknown index [idx]")));

        VerificationException e = expectThrows(VerificationException.class, () -> analyzer.analyze(UNRESOLVED_RELATION));

        assertThat(e.getMessage(), containsString("Unknown index [idx]"));
    }

    public void testIndexWithClusterResolution() {
        EsIndex idx = EsIndexGenerator.esIndex("cluster:idx");
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(unresolvedRelation("cluster:idx"));
        var limit = as(plan, Limit.class);

        assertEquals(new EsRelation(EMPTY, idx.name(), IndexMode.STANDARD, idx.indexNameWithModes(), NO_FIELDS), limit.child());
    }

    public void testAttributeResolution() {
        EsIndex idx = EsIndexGenerator.esIndex("idx", LoadMapping.loadMapping("mapping-one-field.json"));
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(EMPTY, UNRESOLVED_RELATION, List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no"))))
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertThat(
            eval.fields().get(0),
            equalToIgnoringIds(new Alias(EMPTY, "e", new FieldAttribute(EMPTY, "emp_no", idx.mapping().get("emp_no"))))
        );

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
        EsIndex idx = EsIndexGenerator.esIndex("idx");
        Analyzer analyzer = analyzer(IndexResolution.valid(idx));

        var plan = analyzer.analyze(
            new Eval(
                EMPTY,
                new Row(EMPTY, List.of(new Alias(EMPTY, "emp_no", new Literal(EMPTY, 1, INTEGER)))),
                List.of(new Alias(EMPTY, "e", new UnresolvedAttribute(EMPTY, "emp_no")))
            )
        );

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertThat(
            eval.fields().get(0),
            equalToIgnoringIds(new Alias(EMPTY, "e", new ReferenceAttribute(EMPTY, "emp_no", DataType.INTEGER)))
        );

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
        assertProjection(
            """
                from test
                | keep *
                """,
            "_meta_field",
            "emp_no",
            "first_name",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "last_name",
            "long_noidx",
            "salary"
        );
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
        assertProjection(
            """
                from test
                """,
            "_meta_field",
            "emp_no",
            "first_name",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "last_name",
            "long_noidx",
            "salary"
        );
        assertProjectionTypes(
            """
                from test
                """,
            DataType.KEYWORD,
            INTEGER,
            DataType.KEYWORD,
            DataType.TEXT,
            DATETIME,
            DataType.TEXT,
            DataType.KEYWORD,
            INTEGER,
            DataType.KEYWORD,
            DataType.LONG,
            INTEGER
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
        assertProjection(
            """
                from test
                | keep first_name, *, last_name
                """,
            "first_name",
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "last_name"
        );
        assertProjection(
            """
                from test
                | keep first_name, last_name, *
                """,
            "first_name",
            "last_name",
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary"
        );
        assertProjection(
            """
                from test
                | keep *, first_name, last_name
                """,
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "first_name",
            "last_name"
        );

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
        assertProjection(
            """
                from test
                | keep first_name, *, *name
                """,
            "first_name",
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "last_name"
        );
        assertProjection(
            """
                from test
                | keep first*, *, last_name, first_name
                """,
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "last_name",
            "first_name"
        );
        assertProjection(
            """
                from test
                | keep first*, *, last_name, fir*
                """,
            "_meta_field",
            "emp_no",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "last_name",
            "first_name"
        );
        assertProjection(
            """
                from test
                | keep *, job*
                """,
            "_meta_field",
            "emp_no",
            "first_name",
            "gender",
            "hire_date",
            "languages",
            "last_name",
            "long_noidx",
            "salary",
            "job",
            "job.raw"
        );
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
            """, "_meta_field", "emp_no", "gender", "hire_date", "job", "job.raw", "languages", "long_noidx", "salary");
    }

    public void testProjectDropNoStarPattern() {
        assertProjection("""
            from test
            | drop *_name
            """, "_meta_field", "emp_no", "gender", "hire_date", "job", "job.raw", "languages", "long_noidx", "salary");
    }

    public void testProjectOrderPatternWithRest() {
        assertProjection(
            """
                from test
                | keep *name, *, emp_no
                """,
            "first_name",
            "last_name",
            "_meta_field",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "long_noidx",
            "salary",
            "emp_no"
        );
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
            """, "_meta_field", "emp_no", "first_name", "gender", "hire_date", "job", "job.raw", "languages", "last_name", "long_noidx");
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
            """, "_meta_field", "e", "gender", "hire_date", "job", "job.raw", "languages", "last_name", "long_noidx", "salary");
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
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        as(limit.child(), EsRelation.class);
    }

    public void testImplicitMaxLimitAfterLimit() {
        for (int i = -1; i <= 1; i++) {
            var plan = analyze("from test | limit " + (MAX_LIMIT + i));
            var limit = as(plan, Limit.class);
            assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
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
            assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
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
            assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
        }
    }

    public void testImplicitDefaultLimitAfterBreakerAndNonBreakers() {
        for (var breaker : List.of("stats c = count(salary) by last_name", "eval c = salary | sort c")) {
            var plan = analyze("from test | " + breaker + " | eval cc = c * 10 | where cc > 0");
            var limit = as(plan, Limit.class);
            assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
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
            """, "first argument of [date_format(int)] must be [datetime or date_nanos], found value [int] type [integer]");
    }

    public void testDateFormatOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_format(float)
            """, "first argument of [date_format(float)] must be [datetime or date_nanos], found value [float] type [double]");
    }

    public void testDateFormatOnText() {
        verifyUnsupported("""
            from test
            | eval date_format(keyword)
            """, "first argument of [date_format(keyword)] must be [datetime or date_nanos], found value [keyword] type [keyword]");
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
            """, "second argument of [date_trunc(1 month, int)] must be [date_nanos or datetime], found value [int] type [integer]");
    }

    public void testDateTruncOnFloat() {
        verifyUnsupported("""
            from test
            | eval date_trunc(1 month, float)
            """, "second argument of [date_trunc(1 month, float)] must be [date_nanos or datetime], found value [float] type [double]");
    }

    public void testDateTruncOnText() {
        verifyUnsupported(
            """
                from test
                | eval date_trunc(1 month, keyword)
                """,
            "second argument of [date_trunc(1 month, keyword)] must be [date_nanos or datetime], found value [keyword] type [keyword]"
        );
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
        assertThat(min.arguments(), hasSize(3));    // field + filter + window
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
        assertThat(min.arguments(), hasSize(3));    // field + filter
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
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(0));
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
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(2));
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
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(10));
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
        final String supportedTypes = "aggregate_metric_double or boolean or cartesian_point or cartesian_shape or date_nanos or datetime "
            + "or dense_vector or geo_point or geo_shape or geohash or geohex or geotile or ip or numeric or string or version";
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

        AnalyzerContext context = testAnalyzerContext(
            configuration("from test"),
            new EsqlFunctionRegistry(),
            indexResolutions(testIndex),
            enrichResolution,
            emptyInferenceResolution()
        );
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
        AnalyzerContext context = testAnalyzerContext(
            configuration(query),
            new EsqlFunctionRegistry(),
            indexResolutions(testIndex),
            enrichResolution,
            emptyInferenceResolution()
        );
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
            equalTo(Set.of("network.connections", "network.bytes_in", "network.bytes_out", "network.message_in", "network.message_out"))
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
            from test
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
            Found 6 problems
            line 2:12: argument of [avg(x)] must be [aggregate_metric_double,\
             exponential_histogram or numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:20: argument of [count_distinct(x)] must be [any exact type except unsigned_long, _source, or counter types],\
             found value [x] type [unsigned_long]
            line 2:47: argument of [median(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:58: argument of [median_absolute_deviation(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]
            line 2:96: first argument of [percentile(x, 10)] must be [exponential_histogram or numeric except unsigned_long],\
             found value [x] type [unsigned_long]
            line 2:115: argument of [sum(x)] must be [aggregate_metric_double,\
             exponential_histogram or numeric except unsigned_long or counter types],\
             found value [x] type [unsigned_long]""");

        verifyUnsupported("""
            row x = to_version("1.2")
            | stats  avg(x), median(x), median_absolute_deviation(x), percentile(x, 10), sum(x)
            """, """
            Found 5 problems
            line 2:10: argument of [avg(x)] must be [aggregate_metric_double,\
             exponential_histogram or numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:18: argument of [median(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:29: argument of [median_absolute_deviation(x)] must be [numeric except unsigned_long or counter types],\
             found value [x] type [version]
            line 2:59: first argument of [percentile(x, 10)] must be [exponential_histogram or numeric except unsigned_long],\
             found value [x] type [version]
            line 2:78: argument of [sum(x)] must be [aggregate_metric_double,\
             exponential_histogram or numeric except unsigned_long or counter types],\
             found value [x] type [version]""");
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
        Function<String, String> noText = (type) -> type.equals("text") ? "keyword" : type;
        assumeTrue(
            "Ignore tests with TEXT and KEYWORD combinations because they are now valid",
            noText.apply(fields[first][0]).equals(noText.apply(fields[second][0])) == false
        );

        String signature = "mv_append(" + fields[first][0] + ", " + fields[second][0] + ")";
        verifyUnsupported(
            " from test | eval " + signature,
            "second argument of ["
                + signature
                + "] must be ["
                + noText.apply(fields[first][1])
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
            | LOOKUP_🐔 int_number_names ON int
            """;
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:3: mismatched input 'LOOKUP_🐔' expecting {"));
            return;
        }
        LogicalPlan plan = analyze(query);
        var limit = as(plan, Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(1000));

        var lookup = as(limit.child(), Lookup.class);
        assertThat(as(lookup.tableName(), Literal.class).value(), equalTo(BytesRefs.toBytesRef("int_number_names")));
        assertMap(lookup.matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(
            lookup.localRelation().output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("int{f}")).item(startsWith("name{f}"))
        );

        var project = as(lookup.child(), EsqlProject.class);
        assertThat(project.projections().stream().map(Object::toString).toList(), hasItem(matchesRegex("languages\\{f}#\\d+ AS int#\\d+")));

        var esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // Lookup's output looks sensible too
        assertMap(
            lookup.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                // TODO prune unused columns down through the join
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("hire_date{f}"))
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
                 * As is the name column from the right side.
                 */
                .item(containsString("name{f}"))
        );
    }

    public void testLookupMissingField() {
        String query = """
              FROM test
            | LOOKUP_🐔 int_number_names ON garbage
            """;
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 2:3: mismatched input 'LOOKUP_🐔' expecting {"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("Unknown column in lookup target [garbage]"));
    }

    public void testLookupMissingTable() {
        String query = """
              FROM test
            | LOOKUP_🐔 garbage ON a
            """;
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 2:3: mismatched input 'LOOKUP_🐔' expecting {"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("Unknown table [garbage]"));
    }

    public void testLookupMatchTypeWrong() {
        String query = """
              FROM test
            | RENAME last_name AS int
            | LOOKUP_🐔 int_number_names ON int
            """;
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:3: mismatched input 'LOOKUP_🐔' expecting {"));
            return;
        }
        var e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString("column type mismatch, table column was [integer] and original column was [keyword]"));
    }

    public void testLookupJoinUnknownIndex() {
        String errorMessage = "Unknown index [foobar]";
        IndexResolution missingLookupIndex = IndexResolution.invalid(errorMessage);

        Analyzer analyzerMissingLookupIndex = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                analyzerDefaultMapping(),
                Map.of("foobar", missingLookupIndex),
                defaultEnrichResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        String query = "FROM test | LOOKUP JOIN foobar ON last_name";

        VerificationException e = expectThrows(VerificationException.class, () -> analyze(query, analyzerMissingLookupIndex));
        assertThat(e.getMessage(), containsString("1:25: " + errorMessage));

        String query2 = "FROM test | LOOKUP JOIN foobar ON missing_field";

        e = expectThrows(VerificationException.class, () -> analyze(query2, analyzerMissingLookupIndex));
        assertThat(e.getMessage(), containsString("1:25: " + errorMessage));
        assertThat(e.getMessage(), not(containsString("[missing_field]")));
    }

    public void testLookupJoinUnknownField() {
        String query = "FROM test | LOOKUP JOIN languages_lookup ON last_name";
        String errorMessage = "1:45: Unknown column [last_name] in right side of join";

        VerificationException e = expectThrows(VerificationException.class, () -> analyze(query));
        assertThat(e.getMessage(), containsString(errorMessage));

        String query2 = "FROM test | LOOKUP JOIN languages_lookup ON language_code";
        String errorMessage2 = "1:45: Unknown column [language_code] in left side of join";

        e = expectThrows(VerificationException.class, () -> analyze(query2));
        assertThat(e.getMessage(), containsString(errorMessage2));

        String query3 = "FROM test | LOOKUP JOIN languages_lookup ON missing_altogether";
        String errorMessage3 = "1:45: Unknown column [missing_altogether] in ";

        e = expectThrows(VerificationException.class, () -> analyze(query3));
        assertThat(e.getMessage(), containsString(errorMessage3 + "left side of join"));
        assertThat(e.getMessage(), containsString(errorMessage3 + "right side of join"));
    }

    public void testMultipleLookupJoinsGiveDifferentAttributes() {
        // The field attributes that get contributed by different LOOKUP JOIN commands must have different name ids,
        // even if they have the same names. Otherwise, things like dependency analysis - like in PruneColumns - cannot work based on
        // name ids and shadowing semantics proliferate into all kinds of optimizer code.

        String query = "FROM test"
            + "| EVAL language_code = languages"
            + "| LOOKUP JOIN languages_lookup ON language_code"
            + "| LOOKUP JOIN languages_lookup ON language_code";
        LogicalPlan analyzedPlan = analyze(query);

        List<AttributeSet> lookupFields = new ArrayList<>();
        List<Set<String>> lookupFieldNames = new ArrayList<>();
        analyzedPlan.forEachUp(EsRelation.class, esRelation -> {
            if (esRelation.indexMode() == IndexMode.LOOKUP) {
                lookupFields.add(esRelation.outputSet());
                lookupFieldNames.add(esRelation.outputSet().stream().map(NamedExpression::name).collect(Collectors.toSet()));
            }
        });

        assertEquals(lookupFieldNames.size(), 2);
        assertEquals(lookupFieldNames.get(0), lookupFieldNames.get(1));

        assertEquals(lookupFields.size(), 2);
        AttributeSet intersection = lookupFields.get(0).intersect(lookupFields.get(1));
        assertEquals(AttributeSet.EMPTY, intersection);
    }

    // Lookup modes are now tested on index resulution

    public void testImplicitCasting() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = concat("2024", "-04", "-01") + 1 day
            """));

        assertThat(
            e.getMessage(),
            containsString("first argument of [concat(\"2024\", \"-04\", \"-01\") + 1 day] must be [date_nanos, datetime or numeric]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = to_string(null) - 1 day
            """));

        assertThat(e.getMessage(), containsString("first argument of [to_string(null) - 1 day] must be [date_nanos, datetime or numeric]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
             from test | eval x = concat("2024", "-04", "-01") + "1 day"
            """));

        assertThat(
            e.getMessage(),
            containsString("first argument of [concat(\"2024\", \"-04\", \"-01\") + \"1 day\"] must be [date_nanos, datetime or numeric]")
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

    public void testDenseVectorImplicitCastingKnn() {
        checkDenseVectorCastingHexKnn("float_vector");
        checkDenseVectorCastingKnn("float_vector");
        checkDenseVectorCastingKnn("byte_vector");
        checkDenseVectorCastingHexKnn("byte_vector");
        checkDenseVectorEvalCastingKnn("byte_vector");
        checkDenseVectorCastingKnn("bit_vector");
        checkDenseVectorCastingHexKnn("bit_vector");
        checkDenseVectorEvalCastingKnn("bit_vector");
    }

    private static void checkDenseVectorCastingKnn(String fieldName) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | where knn(%s, [0, 1, 2])
            """, fieldName), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        var conversion = as(knn.query(), ToDenseVector.class);
        var literal = as(conversion.field(), Literal.class);
        assertThat(literal.value(), equalTo(List.of(0, 1, 2)));
    }

    private static void checkDenseVectorCastingHexKnn(String fieldName) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | where knn(%s, "000102")
            """, fieldName), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        var queryVector = as(knn.query(), Literal.class);
        assertEquals(DataType.DENSE_VECTOR, queryVector.dataType());
        assertThat(queryVector.value(), equalTo(List.of(0.0f, 1.0f, 2.0f)));
    }

    private static void checkDenseVectorEvalCastingKnn(String fieldName) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | eval query = to_dense_vector([0, 1, 2]) | where knn(%s, query)
            """, fieldName), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        var queryVector = as(knn.query(), ReferenceAttribute.class);
        assertEquals(DataType.DENSE_VECTOR, queryVector.dataType());
        assertThat(queryVector.name(), is("query"));
    }

    public void testDenseVectorImplicitCastingKnnQueryParams() {
        checkDenseVectorCastingKnnQueryParams("float_vector");
        checkDenseVectorCastingKnnQueryParams("byte_vector");
        checkDenseVectorCastingKnnQueryParams("bit_vector");
    }

    private void checkDenseVectorCastingKnnQueryParams(String fieldName) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | where knn(%s, ?query_vector)
            """, fieldName), "mapping-dense_vector.json", new QueryParams(List.of(paramAsConstant("query_vector", List.of(0, 1, 2)))));

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        var queryVector = as(knn.query(), ToDenseVector.class);
        var literal = as(queryVector.field(), Literal.class);
        assertThat(literal.value(), equalTo(List.of(0, 1, 2)));
    }

    public void testDenseVectorImplicitCastingSimilarityFunctions() {
        if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorImplicitCastingSimilarityFunction(
                "v_cosine(float_vector, [0.342, 0.164, 0.234])",
                List.of(0.342, 0.164, 0.234)
            );
            checkDenseVectorImplicitCastingSimilarityFunction("v_cosine(byte_vector, [1, 2, 3])", List.of(1, 2, 3));
        }
        if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorImplicitCastingSimilarityFunction(
                "v_dot_product(float_vector, [0.342, 0.164, 0.234])",
                List.of(0.342, 0.164, 0.234)
            );
            checkDenseVectorImplicitCastingSimilarityFunction("v_dot_product(byte_vector, [1, 2, 3])", List.of(1, 2, 3));
        }
        if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorImplicitCastingSimilarityFunction(
                "v_l1_norm(float_vector, [0.342, 0.164, 0.234])",
                List.of(0.342, 0.164, 0.234)
            );
            checkDenseVectorImplicitCastingSimilarityFunction("v_l1_norm(byte_vector, [1, 2, 3])", List.of(1, 2, 3));
        }
        if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorImplicitCastingSimilarityFunction(
                "v_l2_norm(float_vector, [0.342, 0.164, 0.234])",
                List.of(0.342, 0.164, 0.234)
            );
            checkDenseVectorImplicitCastingSimilarityFunction("v_l2_norm(float_vector, [1, 2, 3])", List.of(1, 2, 3));
            checkDenseVectorImplicitCastingSimilarityFunction("v_l2_norm(byte_vector, [1, 2, 3])", List.of(1, 2, 3));
            if (EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE_BIT_ELEMENTS.isEnabled()) {
                checkDenseVectorImplicitCastingSimilarityFunction("v_l2_norm(bit_vector, [1, 2])", List.of(1, 2));
            }
        }
        if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorImplicitCastingSimilarityFunction(
                "v_hamming(byte_vector, [0.342, 0.164, 0.234])",
                List.of(0.342, 0.164, 0.234)
            );
            checkDenseVectorImplicitCastingSimilarityFunction("v_hamming(byte_vector, [1, 2, 3])", List.of(1, 2, 3));
            if (EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE_BIT_ELEMENTS.isEnabled()) {
                checkDenseVectorImplicitCastingSimilarityFunction("v_hamming(bit_vector, [1, 2])", List.of(1, 2));
            }
        }
    }

    private void checkDenseVectorImplicitCastingSimilarityFunction(String similarityFunction, List<Number> expectedElems) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | eval similarity = %s
            """, similarityFunction), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        var alias = as(eval.fields().get(0), Alias.class);
        assertEquals("similarity", alias.name());
        var similarity = as(alias.child(), VectorSimilarityFunction.class);
        var left = as(similarity.left(), FieldAttribute.class);
        assertThat(List.of("float_vector", "byte_vector", "bit_vector"), hasItem(left.name()));
        var right = as(similarity.right(), ToDenseVector.class);
        var literal = as(right.field(), Literal.class);
        assertThat(literal.value(), equalTo(expectedElems));
    }

    public void testDenseVectorEvalCastingSimilarityFunctions() {
        if (EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorEvalCastingSimilarityFunction("v_cosine(float_vector, query)");
            checkDenseVectorEvalCastingSimilarityFunction("v_cosine(byte_vector, query)");
        }
        if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorEvalCastingSimilarityFunction("v_dot_product(float_vector, query)");
            checkDenseVectorEvalCastingSimilarityFunction("v_dot_product(byte_vector, query)");
        }
        if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorEvalCastingSimilarityFunction("v_l1_norm(float_vector, query)");
            checkDenseVectorEvalCastingSimilarityFunction("v_l1_norm(byte_vector, query)");
        }
        if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorEvalCastingSimilarityFunction("v_l2_norm(float_vector, query)");
            checkDenseVectorEvalCastingSimilarityFunction("v_l2_norm(float_vector, query)");
        }
        if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkDenseVectorEvalCastingSimilarityFunction("v_hamming(byte_vector, query)");
            checkDenseVectorEvalCastingSimilarityFunction("v_hamming(byte_vector, query)");
        }
    }

    private void checkDenseVectorEvalCastingSimilarityFunction(String similarityFunction) {
        var plan = analyze(String.format(Locale.ROOT, """
            from test | eval query = to_dense_vector([0.342, 0.164, 0.234]) | eval similarity = %s
            """, similarityFunction), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        var alias = as(eval.fields().get(0), Alias.class);
        assertEquals("similarity", alias.name());
        var similarity = as(alias.child(), VectorSimilarityFunction.class);
        var left = as(similarity.left(), FieldAttribute.class);
        assertThat(List.of("float_vector", "byte_vector"), hasItem(left.name()));
        var right = as(similarity.right(), ReferenceAttribute.class);
        assertThat(right.dataType(), is(DENSE_VECTOR));
        assertThat(right.name(), is("query"));
    }

    public void testVectorFunctionHexImplicitCastingError() {
        checkVectorFunctionHexImplicitCastingError("where knn(float_vector, \"notcorrect\")");
        if (EsqlCapabilities.Cap.DOT_PRODUCT_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkVectorFunctionHexImplicitCastingError("eval s = v_dot_product(\"notcorrect\", 0.342)");
        }
        if (EsqlCapabilities.Cap.L1_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkVectorFunctionHexImplicitCastingError("eval s = v_l1_norm(\"notcorrect\", 0.342)");
        }
        if (EsqlCapabilities.Cap.L2_NORM_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkVectorFunctionHexImplicitCastingError("eval s = v_l2_norm(\"notcorrect\", 0.342)");
        }
        if (EsqlCapabilities.Cap.HAMMING_VECTOR_SIMILARITY_FUNCTION.isEnabled()) {
            checkVectorFunctionHexImplicitCastingError("eval s = v_hamming(\"notcorrect\", 0.342)");
        }
    }

    private void checkVectorFunctionHexImplicitCastingError(String clause) {
        var query = "from test | " + clause;
        VerificationException error = expectThrows(VerificationException.class, () -> analyze(query, "mapping-dense_vector.json"));
        assertThat(
            error.getMessage(),
            containsString(
                "Cannot convert string [notcorrect] to [DENSE_VECTOR], "
                    + "error [notcorrect is not a valid hex string: not a hexadecimal digit: \"n\" = 110]"
            )
        );
    }

    public void testMagnitudePlanWithDenseVectorImplicitCasting() {
        assumeTrue("v_magnitude not available", EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled());

        var plan = analyze(String.format(Locale.ROOT, """
            from test | eval scalar = v_magnitude([1, 2, 3])
            """), "mapping-dense_vector.json");

        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        var alias = as(eval.fields().get(0), Alias.class);
        assertEquals("scalar", alias.name());
        var scalar = as(alias.child(), Magnitude.class);
        var child = as(scalar.field(), ToDenseVector.class);
        var literal = as(child.field(), Literal.class);
        assertThat(literal.value(), equalTo(List.of(1, 2, 3)));
    }

    public void testTimeseriesDefaultLimitIs1B() {
        Analyzer analyzer = analyzer(tsdbIndexResolution());
        // Tuples of (query, isTimeseries)
        for (var queryExp : List.of(
            Tuple.tuple("TS test | STATS avg(rate(network.bytes_in))", true),
            Tuple.tuple("TS test", false),
            Tuple.tuple("TS test | STATS avg(rate(network.bytes_in)) BY tbucket=bucket(@timestamp, 1 minute)| sort tbucket", true),
            Tuple.tuple("FROM test | STATS avg(to_long(network.bytes_in))", false)
        )) {
            var query = queryExp.v1();
            var expectedLimit = queryExp.v2() ? DEFAULT_TIMESERIES_LIMIT : DEFAULT_LIMIT;
            var plan = analyze(query, analyzer);
            var limit = as(plan, Limit.class);
            try {
                assertThat(as(limit.limit(), Literal.class).value(), equalTo(expectedLimit));
            } catch (AssertionError e) {
                throw new AssertionError("Failed for query: " + query, e);
            }
        }
    }

    public void testRateRequiresCounterTypes() {
        Analyzer analyzer = analyzer(tsdbIndexResolution());
        var query = "TS test | STATS avg(rate(network.connections))";
        VerificationException error = expectThrows(VerificationException.class, () -> analyze(query, analyzer));
        assertThat(
            error.getMessage(),
            containsString(
                "first argument of [rate(network.connections)] must be"
                    + " [counter_long, counter_integer or counter_double], found value [network.connections] type [long]"
            )
        );
    }

    public void testConditionalFunctionsWithMixedNumericTypes() {
        LogicalPlan plan = analyze("""
            from test
            | eval x = coalesce(salary_change, null, 0), y = coalesce(languages, null, 0), z = coalesce(languages.long, null, 0)
            , w = coalesce(salary_change, null, 0::long)
            | keep x, y, z, w
            """, "mapping-default.json");
        validateConditionalFunctions(plan);

        plan = analyze("""
            from test
            | eval x = case(languages == 1, salary_change, languages == 2, salary, languages == 3, salary_change.long, 0)
                   , y = case(languages == 1, salary_change.int, languages == 2, salary, 0)
                   , z = case(languages == 1, salary_change.long, languages == 2, salary, 0::long)
                   , w = case(languages == 1, salary_change, languages == 2, salary, languages == 3, salary_change.long, null)
            | keep x, y, z, w
            """, "mapping-default.json");
        validateConditionalFunctions(plan);

        plan = analyze("""
            from test
            | eval x = greatest(salary_change, salary, salary_change.long)
                   , y = least(salary_change.int, salary)
                   , z = greatest(salary_change.long, salary, null)
                   , w = least(null, salary_change, salary_change.long, salary, null)
            | keep x, y, z, w
            """, "mapping-default.json");
        validateConditionalFunctions(plan);
    }

    private void validateConditionalFunctions(LogicalPlan plan) {
        var limit = as(plan, Limit.class);
        var esqlProject = as(limit.child(), EsqlProject.class);
        List<?> projections = esqlProject.projections();
        var projection = as(projections.get(0), ReferenceAttribute.class);
        assertEquals(projection.name(), "x");
        assertEquals(projection.dataType(), DataType.DOUBLE);
        projection = as(projections.get(1), ReferenceAttribute.class);
        assertEquals(projection.name(), "y");
        assertEquals(projection.dataType(), INTEGER);
        projection = as(projections.get(2), ReferenceAttribute.class);
        assertEquals(projection.name(), "z");
        assertEquals(projection.dataType(), DataType.LONG);
        projection = as(projections.get(3), ReferenceAttribute.class);
        assertEquals(projection.name(), "w");
        assertEquals(projection.dataType(), DataType.DOUBLE);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(1000));
    }

    public void testNamedParamsForIdentifiers() {
        assertProjectionWithMapping(
            """
                from test
                | eval ?f1 = ?fn1(?f2)
                | where ?f1 == ?f2
                | stats ?f8 = ?fn2(?f3.?f4.?f5) by ?f3.?f6.?f7
                | sort ?f36.?f7, ?f8
                | keep ?f367, ?f8
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsIdentifier("f1", "a"),
                    paramAsIdentifier("f2", "keyword"),
                    paramAsIdentifier("f3", "some"),
                    paramAsIdentifier("f4", "dotted"),
                    paramAsIdentifier("f5", "field"),
                    paramAsIdentifier("f6", "string"),
                    paramAsIdentifier("f7", "typical"),
                    paramAsIdentifier("f8", "y"),
                    paramAsIdentifier("f36", "some.string"),
                    paramAsIdentifier("f367", "some.string.typical"),
                    paramAsIdentifier("fn1", "trim"),
                    paramAsIdentifier("fn2", "count")
                )
            ),
            "some.string.typical",
            "y"
        );

        assertProjectionWithMapping(
            """
                from test
                | eval ?f1 = ?fn1(?f2)
                | where ?f1 == ?f2
                | mv_expand ?f3.?f4.?f5
                | dissect ?f8 "%{bar}"
                | grok ?f2 "%{WORD:foo}"
                | rename ?f9 as ?f10
                | sort ?f3.?f6.?f7
                | drop ?f11
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsIdentifier("f1", "a"),
                    paramAsIdentifier("f2", "keyword"),
                    paramAsIdentifier("f3", "some"),
                    paramAsIdentifier("f4", "dotted"),
                    paramAsIdentifier("f5", "field"),
                    paramAsIdentifier("f6", "string"),
                    paramAsIdentifier("f7", "typical"),
                    paramAsIdentifier("f8", "text"),
                    paramAsIdentifier("f9", "date"),
                    paramAsIdentifier("f10", "datetime"),
                    paramAsIdentifier("f11", "bool"),
                    paramAsIdentifier("fn1", "trim")
                )
            ),
            "binary",
            "binary_stored",
            "datetime",
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
            "x.y.z.w",
            "a",
            "bar",
            "foo"
        );
    }

    public void testInvalidNamedParamsForIdentifiers() {
        // missing field
        assertError(
            """
                from test
                | eval ?f1 = ?fn1(?f2)
                | keep ?f3
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsIdentifier("f1", "a"),
                    paramAsIdentifier("f2", "keyword"),
                    paramAsIdentifier("f3", "some.string.nonexisting"),
                    paramAsIdentifier("fn1", "trim")
                )
            ),
            "Unknown column [some.string.nonexisting]"
        );

        // field name pattern is not supported in where/stats/sort/dissect/grok, they only take identifier
        // eval/rename/enrich/mvexpand are covered in StatementParserTests
        for (String invalidParam : List.of(
            "where ?f1 == \"a\"",
            "stats x = count(?f1)",
            "sort ?f1",
            "dissect ?f1 \"%{bar}\"",
            "grok ?f1 \"%{WORD:foo}\""
        )) {
            for (String pattern : List.of("keyword*", "*")) {
                assertError(
                    "from test | " + invalidParam,
                    "mapping-multi-field-with-nested.json",
                    new QueryParams(List.of(paramAsPattern("f1", pattern))),
                    "Unresolved pattern [" + pattern + "]"
                );
            }
        }

        // pattern and constant for function are covered in StatementParserTests
        for (String pattern : List.of("count*", "*")) {
            assertError(
                "from test | stats x = ?fn1(*)",
                "mapping-multi-field-with-nested.json",
                new QueryParams(List.of(paramAsIdentifier("fn1", pattern))),
                "Unknown function [" + pattern + "]"
            );
        }

        // identifier provided in param is not expected to be in backquote
        List<String> commands = List.of(
            "eval x = ?f1",
            "where ?f1 == \"a\"",
            "stats x = count(?f1)",
            "sort ?f1",
            "dissect ?f1 \"%{bar}\"",
            "grok ?f1 \"%{WORD:foo}\"",
            "mv_expand ?f1"
        );
        for (Object command : commands) {
            assertError(
                "from test | " + command,
                "mapping-multi-field-with-nested.json",
                new QueryParams(List.of(paramAsIdentifier("f1", "`keyword`"))),
                "Unknown column [`keyword`]"
            );
        }
    }

    public void testNamedDoubleParamsForIdentifiers() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        assertProjectionWithMapping(
            """
                from test
                | eval ??f1 = ??fn1(??f2)
                | where ??f1 == ??f2
                | stats ??f8 = ??fn2(??f3.??f4.??f5) by ??f3.??f6.??f7
                | sort ??f36.??f7, ??f8
                | keep ??f367, ??f8
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "a"),
                    paramAsConstant("f2", "keyword"),
                    paramAsConstant("f3", "some"),
                    paramAsConstant("f4", "dotted"),
                    paramAsConstant("f5", "field"),
                    paramAsConstant("f6", "string"),
                    paramAsConstant("f7", "typical"),
                    paramAsConstant("f8", "y"),
                    paramAsConstant("f36", "some.string"),
                    paramAsConstant("f367", "some.string.typical"),
                    paramAsConstant("fn1", "trim"),
                    paramAsConstant("fn2", "count")
                )
            ),
            "some.string.typical",
            "y"
        );

        assertProjectionWithMapping(
            """
                from test
                | eval ??f1 = ??fn1(??f2)
                | where ??f1 == ??f2
                | mv_expand ??f3.??f4.??f5
                | dissect ??f8 "%{bar}"
                | grok ??f2 "%{WORD:foo}"
                | rename ??f9 as ??f10
                | sort ??f3.??f6.??f7
                | drop ??f11
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "a"),
                    paramAsConstant("f2", "keyword"),
                    paramAsConstant("f3", "some"),
                    paramAsConstant("f4", "dotted"),
                    paramAsConstant("f5", "field"),
                    paramAsConstant("f6", "string"),
                    paramAsConstant("f7", "typical"),
                    paramAsConstant("f8", "text"),
                    paramAsConstant("f9", "date"),
                    paramAsConstant("f10", "datetime"),
                    paramAsConstant("f11", "bool"),
                    paramAsConstant("fn1", "trim")
                )
            ),
            "binary",
            "binary_stored",
            "datetime",
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
            "x.y.z.w",
            "a",
            "bar",
            "foo"
        );

        assertProjectionWithMapping(
            """
                FROM test
                | EVAL ??f1 = ??f2
                | LOOKUP JOIN languages_lookup ON ??f1
                | KEEP ??f3.??f6.??f7
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "language_code"),
                    paramAsConstant("f2", "int"),
                    paramAsConstant("f3", "some"),
                    paramAsConstant("f6", "string"),
                    paramAsConstant("f7", "typical")
                )
            ),
            "some.string.typical"
        );
    }

    public void testInvalidNamedDoubleParamsForIdentifiers() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        // missing field
        assertError(
            """
                from test
                | eval ??f1 = ??fn1(??f2)
                | keep ??f3
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "a"),
                    paramAsConstant("f2", "keyword"),
                    paramAsConstant("f3", "some.string.nonexisting"),
                    paramAsConstant("fn1", "trim")
                )
            ),
            "Unknown column [some.string.nonexisting]"
        );

        // field name pattern is not supported in where/stats/sort/dissect/grok, they only take identifier
        // eval/rename/enrich/mvexpand are covered in StatementParserTests
        for (String invalidParam : List.of(
            "where ??f1 == \"a\"",
            "stats x = count(??f1)",
            "sort ??f1",
            "dissect ??f1 \"%{bar}\"",
            "grok ??f1 \"%{WORD:foo}\"",
            "lookup join languages_lookup on ??f1"
        )) {
            for (String pattern : List.of("keyword*", "*")) {
                assertError(
                    "from test | " + invalidParam,
                    "mapping-multi-field-with-nested.json",
                    new QueryParams(List.of(paramAsConstant("f1", pattern))),
                    "Unknown column [" + pattern + "]"
                );
            }
        }

        // pattern and constant for function are covered in StatementParserTests
        for (String pattern : List.of("count*", "*")) {
            assertError(
                "from test | stats x = ??fn1(*)",
                "mapping-multi-field-with-nested.json",
                new QueryParams(List.of(paramAsConstant("fn1", pattern))),
                "Unknown function [" + pattern + "]"
            );
        }

        // identifier provided in param is not expected to be in backquote
        List<String> commands = List.of(
            "eval x = ??f1",
            "where ??f1 == \"a\"",
            "stats x = count(??f1)",
            "sort ??f1",
            "dissect ??f1 \"%{bar}\"",
            "grok ??f1 \"%{WORD:foo}\"",
            "mv_expand ??f1",
            "lookup join languages_lookup on ??f1"
        );
        for (Object command : commands) {
            assertError(
                "from test | " + command,
                "mapping-multi-field-with-nested.json",
                new QueryParams(List.of(paramAsConstant("f1", "`keyword`"))),
                "Unknown column [`keyword`]"
            );
        }
    }

    public void testNamedParamsForIdentifierPatterns() {
        assertProjectionWithMapping(
            """
                from test
                | keep ?f1, ?f2.?f3
                | drop ?f1.?f6.?f7.?f8, ?f2.?f4, ?f2.?f5.?f3
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(
                List.of(
                    paramAsPattern("f1", "x*"),
                    paramAsIdentifier("f2", "some"),
                    paramAsPattern("f3", "*"),
                    paramAsPattern("f4", "ambiguous*"),
                    paramAsIdentifier("f5", "dotted"),
                    paramAsIdentifier("f6", "y"),
                    paramAsIdentifier("f7", "z"),
                    paramAsIdentifier("f8", "v")
                )
            ),
            "x",
            "x.y",
            "x.y.z",
            "x.y.z.w",
            "some.string",
            "some.string.normalized",
            "some.string.typical"
        );
    }

    public void testInvalidNamedParamsForIdentifierPatterns() {
        // missing pattern
        assertError(
            """
                from test | keep ?f1
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(List.of(paramAsPattern("f1", "a*"))),
            "No matches found for pattern [a*]"
        );
        // invalid type
        assertError(
            """
                from test | keep ?f1
                """,
            "mapping-multi-field-with-nested.json",
            new QueryParams(List.of(paramAsIdentifier("f1", "x*"))),
            "Unknown column [x*], did you mean [x]?"
        );
    }

    public void testFromEnrichAndMatchColonUsage() {
        LogicalPlan plan = analyze("""
            from *:test
            | EVAL x = to_string(languages)
            | ENRICH _any:languages ON x
            | WHERE first_name: "Anna"
            """, "*:test", "mapping-default.json");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var match = as(filter.condition(), MatchOperator.class);
        var enrich = as(filter.child(), Enrich.class);
        assertEquals(enrich.mode(), Enrich.Mode.ANY);
        assertEquals(enrich.policy().getMatchField(), "language_code");
        var eval = as(enrich.child(), Eval.class);
        var esRelation = as(eval.child(), EsRelation.class);
        assertEquals(esRelation.indexPattern(), "*:test"); // This tests nothing, as whatever appears here comes from the test itself
    }

    public void testFunctionNamedParamsAsFunctionArgument() {
        LogicalPlan plan = analyze("""
            from test
            | WHERE MATCH(first_name, "Anna Smith", {"minimum_should_match": 2.0})
            """);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match match = as(filter.condition(), Match.class);
        MapExpression me = as(match.options(), MapExpression.class);
        assertEquals(1, me.entryExpressions().size());
        EntryExpression ee = as(me.entryExpressions().get(0), EntryExpression.class);
        assertEquals(new Literal(EMPTY, BytesRefs.toBytesRef("minimum_should_match"), DataType.KEYWORD), ee.key());
        assertEquals(new Literal(EMPTY, 2.0, DataType.DOUBLE), ee.value());
        assertEquals(DataType.DOUBLE, ee.dataType());
    }

    public void testFunctionNamedParamsAsFunctionArgument1() {
        LogicalPlan plan = analyze("""
            from test
            | WHERE QSTR("first_name: Anna", {"minimum_should_match": 3.0})
            """);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        QueryString qstr = as(filter.condition(), QueryString.class);
        MapExpression me = as(qstr.options(), MapExpression.class);
        assertEquals(1, me.entryExpressions().size());
        EntryExpression ee = as(me.entryExpressions().get(0), EntryExpression.class);
        assertEquals(new Literal(EMPTY, BytesRefs.toBytesRef("minimum_should_match"), DataType.KEYWORD), ee.key());
        assertEquals(new Literal(EMPTY, 3.0, DataType.DOUBLE), ee.value());
        assertEquals(DataType.DOUBLE, ee.dataType());
    }

    public void testFunctionNamedParamsAsFunctionArgument2() {
        LogicalPlan plan = analyze("""
            from test
            | WHERE MULTI_MATCH("Anna Smith", first_name, last_name, {"minimum_should_match": 3.0})
            """);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        MultiMatch mm = as(filter.condition(), MultiMatch.class);
        MapExpression me = as(mm.options(), MapExpression.class);
        assertEquals(1, me.entryExpressions().size());
        EntryExpression ee = as(me.entryExpressions().get(0), EntryExpression.class);
        assertEquals(new Literal(EMPTY, BytesRefs.toBytesRef("minimum_should_match"), DataType.KEYWORD), ee.key());
        assertEquals(new Literal(EMPTY, 3.0, DataType.DOUBLE), ee.value());
        assertEquals(DataType.DOUBLE, ee.dataType());
    }

    public void testResolveInsist_fieldExists_insistedOutputContainsNoUnmappedFields() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        LogicalPlan plan = analyze("FROM test | INSIST_🐔 emp_no");

        Attribute last = plan.output().getLast();
        assertThat(last.name(), is("emp_no"));
        assertThat(last.dataType(), is(INTEGER));
        assertThat(
            plan.output()
                .stream()
                .filter(a -> a instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField)
                .toList(),
            is(empty())
        );
    }

    public void testInsist_afterRowThrowsException() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("ROW x = 1 | INSIST_🐔 x", analyzer(TEST_VERIFIER))
        );
        assertThat(e.getMessage(), containsString("[insist] can only be used after [from] or [insist] commands, but was [ROW x = 1]"));
    }

    public void testResolveInsist_fieldDoesNotExist_createsUnmappedField() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        LogicalPlan plan = analyze("FROM test | INSIST_🐔 foo");

        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        assertThat(insist.output(), hasSize(analyze("FROM test").output().size() + 1));
        var expectedAttribute = new FieldAttribute(Source.EMPTY, "foo", new PotentiallyUnmappedKeywordEsField("foo"));
        assertThat(insist.insistedAttributes(), equalToIgnoringIds(List.of(expectedAttribute)));
        assertThat(insist.output().getLast(), equalToIgnoringIds(expectedAttribute));
    }

    public void testResolveInsist_multiIndexFieldPartiallyMappedWithSingleKeywordType_createsUnmappedField() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("keyword")),
                        fieldCapabilitiesIndexResponse("bar", Map.of())
                    ),
                    List.of()
                )
            )
        );

        String query = "FROM foo, bar | INSIST_🐔 message";
        var plan = analyze(query, analyzer(indexResolutions(resolution), TEST_VERIFIER, configuration(query)));
        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        var attribute = (FieldAttribute) EsqlTestUtils.singleValue(insist.output());
        assertThat(attribute.name(), is("message"));
        assertThat(attribute.field(), is(new PotentiallyUnmappedKeywordEsField("message")));
    }

    public void testResolveInsist_multiIndexFieldExistsWithSingleTypeButIsNotKeywordAndMissingCast_createsAnInvalidMappedField() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("long")),
                        fieldCapabilitiesIndexResponse("bar", Map.of())
                    ),
                    List.of()
                )
            )
        );
        var plan = analyze("FROM foo, bar | INSIST_🐔 message", analyzer(resolution, TEST_VERIFIER));
        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        var attribute = (UnsupportedAttribute) EsqlTestUtils.singleValue(insist.output());
        assertThat(attribute.name(), is("message"));

        String expected = "Cannot use field [message] due to ambiguities being mapped as [2] incompatible types: "
            + "[keyword] enforced by INSIST command, and [long] in index mappings";
        assertThat(attribute.unresolvedMessage(), is(expected));
    }

    public void testResolveInsist_multiIndexFieldPartiallyExistsWithMultiTypesNoKeyword_createsAnInvalidMappedField() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("long")),
                        fieldCapabilitiesIndexResponse("bar", messageResponseMap("date")),
                        fieldCapabilitiesIndexResponse("bazz", Map.of())
                    ),
                    List.of()
                )
            )
        );
        var plan = analyze("FROM foo, bar | INSIST_🐔 message", analyzer(resolution, TEST_VERIFIER));
        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        var attr = (UnsupportedAttribute) EsqlTestUtils.singleValue(insist.output());

        String expected = "Cannot use field [message] due to ambiguities being mapped as [3] incompatible types: "
            + "[keyword] enforced by INSIST command, [datetime] in [bar], [long] in [foo]";
        assertThat(attr.unresolvedMessage(), is(expected));
    }

    public void testResolveInsist_multiIndexSameMapping_fieldIsMapped() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("long")),
                        fieldCapabilitiesIndexResponse("bar", messageResponseMap("long"))
                    ),
                    List.of()
                )
            )
        );
        var plan = analyze("FROM foo, bar | INSIST_🐔 message", analyzer(resolution, TEST_VERIFIER));
        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        var attribute = (FieldAttribute) EsqlTestUtils.singleValue(insist.output());
        assertThat(attribute.name(), is("message"));
        assertThat(attribute.dataType(), is(DataType.LONG));
    }

    public void testResolveInsist_multiIndexFieldPartiallyExistsWithMultiTypesWithKeyword_createsAnInvalidMappedField() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("long")),
                        fieldCapabilitiesIndexResponse("bar", messageResponseMap("date")),
                        fieldCapabilitiesIndexResponse("bazz", messageResponseMap("keyword")),
                        fieldCapabilitiesIndexResponse("qux", Map.of())
                    ),
                    List.of()
                )
            )
        );
        var plan = analyze("FROM foo, bar | INSIST_🐔 message", analyzer(resolution, TEST_VERIFIER));
        var limit = as(plan, Limit.class);
        var insist = as(limit.child(), Insist.class);
        var attr = (UnsupportedAttribute) EsqlTestUtils.singleValue(insist.output());

        String expected = "Cannot use field [message] due to ambiguities being mapped as [3] incompatible types: "
            + "[datetime] in [bar], [keyword] enforced by INSIST command and in [bazz], [long] in [foo]";
        assertThat(attr.unresolvedMessage(), is(expected));
    }

    public void testResolveInsist_multiIndexFieldPartiallyExistsWithMultiTypesWithCast_castsAreNotSupported() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        IndexResolution resolution = IndexResolver.mergedMappings(
            "foo,bar",
            fieldsInfoOnCurrentVersion(
                new FieldCapabilitiesResponse(
                    List.of(
                        fieldCapabilitiesIndexResponse("foo", messageResponseMap("long")),
                        fieldCapabilitiesIndexResponse("bar", messageResponseMap("date")),
                        fieldCapabilitiesIndexResponse("bazz", Map.of())
                    ),
                    List.of()
                )
            )
        );
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("FROM foo, bar | INSIST_🐔 message | EVAL message = message :: keyword", analyzer(resolution, TEST_VERIFIER))
        );
        // This isn't the most informative error, but it'll do for now.
        assertThat(
            e.getMessage(),
            containsString("EVAL does not support type [unsupported] as the return data type of expression [message]")
        );
    }

    public void testResolveDenseVector() {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder()
            .withIndexResponses(
                List.of(fieldCapabilitiesIndexResponse("foo", Map.of("v", new IndexFieldCapabilitiesBuilder("v", "dense_vector").build())))
            )
            .build();
        {
            IndexResolution resolution = IndexResolver.mergedMappings(
                "foo",
                new IndexResolver.FieldsInfo(caps, TransportVersion.minimumCompatible(), false, true, true)
            );
            var plan = analyze("FROM foo", analyzer(resolution, TEST_VERIFIER));
            assertThat(plan.output(), hasSize(1));
            assertThat(plan.output().getFirst().dataType(), equalTo(DENSE_VECTOR));
        }
        {
            IndexResolution resolution = IndexResolver.mergedMappings(
                "foo",
                new IndexResolver.FieldsInfo(caps, TransportVersion.minimumCompatible(), false, true, false)
            );
            var plan = analyze("FROM foo", analyzer(resolution, TEST_VERIFIER));
            assertThat(plan.output(), hasSize(1));
            assertThat(plan.output().getFirst().dataType(), equalTo(UNSUPPORTED));
        }
    }

    public void testResolveAggregateMetricDouble() {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder()
            .withIndexResponses(
                List.of(
                    fieldCapabilitiesIndexResponse(
                        "foo",
                        Map.of("v", new IndexFieldCapabilitiesBuilder("v", "aggregate_metric_double").build())
                    )
                )
            )
            .build();
        {
            IndexResolution resolution = IndexResolver.mergedMappings(
                "foo",
                new IndexResolver.FieldsInfo(caps, TransportVersion.minimumCompatible(), false, true, true)
            );
            var plan = analyze("FROM foo", analyzer(resolution, TEST_VERIFIER));
            assertThat(plan.output(), hasSize(1));
            assertThat(
                plan.output().getFirst().dataType(),
                equalTo(EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled() ? AGGREGATE_METRIC_DOUBLE : UNSUPPORTED)
            );
        }
        {
            IndexResolution resolution = IndexResolver.mergedMappings(
                "foo",
                new IndexResolver.FieldsInfo(caps, TransportVersion.minimumCompatible(), false, false, true)
            );
            var plan = analyze("FROM foo", analyzer(resolution, TEST_VERIFIER));
            assertThat(plan.output(), hasSize(1));
            assertThat(plan.output().getFirst().dataType(), equalTo(UNSUPPORTED));
        }
    }

    public void testBasicFork() {
        LogicalPlan plan = analyze("""
            from test
            | KEEP emp_no, first_name, last_name
            | WHERE first_name == "Chris"
            | FORK ( WHERE emp_no > 1 )
                   ( WHERE emp_no > 2 )
                   ( WHERE emp_no > 3 | SORT emp_no | LIMIT 7 )
                   ( SORT emp_no )
                   ( LIMIT 9 )
            """);

        var expectedOutput = List.of("emp_no", "first_name", "last_name", "_fork");
        Limit limit = as(plan, Limit.class);
        Fork fork = as(limit.child(), Fork.class);

        var subPlans = fork.children();
        assertThat(subPlans.size(), equalTo(5));

        // fork branch 1
        limit = as(subPlans.get(0), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        EsqlProject project = as(limit.child(), EsqlProject.class);
        List<String> projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        Eval eval = as(project.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork1"))));
        Filter filter = as(eval.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).right(), equalTo(literal(1)));

        filter = as(filter.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        project = as(filter.child(), EsqlProject.class);
        var esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 2
        limit = as(subPlans.get(1), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork2"))));
        filter = as(eval.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).right(), equalTo(literal(2)));

        filter = as(filter.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        project = as(filter.child(), EsqlProject.class);
        esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 3
        limit = as(subPlans.get(2), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork3"))));
        limit = as(eval.child(), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(7));
        var orderBy = as(limit.child(), OrderBy.class);
        filter = as(orderBy.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).right(), equalTo(literal(3)));
        filter = as(filter.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        project = as(filter.child(), EsqlProject.class);
        esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 4
        limit = as(subPlans.get(3), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork4"))));
        orderBy = as(eval.child(), OrderBy.class);
        filter = as(orderBy.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        project = as(filter.child(), EsqlProject.class);
        esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 5
        limit = as(subPlans.get(4), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork5"))));
        limit = as(eval.child(), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(9));
        filter = as(limit.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        project = as(filter.child(), EsqlProject.class);
        esRelation = as(project.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));
    }

    public void testForkBranchesWithDifferentSchemas() {
        LogicalPlan plan = analyze("""
            from test
            | WHERE first_name == "Chris"
            | KEEP emp_no, first_name
            | FORK ( WHERE emp_no > 3 | SORT emp_no | LIMIT 7 )
                   ( WHERE emp_no > 2 | EVAL xyz = "def" )
                   ( DISSECT first_name "%{d} %{e} %{f}"
                   | STATS x = MIN(d::double), y = MAX(e::double) WHERE d::double > 1000
                   | EVAL xyz = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        Fork fork = as(limit.child(), Fork.class);

        var subPlans = fork.children();
        var expectedOutput = List.of("emp_no", "first_name", "_fork", "xyz", "x", "y");

        // fork branch 1
        limit = as(subPlans.get(0), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(MAX_LIMIT));
        EsqlProject project = as(limit.child(), EsqlProject.class);
        List<String> projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));

        Eval eval = as(project.child(), Eval.class);
        assertEquals(eval.fields().size(), 3);

        Set<String> evalFieldNames = eval.fields().stream().map(a -> a.name()).collect(Collectors.toSet());
        assertThat(evalFieldNames, equalTo(Set.of("x", "xyz", "y")));

        for (Alias a : eval.fields()) {
            assertThat(as(a.child(), Literal.class).value(), equalTo(null));
        }

        eval = as(eval.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork1"))));
        limit = as(eval.child(), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(7));
        var orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).right(), equalTo(literal(3)));

        project = as(filter.child(), EsqlProject.class);
        filter = as(project.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        var esRelation = as(filter.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 2
        limit = as(subPlans.get(1), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertEquals(eval.fields().size(), 2);
        evalFieldNames = eval.fields().stream().map(a -> a.name()).collect(Collectors.toSet());
        assertThat(evalFieldNames, equalTo(Set.of("x", "y")));

        for (Alias a : eval.fields()) {
            assertThat(as(a.child(), Literal.class).value(), equalTo(null));
        }

        eval = as(eval.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork2"))));
        eval = as(eval.child(), Eval.class);
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), equalTo("xyz"));
        assertThat(as(alias.child(), Literal.class), equalTo(string("def")));
        filter = as(eval.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).right(), equalTo(literal(2)));

        project = as(filter.child(), EsqlProject.class);
        filter = as(project.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        esRelation = as(filter.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));

        // fork branch 3
        limit = as(subPlans.get(2), Limit.class);
        assertThat(as(limit.limit(), Literal.class).value(), equalTo(DEFAULT_LIMIT));
        project = as(limit.child(), EsqlProject.class);
        projectColumns = project.expressions().stream().map(exp -> as(exp, Attribute.class).name()).toList();
        assertThat(projectColumns, equalTo(expectedOutput));
        eval = as(project.child(), Eval.class);
        assertEquals(eval.fields().size(), 2);
        evalFieldNames = eval.fields().stream().map(a -> a.name()).collect(Collectors.toSet());
        assertThat(evalFieldNames, equalTo(Set.of("emp_no", "first_name")));

        for (Alias a : eval.fields()) {
            assertThat(as(a.child(), Literal.class).value(), equalTo(null));
        }

        eval = as(eval.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", string("fork3"))));

        eval = as(eval.child(), Eval.class);
        alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), equalTo("xyz"));
        assertThat(as(alias.child(), Literal.class), equalTo(string("abc")));

        Aggregate aggregate = as(eval.child(), Aggregate.class);
        assertEquals(aggregate.aggregates().size(), 2);
        alias = as(aggregate.aggregates().get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));

        alias = as(aggregate.aggregates().get(1), Alias.class);
        assertThat(alias.name(), equalTo("y"));
        FilteredExpression filteredExp = as(alias.child(), FilteredExpression.class);

        GreaterThan greaterThan = as(filteredExp.filter(), GreaterThan.class);
        assertThat(as(greaterThan.right(), Literal.class).value(), equalTo(1000));

        Dissect dissect = as(aggregate.child(), Dissect.class);
        assertThat(dissect.parser().pattern(), equalTo("%{d} %{e} %{f}"));
        assertThat(as(dissect.input(), FieldAttribute.class).name(), equalTo("first_name"));

        project = as(dissect.child(), EsqlProject.class);
        filter = as(project.child(), Filter.class);
        assertThat(as(filter.condition(), Equals.class).right(), equalTo(string("Chris")));
        esRelation = as(filter.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), equalTo("test"));
    }

    public void testForkError() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | FORK ( WHERE emp_no > 1 )
                   ( WHERE foo > 1 )
            """));
        assertThat(e.getMessage(), containsString("Unknown column [foo]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | FORK ( WHERE bar == 1 )
                   ( WHERE emp_no > 1 )
            """));
        assertThat(e.getMessage(), containsString("Unknown column [bar]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | FORK ( WHERE emp_no > 1 )
                   ( WHERE emp_no > 2 )
                   ( WHERE emp_no > 3 )
                   ( WHERE emp_no > 4 )
                   ( WHERE emp_no > 5 )
                   ( WHERE emp_no > 6 | SORT baz )
            """));
        assertThat(e.getMessage(), containsString("Unknown column [baz]"));

        var pe = expectThrows(ParsingException.class, () -> analyze("""
            from test
            | FORK ( WHERE emp_no > 1 )
                   ( WHERE emp_no > 2 )
                   ( WHERE emp_no > 3 | LIMIT me)
                   ( WHERE emp_no > 4 )
                   ( WHERE emp_no > 5 )
                   ( WHERE emp_no > 6 | SORT emp_no | LIMIT 5 )
            """));
        assertThat(pe.getMessage(), containsString("mismatched input 'me' expecting {"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK ( WHERE emp_no > 1 )
                   ( WHERE emp_no > 2 | SORT emp_no | LIMIT 10 | EVAL x = abc + 2 )
            """));
        assertThat(e.getMessage(), containsString("Unknown column [abc]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK ( STATS a = CONCAT(first_name, last_name) BY emp_no )
                   ( WHERE emp_no > 2 | SORT emp_no | LIMIT 10 )
            """));
        assertThat(
            e.getMessage(),
            containsString("column [first_name] must appear in the STATS BY clause or be used in an aggregate function")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK ( DISSECT emp_no "%{abc} %{def}" )
                   ( WHERE emp_no > 2 | SORT emp_no | LIMIT 10 )
            """));
        assertThat(
            e.getMessage(),
            containsString("Dissect only supports KEYWORD or TEXT values, found expression [emp_no] type [INTEGER]")
        );

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK ( EVAL c = COUNT(first_name) )
                   ( WHERE emp_no > 2 | SORT emp_no | LIMIT 10 )
            """));
        assertThat(e.getMessage(), containsString("aggregate function [COUNT(first_name)] not allowed outside STATS command"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK (EVAL a = 1) (EVAL a = 2)
            | FORK (EVAL b = 3) (EVAL b = 4)
            """));
        assertThat(e.getMessage(), containsString("Only a single FORK command is supported, but found multiple"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            FROM test
            | FORK (FORK (WHERE true) (WHERE true))
                   (WHERE true)
            """));
        assertThat(e.getMessage(), containsString("Only a single FORK command is supported, but found multiple"));
    }

    public void testValidFuse() {
        LogicalPlan plan = analyze("""
             from test metadata _id, _index, _score
             | fork ( where first_name:"foo" )
                    ( where first_name:"bar" )
             | fuse
            """);

        Limit limit = as(plan, Limit.class);

        Aggregate aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings().size(), equalTo(2));

        FuseScoreEval scoreEval = as(aggregate.child(), FuseScoreEval.class);
        assertThat(scoreEval.score(), instanceOf(ReferenceAttribute.class));
        assertThat(scoreEval.score().name(), equalTo("_score"));
        assertThat(scoreEval.discriminator(), instanceOf(ReferenceAttribute.class));
        assertThat(scoreEval.discriminator().name(), equalTo("_fork"));

        assertThat(scoreEval.child(), instanceOf(Fork.class));
    }

    public void testFuseError() {
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | fuse
            """));
        assertThat(e.getMessage(), containsString("Unknown column [_score]"));
        assertThat(e.getMessage(), containsString("Unknown column [_fork]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test
            | FORK ( WHERE emp_no == 1 )
                   ( WHERE emp_no > 1 )
            | FUSE
            """));
        assertThat(e.getMessage(), containsString("Unknown column [_score]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test metadata _score, _id
            | FORK ( WHERE emp_no == 1 )
                   ( WHERE emp_no > 1 )
            | FUSE
            """));
        assertThat(e.getMessage(), containsString("Unknown column [_index]"));

        e = expectThrows(VerificationException.class, () -> analyze("""
            from test metadata _score, _index
            | FORK ( WHERE emp_no == 1 )
                   ( WHERE emp_no > 1 )
            | FUSE
            """));
        assertThat(e.getMessage(), containsString("Unknown column [_id]"));
    }

    // TODO There's too much boilerplate involved here! We need a better way of creating FieldCapabilitiesResponses from a mapping or index.
    private static FieldCapabilitiesIndexResponse fieldCapabilitiesIndexResponse(
        String indexName,
        Map<String, IndexFieldCapabilities> fields
    ) {
        String indexMappingHash = new String(
            MessageDigests.sha256().digest(fields.toString().getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        return new FieldCapabilitiesIndexResponse(indexName, indexMappingHash, fields, false, IndexMode.STANDARD);
    }

    private static Map<String, IndexFieldCapabilities> messageResponseMap(String date) {
        return Map.of("message", new IndexFieldCapabilitiesBuilder("message", date).build());
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

    private void assertProjectionWithMapping(String query, String mapping, QueryParams params, String... names) {
        var plan = analyze(query, mapping.toString(), params);
        var limit = as(plan, Limit.class);
        assertThat(Expressions.names(limit.output()), contains(names));
    }

    private void assertError(String query, String mapping, QueryParams params, String error) {
        Throwable e = expectThrows(VerificationException.class, () -> analyze(query, mapping, params));
        assertThat(e.getMessage(), containsString(error));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }

    // TODO: drop after next minor release
    public static List<String> withInlinestatsWarning(List<String> warnings) {
        List<String> allWarnings = warnings != null ? new ArrayList<>(warnings) : new ArrayList<>();
        allWarnings.add("INLINESTATS is deprecated, use INLINE STATS instead");
        return allWarnings;
    }

    private static LogicalPlan analyzeWithEmptyFieldCapsResponse(String query) throws IOException {
        List<FieldCapabilitiesIndexResponse> idxResponses = List.of(
            new FieldCapabilitiesIndexResponse("idx", "idx", Map.of(), true, IndexMode.STANDARD)
        );
        IndexResolver.FieldsInfo caps = fieldsInfoOnCurrentVersion(new FieldCapabilitiesResponse(idxResponses, List.of()));
        IndexResolution resolution = IndexResolver.mergedMappings("test*", caps);
        var analyzer = analyzer(indexResolutions(resolution), TEST_VERIFIER, configuration(query));
        return analyze(query, analyzer);
    }

    private void assertEmptyEsRelation(LogicalPlan plan) {
        assertThat(plan, instanceOf(EsRelation.class));
        EsRelation esRelation = (EsRelation) plan;
        assertThat(esRelation.output(), equalTo(NO_FIELDS));
    }

    public void testTextEmbeddingResolveInferenceId() {
        LogicalPlan plan = analyze(
            String.format(Locale.ROOT, """
                FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING("italian food recipe", "%s")""", TEXT_EMBEDDING_INFERENCE_ID),
            "mapping-books.json"
        );

        Eval eval = as(as(plan, Limit.class).child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), equalTo("embedding"));
        TextEmbedding function = as(alias.child(), TextEmbedding.class);

        assertThat(function.inputText(), equalTo(string("italian food recipe")));
        assertThat(function.inferenceId(), equalTo(string(TEXT_EMBEDDING_INFERENCE_ID)));
    }

    public void testTextEmbeddingFunctionResolveType() {
        LogicalPlan plan = analyze(
            String.format(Locale.ROOT, """
                FROM books METADATA _score| EVAL embedding = TEXT_EMBEDDING("italian food recipe", "%s")""", TEXT_EMBEDDING_INFERENCE_ID),
            "mapping-books.json"
        );

        Eval eval = as(as(plan, Limit.class).child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), equalTo("embedding"));

        TextEmbedding function = as(alias.child(), TextEmbedding.class);

        assertThat(function.foldable(), equalTo(true));
        assertThat(function.dataType(), equalTo(DENSE_VECTOR));
    }

    public void testTextEmbeddingFunctionMissingInferenceIdError() {
        VerificationException ve = expectThrows(
            VerificationException.class,
            () -> analyze(
                String.format(Locale.ROOT, """
                    FROM books METADATA _score| EVAL embedding = TEXT_EMBEDDING("italian food recipe", "%s")""", "unknow-inference-id"),
                "mapping-books.json"
            )
        );

        assertThat(ve.getMessage(), containsString("unresolved inference [unknow-inference-id]"));
    }

    public void testTextEmbeddingFunctionInvalidInferenceIdError() {
        String inferenceId = randomInferenceIdOtherThan(TEXT_EMBEDDING_INFERENCE_ID);
        VerificationException ve = expectThrows(
            VerificationException.class,
            () -> analyze(
                String.format(Locale.ROOT, """
                    FROM books METADATA _score| EVAL embedding = TEXT_EMBEDDING("italian food recipe", "%s")""", inferenceId),
                "mapping-books.json"
            )
        );

        assertThat(
            ve.getMessage(),
            containsString(String.format(Locale.ROOT, "cannot use inference endpoint [%s] with task type", inferenceId))
        );
    }

    public void testTextEmbeddingFunctionWithoutModel() {
        ParsingException ve = expectThrows(ParsingException.class, () -> analyze("""
            FROM books METADATA _score| EVAL embedding = TEXT_EMBEDDING("italian food recipe")""", "mapping-books.json"));

        assertThat(
            ve.getMessage(),
            containsString(" error building [text_embedding]: function [text_embedding] expects exactly two arguments")
        );
    }

    public void testKnnFunctionWithTextEmbedding() {
        LogicalPlan plan = analyze(
            String.format(Locale.ROOT, """
                from test | where KNN(float_vector, TEXT_EMBEDDING("italian food recipe", "%s"))""", TEXT_EMBEDDING_INFERENCE_ID),
            "mapping-dense_vector.json"
        );

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Knn knn = as(filter.condition(), Knn.class);
        assertThat(knn.field(), instanceOf(FieldAttribute.class));
        assertThat(((FieldAttribute) knn.field()).name(), equalTo("float_vector"));

        TextEmbedding textEmbedding = as(knn.query(), TextEmbedding.class);
        assertThat(textEmbedding.inputText(), equalTo(string("italian food recipe")));
        assertThat(textEmbedding.inferenceId(), equalTo(string(TEXT_EMBEDDING_INFERENCE_ID)));
    }

    public void testResolveRerankInferenceId() {
        {
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");
            Rerank rerank = as(as(plan, Limit.class).child(), Rerank.class);
            assertThat(rerank.inferenceId(), equalTo(string("reranking-inference-id")));
        }

        {
            VerificationException ve = expectThrows(VerificationException.class, () -> analyze("""
                FROM books METADATA _score
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "completion-inference-id" }
                """, "mapping-books.json"));

            assertThat(
                ve.getMessage(),
                containsString(
                    "cannot use inference endpoint [completion-inference-id] with task type [completion] within a Rerank command. "
                        + "Only inference endpoints with the task type [rerank] are supported"
                )
            );
        }

        {
            VerificationException ve = expectThrows(VerificationException.class, () -> analyze("""
                FROM books METADATA _score
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "error-inference-id" }
                """, "mapping-books.json"));

            assertThat(ve.getMessage(), containsString("error with inference resolution"));
        }

        {
            VerificationException ve = expectThrows(VerificationException.class, () -> analyze("""
                FROM books  METADATA _score
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "unknown-inference-id" }
                """, "mapping-books.json"));
            assertThat(ve.getMessage(), containsString("unresolved inference [unknown-inference-id]"));
        }
    }

    public void testResolveRerankFields() {
        {
            // Single field.
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | WHERE title:"italian food recipe" OR description:"italian food recipe"
                | KEEP description, title, year, _score
                | DROP description
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);
            EsqlProject keep = as(rerank.child(), EsqlProject.class);
            EsqlProject drop = as(keep.child(), EsqlProject.class);
            Filter filter = as(drop.child(), Filter.class);
            EsRelation relation = as(filter.child(), EsRelation.class);

            Attribute titleAttribute = getAttributeByName(relation.output(), "title");
            assertThat(getAttributeByName(relation.output(), "title"), notNullValue());

            assertThat(rerank.queryText(), equalTo(string("italian food recipe")));
            assertThat(rerank.inferenceId(), equalTo(string("reranking-inference-id")));
            assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", titleAttribute))));
            assertThat(rerank.scoreAttribute(), equalTo(getAttributeByName(relation.output(), MetadataAttribute.SCORE)));
        }

        {
            // Multiple fields.
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | WHERE title:"food"
                | RERANK "food" ON title, description=SUBSTRING(description, 0, 100), yearRenamed=year
                  WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);
            Filter filter = as(rerank.child(), Filter.class);
            EsRelation relation = as(filter.child(), EsRelation.class);

            assertThat(rerank.queryText(), equalTo(string("food")));
            assertThat(rerank.inferenceId(), equalTo(string("reranking-inference-id")));

            assertThat(rerank.rerankFields(), hasSize(3));
            Attribute titleAttribute = getAttributeByName(relation.output(), "title");
            assertThat(titleAttribute, notNullValue());
            assertThat(rerank.rerankFields().get(0), equalToIgnoringIds(alias("title", titleAttribute)));

            Attribute descriptionAttribute = getAttributeByName(relation.output(), "description");
            assertThat(descriptionAttribute, notNullValue());
            Alias descriptionAlias = rerank.rerankFields().get(1);
            assertThat(descriptionAlias.name(), equalTo("description"));
            assertThat(
                as(descriptionAlias.child(), Substring.class).children(),
                equalTo(List.of(descriptionAttribute, literal(0), literal(100)))
            );

            Attribute yearAttribute = getAttributeByName(relation.output(), "year");
            assertThat(yearAttribute, notNullValue());
            assertThat(rerank.rerankFields().get(2), equalToIgnoringIds(alias("yearRenamed", yearAttribute)));

            assertThat(rerank.scoreAttribute(), equalTo(getAttributeByName(relation.output(), MetadataAttribute.SCORE)));
        }

        {
            VerificationException ve = expectThrows(
                VerificationException.class,
                () -> analyze("""
                    FROM books METADATA _score
                    | RERANK \"italian food recipe\" ON missingField WITH { "inference_id" : "reranking-inference-id" }
                    """, "mapping-books.json")

            );
            assertThat(ve.getMessage(), containsString("Unknown column [missingField]"));
        }
    }

    public void testResolveRerankScoreField() {
        {
            // When the metadata field is required in FROM, it is reused.
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | WHERE title:"italian food recipe" OR description:"italian food recipe"
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);
            Filter filter = as(rerank.child(), Filter.class);
            EsRelation relation = as(filter.child(), EsRelation.class);

            Attribute metadataScoreAttribute = getAttributeByName(relation.output(), MetadataAttribute.SCORE);
            assertThat(rerank.scoreAttribute(), equalTo(metadataScoreAttribute));
            assertThat(rerank.output(), hasItem(metadataScoreAttribute));
        }

        {
            // When the metadata field is not required in FROM, it is added to the output of RERANK
            LogicalPlan plan = analyze("""
                FROM books
                | WHERE title:"italian food recipe" OR description:"italian food recipe"
                | RERANK "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);
            Filter filter = as(rerank.child(), Filter.class);
            EsRelation relation = as(filter.child(), EsRelation.class);

            assertThat(relation.output().stream().noneMatch(attr -> attr.name().equals(MetadataAttribute.SCORE)), is(true));
            assertThat(rerank.scoreAttribute(), equalToIgnoringIds(MetadataAttribute.create(EMPTY, MetadataAttribute.SCORE)));
            assertThat(rerank.output(), hasItem(rerank.scoreAttribute()));
        }

        {
            // When using a custom fields that does not exist
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | WHERE title:"italian food recipe" OR description:"italian food recipe"
                | RERANK rerank_score = "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);

            Attribute scoreAttribute = rerank.scoreAttribute();
            assertThat(scoreAttribute.name(), equalTo("rerank_score"));
            assertThat(scoreAttribute.dataType(), equalTo(DOUBLE));
            assertThat(rerank.output(), hasItem(scoreAttribute));
        }

        {
            // When using a custom fields that already exists
            LogicalPlan plan = analyze("""
                FROM books METADATA _score
                | WHERE title:"italian food recipe" OR description:"italian food recipe"
                | EVAL rerank_score = _score
                | RERANK rerank_score = "italian food recipe" ON title WITH { "inference_id" : "reranking-inference-id" }
                """, "mapping-books.json");

            Limit limit = as(plan, Limit.class); // Implicit limit added by AddImplicitLimit rule.
            Rerank rerank = as(limit.child(), Rerank.class);

            Attribute scoreAttribute = rerank.scoreAttribute();
            assertThat(scoreAttribute.name(), equalTo("rerank_score"));
            assertThat(scoreAttribute.dataType(), equalTo(DOUBLE));
            assertThat(rerank.output(), hasItem(scoreAttribute));
            assertThat(rerank.child().output().stream().anyMatch(scoreAttribute::equals), is(true));
        }
    }

    public void testRerankInvalidQueryTypes() {
        assertError("""
            FROM books METADATA _score
            | RERANK rerank_score = 42 ON title WITH { "inference_id" : "reranking-inference-id" }
            """, "mapping-books.json", new QueryParams(), "query must be a valid string in RERANK, found [42]");

        assertError("""
            FROM books METADATA _score
            | RERANK rerank_score = null ON title WITH { "inference_id" : "reranking-inference-id" }
            """, "mapping-books.json", new QueryParams(), "query must be a valid string in RERANK, found [null]");
    }

    public void testRerankFieldsInvalidTypes() {
        List<String> invalidFieldNames = List.of("date", "date_nanos", "ip", "version", "dense_vector");

        for (String fieldName : invalidFieldNames) {
            LogManager.getLogger(AnalyzerTests.class).warn("[{}]", fieldName);
            assertError(
                "FROM books METADATA _score | RERANK rerank_score = \"test query\" ON "
                    + fieldName
                    + " WITH { \"inference_id\" : \"reranking-inference-id\" }",
                "mapping-all-types.json",
                new QueryParams(),
                "rerank field must be a valid string, numeric or boolean expression, found [" + fieldName + "]"
            );
        }
    }

    public void testRerankFieldValidTypes() {
        List<String> validFieldNames = List.of(
            "boolean",
            "byte",
            "constant_keyword-foo",
            "double",
            "float",
            "half_float",
            "scaled_float",
            "integer",
            "keyword",
            "long",
            "unsigned_long",
            "short",
            "text",
            "wildcard"
        );

        for (String fieldName : validFieldNames) {
            LogicalPlan plan = analyze(
                "FROM books METADATA _score | RERANK rerank_score = \"test query\" ON `"
                    + fieldName
                    + "` WITH { \"inference_id\" : \"reranking-inference-id\" }",
                "mapping-all-types.json"
            );

            Rerank rerank = as(as(plan, Limit.class).child(), Rerank.class);
            EsRelation relation = as(rerank.child(), EsRelation.class);
            Attribute fieldAttribute = getAttributeByName(relation.output(), fieldName);
            if (DataType.isString(fieldAttribute.dataType())) {
                assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias(fieldName, fieldAttribute))));

            } else {
                assertThat(
                    rerank.rerankFields(),
                    equalToIgnoringIds(List.of(alias(fieldName, new ToString(fieldAttribute.source(), fieldAttribute))))
                );
            }
        }
    }

    public void testInvalidValidRerankQuery() {
        assertError("""
            FROM books METADATA _score
            | RERANK rerank_score = 42 ON title WITH { "inference_id" : "reranking-inference-id" }
            """, "mapping-books.json", new QueryParams(), "query must be a valid string in RERANK, found [42]");
    }

    public void testResolveCompletionInferenceId() {
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate this text in French\\n", description) WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Completion completion = as(as(plan, Limit.class).child(), Completion.class);
        assertThat(completion.inferenceId(), equalTo(string("completion-inference-id")));
    }

    public void testResolveCompletionInferenceIdInvalidTaskType() {
        assertError(
            """
                FROM books METADATA _score
                | COMPLETION CONCAT("Translate this text in French\\n", description) WITH { "inference_id" : "reranking-inference-id" }
                """,
            "mapping-books.json",
            new QueryParams(),
            "cannot use inference endpoint [reranking-inference-id] with task type [rerank] within a Completion command."
                + " Only inference endpoints with the task type [completion] are supported"
        );
    }

    public void testResolveCompletionInferenceMissingInferenceId() {
        assertError("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate the following text in French\\n", description) WITH { "inference_id" : "unknown-inference-id" }
            """, "mapping-books.json", new QueryParams(), "unresolved inference [unknown-inference-id]");
    }

    public void testResolveCompletionInferenceIdResolutionError() {
        assertError("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate the following text in French\\n", description) WITH { "inference_id" : "error-inference-id" }
            """, "mapping-books.json", new QueryParams(), "error with inference resolution");
    }

    public void testResolveCompletionTargetField() {
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION translation = CONCAT("Translate the following text in French\\n", description)
              WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Completion completion = as(as(plan, Limit.class).child(), Completion.class);
        assertThat(completion.targetField(), equalToIgnoringIds(referenceAttribute("translation", DataType.KEYWORD)));
    }

    public void testResolveCompletionDefaultTargetField() {
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate this text in French\\n", description) WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Completion completion = as(as(plan, Limit.class).child(), Completion.class);
        assertThat(completion.targetField(), equalToIgnoringIds(referenceAttribute("completion", DataType.KEYWORD)));
    }

    public void testResolveCompletionPrompt() {
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate this text in French\\n", description) WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Completion completion = as(as(plan, Limit.class).child(), Completion.class);
        EsRelation esRelation = as(completion.child(), EsRelation.class);

        assertThat(
            as(completion.prompt(), Concat.class).children(),
            equalTo(List.of(string("Translate this text in French\n"), getAttributeByName(esRelation.output(), "description")))
        );
    }

    public void testResolveCompletionPromptInvalidType() {
        assertError("""
            FROM books METADATA _score
            | COMPLETION LENGTH(description) WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json", new QueryParams(), "prompt must be of type [text] but is [integer]");
    }

    public void testResolveCompletionOutputFieldOverwriteInputField() {
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION description = CONCAT("Translate the following text in French\\n", description)
              WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Completion completion = as(as(plan, Limit.class).child(), Completion.class);
        assertThat(completion.targetField(), equalToIgnoringIds(referenceAttribute("description", DataType.KEYWORD)));

        EsRelation esRelation = as(completion.child(), EsRelation.class);
        assertThat(getAttributeByName(completion.output(), "description"), equalTo(completion.targetField()));
        assertThat(getAttributeByName(esRelation.output(), "description"), not(equalTo(completion.targetField())));
    }

    public void testFoldableCompletionTransformedToEval() {
        // Test that a foldable Completion plan (with literal prompt) is transformed to Eval with CompletionFunction
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION "Translate this text in French" WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Eval eval = as(as(plan, Limit.class).child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));

        Alias alias = eval.fields().get(0);
        assertThat(alias.name(), equalTo("completion"));
        assertThat(alias.child(), instanceOf(CompletionFunction.class));

        CompletionFunction completionFunction = as(alias.child(), CompletionFunction.class);
        assertThat(completionFunction.prompt(), equalTo(string("Translate this text in French")));
        assertThat(completionFunction.inferenceId(), equalTo(string("completion-inference-id")));
        assertThat(completionFunction.taskType(), equalTo(org.elasticsearch.inference.TaskType.COMPLETION));
    }

    public void testFoldableCompletionWithCustomTargetFieldTransformedToEval() {
        // Test that a foldable Completion plan with custom target field is transformed correctly
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION translation = "Translate this text" WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Eval eval = as(as(plan, Limit.class).child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));

        Alias alias = eval.fields().get(0);
        assertThat(alias.name(), equalTo("translation"));
        assertThat(alias.child(), instanceOf(CompletionFunction.class));

        CompletionFunction completionFunction = as(alias.child(), CompletionFunction.class);
        assertThat(completionFunction.prompt(), equalTo(string("Translate this text")));
        assertThat(completionFunction.inferenceId(), equalTo(string("completion-inference-id")));
    }

    public void testFoldableCompletionWithFoldableExpressionTransformedToEval() {
        // Test that a foldable Completion plan with a foldable expression (not just a literal) is transformed correctly
        // Using CONCAT with all literal arguments to ensure it's foldable during analysis
        LogicalPlan plan = analyze("""
            FROM books METADATA _score
            | COMPLETION CONCAT("Translate", " ", "this text") WITH { "inference_id" : "completion-inference-id" }
            """, "mapping-books.json");

        Eval eval = as(as(plan, Limit.class).child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));

        Alias alias = eval.fields().get(0);
        assertThat(alias.name(), equalTo("completion"));
        assertThat(alias.child(), instanceOf(CompletionFunction.class));

        CompletionFunction completionFunction = as(alias.child(), CompletionFunction.class);
        // The prompt should be a Concat expression that is foldable (all arguments are literals)
        assertThat(completionFunction.prompt(), instanceOf(Concat.class));
        assertThat(completionFunction.prompt().foldable(), equalTo(true));
        assertThat(completionFunction.inferenceId(), equalTo(string("completion-inference-id")));
    }

    public void testResolveGroupingsBeforeResolvingImplicitReferencesToGroupings() {
        var plan = analyze("""
            FROM test
            | EVAL date = "2025-01-01"::datetime
            | STATS c = count(emp_no) BY d = (date == "2025-01-01")
            """, "mapping-default.json");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(2));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("c", a.name());
        Count c = as(a.child(), Count.class);
        FieldAttribute fa = as(c.field(), FieldAttribute.class);
        assertEquals("emp_no", fa.name());
        ReferenceAttribute ra = as(aggregates.get(1), ReferenceAttribute.class); // reference in aggregates is resolved
        assertEquals("d", ra.name());
        List<Expression> groupings = agg.groupings();
        assertEquals(1, groupings.size());
        a = as(groupings.get(0), Alias.class); // reference in groupings is resolved
        assertEquals("d", ra.name());
        Equals equals = as(a.child(), Equals.class);
        ra = as(equals.left(), ReferenceAttribute.class);
        assertEquals("date", ra.name());
        Literal literal = as(equals.right(), Literal.class);
        assertEquals("2025-01-01T00:00:00.000Z", dateTimeToString(Long.parseLong(literal.value().toString())));
        assertEquals(DATETIME, literal.dataType());
    }

    public void testResolveGroupingsBeforeResolvingExplicitReferencesToGroupings() {
        var plan = analyze("""
            FROM test
            | EVAL date = "2025-01-01"::datetime
            | STATS c = count(emp_no), x = d::int + 1 BY d = (date == "2025-01-01")
            """, "mapping-default.json");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(3));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("c", a.name());
        Count c = as(a.child(), Count.class);
        FieldAttribute fa = as(c.field(), FieldAttribute.class);
        assertEquals("emp_no", fa.name());
        a = as(aggregates.get(1), Alias.class); // explicit reference to groupings is resolved
        assertEquals("x", a.name());
        Add add = as(a.child(), Add.class);
        ToInteger toInteger = as(add.left(), ToInteger.class);
        ReferenceAttribute ra = as(toInteger.field(), ReferenceAttribute.class);
        assertEquals("d", ra.name());
        ra = as(aggregates.get(2), ReferenceAttribute.class); // reference in aggregates is resolved
        assertEquals("d", ra.name());
        List<Expression> groupings = agg.groupings();
        assertEquals(1, groupings.size());
        a = as(groupings.get(0), Alias.class); // reference in groupings is resolved
        assertEquals("d", ra.name());
        Equals equals = as(a.child(), Equals.class);
        ra = as(equals.left(), ReferenceAttribute.class);
        assertEquals("date", ra.name());
        Literal literal = as(equals.right(), Literal.class);
        assertEquals("2025-01-01T00:00:00.000Z", dateTimeToString(Long.parseLong(literal.value().toString())));
        assertEquals(DATETIME, literal.dataType());
    }

    public void testBucketWithIntervalInStringInBothAggregationAndGrouping() {
        var plan = analyze("""
            FROM test
            | STATS c = count(emp_no), b = BUCKET(hire_date, "1 year") + 1 year BY yr = BUCKET(hire_date, "1 year")
            """, "mapping-default.json");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(3));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("c", a.name());
        Count c = as(a.child(), Count.class);
        FieldAttribute fa = as(c.field(), FieldAttribute.class);
        assertEquals("emp_no", fa.name());
        a = as(aggregates.get(1), Alias.class); // explicit reference to groupings is resolved
        assertEquals("b", a.name());
        Add add = as(a.child(), Add.class);
        Bucket bucket = as(add.left(), Bucket.class);
        fa = as(bucket.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        Literal literal = as(bucket.buckets(), Literal.class);
        Literal oneYear = new Literal(EMPTY, Period.ofYears(1), DATE_PERIOD);
        assertEquals(oneYear, literal);
        literal = as(add.right(), Literal.class);
        assertEquals(oneYear, literal);
        ReferenceAttribute ra = as(aggregates.get(2), ReferenceAttribute.class); // reference in aggregates is resolved
        assertEquals("yr", ra.name());
        List<Expression> groupings = agg.groupings();
        assertEquals(1, groupings.size());
        a = as(groupings.get(0), Alias.class); // reference in groupings is resolved
        assertEquals("yr", ra.name());
        bucket = as(a.child(), Bucket.class);
        fa = as(bucket.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        literal = as(bucket.buckets(), Literal.class);
        assertEquals(oneYear, literal);
    }

    public void testBucketWithIntervalInStringInGroupingReferencedInAggregation() {
        var plan = analyze("""
            FROM test
            | STATS c = count(emp_no), b = yr + 1 year BY yr = BUCKET(hire_date, "1 year")
            """, "mapping-default.json");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(3));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("c", a.name());
        Count c = as(a.child(), Count.class);
        FieldAttribute fa = as(c.field(), FieldAttribute.class);
        assertEquals("emp_no", fa.name());
        a = as(aggregates.get(1), Alias.class); // explicit reference to groupings is resolved
        assertEquals("b", a.name());
        Add add = as(a.child(), Add.class);
        ReferenceAttribute ra = as(add.left(), ReferenceAttribute.class);
        assertEquals("yr", ra.name());
        Literal oneYear = new Literal(EMPTY, Period.ofYears(1), DATE_PERIOD);
        Literal literal = as(add.right(), Literal.class);
        assertEquals(oneYear, literal);
        ra = as(aggregates.get(2), ReferenceAttribute.class); // reference in aggregates is resolved
        assertEquals("yr", ra.name());
        List<Expression> groupings = agg.groupings();
        assertEquals(1, groupings.size());
        a = as(groupings.get(0), Alias.class); // reference in groupings is resolved
        assertEquals("yr", ra.name());
        Bucket bucket = as(a.child(), Bucket.class);
        fa = as(bucket.field(), FieldAttribute.class);
        assertEquals("hire_date", fa.name());
        literal = as(bucket.buckets(), Literal.class);
        assertEquals(oneYear, literal);
    }

    public void testImplicitCastingForDateAndDateNanosFields() {
        IndexResolution indexWithUnionTypedFields = indexWithDateDateNanosUnionType();
        Analyzer analyzer = AnalyzerTestUtils.analyzer(indexWithUnionTypedFields);

        // Validate if a union typed field is cast to a type explicitly, implicit casting won't be applied again, and include some cases of
        // nested casting as well.
        LogicalPlan plan = analyze("""
            FROM index*
            | Eval a = date_and_date_nanos, b = date_and_date_nanos::datetime, c = date_and_date_nanos::date_nanos,
                   d = date_and_date_nanos::datetime::datetime, e = date_and_date_nanos::datetime::date_nanos,
                   f = date_and_date_nanos::date_nanos::datetime, g = date_and_date_nanos::date_nanos::date_nanos,
                   h = date_and_date_nanos::datetime::long, i = date_and_date_nanos::date_nanos::long,
                   j = date_and_date_nanos::long::datetime, k = date_and_date_nanos::long::date_nanos
            """, analyzer);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(13, projections.size());
        // implicit casting
        FieldAttribute fa = as(projections.get(0), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "date_and_date_nanos", DATE_NANOS, fa);
        // long is not cast to date_nanos
        UnsupportedAttribute ua = as(projections.get(1), UnsupportedAttribute.class);
        verifyNameAndType(ua.name(), ua.dataType(), "date_and_date_nanos_and_long", UNSUPPORTED);
        // implicit casting
        ReferenceAttribute ra = as(projections.get(2), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "a", DATE_NANOS);
        // explicit casting
        ra = as(projections.get(3), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "b", DATETIME);
        ra = as(projections.get(4), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "c", DATE_NANOS);
        ra = as(projections.get(5), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "d", DATETIME);
        ra = as(projections.get(6), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "e", DATE_NANOS);
        ra = as(projections.get(7), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "f", DATETIME);
        ra = as(projections.get(8), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "g", DATE_NANOS);
        ra = as(projections.get(9), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "h", LONG);
        ra = as(projections.get(10), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "i", LONG);
        ra = as(projections.get(11), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "j", DATETIME);
        ra = as(projections.get(12), ReferenceAttribute.class);
        verifyNameAndType(ra.name(), ra.dataType(), "k", DATE_NANOS);

        Limit limit = as(project.child(), Limit.class);
        // original Eval coded in the query
        Eval eval = as(limit.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(11, aliases.size());
        // implicit casting
        Alias a = aliases.get(0); // a = date_and_date_nanos
        verifyNameAndTypeAndMultiTypeEsField(a.name(), a.dataType(), "a", DATE_NANOS, a.child());
        // explicit casting
        a = aliases.get(1); // b = date_and_date_nanos::datetime
        verifyNameAndTypeAndMultiTypeEsField(a.name(), a.dataType(), "b", DATETIME, a.child());
        a = aliases.get(2); // c = date_and_date_nanos::date_nanos
        verifyNameAndTypeAndMultiTypeEsField(a.name(), a.dataType(), "c", DATE_NANOS, a.child());
        a = aliases.get(3); // d = date_and_date_nanos::datetime::datetime
        verifyNameAndType(a.name(), a.dataType(), "d", DATETIME);
        ToDatetime toDatetime = as(a.child(), ToDatetime.class);
        fa = as(toDatetime.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(
            fa.name(),
            fa.dataType(),
            "$$date_and_date_nanos$converted_to$datetime",
            DATETIME,
            toDatetime.field()
        );
        a = aliases.get(4); // e = date_and_date_nanos::datetime::date_nanos
        verifyNameAndType(a.name(), a.dataType(), "e", DATE_NANOS);
        ToDateNanos toDateNanos = as(a.child(), ToDateNanos.class);
        fa = as(toDateNanos.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(
            fa.name(),
            fa.dataType(),
            "$$date_and_date_nanos$converted_to$datetime",
            DATETIME,
            toDateNanos.field()
        );
        a = aliases.get(5); // f = date_and_date_nanos::date_nanos::datetime
        verifyNameAndType(a.name(), a.dataType(), "f", DATETIME);
        toDatetime = as(a.child(), ToDatetime.class);
        fa = as(toDatetime.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$date_nanos", DATE_NANOS, fa);
        a = aliases.get(6); // g = date_and_date_nanos::date_nanos::date_nanos
        verifyNameAndType(a.name(), a.dataType(), "g", DATE_NANOS);
        toDateNanos = as(a.child(), ToDateNanos.class);
        fa = as(toDateNanos.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$date_nanos", DATE_NANOS, fa);
        a = aliases.get(7); // h = date_and_date_nanos::datetime::long
        verifyNameAndType(a.name(), a.dataType(), "h", LONG);
        ToLong toLong = as(a.child(), ToLong.class);
        fa = as(toLong.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$datetime", DATETIME, fa);
        a = aliases.get(8); // i = date_and_date_nanos::date_nanos::long
        verifyNameAndType(a.name(), a.dataType(), "i", LONG);
        toLong = as(a.child(), ToLong.class);
        fa = as(toLong.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$date_nanos", DATE_NANOS, fa);
        a = aliases.get(9); // j = date_and_date_nanos::long::datetime
        verifyNameAndType(a.name(), a.dataType(), "j", DATETIME);
        toDatetime = as(a.child(), ToDatetime.class);
        fa = as(toDatetime.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$long", LONG, fa);
        a = aliases.get(10); // k = date_and_date_nanos::long::date_nanos
        verifyNameAndType(a.name(), a.dataType(), "k", DATE_NANOS);
        toDateNanos = as(a.child(), ToDateNanos.class);
        fa = as(toDateNanos.field(), FieldAttribute.class);
        verifyNameAndTypeAndMultiTypeEsField(fa.name(), fa.dataType(), "$$date_and_date_nanos$converted_to$long", LONG, fa);
        EsRelation esRelation = as(eval.child(), EsRelation.class);
        assertEquals("index*", esRelation.indexPattern());
    }

    public void testGroupingOverridesInStats() {
        verifyUnsupported("""
            from test
            | stats MIN(salary) BY x = languages, x = x + 1
            """, "Found 1 problem\n" + "line 2:43: Unknown column [x]", "mapping-default.json");
    }

    public void testGroupingOverridesInInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        verifyUnsupported("""
            from test
            | inline stats MIN(salary) BY x = languages, x = x + 1
            """, "Found 1 problem\n" + "line 2:50: Unknown column [x]", "mapping-default.json");
    }

    public void testInlineStatsStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        verifyUnsupported("""
            from test
            | inline stats stats
            """, "Found 1 problem\n" + "line 2:16: Unknown column [stats]", "mapping-default.json");
        // TODO: drop after next minor release
        verifyUnsupported("""
            from test
            | inlinestats stats
            """, "Found 1 problem\n" + "line 2:15: Unknown column [stats]", "mapping-default.json");
    }

    public void testTBucketWithDatePeriodInBothAggregationAndGrouping() {
        LogicalPlan plan = analyze("""
            FROM sample_data
            | STATS min = MIN(@timestamp), max = MAX(@timestamp) BY bucket = TBUCKET(1 week)
            | SORT min
            """, "mapping-sample_data.json");

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Aggregate agg = as(orderBy.child(), Aggregate.class);

        List<? extends NamedExpression> aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(3));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("min", a.name());
        Min min = as(a.child(), Min.class);
        FieldAttribute fa = as(min.field(), FieldAttribute.class);
        assertEquals("@timestamp", fa.name());
        a = as(aggregates.get(1), Alias.class);
        assertEquals("max", a.name());
        Max max = as(a.child(), Max.class);
        fa = as(max.field(), FieldAttribute.class);
        assertEquals("@timestamp", fa.name());
        ReferenceAttribute ra = as(aggregates.get(2), ReferenceAttribute.class);
        assertEquals("bucket", ra.name());

        List<Expression> groupings = agg.groupings();
        assertEquals(1, groupings.size());
        a = as(groupings.get(0), Alias.class); // reference in groupings is resolved
        TBucket tbucket = as(a.child(), TBucket.class);
        fa = as(tbucket.timestamp(), FieldAttribute.class);
        assertEquals("@timestamp", fa.name());
        Literal literal = as(tbucket.buckets(), Literal.class);
        Literal oneWeek = new Literal(EMPTY, Period.ofWeeks(1), DATE_PERIOD);
        assertEquals(oneWeek, literal);
    }

    private void verifyNameAndType(String actualName, DataType actualType, String expectedName, DataType expectedType) {
        assertEquals(expectedName, actualName);
        assertEquals(expectedType, actualType);
    }

    public void testImplicitCastingForAggregateMetricDouble() {
        assumeTrue(
            "aggregate metric double implicit casting must be available",
            EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled()
        );
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            new EsField("@timestamp", DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "cluster",
            new EsField("cluster", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "metric_field",
            new InvalidMappedField("metric_field", Map.of("aggregate_metric_double", Set.of("k8s-downsampled"), "double", Set.of("k8s")))
        );

        var esIndex = new EsIndex(
            "k8s*",
            mapping,
            Map.of("k8s", IndexMode.TIME_SERIES, "k8s-downsampled", IndexMode.TIME_SERIES),
            Set.of()
        );
        var analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(esIndex),
                defaultEnrichResolution(),
                defaultInferenceResolution()
            ),
            TEST_VERIFIER
        );
        var e = expectThrows(VerificationException.class, () -> analyze("""
            from k8s* | stats std_dev(metric_field)
            """, analyzer));
        assertThat(
            e.getMessage(),
            containsString("Cannot use field [metric_field] due to ambiguities being mapped as [2] incompatible types")
        );

        var plan = analyze("""
            from k8s* | stats max = max(metric_field),
            avg = avg(metric_field),
            sum = sum(metric_field),
            min = min(metric_field),
            count = count(metric_field)
            """, analyzer);
        assertProjection(plan, "max", "avg", "sum", "min", "count");

        var plan2 = analyze("""
            TS k8s* | stats s1 = sum(sum_over_time(metric_field)),
            s2 = sum(avg_over_time(metric_field)),
            min = min(max_over_time(metric_field)),
            count = count(count_over_time(metric_field)),
            avg = avg(min_over_time(metric_field))
            by cluster, time_bucket = bucket(@timestamp,1minute)
            """, analyzer);
        assertProjection(plan2, "s1", "s2", "min", "count", "avg", "cluster", "time_bucket");
    }

    public void testSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(2, order.size());
        ReferenceAttribute empNo = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        ReferenceAttribute languageCode = as(order.get(1).child(), ReferenceAttribute.class);
        assertEquals("language_code", languageCode.name());
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        Literal limitLiteral = as(subqueryLimit.limit(), Literal.class);
        assertEquals(1000, limitLiteral.value());
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        List<? extends NamedExpression> projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from languages index
        assertEquals(2, aliases.size());
        assertEquals("language_code", aliases.get(0).name());
        Literal nullLiteral = as(aliases.get(0).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(INTEGER, nullLiteral.dataType());
        assertEquals("language_name", aliases.get(1).name());
        nullLiteral = as(aliases.get(1).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(KEYWORD, nullLiteral.dataType());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        limitLiteral = as(subqueryLimit.limit(), Literal.class);
        assertEquals(1000, limitLiteral.value());
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from test index
        assertEquals(11, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
    }

    /**
     * If there is only one subquery in the main from command, the subquery is merged into the main index pattern
     */
    public void testSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM (FROM languages | WHERE language_code > 1)
            | WHERE language_name is not null
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        FieldAttribute language_name = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    public void testMultipleSubqueriesInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            , (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        EsqlProject rename = as(mvExpand.child(), EsqlProject.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        Literal limitLiteral = as(subqueryLimit.limit(), Literal.class);
        assertEquals(1000, limitLiteral.value());
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(4, aliases.size());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), EsqlProject.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(2), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(3), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
    }

    public void testMultipleSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        EsqlProject rename = as(mvExpand.child(), EsqlProject.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        EsRelation subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), EsqlProject.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(2), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

    }

    public void testNestedSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryLimit = as(unionAll.children().get(0), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    public void testNestedSubqueryInFromWithMetadata() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10) metadata _index
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
        List<Attribute> output = subqueryIndex.output();
        assertEquals(12, output.size());
        MetadataAttribute metadataAttribute = as(output.get(11), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryLimit = as(unionAll.children().get(0), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(2, output.size());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(4, output.size());
    }

    public void testNestedSubqueriesInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM (FROM test, (FROM sample_data | STATS count(*)) | WHERE emp_no > 10)
            | WHERE languages is not null
            | SORT emp_no, languages
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> orderKeys = orderBy.order();
        assertEquals(2, orderKeys.size());
        ReferenceAttribute emp_no = as(orderKeys.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        ReferenceAttribute languages = as(orderKeys.get(1).child(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        Filter filter = as(orderBy.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        languages = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    /*
     * When there are mixed date types between the main query and the subquery, the fields/references need to be casted to a common type
     * in the UnionAll legs, otherwise  FORK's postAnalysisPlanVerification will fail. The common type can be date_nanos,
     * keyword with null if the fields are not referenced in the main query, or unsupported if the fields are referenced in the main query.
     */
    public void testMixedDataTypesInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | SORT emp_no
            """, "mapping-default.json");

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute emp_no = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testMixedDataTypesWithExplicitCastingInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """, "mapping-default.json");

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias a = aliases.get(0);
        assertEquals("still_hired", a.name());
        ReferenceAttribute still_hired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$still_hired$converted_to$keyword", still_hired.name());
        a = aliases.get(1);
        assertEquals("is_rehired", a.name());
        ReferenceAttribute is_rehired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$is_rehired$converted_to$keyword", is_rehired.name());
        Filter filter = as(eval.child(), Filter.class);
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(1, aliases.size());
        a = aliases.get(0);
        assertEquals("emp_no", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(28, output.size());
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testMixedDataTypesWithMultipleExplicitCastingInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL x = emp_no::long, y = emp_no::string, z = emp_no::double, first_name = first_name::string
            | WHERE z > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """, "mapping-default.json");

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(28, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias a = aliases.get(0);
        assertEquals("still_hired", a.name());
        ReferenceAttribute still_hired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$still_hired$converted_to$keyword", still_hired.name());
        a = aliases.get(1);
        assertEquals("is_rehired", a.name());
        ReferenceAttribute is_rehired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$is_rehired$converted_to$keyword", is_rehired.name());
        Filter filter = as(eval.child(), Filter.class);
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(4, aliases.size());
        a = aliases.get(0);
        assertEquals("x", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        a = aliases.get(1);
        assertEquals("y", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$keyword", emp_no.name());
        a = aliases.get(2);
        assertEquals("z", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$double", emp_no.name());
        a = aliases.get(3);
        assertEquals("first_name", a.name());
        ReferenceAttribute first_name = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$first_name$converted_to$keyword", first_name.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(31, output.size());
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testSubqueryWithUnionAllOutputOverwritten() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM test, (FROM test_mixed_types | WHERE languages > 1)
            | EVAL emp_no = languages::long
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(24, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute language_code = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$languages$converted_to$long", language_code.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(25, output.size());
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    public void testSubqueryWithTimeSeriesIndexInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM k8s, (FROM sample_data), (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, "k8s-downsampled-mappings.json");

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(24, output.size());
        assertEquals(3, unionAll.children().size());

        limit = as(unionAll.children().get(0), Limit.class);
        EsqlProject esqlProject = as(limit.child(), EsqlProject.class);
        Eval eval = as(esqlProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(1), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        relation = as(subquery.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(2), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithTimeSeriesIndexInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM sample_data,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, "mapping-sample_data.json");

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        limit = as(unionAll.children().get(0), Limit.class);
        EsqlProject esqlProject = as(limit.child(), EsqlProject.class);
        Eval eval = as(esqlProject.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(1), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(2), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithTimeSeriesIndexInMainQueryAndSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM k8s,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, "k8s-downsampled-mappings.json");

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        limit = as(unionAll.children().get(0), Limit.class);
        EsqlProject esqlProject = as(limit.child(), EsqlProject.class);
        Eval eval = as(esqlProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(1), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        limit = as(unionAll.children().get(2), Limit.class);
        esqlProject = as(limit.child(), EsqlProject.class);
        eval = as(esqlProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testSubqueryWithFullTextFunctionInMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyze("""
            FROM sample_data, (FROM sample_data | WHERE message:"error")
            | WHERE match(client_ip,"127.0.0.1")
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match matchFunction = as(filter.condition(), Match.class);
        ReferenceAttribute clientIP = as(matchFunction.field(), ReferenceAttribute.class);
        assertEquals("client_ip", clientIP.name());
        Literal literal = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("127.0.0.1"), literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        // all fields from the two indices
        assertEquals(4, output.size());
        assertEquals(2, unionAll.children().size());

        Limit subqueryLimit = as(unionAll.children().get(0), Limit.class);
        EsqlProject subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryLimit = as(unionAll.children().get(1), Limit.class);
        subqueryProject = as(subqueryLimit.child(), EsqlProject.class);
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        MatchOperator matchOperator = as(subqueryFilter.condition(), MatchOperator.class);
        FieldAttribute message = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("message", message.name());
        literal = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("error"), literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    public void testLookupJoinOnFieldNotAnywhereElse() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION_BUGFIX.isEnabled()
        );

        String query = "FROM test | LOOKUP JOIN languages_lookup "
            + "ON languages == language_code AND MATCH(language_name, \"English\")"
            + "| KEEP languages";

        LogicalPlan analyzedPlan = analyze(query, Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        Limit limit = as(analyzedPlan, Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertEquals(1000, as(limit.limit(), Literal.class).value());

        EsqlProject project = as(limit.child(), EsqlProject.class);
        assertEquals(1, project.projections().size());

        LookupJoin lookupJoin = as(project.child(), LookupJoin.class);

        // Verify join condition contains MATCH function
        assertThat(lookupJoin.config().joinOnConditions(), notNullValue());
        Expression joinCondition = lookupJoin.config().joinOnConditions();
        // Check that the join condition contains a MATCH function (it should be an AND expression)
        assertThat(joinCondition.toString(), containsString("MATCH"));

        // Verify left relation is test index
        EsRelation leftRelation = as(lookupJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Verify right relation is languages_lookup with LOOKUP mode
        EsRelation rightRelation = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", rightRelation.indexPattern());
        assertEquals(IndexMode.LOOKUP, rightRelation.indexMode());
    }

    private void verifyNameAndTypeAndMultiTypeEsField(
        String actualName,
        DataType actualType,
        String expectedName,
        DataType expectedType,
        Expression e
    ) {
        assertEquals(expectedName, actualName);
        assertEquals(expectedType, actualType);
        assertTrue(isMultiTypeEsField(e));
    }

    private boolean isMultiTypeEsField(Expression e) {
        return e instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField;
    }

    @Override
    protected IndexAnalyzers createDefaultIndexAnalyzers() {
        return super.createDefaultIndexAnalyzers();
    }

    static Alias alias(String name, Expression value) {
        return new Alias(EMPTY, name, value);
    }

    static Literal string(String value) {
        return new Literal(EMPTY, BytesRefs.toBytesRef(value), DataType.KEYWORD);
    }

    static Literal literal(int value) {
        return new Literal(EMPTY, value, INTEGER);
    }

    static IndexResolver.FieldsInfo fieldsInfoOnCurrentVersion(FieldCapabilitiesResponse caps) {
        return new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false);
    }
}
