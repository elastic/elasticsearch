/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForExistingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForMissingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.DOWN;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LocalLogicalPlanOptimizerTests extends ESTestCase {

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static Analyzer allTypesAnalyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static Map<String, EsField> mapping;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext());

        analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var allTypesMapping = loadMapping("mapping-all-types.json");
        EsIndex testAll = EsIndexGenerator.esIndex("test_all", allTypesMapping, Map.of("test_all", IndexMode.STANDARD));
        allTypesAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(testAll),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    /**
     * Expects
     * LocalRelation[[first_name{f}#4],EMPTY]
     */
    public void testMissingFieldInFilterNumeric() {
        var plan = plan("""
              from test
            | where emp_no > 10
            | keep first_name
            """);

        var testStats = statsForMissingField("emp_no");
        var localPlan = localPlan(plan, testStats);

        var empty = asEmptyRelation(localPlan);
        assertThat(Expressions.names(empty.output()), contains("first_name"));
    }

    /**
     * Expects
     * LocalRelation[[first_name{f}#4],EMPTY]
     */
    public void testMissingFieldInFilterString() {
        var plan = plan("""
              from test
            | where starts_with(last_name, "abc")
            | keep first_name
            """);

        var testStats = statsForMissingField("last_name");
        var localPlan = localPlan(plan, testStats);

        var empty = asEmptyRelation(localPlan);
        assertThat(Expressions.names(empty.output()), contains("first_name"));
    }

    /**
     * Expects
     * Project[[last_name{r}#7]]
     * \_Eval[[null[KEYWORD] AS last_name]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     */
    public void testMissingFieldInProject() {
        var plan = plan("""
              from test
            | keep last_name
            """);

        var testStats = statsForMissingField("last_name");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("last_name"));
        as(projections.get(0), ReferenceAttribute.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("last_name"));
        var alias = as(eval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(nullValue()));
        assertThat(literal.dataType(), is(KEYWORD));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
        assertThat(Expressions.names(source.output()), not(contains("last_name")));
    }

    /*
     * Expects a similar plan to testMissingFieldInProject() above, except for the Alias's child value
     * Project[[last_name{r}#4]]
     * \_Eval[[[66 6f 6f][KEYWORD] AS last_name]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testReassignedMissingFieldInProject() {
        var plan = plan("""
              from test
            | keep last_name
            | eval last_name = "foo"
            """);

        var testStats = statsForMissingField("last_name");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("last_name"));
        as(projections.get(0), ReferenceAttribute.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("last_name"));
        var alias = as(eval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(new BytesRef("foo")));
        assertThat(literal.dataType(), is(KEYWORD));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
        assertThat(Expressions.names(source.output()), not(contains("last_name")));
    }

    /**
     * Expects
     * EsqlProject[[first_name{f}#4]]
     * \_Limit[10000[INTEGER]]
     * \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
     */
    public void testMissingFieldInSort() {
        var plan = plan("""
              from test
            | sort last_name
            | keep first_name
            """);

        var testStats = statsForMissingField("last_name");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("first_name"));

        var limit = as(project.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
        assertThat(Expressions.names(source.output()), not(contains("last_name")));
    }

    /**
     * Expects
     * EsqlProject[[first_name{f}#7, last_name{r}#17]]
     * \_Limit[1000[INTEGER],true]
     *   \_MvExpand[last_name{f}#10,last_name{r}#17]
     *     \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{r}#10, long_noidx{f}#16, salary{f}#11]]
     *       \_Eval[[null[KEYWORD] AS last_name]]
     *         \_Limit[1000[INTEGER],false]
     *           \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testMissingFieldInMvExpand() {
        var plan = plan("""
              from test
            | mv_expand last_name
            | keep first_name, last_name
            """);

        var testStats = statsForMissingField("last_name");
        var localPlan = localPlan(plan, testStats);

        // It'd be much better if this project was pushed down past the MvExpand, because MvExpand's cost scales with the number of
        // involved attributes/columns.
        var project = as(localPlan, EsqlProject.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("first_name", "last_name"));

        var limit1 = asLimit(project.child(), 1000, true);
        var mvExpand = as(limit1.child(), MvExpand.class);
        var project2 = as(mvExpand.child(), Project.class);
        var eval = as(project2.child(), Eval.class);
        assertEquals(eval.fields().size(), 1);
        var lastName = eval.fields().get(0);
        assertEquals(lastName.name(), "last_name");
        assertEquals(lastName.child(), new Literal(EMPTY, null, KEYWORD));
        var limit2 = asLimit(eval.child(), 1000, false);
        var relation = as(limit2.child(), EsRelation.class);
        assertThat(Expressions.names(relation.output()), not(contains("last_name")));
    }

    public static class MockFieldAttributeCommand extends UnaryPlan {
        public FieldAttribute field;

        public MockFieldAttributeCommand(Source source, LogicalPlan child, FieldAttribute field) {
            super(source, child);
            this.field = field;
        }

        @Override
        protected AttributeSet computeReferences() {
            return AttributeSet.EMPTY;
        }

        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public UnaryPlan replaceChild(LogicalPlan newChild) {
            return new MockFieldAttributeCommand(source(), newChild, field);
        }

        @Override
        public boolean expressionsResolved() {
            return true;
        }

        @Override
        public List<Attribute> output() {
            return List.of(field);
        }

        @Override
        protected NodeInfo<? extends LogicalPlan> info() {
            return NodeInfo.create(this, MockFieldAttributeCommand::new, child(), field);
        }
    }

    public void testMissingFieldInNewCommand() {
        var testStats = statsForMissingField("last_name");
        localPlan(
            new MockFieldAttributeCommand(
                EMPTY,
                new Row(EMPTY, List.of()),
                new FieldAttribute(EMPTY, "last_name", new EsField("last_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
            ),
            testStats
        );

        var plan = plan("""
              from test
            """);
        var initialRelation = plan.collectLeaves().get(0);
        FieldAttribute lastName = null;
        for (Attribute attr : initialRelation.output()) {
            if (attr.name().equals("last_name")) {
                lastName = (FieldAttribute) attr;
            }
        }

        // Expects
        // MockFieldAttributeCommand[last_name{f}#7]
        // \_Project[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
        // ages{f}#6, last_name{r}#7, long_noidx{f}#13, salary{f}#8]]
        // \_Eval[[null[KEYWORD] AS last_name]]
        // \_Limit[1000[INTEGER],false]
        // \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
        LogicalPlan localPlan = localPlan(new MockFieldAttributeCommand(EMPTY, plan, lastName), testStats);

        var mockCommand = as(localPlan, MockFieldAttributeCommand.class);
        var project = as(mockCommand.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000);
        var relation = as(limit.child(), EsRelation.class);

        assertThat(Expressions.names(eval.fields()), contains("last_name"));
        var literal = as(eval.fields().get(0), Alias.class);
        assertEquals(literal.child(), new Literal(EMPTY, null, KEYWORD));
        assertThat(Expressions.names(relation.output()), not(contains("last_name")));

        assertEquals(Expressions.names(initialRelation.output()), Expressions.names(project.output()));
    }

    /**
     * Expects
     * EsqlProject[[x{r}#3]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_Limit[10000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     */
    public void testMissingFieldInEval() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1
            | keep x
            """);

        var testStats = statsForMissingField("emp_no");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));

        var alias = as(eval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(nullValue()));
        assertThat(literal.dataType(), is(INTEGER));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * LocalRelation[[first_name{f}#4],EMPTY]
     */
    public void testMissingFieldInFilterNumericWithReference() {
        var plan = plan("""
              from test
            | eval x = emp_no
            | where x > 10
            | keep first_name
            """);

        var testStats = statsForMissingField("emp_no");
        var localPlan = localPlan(plan, testStats);

        var local = as(localPlan, LocalRelation.class);
        assertThat(Expressions.names(local.output()), contains("first_name"));
    }

    /**
     * Expects
     * LocalRelation[[first_name{f}#4],EMPTY]
     */
    public void testMissingFieldInFilterNumericWithReferenceToEval() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1
            | where x > 10
            | keep first_name
            """);

        var testStats = statsForMissingField("emp_no");
        var localPlan = localPlan(plan, testStats);

        var local = as(localPlan, LocalRelation.class);
        assertThat(Expressions.names(local.output()), contains("first_name"));
    }

    /**
     * Expects
     * LocalRelation[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, languages{f}#8, last_name{f}#9, salary{f}#10, x
     * {r}#3],EMPTY]
     */
    public void testMissingFieldInFilterNoProjection() {
        var plan = plan("""
              from test
            | eval x = emp_no
            | where x > 10
            """);

        var testStats = statsForMissingField("emp_no");
        var localPlan = localPlan(plan, testStats);

        var local = as(localPlan, LocalRelation.class);
        assertThat(
            Expressions.names(local.output()),
            contains(
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
                "salary",
                "x"
            )
        );
    }

    public void testSparseDocument() throws Exception {
        var query = """
            from large
            | keep field00*
            | limit 10
            """;

        int size = 256;
        Map<String, EsField> large = Maps.newLinkedHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            var name = String.format(Locale.ROOT, "field%03d", i);
            large.put(name, new EsField(name, INTEGER, emptyMap(), true, false, EsField.TimeSeriesFieldType.NONE));
        }

        SearchStats searchStats = statsForExistingField("field000", "field001", "field002", "field003", "field004");

        EsIndex index = EsIndexGenerator.esIndex("large", large, Map.of("large", IndexMode.STANDARD));
        var logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext());

        var analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(index),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var analyzed = analyzer.analyze(parser.createStatement(query));
        var optimized = logicalOptimizer.optimize(analyzed);
        var localContext = new LocalLogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), searchStats);
        var plan = new LocalLogicalPlanOptimizer(localContext).localOptimize(optimized);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(10));
        assertThat(
            Expressions.names(project.projections()),
            contains("field000", "field001", "field002", "field003", "field004", "field005", "field006", "field007", "field008", "field009")
        );
        var eval = as(project.child(), Eval.class);
        var field = eval.fields().get(0);
        assertThat(Expressions.name(field), is("field005"));
        assertThat(Alias.unwrap(field).fold(FoldContext.small()), nullValue());
    }

    // InferIsNotNull

    public void testIsNotNullOnIsNullField() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        Expression inn = isNotNull(fieldA);
        Filter f = new Filter(EMPTY, relation, inn);

        assertEquals(f, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithOneField() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        Expression inn = isNotNull(new Add(EMPTY, fieldA, ONE));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithTwoFields() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        var fieldB = getFieldAttribute("b");
        Expression inn = isNotNull(new Add(EMPTY, fieldA, fieldB));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnFunctionWithOneField() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        var pattern = L("abc");
        Expression inn = isNotNull(new And(EMPTY, new StartsWith(EMPTY, fieldA, pattern), greaterThanOf(new Add(EMPTY, ONE, TWO), THREE)));

        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnFunctionWithTwoFields() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        var fieldB = getFieldAttribute("b");
        Expression inn = isNotNull(new StartsWith(EMPTY, fieldA, fieldB));

        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnCoalesce() {
        var plan = localPlan("""
              from test
            | where coalesce(emp_no, salary) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var coalesce = as(inn.children().get(0), Coalesce.class);
        assertThat(Expressions.names(coalesce.children()), contains("emp_no", "salary"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnExpression() {
        var plan = localPlan("""
              from test
            | eval x = emp_no + 1
            | where x is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        assertThat(Expressions.names(inn.children()), contains("x"));
        var eval = as(filter.child(), Eval.class);
        filter = as(eval.child(), Filter.class);
        inn = as(filter.condition(), IsNotNull.class);
        assertThat(Expressions.names(inn.children()), contains("emp_no"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnCase() {
        var plan = localPlan("""
              from test
            | where case(emp_no > 10000, "1", salary < 50000, "2", first_name) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var caseF = as(inn.children().get(0), Case.class);
        assertThat(Expressions.names(caseF.children()), contains("emp_no > 10000", "\"1\"", "salary < 50000", "\"2\"", "first_name"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnCase_With_IS_NULL() {
        var plan = localPlan("""
              from test
            | where case(emp_no IS NULL, "1", salary IS NOT NULL, "2", first_name) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var caseF = as(inn.children().get(0), Case.class);
        assertThat(Expressions.names(caseF.children()), contains("emp_no IS NULL", "\"1\"", "salary IS NOT NULL", "\"2\"", "first_name"));
        var source = as(filter.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false]
     * \_Filter[RLIKE(first_name{f}#4, "VALÜ*", true)]
     *   \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     */
    public void testReplaceUpperStringCasinqgWithInsensitiveRLike() {
        var plan = localPlan("FROM test | WHERE TO_UPPER(TO_LOWER(TO_UPPER(first_name))) RLIKE \"VALÜ*\"");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var rlike = as(filter.condition(), RLike.class);
        var field = as(rlike.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertThat(rlike.pattern().pattern(), is("VALÜ*"));
        assertThat(rlike.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    /*
     *Limit[1000[INTEGER],false]
     * \_Filter[RLikeList(first_name{f}#4, "("VALÜ*", "TEST*")", true)]
     *  \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     */
    public void testReplaceUpperStringCasinqWithInsensitiveRLikeList() {
        var plan = localPlan("FROM test | WHERE TO_UPPER(TO_LOWER(TO_UPPER(first_name))) RLIKE (\"VALÜ*\", \"TEST*\")");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var rLikeList = as(filter.condition(), RLikeList.class);
        var field = as(rLikeList.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertEquals(2, rLikeList.pattern().patternList().size());
        assertThat(rLikeList.pattern().patternList().get(0).pattern(), is("VALÜ*"));
        assertThat(rLikeList.pattern().patternList().get(1).pattern(), is("TEST*"));
        assertThat(rLikeList.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as above, but lower case pattern
    public void testReplaceLowerStringCasingWithInsensitiveRLike() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE \"valü*\"");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var rlike = as(filter.condition(), RLike.class);
        var field = as(rlike.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertThat(rlike.pattern().pattern(), is("valü*"));
        assertThat(rlike.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as above, but lower case pattern and list of patterns
    public void testReplaceLowerStringCasingWithInsensitiveRLikeList() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE (\"valü*\", \"test*\")");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var rLikeList = as(filter.condition(), RLikeList.class);
        var field = as(rLikeList.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertEquals(2, rLikeList.pattern().patternList().size());
        assertThat(rLikeList.pattern().patternList().get(0).pattern(), is("valü*"));
        assertThat(rLikeList.pattern().patternList().get(1).pattern(), is("test*"));
        assertThat(rLikeList.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as above, but lower case pattern and list of patterns, one of which is upper case
    public void testReplaceLowerStringCasingWithMixedCaseRLikeList() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE (\"valü*\", \"TEST*\")");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var rLikeList = as(filter.condition(), RLikeList.class);
        var field = as(rLikeList.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertEquals(1, rLikeList.pattern().patternList().size());
        assertThat(rLikeList.pattern().patternList().get(0).pattern(), is("valü*"));
        assertThat(rLikeList.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    /**
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     */
    public void testReplaceStringCasingAndRLikeWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE \"VALÜ*\"");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    // same plan as in testReplaceUpperStringCasingWithInsensitiveRLike, but with LIKE instead of RLIKE
    public void testReplaceUpperStringCasingWithInsensitiveLike() {
        var plan = localPlan("FROM test | WHERE TO_UPPER(TO_LOWER(TO_UPPER(first_name))) LIKE \"VALÜ*\"");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var wlike = as(filter.condition(), WildcardLike.class);
        var field = as(wlike.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertThat(wlike.pattern().pattern(), is("VALÜ*"));
        assertThat(wlike.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as in testReplaceUpperStringCasingWithInsensitiveRLikeList, but with LIKE instead of RLIKE
    public void testReplaceUpperStringCasingWithInsensitiveLikeList() {
        var plan = localPlan("FROM test | WHERE TO_UPPER(TO_LOWER(TO_UPPER(first_name))) LIKE (\"VALÜ*\", \"TEST*\")");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var likeList = as(filter.condition(), WildcardLikeList.class);
        var field = as(likeList.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertEquals(2, likeList.pattern().patternList().size());
        assertThat(likeList.pattern().patternList().get(0).pattern(), is("VALÜ*"));
        assertThat(likeList.pattern().patternList().get(1).pattern(), is("TEST*"));
        assertThat(likeList.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as above, but mixed case pattern and list of patterns
    public void testReplaceLowerStringCasingWithMixedCaseLikeList() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE (\"TEST*\", \"valü*\", \"vaLü*\")");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var likeList = as(filter.condition(), WildcardLikeList.class);
        var field = as(likeList.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        // only the all lowercase pattern is kept, the mixed case and all uppercase patterns are ignored
        assertEquals(1, likeList.pattern().patternList().size());
        assertThat(likeList.pattern().patternList().get(0).pattern(), is("valü*"));
        assertThat(likeList.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    // same plan as above, but lower case pattern
    public void testReplaceLowerStringCasingWithInsensitiveLike() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE \"valü*\"");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var wlike = as(filter.condition(), WildcardLike.class);
        var field = as(wlike.field(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        assertThat(wlike.pattern().pattern(), is("valü*"));
        assertThat(wlike.caseInsensitive(), is(true));
        var source = as(filter.child(), EsRelation.class);
    }

    /**
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     */
    public void testReplaceStringCasingAndLikeWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE \"VALÜ*\"");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     */
    public void testReplaceStringCasingAndLikeListWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE (\"VALÜ*\", \"TEST*\")");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     */
    public void testReplaceStringCasingAndRLikeListWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE (\"VALÜ*\", \"TEST*\")");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[],[SUM($$integer_long_field$converted_to$long{f$}#5,true[BOOLEAN]) AS sum(integer_long_field::long)#3]]
     *   \_Filter[ISNOTNULL($$integer_long_field$converted_to$long{f$}#5)]
     *     \_EsRelation[test*][!integer_long_field, $$integer_long_field$converted..]
     */
    public void testUnionTypesInferNonNullAggConstraint() {
        LogicalPlan coordinatorOptimized = plan("FROM test* | STATS sum(integer_long_field::long)", analyzerWithUnionTypeMapping());
        var plan = localPlan(coordinatorOptimized, TEST_SEARCH_STATS);

        var limit = asLimit(plan, 1000);
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        var relation = as(filter.child(), EsRelation.class);

        var isNotNull = as(filter.condition(), IsNotNull.class);
        var unionTypeField = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("$$integer_long_field$converted_to$long", unionTypeField.name());
        assertEquals("integer_long_field", unionTypeField.fieldName().string());
    }

    /**
     * \_Aggregate[[first_name{r}#7, $$first_name$temp_name$17{r}#18],[SUM(salary{f}#11,true[BOOLEAN]) AS SUM(salary)#5, first_nam
     * e{r}#7, first_name{r}#7 AS last_name#10]]
     *   \_Eval[[null[KEYWORD] AS first_name#7, null[KEYWORD] AS $$first_name$temp_name$17#18]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testGroupingByMissingFields() {
        var plan = plan("FROM test | STATS SUM(salary) BY first_name, last_name");
        var testStats = statsForMissingField("first_name", "last_name");
        var localPlan = localPlan(plan, testStats);
        Limit limit = as(localPlan, Limit.class);
        Aggregate aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(2));
        ReferenceAttribute grouping1 = as(aggregate.groupings().get(0), ReferenceAttribute.class);
        ReferenceAttribute grouping2 = as(aggregate.groupings().get(1), ReferenceAttribute.class);
        Eval eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        Alias eval1 = eval.fields().get(0);
        Literal literal1 = as(eval1.child(), Literal.class);
        assertNull(literal1.value());
        assertThat(literal1.dataType(), is(KEYWORD));
        Alias eval2 = eval.fields().get(1);
        Literal literal2 = as(eval2.child(), Literal.class);
        assertNull(literal2.value());
        assertThat(literal2.dataType(), is(KEYWORD));
        assertThat(grouping1.id(), equalTo(eval1.id()));
        assertThat(grouping2.id(), equalTo(eval2.id()));
        as(eval.child(), EsRelation.class);
    }

    public void testVerifierOnMissingReferences() throws Exception {
        var plan = localPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        // emulate a rule that adds an invalid field
        var invalidPlan = new OrderBy(
            limit.source(),
            limit,
            asList(new Order(limit.source(), salary, Order.OrderDirection.ASC, Order.NullsPosition.FIRST))
        );

        var localContext = new LocalLogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TEST_SEARCH_STATS);
        LocalLogicalPlanOptimizer localLogicalPlanOptimizer = new LocalLogicalPlanOptimizer(localContext);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> localLogicalPlanOptimizer.localOptimize(invalidPlan));
        assertThat(e.getMessage(), containsString("Plan [OrderBy[[Order[salary"));
        assertThat(e.getMessage(), containsString(" optimized incorrectly due to missing references [salary"));
    }

    private LocalLogicalPlanOptimizer getCustomRulesLocalLogicalPlanOptimizer(List<RuleExecutor.Batch<LogicalPlan>> batches) {
        LocalLogicalOptimizerContext context = new LocalLogicalOptimizerContext(
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            TEST_SEARCH_STATS
        );
        LocalLogicalPlanOptimizer customOptimizer = new LocalLogicalPlanOptimizer(context) {
            @Override
            protected List<Batch<LogicalPlan>> batches() {
                return batches;
            }
        };
        return customOptimizer;
    }

    public void testVerifierOnAdditionalAttributeAdded() throws Exception {
        var plan = localPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        Holder<Integer> appliedCount = new Holder<>(0);
        // use a custom rule that adds another output attribute
        var customRuleBatch = new RuleExecutor.Batch<>(
            "CustomRuleBatch",
            RuleExecutor.Limiter.ONCE,
            new OptimizerRules.ParameterizedOptimizerRule<Aggregate, LocalLogicalOptimizerContext>(UP) {

                @Override
                protected LogicalPlan rule(Aggregate plan, LocalLogicalOptimizerContext context) {
                    // This rule adds a missing attribute to the plan output
                    // We only want to apply it once, so we use a static counter
                    if (appliedCount.get() == 0) {
                        appliedCount.set(appliedCount.get() + 1);
                        Literal additionalLiteral = new Literal(EMPTY, "additional literal", INTEGER);
                        return new Eval(plan.source(), plan, List.of(new Alias(EMPTY, "additionalAttribute", additionalLiteral)));
                    }
                    return plan;
                }

            }
        );
        LocalLogicalPlanOptimizer customRulesLocalLogicalPlanOptimizer = getCustomRulesLocalLogicalPlanOptimizer(List.of(customRuleBatch));
        Exception e = expectThrows(VerificationException.class, () -> customRulesLocalLogicalPlanOptimizer.localOptimize(plan));
        assertThat(e.getMessage(), containsString("Output has changed from"));
        assertThat(e.getMessage(), containsString("additionalAttribute"));
    }

    public void testVerifierOnAttributeDatatypeChanged() {
        var plan = localPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        Holder<Integer> appliedCount = new Holder<>(0);
        // use a custom rule that changes the datatype of an output attribute
        var customRuleBatch = new RuleExecutor.Batch<>(
            "CustomRuleBatch",
            RuleExecutor.Limiter.ONCE,
            new OptimizerRules.ParameterizedOptimizerRule<LogicalPlan, LocalLogicalOptimizerContext>(DOWN) {
                @Override
                protected LogicalPlan rule(LogicalPlan plan, LocalLogicalOptimizerContext context) {
                    // We only want to apply it once, so we use a static counter
                    if (appliedCount.get() == 0) {
                        appliedCount.set(appliedCount.get() + 1);
                        Limit limit = as(plan, Limit.class);
                        Limit newLimit = new Limit(plan.source(), limit.limit(), limit.child()) {
                            @Override
                            public List<Attribute> output() {
                                List<Attribute> oldOutput = super.output();
                                List<Attribute> newOutput = new ArrayList<>(oldOutput);
                                newOutput.set(0, oldOutput.get(0).withDataType(DataType.DATETIME));
                                return newOutput;
                            }
                        };
                        return newLimit;
                    }
                    return plan;
                }

            }
        );
        LocalLogicalPlanOptimizer customRulesLocalLogicalPlanOptimizer = getCustomRulesLocalLogicalPlanOptimizer(List.of(customRuleBatch));
        Exception e = expectThrows(VerificationException.class, () -> customRulesLocalLogicalPlanOptimizer.localOptimize(plan));
        assertThat(e.getMessage(), containsString("Output has changed from"));
    }

    /**
     * Input:
     * Project[[key{f}#2, int{f}#3, field1{f}#7, field2{f}#8]]
     * \_Join[LEFT,[key{f}#2],[key{f}#6],null]
     *   |_EsRelation[JLfQlKmn][key{f}#2, int{f}#3, field1{f}#4, field2{f}#5]
     *   \_EsRelation[HQtEBOWq][LOOKUP][key{f}#6, field1{f}#7, field2{f}#8]
     *
     * Output:
     * Project[[key{r}#2, int{f}#3, field1{r}#7, field1{r}#7 AS field2#8]]
     * \_Eval[[null[KEYWORD] AS key#2, null[INTEGER] AS field1#7]]
     *   \_EsRelation[JLfQlKmn][key{f}#2, int{f}#3, field1{f}#4, field2{f}#5]
     */
    public void testPruneLeftJoinOnNullMatchingFieldAndShadowingAttributes() {
        var keyLeft = getFieldAttribute("key", KEYWORD);
        var intFieldLeft = getFieldAttribute("int");
        var fieldLeft1 = getFieldAttribute("field1");
        var fieldLeft2 = getFieldAttribute("field2");
        var leftRelation = EsqlTestUtils.relation(IndexMode.STANDARD)
            .withAttributes(List.of(keyLeft, intFieldLeft, fieldLeft1, fieldLeft2));

        var keyRight = getFieldAttribute("key", KEYWORD);
        var fieldRight1 = getFieldAttribute("field1");
        var fieldRight2 = getFieldAttribute("field2");
        var rightRelation = EsqlTestUtils.relation(IndexMode.LOOKUP).withAttributes(List.of(keyRight, fieldRight1, fieldRight2));

        JoinConfig joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(keyLeft), List.of(keyRight), null);
        var join = new Join(EMPTY, leftRelation, rightRelation, joinConfig);
        var project = new Project(EMPTY, join, List.of(keyLeft, intFieldLeft, fieldRight1, fieldRight2));

        var testStats = statsForMissingField("key");
        var localPlan = localPlan(project, testStats);

        var projectOut = as(localPlan, Project.class);
        var projectionsOut = projectOut.projections();
        assertThat(Expressions.names(projectionsOut), contains("key", "int", "field1", "field2"));
        assertThat(projectionsOut.get(0).id(), is(keyLeft.id()));
        assertThat(projectionsOut.get(1).id(), is(intFieldLeft.id()));
        assertThat(projectionsOut.get(2).id(), is(fieldRight1.id())); // id must remain from the RHS.
        var aliasField2 = as(projectionsOut.get(3), Alias.class); // the projection must contain an alias ...
        assertThat(aliasField2.id(), is(fieldRight2.id())); // ... with the same id as the original field.

        var eval = as(projectOut.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("key", "field1"));
        var keyEval = as(Alias.unwrap(eval.fields().get(0)), Literal.class);
        assertThat(keyEval.value(), is(nullValue()));
        assertThat(keyEval.dataType(), is(KEYWORD));
        var field1Eval = as(Alias.unwrap(eval.fields().get(1)), Literal.class);
        assertThat(field1Eval.value(), is(nullValue()));
        assertThat(field1Eval.dataType(), is(INTEGER));
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * Expected:
     * EsqlProject[[!alias_integer, boolean{f}#7, byte{f}#8, constant_keyword-foo{f}#9, date{f}#10, date_nanos{f}#11, dense_vector
     * {f}#26, double{f}#12, float{f}#13, half_float{f}#14, integer{f}#16, ip{f}#17, keyword{f}#18, long{f}#19, scaled_float{f}#15,
     * semantic_text{f}#25, short{f}#21, text{f}#22, unsigned_long{f}#20, version{f}#23, wildcard{f}#24, s{r}#5]]
     * \_Eval[[$$dense_vector$V_DOT_PRODUCT$27{f}#27 AS s#5]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test_all][$$dense_vector$V_DOT_PRODUCT$27{f}#27, !alias_integer,
     */
    public void testVectorFunctionsReplaced() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // EsqlProject[[!alias_integer, boolean{f}#7, byte{f}#8, ... s{r}#5]]
        var project = as(plan, EsqlProject.class);
        // Does not contain the extracted field
        assertFalse(Expressions.names(project.projections()).stream().anyMatch(s -> s.startsWith(testCase.toFieldAttrName())));

        // Eval[[$$dense_vector$V_DOT_PRODUCT$27{f}#27 AS s#5]]
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("s"));

        // Check replaced field attribute
        FieldAttribute fieldAttr = (FieldAttribute) alias.child();
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(blockLoaderFunctionConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(blockLoaderFunctionConfig.vector(), equalTo(testCase.vector()));

        // Limit[1000[INTEGER],false,false]
        var limit = as(eval.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        // EsRelation[types_all]
        var esRelation = as(limit.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(fieldAttr));
    }

    /**
     * Expected:
     * EsqlProject[[s{r}#4]]
     * \_TopN[[Order[s{r}#4,DESC,FIRST]],1[INTEGER]]
     *   \_Eval[[$$dense_vector$replaced$28{t}#28 AS s#4]]
     *     \_EsRelation[types][$$dense_vector$replaced$28{t}#28, !alias_integer, b..]
     */
    public void testVectorFunctionsReplacedWithTopN() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // EsqlProject[[s{r}#4]]
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("s"));

        // TopN[[Order[s{r}#4,DESC,FIRST]],1[INTEGER]]
        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1));
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().getFirst(), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), equalTo("s"));

        // Eval[[$$dense_vector$replaced$28{t}#28 AS s#4]]
        var eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("s"));

        // Check replaced field attribute
        FieldAttribute fieldAttr = (FieldAttribute) alias.child();
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(blockLoaderFunctionConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(blockLoaderFunctionConfig.vector(), equalTo(testCase.vector()));

        // EsRelation[types]
        var esRelation = as(eval.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(fieldAttr));
    }

    public void testVectorFunctionsNotPushedDownWhenNotIndexed() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return field.string().equals("dense_vector") == false;
            }
        });

        // EsqlProject[[s{r}#4]]
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("s"));

        // TopN[[Order[s{r}#4,DESC,FIRST]],1[INTEGER]]
        var topN = as(project.child(), TopN.class);

        // Eval[[$$dense_vector$replaced$28{t}#28 AS s#4]]
        var eval = as(topN.child(), Eval.class);
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("s"));

        // Check similarity function field attribute is NOT replaced
        as(alias.child(), VectorSimilarityFunction.class);

        // EsRelation does not contain a FunctionEsField
        var esRelation = as(eval.child(), EsRelation.class);
        assertFalse(
            esRelation.output()
                .stream()
                .anyMatch(att -> (att instanceof FieldAttribute fieldAttr) && fieldAttr.field() instanceof FunctionEsField)
        );
    }

    public void testVectorFunctionsWhenFieldMissing() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return field.string().equals("dense_vector") == false;
            }
        });

        // Project[[s{r}#5]]
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("s"));

        // TopN[[Order[s{r}#5,DESC,FIRST]],1[INTEGER],false]
        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1));

        // Evaluates expression as null, as the field is missing
        var eval = as(topN.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("s"));
        var alias = as(eval.fields().getFirst(), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(nullValue()));
        assertThat(literal.dataType(), is(DataType.DOUBLE));

        // EsRelation[test_all] - does not contain a FunctionEsField
        var esRelation = as(eval.child(), EsRelation.class);
        assertFalse(
            esRelation.output()
                .stream()
                .anyMatch(att -> (att instanceof FieldAttribute fieldAttr) && fieldAttr.field() instanceof FunctionEsField)
        );
    }

    public void testVectorFunctionsInWhere() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | where %s > 0.5
            | keep dense_vector
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // EsqlProject[[dense_vector{f}#25]]
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("dense_vector"));

        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var greaterThan = as(filter.condition(), GreaterThan.class);

        // Check left side is the replaced field attribute
        var fieldAttr = as(greaterThan.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(blockLoaderFunctionConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(blockLoaderFunctionConfig.vector(), equalTo(testCase.vector()));

        // Check right side is 0.5
        var literal = as(greaterThan.right(), Literal.class);
        assertThat(literal.value(), equalTo(0.5));
        assertThat(literal.dataType(), is(DataType.DOUBLE));

        // EsRelation[test_all][$$dense_vector$V_DOT_PRODUCT$26{f}#26, !alias_integer, ..]
        var esRelation = as(filter.child(), EsRelation.class);
        assertThat(esRelation.indexPattern(), is("test_all"));
        assertTrue(esRelation.output().contains(fieldAttr));
    }

    public void testVectorFunctionsInStats() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | stats count(*) where %s > 0.5
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Limit[1000[INTEGER],false,false]
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        // Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count(*) where v_dot_product(dense_vector, [1.0, 2.0, 3.0]) > 0.5#3]]
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(0));
        assertThat(aggregate.aggregates(), hasSize(1));

        // Check the Count aggregate with filter
        var countAlias = as(aggregate.aggregates().getFirst(), Alias.class);
        var count = as(countAlias.child(), Count.class);

        // Check the filter on the Count aggregate
        assertThat(count.filter(), equalTo(Literal.TRUE));

        // Filter[$$dense_vector$V_DOT_PRODUCT$26{f}#26 > 0.5[DOUBLE]]
        var filter = as(aggregate.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);

        // Check left side is the replaced field attribute
        var fieldAttr = as(filterCondition.left(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(blockLoaderFunctionConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(blockLoaderFunctionConfig.vector(), equalTo(testCase.vector()));

        // Verify the filter condition matches the aggregate filter
        var filterFieldAttr = as(filterCondition.left(), FieldAttribute.class);
        assertThat(filterFieldAttr, is(fieldAttr));

        // EsRelation[test_all][$$dense_vector$V_DOT_PRODUCT$26{f}#26, !alias_integer, ..]
        var esRelation = as(filter.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(filterFieldAttr));
    }

    public void testVectorFunctionsUpdateIntermediateProjections() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | keep *
            | mv_expand keyword
            | eval similarity = %s
            | sort similarity desc, keyword asc
            | limit 1
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // EsqlProject with all fields including similarity and keyword
        var project = as(plan, EsqlProject.class);
        assertTrue(Expressions.names(project.projections()).contains("similarity"));
        assertTrue(Expressions.names(project.projections()).contains("keyword"));

        var topN = as(project.child(), TopN.class);

        var eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), equalTo("similarity"));

        // Check replaced field attribute
        var fieldAttr = as(alias.child(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(fieldAttr.name(), startsWith(testCase.toFieldAttrName()));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(blockLoaderFunctionConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(blockLoaderFunctionConfig.vector(), equalTo(testCase.vector()));

        // MvExpand[keyword{f}#23,keyword{r}#32]
        var mvExpand = as(eval.child(), MvExpand.class);
        assertThat(Expressions.name(mvExpand.target()), equalTo("keyword"));

        // Inner EsqlProject with the pushed down function
        var innerProject = as(mvExpand.child(), EsqlProject.class);
        assertTrue(Expressions.names(innerProject.projections()).contains("keyword"));
        assertTrue(
            innerProject.projections()
                .stream()
                .anyMatch(p -> (p instanceof FieldAttribute fa) && fa.name().startsWith(testCase.toFieldAttrName()))
        );

        // EsRelation[test_all][$$dense_vector$V_COSINE$33{f}#33, !alias_in..]
        var esRelation = as(innerProject.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(fieldAttr));
    }

    public void testVectorFunctionsWithDuplicateFunctions() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        // Generate two random test cases - one for duplicate usage, one for the second set
        SimilarityFunctionTestCase testCase1 = SimilarityFunctionTestCase.random("dense_vector");
        SimilarityFunctionTestCase testCase2 = randomValueOtherThan(testCase1, () -> SimilarityFunctionTestCase.random("dense_vector"));
        SimilarityFunctionTestCase testCase3 = randomValueOtherThanMany(
            tc -> (tc.equals(testCase1) || tc.equals(testCase2)),
            () -> SimilarityFunctionTestCase.random("dense_vector")
        );

        String query = String.format(
            Locale.ROOT,
            """
                from test_all
                | eval s1 = %s, s2 = %s * 2 / 3
                | where %s + 5 + %s > 0
                | eval r2 = %s + %s
                | keep s1, s2, r2
                """,
            testCase1.toQuery(),
            testCase1.toQuery(),
            testCase1.toQuery(),
            testCase2.toQuery(),
            testCase2.toQuery(),
            testCase3.toQuery()
        );

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // EsqlProject[[s1{r}#5, s2{r}#8, r2{r}#14]]
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("s1", "s2", "r2"));

        // Eval with s1, s2, r2
        var evalS1 = as(project.child(), Eval.class);
        assertThat(evalS1.fields(), hasSize(3));

        // Check s1 = $$dense_vector$V_DOT_PRODUCT$...
        var s1Alias = as(evalS1.fields().getFirst(), Alias.class);
        assertThat(s1Alias.name(), equalTo("s1"));
        var s1FieldAttr = as(s1Alias.child(), FieldAttribute.class);
        assertThat(s1FieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(s1FieldAttr.name(), startsWith(testCase1.toFieldAttrName()));
        var s1Field = as(s1FieldAttr.field(), FunctionEsField.class);
        var s1Config = as(s1Field.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(s1Config.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(s1Config.vector(), equalTo(testCase1.vector()));

        // Check s2 = $$dense_vector$V_DOT_PRODUCT$1606418432 * 2 / 3 (same field as s1)
        var s2Alias = as(evalS1.fields().get(1), Alias.class);
        assertThat(s2Alias.name(), equalTo("s2"));
        var s2Div = as(s2Alias.child(), Div.class);
        var s2Mul = as(s2Div.left(), Mul.class);
        var s2FieldAttr = as(s2Mul.left(), FieldAttribute.class);
        assertThat(s1FieldAttr, is(s2FieldAttr));

        // Check r2 = $$dense_vector$V_L1NORM$... + $$dense_vector$V_HAMMING$... (two different fields)
        var r2Alias = as(evalS1.fields().get(2), Alias.class);
        assertThat(r2Alias.name(), equalTo("r2"));
        var r2Add = as(r2Alias.child(), Add.class);

        // Left side should be testCase2 (L1NORM)
        var r2LeftFieldAttr = as(r2Add.left(), FieldAttribute.class);
        assertThat(r2LeftFieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(r2LeftFieldAttr.name(), startsWith(testCase2.toFieldAttrName()));
        var r2LeftField = as(r2LeftFieldAttr.field(), FunctionEsField.class);
        var r2LeftConfig = as(r2LeftField.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(r2LeftConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(r2LeftConfig.vector(), equalTo(testCase2.vector()));

        // Right side should be testCase3 (different HAMMING)
        var r2RightFieldAttr = as(r2Add.right(), FieldAttribute.class);
        assertThat(r2RightFieldAttr.fieldName().string(), equalTo("dense_vector"));
        assertThat(r2RightFieldAttr.name(), startsWith(testCase3.toFieldAttrName()));
        var r2RightField = as(r2RightFieldAttr.field(), FunctionEsField.class);
        var r2RightConfig = as(r2RightField.functionConfig(), DenseVectorFieldMapper.VectorSimilarityFunctionConfig.class);
        assertThat(r2RightConfig.similarityFunction(), instanceOf(DenseVectorFieldMapper.SimilarityFunction.class));
        assertThat(r2RightConfig.vector(), equalTo(testCase3.vector()));

        // Verify the two fields in r2 are different
        assertThat(r2LeftFieldAttr, not(is(r2RightFieldAttr)));

        // Limit[1000[INTEGER],false,false]
        var limit = as(evalS1.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(1000));

        // Filter[testCase1 + 5 + testCase2 > 0] - Filter still has original function calls
        var filter = as(limit.child(), Filter.class);
        var greaterThan = as(filter.condition(), GreaterThan.class);
        assertThat(greaterThan.right(), instanceOf(Literal.class));
        assertThat(((Literal) greaterThan.right()).value(), equalTo(0));

        var filterAdd1 = as(greaterThan.left(), Add.class);
        var filterAdd2 = as(filterAdd1.left(), Add.class);

        // Check the literal 5 in the filter
        assertThat(filterAdd2.right(), instanceOf(Literal.class));
        assertThat(((Literal) filterAdd2.right()).value(), equalTo(5));

        // EsRelation[test_all] - should contain the pushed-down field attributes
        var esRelation = as(filter.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(s1FieldAttr));
        assertTrue(esRelation.output().contains(r2LeftFieldAttr));
        assertTrue(esRelation.output().contains(r2RightFieldAttr));
    }

    private record SimilarityFunctionTestCase(String esqlFunction, String fieldName, float[] vector, String functionName) {

        public String toQuery() {
            String params = randomBoolean() ? fieldName + ", " + Arrays.toString(vector) : Arrays.toString(vector) + ", " + fieldName;
            return esqlFunction + "(" + params + ")";
        }

        public String toFieldAttrName() {
            return "$$" + fieldName + "$" + functionName;
        }

        public static SimilarityFunctionTestCase random(String fieldName) {
            float[] vector = new float[] { randomFloat(), randomFloat(), randomFloat() };
            // Only use DotProduct and CosineSimilarity as they have full pushdown support
            // L1Norm, L2Norm, and Hamming are still in development
            return switch (randomInt(4)) {
                case 0 -> new SimilarityFunctionTestCase("v_dot_product", fieldName, vector, "V_DOT_PRODUCT");
                case 1 -> new SimilarityFunctionTestCase("v_cosine", fieldName, vector, "V_COSINE");
                case 2 -> new SimilarityFunctionTestCase("v_l1_norm", fieldName, vector, "V_L1NORM");
                case 3 -> new SimilarityFunctionTestCase("v_l2_norm", fieldName, vector, "V_L2NORM");
                case 4 -> new SimilarityFunctionTestCase("v_hamming", fieldName, vector, "V_HAMMING");
                default -> throw new IllegalStateException("Unexpected value");
            };
        }
    }

    public void testLengthInEval() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | EVAL l = LENGTH(last_name)
            | KEEP l
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("l"));
        var eval = as(project.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var limit = as(eval.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInWhere() {
        assumeTrue("requires similarity functions", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | WHERE LENGTH(last_name) > 1
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, EsqlProject.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        Attribute lAttr = assertLengthPushdown(as(filter.condition(), GreaterThan.class).left(), "last_name");
        var relation = as(filter.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInStats() {
        assumeTrue("requires 137382", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | STATS l = SUM(LENGTH(last_name))
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(1));
        as(as(agg.aggregates().getFirst(), Alias.class).child(), Sum.class);
        var eval = as(agg.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var relation = as(eval.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInEvalAfterManyRenames() {
        assumeTrue("requires push", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | EVAL l1 = last_name
            | EVAL l2 = l1
            | EVAL l3 = l2
            | EVAL l = LENGTH(l3)
            | KEEP l
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("l"));
        var eval = as(project.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var limit = as(eval.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInWhereAndEval() {
        assumeTrue("requires push", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | WHERE LENGTH(last_name) > 1
            | EVAL l = LENGTH(last_name)
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, EsqlProject.class);
        var eval = as(project.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var limit = as(eval.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        assertThat(as(filter.condition(), GreaterThan.class).left(), is(lAttr));
        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.output(), hasItem(lAttr));
    }

    /**
     * Pushed LENGTH to the same field in a <strong>ton</strong> of unique and curious ways. All
     * of these pushdowns should be fused to one.
     *
     * <pre>{@code
     * Project[[l{r}#23]]
     * \_Eval[[$$SUM$SUM(LENGTH(last>$0{r$}#37 / $$COUNT$$$AVG$SUM(LENGTH(last>$1$1{r$}#41 AS $$AVG$SUM(LENGTH(last>$1#38, $
     * $SUM$SUM(LENGTH(last>$0{r$}#37 + $$AVG$SUM(LENGTH(last>$1{r$}#38 + $$SUM$SUM(LENGTH(last>$2{r$}#39 AS l#23]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[SUM($$LENGTH(last_nam>$SUM$0{r$}#35,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$SUM(LE
     *          NGTH(last>$0#37,
     *          COUNT(a3{r}#11,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$$$AVG$SUM(LENGTH(last>$1$1#41,
     *          SUM($$LENGTH(first_na>$SUM$1{r$}#36,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$SUM(LENGTH(last>$2#39]]
     *       \_Eval[[$$last_name$LENGTH$920787299{f$}#42 AS a3#11, $$last_name$LENGTH$920787299{f$}#42 AS $$LENGTH(last_nam>$SUM$0
     * #35, $$first_name$LENGTH$920787299{f$}#43 AS $$LENGTH(first_na>$SUM$1#36]]
     *         \_Filter[$$last_name$LENGTH$920787299{f$}#42 > 1[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     * }</pre>
     */
    public void testLengthPushdownZoo() {
        assumeTrue("requires push", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | EVAL a1 = LENGTH(last_name), a2 = LENGTH(last_name), a3 = LENGTH(last_name),
                   a4 = abs(LENGTH(last_name)) + a1 + LENGTH(first_name) * 3
            | WHERE a1 > 1 and LENGTH(last_name) > 1
            | STATS l = SUM(LENGTH(last_name)) + AVG(a3) + SUM(LENGTH(first_name))
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("l"));

        // Eval - computes final aggregation result (SUM + AVG + SUM)
        var eval1 = as(project.child(), Eval.class);
        assertThat(eval1.fields(), hasSize(2));
        // The avg is computed as the SUM(LENGTH(last_name)) / COUNT(LENGTH(last_name))
        var avg = eval1.fields().get(0);
        var avgDiv = as(avg.child(), Div.class);
        // SUM(LENGTH(last_name))
        var evalSumLastName = as(avgDiv.left(), ReferenceAttribute.class);
        var evalCountLastName = as(avgDiv.right(), ReferenceAttribute.class);
        var finalAgg = as(eval1.fields().get(1).child(), Add.class);
        var leftFinalAgg = as(finalAgg.left(), Add.class);
        assertThat(leftFinalAgg.left(), equalTo(evalSumLastName));
        assertThat(as(leftFinalAgg.right(), ReferenceAttribute.class).id(), equalTo(avg.id()));
        // SUM(LENGTH(first_name))
        var evalSumFirstName = as(finalAgg.right(), ReferenceAttribute.class);

        // Limit[1000[INTEGER],false,false]
        var limit = as(eval1.child(), Limit.class);

        // Aggregate with 3 aggregates: SUM for last_name, COUNT for last_name
        // (the AVG uses the sum and the count), SUM for first_name
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(3));

        // Eval - pushdown fields: a3, LENGTH(last_name) for SUM, and LENGTH(first_name) for SUM
        var evalPushdown = as(agg.child(), Eval.class);
        assertThat(evalPushdown.fields(), hasSize(3));
        Alias a3Alias = as(evalPushdown.fields().getFirst(), Alias.class);
        assertThat(a3Alias.name(), equalTo("a3"));
        Attribute lastNamePushDownAttr = assertLengthPushdown(a3Alias.child(), "last_name");
        Alias lastNamePushdownAlias = as(evalPushdown.fields().get(1), Alias.class);
        assertLengthPushdown(lastNamePushdownAlias.child(), "last_name");
        Alias firstNamePushdownAlias = as(evalPushdown.fields().get(2), Alias.class);
        Attribute firstNamePushDownAttr = assertLengthPushdown(firstNamePushdownAlias.child(), "first_name");

        // Verify aggregates reference the pushed down fields
        var sumForLastNameAlias = as(agg.aggregates().get(0), Alias.class);
        var sumForLastName = as(sumForLastNameAlias.child(), Sum.class);
        assertThat(as(sumForLastName.field(), ReferenceAttribute.class).id(), equalTo(lastNamePushdownAlias.id()));
        // Checks that the SUM(LENGTH(last_name)) in the final EVAL is the aggregate result here
        assertThat(evalSumLastName.id(), equalTo(sumForLastNameAlias.id()));

        var countForAvgAlias = as(agg.aggregates().get(1), Alias.class);
        var countForAvg = as(countForAvgAlias.child(), Count.class);
        assertThat(as(countForAvg.field(), ReferenceAttribute.class).id(), equalTo(a3Alias.id()));
        // Checks that the COUNT(LENGTH(last_name)) in the final EVAL is the aggregate result here
        assertThat(evalCountLastName.id(), equalTo(countForAvgAlias.id()));

        var sumForFirstNameAlias = as(agg.aggregates().get(2), Alias.class);
        var sumForFirstName = as(sumForFirstNameAlias.child(), Sum.class);
        assertThat(as(sumForFirstName.field(), ReferenceAttribute.class).id(), equalTo(firstNamePushdownAlias.id()));
        // Checks that the SUM(LENGTH(first_name)) in the final EVAL is the aggregate result here
        assertThat(evalSumFirstName.id(), equalTo(sumForFirstNameAlias.id()));

        // Filter[LENGTH(last_name) > 1]
        var filter = as(evalPushdown.child(), Filter.class);
        assertLengthPushdown(as(filter.condition(), GreaterThan.class).left(), "last_name");

        // EsRelation[test] - should contain the pushed-down field attribute
        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.output(), hasItem(lastNamePushDownAttr));
        assertThat(relation.output(), hasItem(firstNamePushDownAttr));
    }

    public void testLengthInStatsTwice() {
        assumeTrue("requires push", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | STATS l = SUM(LENGTH(last_name)) + AVG(LENGTH(last_name))
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
        var eval1 = as(project.child(), Eval.class);
        var limit = as(eval1.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
        var sum = as(as(agg.aggregates().getFirst(), Alias.class).child(), Sum.class);
        var count = as(as(agg.aggregates().get(1), Alias.class).child(), Count.class);
        var eval2 = as(agg.child(), Eval.class);
        assertThat(eval2.fields(), hasSize(1));
        Alias lAlias = as(eval2.fields().getFirst(), Alias.class);
        Attribute lAttr = assertLengthPushdown(lAlias.child(), "last_name");
        var relation = as(eval2.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));

        assertThat(as(sum.field(), ReferenceAttribute.class).id(), equalTo(lAlias.id()));
        assertThat(as(count.field(), ReferenceAttribute.class).id(), equalTo(lAlias.id()));
    }

    public void testLengthTwoFields() {
        assumeTrue("requires push", EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled());
        String query = """
            FROM test
            | STATS last_name = SUM(LENGTH(last_name)), first_name = SUM(LENGTH(first_name))
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
        var sum1 = as(as(agg.aggregates().getFirst(), Alias.class).child(), Sum.class);
        var sum2 = as(as(agg.aggregates().get(1), Alias.class).child(), Sum.class);
        var eval = as(agg.child(), Eval.class);

        assertThat(eval.fields(), hasSize(2));
        Alias lastNameAlias = as(eval.fields().getFirst(), Alias.class);
        Attribute lastNameAttr = assertLengthPushdown(lastNameAlias.child(), "last_name");
        Alias firstNameAlias = as(eval.fields().get(1), Alias.class);
        Attribute firstNameAttr = assertLengthPushdown(firstNameAlias.child(), "first_name");

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.output(), hasItems(lastNameAttr, firstNameAttr));

        assertThat(as(sum1.field(), ReferenceAttribute.class).id(), equalTo(lastNameAlias.id()));
        assertThat(as(sum2.field(), ReferenceAttribute.class).id(), equalTo(firstNameAlias.id()));
    }

    private Attribute assertLengthPushdown(Expression e, String fieldName) {
        FieldAttribute attr = as(e, FieldAttribute.class);
        assertThat(attr.name(), startsWith("$$" + fieldName + "$LENGTH$"));
        var field = as(attr.field(), FunctionEsField.class);
        assertThat(field.functionConfig().function(), is(BlockLoaderFunctionConfig.Function.LENGTH));
        assertThat(field.getName(), equalTo(fieldName));
        assertThat(field.getExactInfo().hasExact(), equalTo(false));
        return attr;
    }

    public void testFullTextFunctionOnMissingField() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = plan(String.format(Locale.ROOT, """
            from test
            | where %s(first_name, "John") or %s(last_name, "Doe")
            """, functionName, functionName));

        var testStats = statsForMissingField("first_name");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);

        // Introduces an Eval with first_name as null literal
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("first_name"));
        var firstNameEval = as(Alias.unwrap(eval.fields().get(0)), Literal.class);
        assertThat(firstNameEval.value(), is(nullValue()));
        assertThat(firstNameEval.dataType(), is(KEYWORD));

        var limit = as(eval.child(), Limit.class);

        // Filter has a single match on last_name only
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        assertThat(Expressions.name(fullTextFunction.field()), equalTo("last_name"));
    }

    public void testKnnOnMissingField() {
        String query = """
            from test_all
            | where knn(dense_vector, [0, 1, 2]) or match(text, "Doe")
            """;

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        var testStats = statsForMissingField("dense_vector");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);

        // Introduces an Eval with first_name as null literal
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("dense_vector"));
        var firstNameEval = as(Alias.unwrap(eval.fields().get(0)), Literal.class);
        assertThat(firstNameEval.value(), is(nullValue()));
        assertThat(firstNameEval.dataType(), is(DENSE_VECTOR));

        var limit = as(eval.child(), Limit.class);

        // Filter has a single match on last_name only
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        assertThat(Expressions.name(fullTextFunction.field()), equalTo("text"));
    }

    private IsNotNull isNotNull(Expression field) {
        return new IsNotNull(EMPTY, field);
    }

    private LocalRelation asEmptyRelation(Object o) {
        var empty = as(o, LocalRelation.class);
        assertThat(empty.supplier(), is(EmptyLocalSupplier.EMPTY));
        return empty;
    }

    private LogicalPlan plan(String query, Analyzer analyzer) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        return logicalOptimizer.optimize(analyzed);
    }

    protected LogicalPlan plan(String query) {
        return plan(query, analyzer);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, Configuration configuration, SearchStats searchStats) {
        var localContext = new LocalLogicalOptimizerContext(configuration, FoldContext.small(), searchStats);
        return new LocalLogicalPlanOptimizer(localContext).localOptimize(plan);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, SearchStats searchStats) {
        return localPlan(plan, EsqlTestUtils.TEST_CFG, searchStats);
    }

    private LogicalPlan localPlan(String query) {
        return localPlan(plan(query), TEST_SEARCH_STATS);
    }

    private static Analyzer analyzerWithUnionTypeMapping() {
        InvalidMappedField unionTypeField = new InvalidMappedField(
            "integer_long_field",
            Map.of("integer", Set.of("test1"), "long", Set.of("test2"))
        );

        EsIndex test = EsIndexGenerator.esIndex(
            "test*",
            Map.of("integer_long_field", unionTypeField),
            Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD)
        );

        return new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public static EsRelation relation() {
        return EsqlTestUtils.relation(randomFrom(IndexMode.values()));
    }
}
