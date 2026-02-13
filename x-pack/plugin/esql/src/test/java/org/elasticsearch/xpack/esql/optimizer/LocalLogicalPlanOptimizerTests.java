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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
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
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForExistingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForMissingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.DOWN;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;
import static org.elasticsearch.xpack.esql.planner.PlannerUtils.breakPlanBetweenCoordinatorAndDataNode;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
public class LocalLogicalPlanOptimizerTests extends AbstractLocalLogicalPlanOptimizerTests {

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
     * Project[[first_name{f}#4]]
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
     * Project[[first_name{f}#7, last_name{r}#17]]
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
        var project = as(localPlan, Project.class);
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
        // \_Project[[last_name{r}#7]]
        // \_Eval[[null[KEYWORD] AS last_name#7]]
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

        assertThat(Expressions.names(project.output()), contains("last_name"));
    }

    /**
     * Expects
     * Project[[x{r}#3]]
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

        var analyzed = analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query));
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
        Expression inn = isNotNull(new Add(EMPTY, fieldA, ONE, TEST_CFG));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithTwoFields() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        var fieldB = getFieldAttribute("b");
        Expression inn = isNotNull(new Add(EMPTY, fieldA, fieldB, TEST_CFG));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnFunctionWithOneField() {
        EsRelation relation = relation();
        var fieldA = getFieldAttribute("a");
        var pattern = L("abc");
        Expression inn = isNotNull(
            new And(EMPTY, new StartsWith(EMPTY, fieldA, pattern), greaterThanOf(new Add(EMPTY, ONE, TWO, TEST_CFG), THREE))
        );

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
     * Project[[!alias_integer, boolean{f}#7, byte{f}#8, constant_keyword-foo{f}#9, date{f}#10, date_nanos{f}#11, dense_vector
     * {f}#26, double{f}#12, float{f}#13, half_float{f}#14, integer{f}#16, ip{f}#17, keyword{f}#18, long{f}#19, scaled_float{f}#15,
     * semantic_text{f}#25, short{f}#21, text{f}#22, unsigned_long{f}#20, version{f}#23, wildcard{f}#24, s{r}#5]]
     * \_Eval[[$$dense_vector$V_DOT_PRODUCT$27{f}#27 AS s#5]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test_all][$$dense_vector$V_DOT_PRODUCT$27{f}#27, !alias_integer,
     */
    public void testVectorFunctionsReplaced() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Project[[!alias_integer, boolean{f}#7, byte{f}#8, ... s{r}#5]]
        var project = as(plan, Project.class);
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
     * Project[[s{r}#4]]
     * \_TopN[[Order[s{r}#4,DESC,FIRST]],1[INTEGER]]
     *   \_Eval[[$$dense_vector$replaced$28{t}#28 AS s#4]]
     *     \_EsRelation[types][$$dense_vector$replaced$28{t}#28, !alias_integer, b..]
     */
    public void testVectorFunctionsReplacedWithTopN() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Project[[s{r}#4]]
        var project = as(plan, Project.class);
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

        // Project[[s{r}#4]]
        var project = as(plan, Project.class);
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

    public void testAggregateMetricDouble() {
        String query = "FROM k8s-downsampled | STATS m = min(network.eth0.tx)";

        LogicalPlan plan = localPlan(plan(query, tsAnalyzer), new EsqlTestUtils.TestSearchStats());

        // Limit[1000[INTEGER],false,false]
        var limit = as(plan, Limit.class);
        // Aggregate[[],[MIN($$min(network.eth>$MIN$0{r$}#30,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#5]]
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(0));
        assertThat(aggregate.aggregates(), hasSize(1));
        // Eval[[$$network.eth0.tx$AMD_MIN$1432505394{f$}#31 AS $$min(network.eth>$MIN$0#30]]
        var eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        var fieldAttr = as(alias.child(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("network.eth0.tx"));
        var field = as(fieldAttr.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig = as(field.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        assertThat(blockLoaderFunctionConfig.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_MIN));

        // EsRelation[k8s-downsampled][@timestamp{f}#6, client.ip{f}#10, cluster{f}#7, eve..]
        var esRelation = as(eval.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(fieldAttr));
    }

    public void testAggregateMetricDoubleWithAvgAndOtherFunctions() {
        String query = """
            from k8s-downsampled
            | STATS s = sum(network.eth0.tx), a = avg(network.eth0.tx)
            """;

        LogicalPlan plan = localPlan(plan(query, tsAnalyzer), new EsqlTestUtils.TestSearchStats());

        // Project[[s{r}#5, a{r}#8]]
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("s", "a"));
        // Eval[[s{r}#5 / $$SUM$a$1{r$}#34 AS a#8]]
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("a"));
        var alias = as(eval.fields().getFirst(), Alias.class);
        var division = as(alias.child(), Div.class);
        assertTrue(Expressions.names(division.arguments()).contains("s"));

        // Limit[1000[INTEGER],false,false]
        var limit = as(eval.child(), Limit.class);
        // Aggregate[[],[SUM($$sum(network.eth>$SUM$0{r$}#35,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS s#5,
        // SUM($$avg(network.eth>$SUM$1{r$}#36,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a$1#34]]
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(0));
        assertThat(aggregate.aggregates(), hasSize(2));
        as(Alias.unwrap(aggregate.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(aggregate.aggregates().get(1)), Sum.class);

        // Eval[[$$network.eth0.tx$AMD_SUM$365493977{f$}#37 AS $$sum(network.eth>$SUM$0#35,
        // $$network.eth0.tx$AMD_COUNT$1570201087{f$}#38 AS $$avg(network.eth>$SUM$1#36]]
        var eval2 = as(aggregate.child(), Eval.class);
        assertThat(eval2.fields(), hasSize(2));

        var alias2_1 = as(eval2.fields().get(0), Alias.class);
        var fieldAttr1 = as(alias2_1.child(), FieldAttribute.class);
        assertThat(fieldAttr1.fieldName().string(), equalTo("network.eth0.tx"));
        var field1 = as(fieldAttr1.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig1 = as(field1.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        assertThat(blockLoaderFunctionConfig1.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_SUM));

        var alias2_2 = as(eval2.fields().get(1), Alias.class);
        var fieldAttr2 = as(alias2_2.child(), FieldAttribute.class);
        assertThat(fieldAttr2.fieldName().string(), equalTo("network.eth0.tx"));
        var field2 = as(fieldAttr2.field(), FunctionEsField.class);
        var blockLoaderFunctionConfig2 = as(field2.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        assertThat(blockLoaderFunctionConfig2.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_COUNT));

        // EsRelation[k8s-downsampled][@timestamp{f}#9, client.ip{f}#13, cluster{f}#10, ev..]
        var esRelation = as(eval2.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(fieldAttr1));
        assertTrue(esRelation.output().contains(fieldAttr2));
    }

    public void testAggregateMetricDoubleTSCommand() {
        String query = """
            TS k8s-downsampled |
            STATS m = max(max_over_time(network.eth0.tx)),
                  c = count(count_over_time(network.eth0.tx)),
                  a = avg(avg_over_time(network.eth0.tx))
            BY pod, time_bucket = BUCKET(@timestamp,5minute)
            """;
        LogicalPlan plan = localPlan(plan(query, tsAnalyzer), new EsqlTestUtils.TestSearchStats());

        // Project[[m{r}#9, c{r}#12, a{r}#15, pod{r}#19, time_bucket{r}#6]]
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("m", "c", "a", "pod", "time_bucket"));
        // Eval[[UNPACKDIMENSION(grouppod_$1{r}#49) AS pod#19,
        // $$SUM$a$0{r$}#41 / $$COUNT$a$1{r$}#42 AS a#15]]
        var eval1 = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval1.fields()), containsInAnyOrder("pod", "a"));
        // Limit[1000000[INTEGER],false,false]
        var limit = as(eval1.child(), Limit.class);
        // Aggregate[[packpod_$1{r}#48, time_bucket{r}#6],
        // [MAX(MAXOVERTIME_$1{r}#44,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#9,
        // COUNT(COUNTOVERTIME_$1{r}#45,true[BOOLEAN],PT0S[TIME_DURATION]) AS c#12,
        // SUM(AVGOVERTIME_$1{r}#46,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$a$0#41,
        // COUNT(AVGOVERTIME_$1{r}#46,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$a$1#42,
        // packpod_$1{r}#48 AS grouppod_$1#49,
        // time_bucket{r}#6 AS time_bucket#6]]
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(2));
        assertThat(aggregate.aggregates(), hasSize(6));
        // Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#50 / COUNTOVERTIME_$1{r}#45 AS AVGOVERTIME_$1#46,
        // PACKDIMENSION(pod{r}#47) AS packpod_$1#48]]
        var eval2 = as(aggregate.child(), Eval.class);
        // TimeSeriesAggregate[[_tsid{m}#43, time_bucket{r}#6],
        // [MAX($$max_over_time(n>$MAX$0{r$}#52,true[BOOLEAN],PT0S[TIME_DURATION]) AS MAXOVERTIME_$1#44,
        // SUM($$count_over_time>$SUM$1{r$}#53,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS COUNTOVERTIME_$1#45,
        // SUM($$avg_over_time(n>$SUM$2{r$}#54,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) AS $$SUM$AVGOVERTIME_$1$0#50,
        // VALUES(pod{f}#19,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#47,
        // time_bucket{r}#6],
        // BUCKET(@timestamp{f}#17,PT5M[TIME_DURATION])]
        var timeSeries = as(eval2.child(), TimeSeriesAggregate.class);

        // Eval[[BUCKET(@timestamp{f}#17,PT5M[TIME_DURATION]) AS time_bucket#6,
        // $$network.eth0.tx$AMD_MAX$476613723{f$}#55 AS $$max_over_time(n>$MAX$0#52,
        // $$network.eth0.tx$AMD_COUNT$1887347767{f$}#56 AS $$count_over_time>$SUM$1#53,
        // $$network.eth0.tx$AMD_SUM$735682543{f$}#57 AS $$avg_over_time(n>$SUM$2#54]]
        var eval3 = as(timeSeries.child(), Eval.class);
        assertThat(eval3.fields(), hasSize(4));
        var maxfieldAttr = as(as(eval3.fields().get(1), Alias.class).child(), FieldAttribute.class);
        var countfieldAttr = as(as(eval3.fields().get(2), Alias.class).child(), FieldAttribute.class);
        var sumfieldAttr = as(as(eval3.fields().get(3), Alias.class).child(), FieldAttribute.class);
        assertThat(maxfieldAttr.fieldName().string(), equalTo("network.eth0.tx"));
        assertThat(countfieldAttr.fieldName().string(), equalTo("network.eth0.tx"));
        assertThat(sumfieldAttr.fieldName().string(), equalTo("network.eth0.tx"));
        var maxField = as(maxfieldAttr.field(), FunctionEsField.class);
        var countField = as(countfieldAttr.field(), FunctionEsField.class);
        var sumField = as(sumfieldAttr.field(), FunctionEsField.class);
        var maxBlockLoaderFunctionConfig = as(maxField.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        var countBlockLoaderFunctionConfig = as(countField.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        var sumBlockLoaderFunctionConfig = as(sumField.functionConfig(), BlockLoaderFunctionConfig.JustFunction.class);
        assertThat(maxBlockLoaderFunctionConfig.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_MAX));
        assertThat(countBlockLoaderFunctionConfig.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_COUNT));
        assertThat(sumBlockLoaderFunctionConfig.function(), equalTo(BlockLoaderFunctionConfig.Function.AMD_SUM));

        // EsRelation[k8s-downsampled][@timestamp{f}#17, client.ip{f}#21, cluster{f}#18, e..]
        var esRelation = as(eval3.child(), EsRelation.class);
        assertTrue(esRelation.output().contains(maxfieldAttr));
        assertTrue(esRelation.output().contains(countfieldAttr));
        assertTrue(esRelation.output().contains(sumfieldAttr));
    }

    /**
     * Proves that we do <strong>not</strong> push to a stub-relation even if it contains
     * a field we could otherwise push. Stub relations don't have a "push" concept.
     */
    public void testAggregateMetricDoubleInlineStats() {
        String query = """
            FROM k8s-downsampled
            | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod
            | SORT @timestamp, cluster, pod
            | KEEP @timestamp, cluster, pod, network.eth0.tx, tx_max
            | LIMIT 9
            """;

        LogicalPlan plan = localPlan(plan(query, tsAnalyzer), new EsqlTestUtils.TestSearchStats());

        // Project[[@timestamp{f}#972, cluster{f}#973, pod{f}#974, network.eth0.tx{f}#991, tx_max{r}#962]]
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("@timestamp", "cluster", "pod", "network.eth0.tx", "tx_max"));
        // TopN[[Order[@timestamp{f}#972,ASC,LAST], Order[cluster{f}#973,ASC,LAST], Order[pod{f}#974,ASC,LAST]],9[INTEGER],false]
        var topN = as(project.child(), TopN.class);
        // InlineJoin[LEFT,[pod{f}#974],[pod{r}#974]]
        var inlineJoin = as(topN.child(), InlineJoin.class);
        // Aggregate[[pod{f}#974],[MAX($$MAX(network.eth>$MAX$0{r$}#996,true[BOOLEAN],PT0S[TIME_DURATION]) AS tx_max#962, pod{f}#974
        var aggregate = as(inlineJoin.right(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(1));
        assertThat(aggregate.aggregates(), hasSize(2));
        as(Alias.unwrap(aggregate.aggregates().get(0)), Max.class);

        // Eval[[FROMAGGREGATEMETRICDOUBLE(network.eth0.tx{f}#991,1[INTEGER]) AS $$MAX(network.eth>$MAX$0#996]]
        var eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        var load = as(alias.child(), FromAggregateMetricDouble.class); // <--- no pushing.
        var fieldAttr = as(load.field(), FieldAttribute.class);
        assertThat(fieldAttr.fieldName().string(), equalTo("network.eth0.tx"));
        as(fieldAttr.field(), EsField.class);

        // StubRelation[[@timestamp{f}#972, ... ]]
        var stubRelation = as(eval.child(), StubRelation.class);
        assertThat(stubRelation.output(), hasItem(fieldAttr));

        // EsRelation[k8s-downsampled][@timestamp{f}#972, client.ip{f}#976, cluster{f}#973, ..]
        var esRelation = as(inlineJoin.left(), EsRelation.class);
        assertThat(esRelation.output(), hasItem(fieldAttr));
    }

    public void testVectorFunctionsWhenFieldMissing() {
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
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | where %s > 0.5
            | keep dense_vector
            """, testCase.toQuery());

        LogicalPlan plan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Project[[dense_vector{f}#25]]
        var project = as(plan, Project.class);
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

        // Project with all fields including similarity and keyword
        var project = as(plan, Project.class);
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

        // Inner Project with the pushed down function
        var innerProject = as(mvExpand.child(), Project.class);
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

        // Project[[s1{r}#5, s2{r}#8, r2{r}#14]]
        var project = as(plan, Project.class);
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
        String query = """
            FROM test
            | EVAL l = LENGTH(last_name)
            | KEEP l
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("l"));
        var eval = as(project.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var limit = as(eval.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInWhere() {
        String query = """
            FROM test
            | WHERE LENGTH(last_name) > 1
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        Attribute lAttr = assertLengthPushdown(as(filter.condition(), GreaterThan.class).left(), "last_name");
        var relation = as(filter.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInStats() {
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
        String query = """
            FROM test
            | EVAL l1 = last_name
            | EVAL l2 = l1
            | EVAL l3 = l2
            | EVAL l = LENGTH(l3)
            | KEEP l
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("l"));
        var eval = as(project.child(), Eval.class);
        Attribute lAttr = assertLengthPushdown(as(eval.fields().getFirst(), Alias.class).child(), "last_name");
        var limit = as(eval.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        assertTrue(relation.output().contains(lAttr));
    }

    public void testLengthInWhereAndEval() {
        String query = """
            FROM test
            | WHERE LENGTH(last_name) > 1
            | EVAL l = LENGTH(last_name)
            """;
        LogicalPlan plan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        var project = as(plan, Project.class);
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
        assertThat(relation.output().stream().filter(a -> {
            if (a instanceof FieldAttribute fa) {
                if (fa.field() instanceof FunctionEsField fef) {
                    return fef.functionConfig().function() == BlockLoaderFunctionConfig.Function.LENGTH;
                }
            }
            return false;
        }).toList(), hasSize(2));
    }

    public void testLengthInStatsTwice() {
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

    private static PhysicalPlan physicalPlan(LogicalPlan logicalPlan, Analyzer analyzer) {
        var mapper = new Mapper();
        return mapper.map(new Versioned<>(logicalPlan, analyzer.context().minimumVersion()));
    }

    public void testReductionPlanForTopNWithPushedDownFunctions() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        var query = String.format(Locale.ROOT, """
                FROM test_all
                | EVAL score = V_DOT_PRODUCT(dense_vector, [1.0, 2.0, 3.0])
                | SORT integer DESC
                | LIMIT 10
                | KEEP text, score
            """);
        var logicalPlan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Verify the logical plan structure:
        // Project[[text{f}#1105, score{r}#1085]]
        var project = as(logicalPlan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("text", "score"));

        // TopN[[Order[integer{f}#1099,DESC,FIRST]],10[INTEGER],false]
        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        var order = as(topN.order().getFirst(), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        var orderField = as(order.child(), FieldAttribute.class);
        assertThat(orderField.name(), equalTo("integer"));

        // Eval[[$$dense_vector$V_DOT_PRODUCT$1451583510{f$}#1110 AS score#1085]]
        var eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var scoreAlias = eval.fields()
            .stream()
            .filter(f -> f.name().equals("score"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 'score' not found in eval"));
        var scoreField = as(scoreAlias, Alias.class);
        var scoreFieldAttr = as(scoreField.child(), FieldAttribute.class);
        assertThat(scoreFieldAttr.name(), startsWith("$$dense_vector$V_DOT_PRODUCT$"));
        assertThat(scoreFieldAttr.fieldName().string(), equalTo("dense_vector"));

        // EsRelation[test_all][!alias_integer, boolean{f}#1090, byte{f}#1091, cons..]
        var relation = as(eval.child(), EsRelation.class);
        assertTrue(relation.output().contains(scoreFieldAttr));

        // Also verify physical plan behavior
        var physicalPlan = physicalPlan(logicalPlan, allTypesAnalyzer);
        var coordAndDataNodePlans = breakPlanBetweenCoordinatorAndDataNode(physicalPlan, TEST_CFG);

        var coordPlan = coordAndDataNodePlans.v1();
        var coordProjectExec = as(coordPlan, ProjectExec.class);
        assertThat(coordProjectExec.projections().stream().map(NamedExpression::name).toList(), containsInAnyOrder("text", "score"));
        var coordTopN = as(coordProjectExec.child(), TopNExec.class);
        var orderAttr = as(coordTopN.order().getFirst().child(), FieldAttribute.class);
        assertThat(orderAttr.name(), equalTo("integer"));

        var reductionPlan = ((PlannerUtils.TopNReduction) PlannerUtils.reductionPlan(coordAndDataNodePlans.v2())).plan();
        var topNExec = as(reductionPlan, TopNExec.class);
        var evalExec = as(topNExec.child(), EvalExec.class);
        var alias = evalExec.fields().get(0);
        assertThat(alias.name(), equalTo("score"));
        var fieldAttr = as(alias.child(), FieldAttribute.class);
        assertThat(fieldAttr.name(), startsWith("$$dense_vector$V_DOT_PRODUCT$"));
        var esSourceExec = as(evalExec.child(), EsSourceExec.class);
        assertTrue(esSourceExec.outputSet().stream().anyMatch(a -> a == fieldAttr));
    }

    public void testReductionPlanForTopNWithPushedDownFunctionsInOrder() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        var query = String.format(Locale.ROOT, """
                FROM test_all
                | EVAL fieldLength = LENGTH(text)
                | SORT fieldLength DESC
                | LIMIT 10
                | KEEP text, fieldLength
            """);
        var logicalPlan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);
        var physicalPlan = physicalPlan(logicalPlan, allTypesAnalyzer);
        var coordAndDataNodePlans = breakPlanBetweenCoordinatorAndDataNode(physicalPlan, TEST_CFG);

        var coordPlan = coordAndDataNodePlans.v1();
        var coordProjectExec = as(coordPlan, ProjectExec.class);
        assertThat(coordProjectExec.projections().stream().map(NamedExpression::name).toList(), containsInAnyOrder("text", "fieldLength"));
        var coordTopN = as(coordProjectExec.child(), TopNExec.class);
        var orderAttr = as(coordTopN.order().getFirst().child(), ReferenceAttribute.class);
        assertThat(orderAttr.name(), equalTo("fieldLength"));

        var reductionPlan = ((PlannerUtils.TopNReduction) PlannerUtils.reductionPlan(coordAndDataNodePlans.v2())).plan();
        var topN = as(reductionPlan, TopNExec.class);
        var eval = as(topN.child(), EvalExec.class);
        var alias = eval.fields().get(0);
        assertThat(alias.name(), equalTo("fieldLength"));
        var fieldAttr = as(alias.child(), FieldAttribute.class);
        assertThat(fieldAttr.name(), startsWith("$$text$LENGTH$"));
        var esSourceExec = as(eval.child(), EsSourceExec.class);
        assertTrue(esSourceExec.outputSet().stream().anyMatch(a -> a == fieldAttr));
    }

    public void testPushableFunctionsInFork() {
        var query = """
            from test_all
            | eval u = v_cosine(dense_vector, [4, 5, 6])
            | fork
                (eval s = length(text) | keep s, u, keyword)
                (eval t = v_dot_product(dense_vector, [1, 2, 3]) | keep t, u, keyword)
            | eval x = length(keyword)
            """;
        var localPlan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        var eval = as(localPlan, Eval.class);
        // Cosine function has not been pushed down as it targets a reference and not a field
        assertThat(eval.fields().getFirst().child(), instanceOf(Length.class));
        var limit = as(eval.child(), Limit.class);
        var fork = as(limit.child(), Fork.class);
        assertThat(fork.children(), hasSize(2));

        // First branch: (eval s = length(text) | keep s, u, keyword)
        var project1 = as(fork.children().get(0), Project.class);
        assertThat(Expressions.names(project1.projections()), containsInAnyOrder("s", "_fork", "t", "u", "keyword"));
        var eval1 = as(project1.child(), Eval.class);
        assertThat(eval1.fields(), hasSize(4));

        // Find the "s" field which should be a pushed down LENGTH function
        var sAlias = eval1.fields()
            .stream()
            .filter(f -> f.name().equals("s"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 's' not found in eval"));
        var sField = as(sAlias, Alias.class);
        var sFieldAttr = as(sField.child(), FieldAttribute.class);
        assertThat(sFieldAttr.name(), startsWith("$$text$LENGTH$"));
        assertThat(sFieldAttr.fieldName().string(), equalTo("text"));

        // Find the "u" field which should be a pushed down V_COSINE function
        var u1Alias = eval1.fields()
            .stream()
            .filter(f -> f.name().equals("u"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 's' not found in eval"));
        var u1Field = as(u1Alias, Alias.class);
        var u1FieldAttr = as(u1Field.child(), FieldAttribute.class);
        assertThat(u1FieldAttr.name(), startsWith("$$dense_vector$V_COSINE$"));
        assertThat(u1FieldAttr.fieldName().string(), equalTo("dense_vector"));

        var limit1 = as(eval1.child(), Limit.class);
        // EsRelation[test_all] - verify pushed down field is in the relation output
        var relation1 = as(limit1.child(), EsRelation.class);
        assertTrue(relation1.output().contains(sFieldAttr));

        // Second branch: (eval t = v_dot_product(dense_vector, [1, 2, 3]) | keep t, u, keyword)
        // Project[[s{r}#55, _fork{r}#4, t{r}#11]]
        var project2 = as(fork.children().get(1), Project.class);
        assertThat(Expressions.names(project2.projections()), containsInAnyOrder("s", "_fork", "t", "u", "keyword"));

        // Eval[[$$dense_vector$V_DOT_PRODUCT$-1468139866{f$}#60 AS t#11, fork2[KEYWORD] AS _fork#4, null[INTEGER] AS s#55]]
        var eval2 = as(project2.child(), Eval.class);
        assertThat(eval2.fields(), hasSize(4));

        // Find the "t" field which should be a pushed down V_DOT_PRODUCT function
        var tAlias = eval2.fields()
            .stream()
            .filter(f -> f.name().equals("t"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 't' not found in eval"));
        var tField = as(tAlias, Alias.class);
        var tFieldAttr = as(tField.child(), FieldAttribute.class);
        assertThat(tFieldAttr.name(), startsWith("$$dense_vector$V_DOT_PRODUCT$"));
        assertThat(tFieldAttr.fieldName().string(), equalTo("dense_vector"));

        // Find the "u" field which should be the same pushed down V_COSINE function
        var u2Alias = eval1.fields()
            .stream()
            .filter(f -> f.name().equals("u"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 's' not found in eval"));
        var u2Field = as(u2Alias, Alias.class);
        assertThat(u1Field, equalTo(u2Field));

        // Limit[1000[INTEGER],false,false]
        var limit2 = as(eval2.child(), Limit.class);

        // EsRelation[test_all] - verify pushed down field is in the relation output
        var relation2 = as(limit2.child(), EsRelation.class);
        assertTrue(relation2.output().contains(tFieldAttr));
    }

    public void testPushableFunctionsInSubqueries() {
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled());

        var query = """
            from test_all, (from test_all | eval s = length(text) | keep s)
            | eval t = v_dot_product(dense_vector, [1, 2, 3])
            | keep s, t
            """;
        var localPlan = localPlan(plan(query, allTypesAnalyzer), TEST_SEARCH_STATS);

        // Project[[s{r}#97, t{r}#9]]
        var project = as(localPlan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("s", "t"));

        // Eval[[DOTPRODUCT(dense_vector{r}#82,[1.0, 2.0, 3.0][DENSE_VECTOR]) AS t#9]]
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        // Find the "t" field which should be a NOT pushed down LENGTH function
        var tAlias = eval.fields()
            .stream()
            .filter(f -> f.name().equals("t"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 't' not found in subquery eval"));
        assertThat(tAlias.child(), instanceOf(DotProduct.class));

        // Limit[1000[INTEGER],false,false]
        var limit = as(eval.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        // UnionAll[[alias_integer{r}#76, boolean{r}#77, byte{r}#78, ...]]
        var unionAll = as(limit.child(), UnionAll.class);
        assertThat(unionAll.children(), hasSize(2));

        // Second branch of UnionAll - contains the subquery
        // Project[[alias_integer{r}#99, boolean{r}#56, ...]]
        var project2 = as(unionAll.children().get(1), Project.class);

        // Eval[[null[KEYWORD] AS alias_integer#55, null[BOOLEAN] AS boolean#56, ...]]
        var eval2 = as(project2.child(), Eval.class);

        var subquery = as(eval2.child(), Subquery.class);
        var subqueryProject = as(subquery.child(), Project.class);
        assertThat(Expressions.names(subqueryProject.projections()), contains("s"));
        var subqueryEval = as(subqueryProject.child(), Eval.class);
        assertThat(subqueryEval.fields(), hasSize(1));

        // Find the "s" field which should be a pushed down LENGTH function
        var sAlias = subqueryEval.fields()
            .stream()
            .filter(f -> f.name().equals("s"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 's' not found in subquery eval"));
        var sField = as(sAlias, Alias.class);
        var sFieldAttr = as(sField.child(), FieldAttribute.class);
        assertThat(sFieldAttr.name(), startsWith("$$text$LENGTH$"));
        assertThat(sFieldAttr.fieldName().string(), equalTo("text"));
        // EsRelation[test_all] - verify pushed down field is in the relation output
        var subqueryRelation = as(subqueryEval.child(), EsRelation.class);
        assertTrue(subqueryRelation.output().contains(sFieldAttr));
    }

    public void testPushDownFunctionsLookupJoin() {
        var query = """
            from test
            | eval s = length(first_name)
            | rename languages AS language_code
            | keep s, language_code, last_name
            | lookup join languages_lookup ON language_code
            | eval t = length(last_name)
            | eval u = length(language_name)
            """;

        var localPlan = localPlan(plan(query, analyzer), TEST_SEARCH_STATS);

        // Project[[s{r}#124, languages{f}#141 AS language_code#127, last_name{f}#142, language_name{f}#150, t{r}#134, u{r}#137]]
        var project = as(localPlan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("s", "language_code", "last_name", "language_name", "t", "u"));

        // Eval[[$$last_name$LENGTH$1912486003{f$}#151 AS t#134, LENGTH(language_name{f}#150) AS u#137]]
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));

        // Find the "t" field which should not be pushed down
        var tAlias = eval.fields()
            .stream()
            .filter(f -> f.name().equals("t"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 't' not found in eval"));
        var tField = as(tAlias, Alias.class);
        var tLength = as(tField.child(), Length.class);
        assertThat(Expressions.name(tLength.field()), equalTo("last_name"));

        // Find the "u" field which should NOT be pushed down - it's LENGTH(language_name{f}#150)
        var uAlias = eval.fields()
            .stream()
            .filter(f -> f.name().equals("u"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 'u' not found in eval"));
        var uField = as(uAlias, Alias.class);
        var uLength = as(uField.child(), Length.class);
        assertThat(Expressions.name(uLength.field()), equalTo("language_name"));

        var limit = as(eval.child(), Limit.class);
        var join = as(limit.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        // Left side of join: Eval[[$$first_name$LENGTH$1912486003{f$}#152 AS s#124]]
        var leftEval = as(join.left(), Eval.class);
        assertThat(leftEval.fields(), hasSize(1));

        // Find the "s" field which should be a pushed down LENGTH function on first_name
        var sAlias = leftEval.fields()
            .stream()
            .filter(f -> f.name().equals("s"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Field 's' not found in left eval"));
        var sField = as(sAlias, Alias.class);
        var sFieldAttr = as(sField.child(), FieldAttribute.class);
        assertThat(sFieldAttr.name(), startsWith("$$first_name$LENGTH$"));
        assertThat(sFieldAttr.fieldName().string(), equalTo("first_name"));

        // Limit[1000[INTEGER],false,false]
        var leftLimit = as(leftEval.child(), Limit.class);

        // EsRelation[test] - verify pushed down field is in the relation output
        var leftRelation = as(leftLimit.child(), EsRelation.class);
        assertTrue(leftRelation.output().contains(sFieldAttr));

        // Right side of join: EsRelation[languages_lookup][LOOKUP][language_code{f}#149, language_name{f}#150, $$last_..]
        var rightRelation = as(join.right(), EsRelation.class);
        assertThat(rightRelation.output().stream().map(Attribute::name).toList(), contains("language_code", "language_name"));
    }

    private IsNotNull isNotNull(Expression field) {
        return new IsNotNull(EMPTY, field);
    }

    private LocalRelation asEmptyRelation(Object o) {
        var empty = as(o, LocalRelation.class);
        assertThat(empty.supplier(), is(EmptyLocalSupplier.EMPTY));
        return empty;
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

    public static EsRelation relation() {
        return EsqlTestUtils.relation(randomFrom(IndexMode.values()));
    }

    // Tests for project metadata field optimization (ReplaceFieldWithConstantOrNull)

    /**
     * Test that project metadata fields like _project.my_tag are replaced with their constant values in a Filter.
     * When the SearchStats returns a constant value for the project metadata field, the MetadataAttribute
     * should be replaced with a Literal containing that constant value.
     *
     * When the constant value matches the comparison value (e.g., "foo" == "foo"), subsequent optimizations
     * will fold this to true and potentially simplify the entire plan further.
     */
    public void testProjectMetadataFieldReplacedWithConstantInFilter() {
        // Create an EsRelation with a project metadata attribute
        var projectTagAttr = new MetadataAttribute(EMPTY, "_project.my_tag", KEYWORD, false);
        var fieldAttr = getFieldAttribute("name");
        var relation = new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", IndexMode.STANDARD),
            List.of(fieldAttr, projectTagAttr)
        );

        // Create a filter that uses the project metadata attribute: WHERE _project.my_tag == "bar"
        // (different value so constant folding doesn't eliminate the filter)
        var filter = new Filter(
            EMPTY,
            new Limit(EMPTY, L(1000), relation),
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals(EMPTY, projectTagAttr, L("bar"))
        );

        // Create SearchStats that returns a constant value for _project.my_tag
        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("_project.my_tag")) {
                    return "foo";
                }
                return null;
            }
        };

        // Run the local optimizer
        var localContext = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        var optimizedPlan = new LocalLogicalPlanOptimizer(localContext).localOptimize(filter);

        // When _project.my_tag is replaced with "foo" and compared to "bar", the result is false,
        // which causes the optimizer to turn this into an empty LocalRelation
        var localRelation = as(optimizedPlan, org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation.class);
        assertThat(localRelation.supplier(), instanceOf(EmptyLocalSupplier.class));
    }

    /**
     * Test that project metadata fields are NOT replaced with null when constantValue returns null.
     * When we can't get the constant value (either because the tag doesn't exist OR because we
     * couldn't access project metadata during optimization), we leave the attribute as is and
     * let the normal ES|QL execution path handle it via block loaders.
     */
    public void testProjectMetadataFieldNotReplacedWhenConstantValueReturnsNull() {
        // Create an EsRelation with a project metadata attribute
        var projectTagAttr = new MetadataAttribute(EMPTY, "_project.custom_tag", KEYWORD, false);
        var fieldAttr = getFieldAttribute("name");
        var relation = new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", IndexMode.STANDARD),
            List.of(fieldAttr, projectTagAttr)
        );

        // Create a filter that uses the project metadata attribute
        var filter = new Filter(
            EMPTY,
            new Limit(EMPTY, L(1000), relation),
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals(EMPTY, projectTagAttr, L("bar"))
        );

        // Create SearchStats that returns null for the project tag (simulating inability to get constant)
        // The default implementation of constantValue already returns null, but we override for clarity
        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                // Return null for all fields (no constant values available)
                return null;
            }
        };

        // Run the local optimizer
        var localContext = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        var optimizedPlan = new LocalLogicalPlanOptimizer(localContext).localOptimize(filter);

        // When constantValue returns null, we do NOT replace the attribute with null.
        // Instead, we leave the filter as is and let execution handle it.
        // The plan should still be a Filter (not an empty LocalRelation)
        var filterResult = as(optimizedPlan, Filter.class);
        // The filter condition should still reference the original MetadataAttribute
        var equals = as(filterResult.condition(), org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals.class);
        assertThat(equals.left(), instanceOf(MetadataAttribute.class));
        assertThat(((MetadataAttribute) equals.left()).name(), equalTo("_project.custom_tag"));
    }

    /**
     * Test that project metadata fields are replaced with constant values in an Eval.
     */
    public void testProjectMetadataFieldReplacedWithConstantInEval() {
        // Create an EsRelation with a project metadata attribute
        var projectTagAttr = new MetadataAttribute(EMPTY, "_project._alias", KEYWORD, false);
        var fieldAttr = getFieldAttribute("name");
        var relation = new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", IndexMode.STANDARD),
            List.of(fieldAttr, projectTagAttr)
        );

        // Create an eval that uses the project metadata attribute: EVAL project_alias = _project._alias
        var eval = new Eval(EMPTY, new Limit(EMPTY, L(1000), relation), List.of(new Alias(EMPTY, "project_alias", projectTagAttr)));

        // Create SearchStats that returns a constant value for _project._alias
        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("_project._alias")) {
                    return "my_project";
                }
                return null;
            }
        };

        // Run the local optimizer
        var localContext = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        var optimizedPlan = new LocalLogicalPlanOptimizer(localContext).localOptimize(eval);

        // Verify the MetadataAttribute was replaced with a Literal
        var optimizedEval = as(optimizedPlan, Eval.class);
        var alias = as(optimizedEval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), equalTo(new BytesRef("my_project")));
    }

    /**
     * Test that non-project metadata fields (like _index) are NOT replaced by this optimization.
     * Standard metadata fields should remain unchanged.
     */
    public void testNonProjectMetadataFieldsNotReplaced() {
        // Create an EsRelation with a standard metadata attribute (_index)
        var indexAttr = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
        var fieldAttr = getFieldAttribute("name");
        var relation = new EsRelation(
            EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", IndexMode.STANDARD),
            List.of(fieldAttr, indexAttr)
        );

        // Create a filter that uses the _index metadata attribute
        var filter = new Filter(
            EMPTY,
            new Limit(EMPTY, L(1000), relation),
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals(EMPTY, indexAttr, L("test"))
        );

        // Create SearchStats (doesn't matter what it returns, _index shouldn't be processed)
        var searchStats = TEST_SEARCH_STATS;

        // Run the local optimizer
        var localContext = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        var optimizedPlan = new LocalLogicalPlanOptimizer(localContext).localOptimize(filter);

        // Verify the _index MetadataAttribute was NOT replaced
        var optimizedFilter = as(optimizedPlan, Filter.class);
        var equals = as(optimizedFilter.condition(), org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals.class);

        // The left side should still be a MetadataAttribute
        var metadataAttr = as(equals.left(), MetadataAttribute.class);
        assertThat(metadataAttr.name(), equalTo("_index"));
    }
}
