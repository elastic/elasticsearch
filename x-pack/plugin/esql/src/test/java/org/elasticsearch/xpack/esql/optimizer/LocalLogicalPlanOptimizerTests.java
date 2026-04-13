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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
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
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
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
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForExistingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForMissingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LocalLogicalPlanOptimizerTests extends AbstractLocalLogicalPlanOptimizerTests {

    /**
     * Expects
     * LocalRelation[[first_name{f}#4],EMPTY]
     */
    public void testMissingFieldInFilterNumeric() {
        var plan = testAnalyzer().coordinatorPlan("""
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
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * Project[[last_name{r}#7]]
     * \_Eval[[null[KEYWORD] AS last_name]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     * }
     */
    public void testMissingFieldInProject() {
        var plan = testAnalyzer().coordinatorPlan("""
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

    /**
     * Expects a similar plan to testMissingFieldInProject() above, except for the Alias's child value
     * {@snippet lang="text":
     * Project[[last_name{r}#4]]
     * \_Eval[[[66 6f 6f][KEYWORD] AS last_name]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }
     */
    public void testReassignedMissingFieldInProject() {
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text" :
     * Project[[first_name{f}#4]]
     * \_Limit[10000[INTEGER]]
     * \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
     * }
     */
    public void testMissingFieldInSort() {
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * Project[[first_name{f}#7, last_name{r}#17]]
     * \_Limit[1000[INTEGER],true]
     *   \_MvExpand[last_name{f}#10,last_name{r}#17]
     *     \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{r}#10, long_noidx{f}#16, salary{f}#11]]
     *       \_Eval[[null[KEYWORD] AS last_name]]
     *         \_Limit[1000[INTEGER],false]
     *           \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }
     */
    public void testMissingFieldInMvExpand() {
        var plan = testAnalyzer().coordinatorPlan("""
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

        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * Project[[x{r}#3]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_Limit[10000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     * }
     */
    public void testMissingFieldInEval() {
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * LocalRelation[[first_name{f}#4],EMPTY]
     * }
     */
    public void testMissingFieldInFilterNumericWithReference() {
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * LocalRelation[[first_name{f}#4],EMPTY]
     * }
     */
    public void testMissingFieldInFilterNumericWithReferenceToEval() {
        var plan = testAnalyzer().coordinatorPlan("""
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
     * {@snippet lang="text":
     * LocalRelation[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, languages{f}#8, last_name{f}#9, salary{f}#10, x
     * {r}#3],EMPTY]
     * }
     */
    public void testMissingFieldInFilterNoProjection() {
        var plan = testAnalyzer().coordinatorPlan("""
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

    /**
     * Test that fields with DataType.NULL from NULLIFY mode are replaced with constant null literals.
     * When unmapped_fields="nullify", the analyzer adds fields with DataType.NULL to EsRelation.
     * The local optimizer's ReplaceFieldWithConstantOrNull rule should replace these with Literal.NULL.
     * {@snippet lang="text":
     * Project[[does_not_exist_field{r}#X]]
     * \_Eval[[null[NULL] AS does_not_exist_field]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][...]
     * }
     */
    public void testNullifyModeFieldReplacedWithNull() {
        assumeTrue("Requires FIX_UNMAPPED_FIELDS_IN_ESRELATION", EsqlCapabilities.Cap.FIX_UNMAPPED_FIELDS_IN_ESRELATION.isEnabled());

        var plan = planWithNullify("from test | keep does_not_exist_field");
        var testStats = statsForMissingField("does_not_exist_field");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("does_not_exist_field"));
        as(projections.get(0), ReferenceAttribute.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("does_not_exist_field"));
        var alias = as(eval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(nullValue()));
        assertThat(literal.dataType(), is(DataType.NULL));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Test that fields with DataType.NULL from NULLIFY mode used in EVAL are correctly replaced.
     * {@snippet lang="text":
     * Project[[x{r}#X]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][...]
     * }
     */
    public void testNullifyModeFieldInEvalReplacedWithNull() {
        assumeTrue("Requires FIX_UNMAPPED_FIELDS_IN_ESRELATION", EsqlCapabilities.Cap.FIX_UNMAPPED_FIELDS_IN_ESRELATION.isEnabled());

        var plan = planWithNullify("""
              from test
            | eval x = does_not_exist_field + 1
            | keep x
            """);

        var testStats = statsForMissingField("does_not_exist_field");
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
     * Test that multiple fields with DataType.NULL are all replaced.
     */
    public void testNullifyModeMultipleFieldsReplacedWithNull() {
        assumeTrue("Requires FIX_UNMAPPED_FIELDS_IN_ESRELATION", EsqlCapabilities.Cap.FIX_UNMAPPED_FIELDS_IN_ESRELATION.isEnabled());

        var plan = planWithNullify("""
              from test
            | eval x = field_a + field_b
            | keep x
            """);

        var testStats = statsForMissingField("field_a", "field_b");
        var localPlan = localPlan(plan, testStats);

        var project = as(localPlan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var eval = as(project.child(), Eval.class);

        var alias = as(eval.fields().get(0), Alias.class);
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.value(), is(nullValue()));
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

        var analyzer = analyzer().addIndex(index).buildAnalyzer();

        var analyzed = analyzer.analyze(TEST_PARSER.parseQuery(query));
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

    public void testIsNullFilterDoesNotPruneDisjunctionBranch() {
        // (nullable IS NOT NULL OR emp_no > 10000) AND nullable IS NULL simplifies to
        // (emp_no > 10000) AND nullable IS NULL — the surviving OR branch must not be pruned.
        var plan = localPlan("""
            FROM test
            | EVAL nullable = languages
            | KEEP emp_no, nullable
            | WHERE nullable IS NOT NULL OR emp_no > 10000
            | WHERE nullable IS NULL
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var conjuncts = Predicates.splitAnd(filter.condition());
        assertThat(conjuncts, hasSize(2));

        var residualBranch = conjuncts.stream()
            .filter(GreaterThan.class::isInstance)
            .map(GreaterThan.class::cast)
            .findFirst()
            .orElseThrow();
        var residualField = as(residualBranch.left(), FieldAttribute.class);
        assertEquals("emp_no", residualField.name());

        var isNull = conjuncts.stream().filter(IsNull.class::isInstance).map(IsNull.class::cast).findFirst().orElseThrow();
        String nullableName = Expressions.name(isNull.field());
        assertTrue(
            "expected nullable field null-check to be preserved",
            "nullable".equals(nullableName) || "languages".equals(nullableName)
        );
        assertThat("local plan should not be pruned to empty", filter.child(), not(instanceOf(LocalRelation.class)));
    }

    public void testIsNullOrDisjunctionWithSeparateWhereClauses() {
        // Two separate WHERE clauses are merged by PushDownAndCombineFilters into the same pattern,
        // so the surviving OR branch must also be preserved when the clauses come from different pipes.
        var plan = localPlan("""
            FROM test
            | WHERE gender IS NOT NULL OR emp_no > 10015
            | WHERE gender IS NULL
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var conjuncts = Predicates.splitAnd(filter.condition());
        assertThat("surviving branch and IS NULL must both be present", conjuncts, hasSize(2));

        assertTrue("expected emp_no GreaterThan conjunct", conjuncts.stream().anyMatch(GreaterThan.class::isInstance));
        assertTrue("expected gender IS NULL conjunct", conjuncts.stream().anyMatch(IsNull.class::isInstance));
        assertThat("local plan should not be pruned to empty", filter.child(), not(instanceOf(LocalRelation.class)));
    }

    public void testIsNullOrDisjunctionWithEvalAlias() {
        // EVAL introduces an alias; PropagateNullable must preserve the surviving OR branch
        // even when the IS NULL targets an alias rather than a direct field.
        var plan = localPlan("""
            FROM test
            | EVAL g = gender
            | KEEP emp_no, g
            | WHERE g IS NOT NULL OR emp_no > 10015
            | WHERE g IS NULL
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var conjuncts = Predicates.splitAnd(filter.condition());
        assertThat("surviving branch and IS NULL must both be present", conjuncts, hasSize(2));
        assertThat("local plan should not be pruned to empty", filter.child(), not(instanceOf(LocalRelation.class)));
    }

    public void testIsNullOrDisjunctionDoesNotPruneToEmptyRelation() {
        // A salary-based surviving branch: (gender IS NOT NULL OR salary > 50000) AND gender IS NULL
        // must keep the salary filter rather than pruning to empty.
        var plan = localPlan("""
            FROM test
            | WHERE (gender IS NOT NULL OR salary > 50000) AND gender IS NULL
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var conjuncts = Predicates.splitAnd(filter.condition());
        assertThat("surviving branch and IS NULL must both be present", conjuncts, hasSize(2));

        assertTrue("expected salary GreaterThan conjunct", conjuncts.stream().anyMatch(GreaterThan.class::isInstance));
        assertThat("local plan should not be pruned to empty", filter.child(), not(instanceOf(LocalRelation.class)));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false]
     * \_Filter[RLIKE(first_name{f}#4, "VALÜ*", true)]
     *   \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     * }
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

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false]
     * \_Filter[RLikeList(first_name{f}#4, "("VALÜ*", "TEST*")", true)]
     *   \_EsRelation[test][_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     * }
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
     * {@snippet lang="text":
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     * }
     */
    public void testReplaceStringCasingAndLikeWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE \"VALÜ*\"");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * {@snippet lang="text":
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     * }
     */
    public void testReplaceStringCasingAndLikeListWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) LIKE (\"VALÜ*\", \"TEST*\")");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * {@snippet lang="text":
     * LocalRelation[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12, langu
     *   ages{f}#6, last_name{f}#7, long_noidx{f}#13, salary{f}#8],EMPTY]
     * }
     */
    public void testReplaceStringCasingAndRLikeListWithLocalRelation() {
        var plan = localPlan("FROM test | WHERE TO_LOWER(TO_UPPER(first_name)) RLIKE (\"VALÜ*\", \"TEST*\")");

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[],[SUM($$integer_long_field$converted_to$long{f$}#5,true[BOOLEAN]) AS sum(integer_long_field::long)#3]]
     *   \_Filter[ISNOTNULL($$integer_long_field$converted_to$long{f$}#5)]
     *     \_EsRelation[test*][!integer_long_field, $$integer_long_field$converted..]
     * }
     */
    public void testUnionTypesInferNonNullAggConstraint() {
        LogicalPlan coordinatorOptimized = optimize(
            analyzerWithUnionTypeMapping().analyze(TEST_PARSER.parseQuery("FROM test* | STATS sum(integer_long_field::long)"))
        );
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
     * {@snippet lang="text":
     * \_Aggregate[[first_name{r}#7, $$first_name$temp_name$17{r}#18],[SUM(salary{f}#11,true[BOOLEAN]) AS SUM(salary)#5, first_nam
     * e{r}#7, first_name{r}#7 AS last_name#10]]
     *   \_Eval[[null[KEYWORD] AS first_name#7, null[KEYWORD] AS $$first_name$temp_name$17#18]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }
     */
    public void testGroupingByMissingFields() {
        var plan = testAnalyzer().coordinatorPlan("FROM test | STATS SUM(salary) BY first_name, last_name");
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
     * {@snippet lang="text":
     * Project[[key{f}#2, int{f}#3, field1{f}#7, field2{f}#8]]
     * \_Join[LEFT,[key{f}#2],[key{f}#6],null]
     *   |_EsRelation[JLfQlKmn][key{f}#2, int{f}#3, field1{f}#4, field2{f}#5]
     *   \_EsRelation[HQtEBOWq][LOOKUP][key{f}#6, field1{f}#7, field2{f}#8]
     * }
     *
     * Output:
     * {@snippet lang="text":
     * Project[[key{r}#2, int{f}#3, field1{r}#7, field1{r}#7 AS field2#8]]
     * \_Eval[[null[KEYWORD] AS key#2, null[INTEGER] AS field1#7]]
     *   \_EsRelation[JLfQlKmn][key{f}#2, int{f}#3, field1{f}#4, field2{f}#5]
     * }
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

    public void testVectorFunctionsNotPushedDownWhenNotIndexed() {
        SimilarityFunctionTestCase testCase = SimilarityFunctionTestCase.random("dense_vector");
        String query = String.format(Locale.ROOT, """
            from test_all
            | eval s = %s
            | sort s desc
            | limit 1
            | keep s
            """, testCase.toQuery());

        LogicalPlan plan = allTypes().searchStats(new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return field.string().equals("dense_vector") == false;
            }
        }).localPlan(query);

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

        LogicalPlan plan = ts().localPlan(query);

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

        LogicalPlan plan = allTypes().searchStats(new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return field.string().equals("dense_vector") == false;
            }
        }).localPlan(query);

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

    public void testFullTextFunctionOnMissingField() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = testAnalyzer().coordinatorPlan(String.format(Locale.ROOT, """
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

        LogicalPlan plan = allTypes().localPlan(query);

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

    public void testFullTextFunctionOnConstantField() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = testAnalyzer().coordinatorPlan(String.format(Locale.ROOT, """
            from test
            | where %s(first_name, "John")
            """, functionName));

        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("first_name")) {
                    return "John";
                }
                return null;
            }
        };
        var localPlan = localPlan(plan, searchStats);

        var limit = as(localPlan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        // The field must remain a FieldAttribute — not replaced with a constant Literal
        assertThat(fullTextFunction.field(), instanceOf(FieldAttribute.class));
        assertThat(Expressions.name(fullTextFunction.field()), equalTo("first_name"));
    }

    public void testConstantFieldReplacedOutsideFullTextFunction() {
        var plan = testAnalyzer().coordinatorPlan("""
            from test
            | where match_phrase(first_name, "John") and last_name == "Doe"
            """);

        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("last_name")) {
                    return "Doe";
                }
                return null;
            }
        };
        var localPlan = localPlan(plan, searchStats);

        var limit = as(localPlan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        // last_name is a constant field with value "Doe", so last_name == "Doe" folds to true
        // and is eliminated from the AND, leaving only the full-text function in the condition.
        // If constant substitution had NOT happened, the condition would still be an And.
        var matchPhrase = as(filter.condition(), SingleFieldFullTextFunction.class);
        assertThat(matchPhrase.field(), instanceOf(FieldAttribute.class));
        assertThat(Expressions.name(matchPhrase.field()), equalTo("first_name"));
    }

    public void testMatchOperatorOnConstantField() {
        var plan = testAnalyzer().coordinatorPlan("""
            from test
            | where first_name : "John"
            """);

        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("first_name")) {
                    return "John";
                }
                return null;
            }
        };
        var localPlan = localPlan(plan, searchStats);

        var limit = as(localPlan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var matchOp = as(filter.condition(), SingleFieldFullTextFunction.class);
        assertThat(matchOp.field(), instanceOf(FieldAttribute.class));
        assertThat(Expressions.name(matchOp.field()), equalTo("first_name"));
    }

    public void testSameConstantFieldProtectedInFullTextAndOutside() {
        var plan = testAnalyzer().coordinatorPlan("""
            from test
            | where match(first_name, "John") and first_name == "John"
            """);

        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("first_name")) {
                    return "John";
                }
                return null;
            }
        };
        var localPlan = localPlan(plan, searchStats);

        var limit = as(localPlan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        // first_name is protected everywhere because it appears in a full-text function;
        // semantic equality in AttributeSet means all references to the same field are kept.
        filter.condition().forEachDown(FieldAttribute.class, fa -> assertThat(Expressions.name(fa), equalTo("first_name")));
        filter.condition().forEachDown(SingleFieldFullTextFunction.class, ftf -> assertThat(ftf.field(), instanceOf(FieldAttribute.class)));
    }

    public void testMultipleFullTextFunctionsOnConstantFields() {
        var plan = testAnalyzer().coordinatorPlan("""
            from test
            | where match(first_name, "John") and match_phrase(last_name, "Doe")
            """);

        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public String constantValue(FieldAttribute.FieldName name) {
                if (name.string().equals("first_name") || name.string().equals("last_name")) {
                    return "constant";
                }
                return null;
            }
        };
        var localPlan = localPlan(plan, searchStats);

        var limit = as(localPlan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        var match = as(and.left(), SingleFieldFullTextFunction.class);
        assertThat(match.field(), instanceOf(FieldAttribute.class));
        assertThat(Expressions.name(match.field()), equalTo("first_name"));
        var matchPhrase = as(and.right(), SingleFieldFullTextFunction.class);
        assertThat(matchPhrase.field(), instanceOf(FieldAttribute.class));
        assertThat(Expressions.name(matchPhrase.field()), equalTo("last_name"));
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

        return analyzer().addIndex(test).buildAnalyzer();
    }

    public static EsRelation relation() {
        return EsqlTestUtils.relation(randomFrom(IndexMode.values()));
    }

    private static Analyzer analyzerWithNullifyMode() {
        return analyzer().unmappedResolution(UnmappedResolution.NULLIFY).addIndex("test", "mapping-basic.json").buildAnalyzer();
    }

    private LogicalPlan planWithNullify(String query) {
        return optimize(analyzerWithNullifyMode().analyze(TEST_PARSER.parseQuery(query)));
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

    public void testTimeSeriesMetadataAttributeNotReplaced() {
        var timeSeriesAttr = new TimeSeriesMetadataAttribute(EMPTY, Set.of());
        var fieldAttr = getFieldAttribute("name");
        var relation = new EsRelation(
            EMPTY,
            "test",
            IndexMode.TIME_SERIES,
            Map.of(),
            Map.of(),
            Map.of("test", IndexMode.TIME_SERIES),
            List.of(fieldAttr, timeSeriesAttr)
        );
        var eval = new Eval(EMPTY, new Limit(EMPTY, L(1000), relation), List.of(new Alias(EMPTY, "ts", timeSeriesAttr)));

        // Simulate missing field stats for the _timeseries field to ensure retention relies on the explicit
        // TimeSeriesMetadataAttribute handling in ReplaceFieldWithConstantOrNull.
        var searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return field.string().equals(fieldAttr.name());
            }
        };
        var localContext = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        var optimizedPlan = new LocalLogicalPlanOptimizer(localContext).localOptimize(eval);

        var optimizedEval = as(optimizedPlan, Eval.class);
        var alias = as(optimizedEval.fields().get(0), Alias.class);
        var optimizedAttr = as(alias.child(), TimeSeriesMetadataAttribute.class);
        assertThat(MetadataAttribute.isTimeSeriesAttribute(optimizedAttr), is(true));
        assertThat(optimizedAttr.withoutFields(), equalTo(Set.of()));
    }
}
