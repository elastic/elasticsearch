/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.ValueCardinality.MAYBE_MULTI;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.ValueCardinality.SINGLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@link ValueCardinality} on analyzed (not optimized) plans, so the nodes the proof walks through are the ones the user wrote.
 * Every query ends in a {@code STATS} and asks about one of its grouping keys, as seen in the aggregate's child.
 */
public class ValueCardinalityTests extends AbstractLogicalPlanOptimizerTests {

    private static final int DEEP_ALIASES = 10_000;

    public ValueCardinalityTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testRawIndexFieldIsUnknown() {
        assertThat(cardinalityOfKey("FROM test | STATS c = COUNT(*) BY languages", "languages"), equalTo(MAYBE_MULTI));
    }

    public void testConstantIsSingle() {
        assertThat(cardinalityOfKey("FROM test | EVAL k = 1 | STATS c = COUNT(*) BY k", "k"), equalTo(SINGLE));
    }

    /**
     * An expression that is not folded yet is unknown, and asking about it must not evaluate it: evaluating {@code 1 / 0} would add
     * a warning to the response, which the test framework reports as a failure.
     */
    public void testUnfoldedConstantExpressionIsUnknownAndEmitsNoWarning() {
        assertThat(cardinalityOfKey("FROM test | EVAL k = 1 / 0 | STATS c = COUNT(*) BY k", "k"), equalTo(MAYBE_MULTI));
        assertThat(cardinalityOfKey("FROM test | EVAL k = 1 + 2 | STATS c = COUNT(*) BY k", "k"), equalTo(MAYBE_MULTI));
    }

    /**
     * The optimizer folds constants before the rules that use this class, so a folded constant is a literal and is single-valued.
     */
    public void testFoldedConstantIsSingleOnAnOptimizedPlan() {
        LogicalPlan optimized = optimizedPlan("FROM test | EVAL k = 1 + 2 | STATS c = COUNT(*) BY k");
        assertThat(cardinalityOfKey(optimized, "k"), equalTo(SINGLE));
    }

    public void testMultivalueReducersAreSingle() {
        for (String reducer : new String[] { "MV_MIN", "MV_MAX", "MV_FIRST", "MV_LAST", "MV_COUNT" }) {
            String query = "FROM test | EVAL k = " + reducer + "(languages) | STATS c = COUNT(*) BY k";
            assertThat(reducer, cardinalityOfKey(query, "k"), equalTo(SINGLE));
        }
    }

    public void testArithmeticOnUnknownIsUnknown() {
        assertThat(cardinalityOfKey("FROM test | EVAL k = languages + 1 | STATS c = COUNT(*) BY k", "k"), equalTo(MAYBE_MULTI));
    }

    public void testArithmeticOnSingleIsStillUnknown() {
        // Only the listed reducers are proven; a function of a single value is not assumed single, as SPLIT shows.
        String query = "FROM test | EVAL a = MV_MIN(languages) | EVAL k = a + 1 | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(MAYBE_MULTI));
    }

    public void testAliasOfSingleIsSingle() {
        String query = "FROM test | EVAL a = MV_MAX(languages) | EVAL k = a | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    public void testAliasOfUnknownIsUnknown() {
        assertThat(cardinalityOfKey("FROM test | EVAL k = languages | STATS c = COUNT(*) BY k", "k"), equalTo(MAYBE_MULTI));
    }

    public void testFieldDefinedFromEarlierFieldOfSameEval() {
        String query = "FROM test | EVAL a = MV_MAX(languages), k = a | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    /**
     * The walk searches the fields of an {@code EVAL} or {@code ROW} backwards from the last field it took. These chains skip fields,
     * take several steps in one node, and leave the node after a step in it, where the next node must be searched in full.
     */
    public void testChainThroughFieldsOfTheSameNode() {
        String skipsFields = "FROM test | EVAL a = MV_MIN(languages), b = 1, c = a, d = salary, k = c | STATS n = COUNT(*) BY k";
        assertThat(cardinalityOfKey(skipsFields, "k"), equalTo(SINGLE));

        String endsAtUnknown = "FROM test | EVAL a = languages, b = MV_MIN(languages), c = a, k = c | STATS n = COUNT(*) BY k";
        assertThat(cardinalityOfKey(endsAtUnknown, "k"), equalTo(MAYBE_MULTI));

        String leavesTheEval = "FROM test | EVAL k0 = MV_MIN(languages) | EVAL a = k0, b = 1, k = a | STATS n = COUNT(*) BY k";
        assertThat(cardinalityOfKey(leavesTheEval, "k"), equalTo(SINGLE));

        assertThat(cardinalityOfKey("ROW a = 1, b = [1, 2], c = a, k = c | STATS n = COUNT(*) BY k", "k"), equalTo(SINGLE));
    }

    public void testRenameOfSingleIsSingle() {
        String query = "FROM test | EVAL a = MV_MAX(languages) | RENAME a AS k | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    public void testRenameOfUnknownIsUnknown() {
        assertThat(cardinalityOfKey("FROM test | RENAME languages AS k | STATS c = COUNT(*) BY k", "k"), equalTo(MAYBE_MULTI));
    }

    public void testMvExpandOutputIsSingle() {
        assertThat(cardinalityOfKey("FROM test | MV_EXPAND languages | STATS c = COUNT(*) BY languages", "languages"), equalTo(SINGLE));
    }

    public void testMvExpandDoesNotProveOtherFields() {
        String query = "FROM test | MV_EXPAND languages | STATS c = COUNT(*) BY salary";
        assertThat(cardinalityOfKey(query, "salary"), equalTo(MAYBE_MULTI));
    }

    public void testFilterSortAndLimitKeepTheProof() {
        String query = "FROM test | EVAL k = MV_MIN(languages) | WHERE k > 1 | SORT k | LIMIT 5 | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    public void testSampleAndLimitByKeepTheProof() {
        String query = "FROM test | EVAL k = MV_MIN(languages) | SAMPLE 0.5 | LIMIT 2 BY gender | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    /** DISSECT and GROK add columns. The columns of the child keep their proof, and the added columns are unknown. */
    public void testGeneratingPlanKeepsTheProofOfTheColumnsOfTheChild() {
        String dissect = "FROM test | EVAL k = MV_MIN(languages) | DISSECT first_name \"%{a}\" | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(dissect, "k"), equalTo(SINGLE));
        String grok = "FROM test | EVAL k = MV_MIN(languages) | GROK first_name \"%{WORD:a}\" | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(grok, "k"), equalTo(SINGLE));

        String generated = "FROM test | DISSECT first_name \"%{a}\" | STATS c = COUNT(*) BY a";
        assertThat(cardinalityOfKey(generated, "a"), equalTo(MAYBE_MULTI));
    }

    /** A generated column that has the name of a column of the child replaces it, so the proof of the old column does not apply. */
    public void testGeneratedColumnThatShadowsAColumnOfTheChildIsUnknown() {
        String query = "FROM test | EVAL k = MV_MIN(languages) | DISSECT first_name \"%{k}\" | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(MAYBE_MULTI));
    }

    public void testKeepAfterEvalKeepsTheProof() {
        String query = "FROM test | EVAL k = MV_MIN(languages) | KEEP k | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    /**
     * Any expression can be asked about, not only a column. Here they are the groupings of a {@code STATS}, which read the columns that
     * the child of the {@code STATS} outputs.
     */
    public void testExpressionAtANode() {
        var plan = analyzerWithSingleValuedFields(Set.of()).query("FROM test | STATS c = COUNT(*) BY MV_MIN(languages), salary + 1");
        Aggregate aggregate = plan.collect(Aggregate.class).get(0);
        assertThat(aggregate.groupings(), hasSize(2));
        assertThat(ValueCardinality.ofOutput(aggregate.groupings().get(0), aggregate.child()), equalTo(SINGLE));
        assertThat(ValueCardinality.ofOutput(aggregate.groupings().get(1), aggregate.child()), equalTo(MAYBE_MULTI));
    }

    /**
     * A grouping that is a column of the child, here a field of an earlier {@code EVAL} and a field of the source.
     */
    public void testGroupingsThatAreColumnsOfTheChild() {
        var plan = analyzerWithSingleValuedFields(Set.of()).query(
            "FROM test | EVAL a = MV_MIN(languages) | STATS c = COUNT(*) BY a, salary"
        );
        Aggregate aggregate = plan.collect(Aggregate.class).get(0);
        assertThat(aggregate.groupings(), hasSize(2));
        assertThat(ValueCardinality.ofOutput(aggregate.groupings().get(0), aggregate.child()), equalTo(SINGLE));
        assertThat(ValueCardinality.ofOutput(aggregate.groupings().get(1), aggregate.child()), equalTo(MAYBE_MULTI));
    }

    /**
     * An {@code EVAL} field can read an earlier field of the same {@code EVAL}. That column is not output by the child, so asking about
     * the output of the child cannot find it. The {@code EVAL} itself has to be passed, as in the first assertion. The second
     * assertion shows the answer for the child: unknown.
     */
    public void testEvalFieldReadsAnEarlierFieldOfTheSameEval() {
        var plan = analyzerWithSingleValuedFields(Set.of()).query("FROM test | EVAL a = MV_MIN(languages), k = a");
        Eval eval = plan.collect(Eval.class).get(0);
        Expression definitionOfK = eval.fields().get(1).child();
        assertThat(ValueCardinality.ofOutput(definitionOfK, eval), equalTo(SINGLE));
        assertThat(ValueCardinality.ofOutput(definitionOfK, eval.child()), equalTo(MAYBE_MULTI));
    }

    /**
     * The optimizer turns {@code SORT} followed by {@code LIMIT} into one {@code TopN}, which keeps the proof like the two nodes it
     * replaces. The other tests run on analyzed plans, so this one runs on an optimized plan, where {@code TopN} is what the walk meets.
     */
    public void testTopNKeepsTheProofOnAnOptimizedPlan() {
        LogicalPlan byReducer = optimizedPlan("FROM test | EVAL k = MV_MIN(languages) | SORT k | LIMIT 5 | STATS c = COUNT(*) BY k");
        assertThat("the optimizer made a TopN", byReducer.collect(TopN.class), hasSize(1));
        assertThat(cardinalityOfKey(byReducer, "k"), equalTo(SINGLE));

        String query = "FROM test | SORT languages | LIMIT 10 | STATS c = COUNT(*) BY languages";
        LogicalPlan byMapping = optimize(analyzerWithSingleValuedFields(Set.of("languages")).query(query));
        assertThat("the optimizer made a TopN", byMapping.collect(TopN.class), hasSize(1));
        assertThat(cardinalityOfKey(byMapping, "languages"), equalTo(SINGLE));
    }

    public void testRowLiteralIsSingle() {
        assertThat(cardinalityOfKey("ROW x = 1 | STATS c = COUNT(*) BY x", "x"), equalTo(SINGLE));
    }

    public void testRowListIsUnknown() {
        assertThat(cardinalityOfKey("ROW x = [1, 2] | STATS c = COUNT(*) BY x", "x"), equalTo(MAYBE_MULTI));
    }

    /**
     * A group key has the same id before and after the {@code STATS}, but not the same answer. As the {@code STATS} outputs it, it is
     * one value per group. As the {@code STATS} reads it, it is whatever the source says.
     */
    public void testGroupKeyAsReadAndAsOutputDiffer() {
        var plan = analyzerWithSingleValuedFields(Set.of()).query("FROM test | STATS c = COUNT(*) BY languages");
        Aggregate aggregate = plan.collect(Aggregate.class).get(0);
        Attribute input = aggregate.child().output().stream().filter(a -> a.name().equals("languages")).findFirst().orElseThrow();
        Attribute output = aggregate.output().stream().filter(a -> a.name().equals("languages")).findFirst().orElseThrow();

        assertThat(output.id(), equalTo(input.id()));
        assertThat(ValueCardinality.ofOutput(input, aggregate.child()), equalTo(MAYBE_MULTI));
        assertThat(ValueCardinality.ofOutput(output, aggregate), equalTo(SINGLE));
    }

    public void testEarlierAggregateCountIsSingle() {
        String query = "FROM test | STATS c = COUNT(*) BY first_name | STATS n = COUNT(*) BY c";
        assertThat(cardinalityOfKey(query, "c"), equalTo(SINGLE));
    }

    public void testEarlierAggregateKeyIsSingle() {
        String query = "FROM test | STATS c = COUNT(*) BY languages | STATS n = COUNT(*) BY languages";
        assertThat(cardinalityOfKey(query, "languages"), equalTo(SINGLE));
    }

    /**
     * On an analyzed plan, an output of a {@code STATS} can be an expression over aggregates, or an aggregate with a filter, instead
     * of one aggregate function. Both are unknown, and asking about them must not fail an assertion.
     */
    public void testEarlierAggregateOutputThatIsNotAnAggregateFunctionIsUnknown() {
        String expression = "FROM test | STATS c = COUNT(*) + 1 | STATS n = COUNT(*) BY c";
        assertThat(cardinalityOfKey(expression, "c"), equalTo(MAYBE_MULTI));

        String filtered = "FROM test | STATS c = COUNT(*) WHERE salary > 1 | STATS n = COUNT(*) BY c";
        assertThat(cardinalityOfKey(filtered, "c"), equalTo(MAYBE_MULTI));
    }

    /** A group key that is an expression, or a renamed column, is single-valued in a later {@code STATS} too. */
    public void testEarlierAggregateExpressionOrRenamedKeyIsSingle() {
        String expression = "FROM test | STATS c = COUNT(*) BY k = languages + 1 | STATS n = COUNT(*) BY k";
        assertThat(cardinalityOfKey(expression, "k"), equalTo(SINGLE));

        String renamed = "FROM test | RENAME languages AS l | STATS c = COUNT(*) BY l | STATS n = COUNT(*) BY l";
        assertThat(cardinalityOfKey(renamed, "l"), equalTo(SINGLE));
    }

    public void testEarlierAggregateValuesIsUnknown() {
        String query = "FROM test | STATS v = VALUES(languages) BY first_name | STATS n = COUNT(*) BY v";
        assertThat(cardinalityOfKey(query, "v"), equalTo(MAYBE_MULTI));
    }

    /**
     * Unlike a {@code STATS} key, a dimension key of a time-series {@code STATS} can hold a list. The dimensions are collected per
     * time series with a form of {@code VALUES}, grouped as one packed value, and unpacked by {@code UnpackDims}, so a list in a
     * document stays a list.
     */
    public void testEarlierTimeSeriesAggregateDimensionKeyIsUnknown() {
        String query = "TS k8s | STATS m = MAX(RATE(network.total_bytes_in)) BY cluster | STATS n = COUNT(*) BY cluster";
        for (LogicalPlan plan : List.of(metricsAnalyzer().query(query), planMetrics(query))) {
            assertThat("the key comes out of UnpackDims", plan.collect(UnpackDims.class), hasSize(1));
            assertThat(cardinalityOfKey(plan, "cluster"), equalTo(MAYBE_MULTI));
        }
    }

    /**
     * The walk follows a chain of any length to its end. It is a loop, so a long chain does not overflow the stack.
     * <p>
     * The {@code EVAL} is built in code, not parsed from a query: analyzing a query with this many aliases is quadratic, and takes
     * much longer than the walk.
     */
    public void testVeryLongAliasChainIsFollowedWithoutOverflow() {
        LogicalPlan source = analyzerWithSingleValuedFields(Set.of()).query("FROM test | KEEP languages");
        Attribute languages = source.output().get(0);
        List<Alias> fields = new ArrayList<>(DEEP_ALIASES);
        Alias last = new Alias(Source.EMPTY, "a0", new MvMin(Source.EMPTY, languages));
        fields.add(last);
        for (int i = 1; i < DEEP_ALIASES; i++) {
            last = new Alias(Source.EMPTY, "a" + i, last.toAttribute());
            fields.add(last);
        }
        Eval eval = new Eval(Source.EMPTY, source, fields);
        assertThat(ValueCardinality.ofOutput(last.toAttribute(), eval), equalTo(SINGLE));
    }

    public void testFieldFlaggedByTheMappingIsSingle() {
        String query = "FROM test | STATS c = COUNT(*) BY languages";
        assertThat(cardinalityOfKey(query, "languages", Set.of("languages")), equalTo(SINGLE));
    }

    public void testFieldNotFlaggedByTheMappingIsUnknown() {
        String query = "FROM test | STATS c = COUNT(*) BY salary";
        assertThat(cardinalityOfKey(query, "salary", Set.of("languages")), equalTo(MAYBE_MULTI));
    }

    public void testRenamedFlaggedFieldIsSingle() {
        String query = "FROM test | RENAME languages AS k | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k", Set.of("languages")), equalTo(SINGLE));
    }

    public void testFlaggedFieldKeptThroughKeepAndFilterIsSingle() {
        String query = "FROM test | WHERE languages > 1 | KEEP languages, salary | SORT languages | STATS c = COUNT(*) BY languages";
        assertThat(cardinalityOfKey(query, "languages", Set.of("languages")), equalTo(SINGLE));
    }

    public void testArithmeticOnFlaggedFieldIsStillUnknown() {
        // Only the field is proven; a function of it is not, as with a function of a reducer.
        String query = "FROM test | EVAL k = languages + 1 | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k", Set.of("languages")), equalTo(MAYBE_MULTI));
    }

    /**
     * An analyzer over {@code test}, where the named top level fields carry the single-value guarantee that {@code IndexResolver}
     * gives a field every index guarantees. Only plain {@link EsField} and {@link KeywordEsField} are used by the fields it flags.
     */
    private TestAnalyzer analyzerWithSingleValuedFields(Set<String> singleValuedFields) {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        for (String name : singleValuedFields) {
            mapping.put(name, asSingleValued(mapping.get(name)));
        }
        var index = new EsIndex("test", mapping, Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of());
        return analyzerWithEnrichPolicies().addIndex(index);
    }

    private static EsField asSingleValued(EsField field) {
        if (field.getClass() == KeywordEsField.class) {
            KeywordEsField keyword = (KeywordEsField) field;
            return new KeywordEsField(
                keyword.getName(),
                keyword.getProperties(),
                keyword.isAggregatable(),
                keyword.getPrecision(),
                keyword.getNormalized(),
                keyword.isAlias(),
                keyword.getTimeSeriesFieldType(),
                true
            );
        }
        assertThat(field.getClass(), equalTo(EsField.class));
        return new EsField(
            field.getName(),
            field.getDataType(),
            field.getProperties(),
            field.isAggregatable(),
            field.isAlias(),
            field.getTimeSeriesFieldType(),
            field.isPassthrough(),
            true
        );
    }

    private ValueCardinality cardinalityOfKey(String query, String keyName) {
        return cardinalityOfKey(query, keyName, Set.of());
    }

    private ValueCardinality cardinalityOfKey(String query, String keyName, Set<String> singleValuedFields) {
        return cardinalityOfKey(analyzerWithSingleValuedFields(singleValuedFields).query(query), keyName);
    }

    private ValueCardinality cardinalityOfKey(LogicalPlan plan, String keyName) {
        var aggregates = plan.collect(Aggregate.class);
        assertThat(aggregates, hasSize(greaterThanOrEqualTo(1)));
        // The last STATS is the one whose key the query asks about; collect lists the outermost first.
        Aggregate aggregate = aggregates.get(0);
        Attribute key = aggregate.child().output().stream().filter(a -> a.name().equals(keyName)).findFirst().orElseThrow();
        return ValueCardinality.ofOutput(key, aggregate.child());
    }
}
