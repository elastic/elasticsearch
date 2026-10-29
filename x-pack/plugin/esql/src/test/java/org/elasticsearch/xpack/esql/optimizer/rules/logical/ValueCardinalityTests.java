/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

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

    public void testKeepAfterEvalKeepsTheProof() {
        String query = "FROM test | EVAL k = MV_MIN(languages) | KEEP k | STATS c = COUNT(*) BY k";
        assertThat(cardinalityOfKey(query, "k"), equalTo(SINGLE));
    }

    public void testRowLiteralIsSingle() {
        assertThat(cardinalityOfKey("ROW x = 1 | STATS c = COUNT(*) BY x", "x"), equalTo(SINGLE));
    }

    public void testRowListIsUnknown() {
        assertThat(cardinalityOfKey("ROW x = [1, 2] | STATS c = COUNT(*) BY x", "x"), equalTo(MAYBE_MULTI));
    }

    public void testEarlierAggregateCountIsSingle() {
        String query = "FROM test | STATS c = COUNT(*) BY first_name | STATS n = COUNT(*) BY c";
        assertThat(cardinalityOfKey(query, "c"), equalTo(SINGLE));
    }

    public void testEarlierAggregateKeyIsSingle() {
        String query = "FROM test | STATS c = COUNT(*) BY languages | STATS n = COUNT(*) BY languages";
        assertThat(cardinalityOfKey(query, "languages"), equalTo(SINGLE));
    }

    public void testEarlierAggregateValuesIsUnknown() {
        String query = "FROM test | STATS v = VALUES(languages) BY first_name | STATS n = COUNT(*) BY v";
        assertThat(cardinalityOfKey(query, "v"), equalTo(MAYBE_MULTI));
    }

    /**
     * A chain longer than the parser allows an expression to be nested answers unknown, and does so without recursing to the end
     * of the chain on the test thread's stack.
     */
    public void testVeryLongAliasChainIsUnknownAndDoesNotOverflow() {
        StringBuilder query = new StringBuilder("FROM test | EVAL a0 = MV_MIN(languages)");
        for (int i = 1; i < DEEP_ALIASES; i++) {
            query.append(", a").append(i).append(" = a").append(i - 1);
        }
        query.append(" | STATS c = COUNT(*) BY a").append(DEEP_ALIASES - 1);
        assertThat(cardinalityOfKey(query.toString(), "a" + (DEEP_ALIASES - 1)), equalTo(MAYBE_MULTI));
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
        var plan = analyzerWithSingleValuedFields(singleValuedFields).query(query);
        var aggregates = plan.collect(Aggregate.class);
        assertThat(aggregates, hasSize(greaterThanOrEqualTo(1)));
        // The last STATS is the one whose key the query asks about; collect lists the outermost first.
        Aggregate aggregate = aggregates.get(0);
        Attribute key = aggregate.child().output().stream().filter(a -> a.name().equals(keyName)).findFirst().orElseThrow();
        return ValueCardinality.of(key, aggregate.child());
    }
}
