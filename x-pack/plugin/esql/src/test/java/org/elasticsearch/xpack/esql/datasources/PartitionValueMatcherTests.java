/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;

/**
 * The walk's folder matching against the read layer's file matching. The safety claim of listing-time pruning is
 * that a folder the walk drops can hold no file the read layer would keep — a subset relation, not equality: the
 * walk may keep too much (the read layer re-prunes), never too little. {@link #testWalkPruneImpliesReadLayerExclusion}
 * pins that relation over a grid of folder spellings and hints, evaluating the read side through
 * {@link FileSplitProvider#matchesPartitionFilters} with the column typed over the FULL value set (the flat-listing
 * world) and the literal as the analyzer would deliver it (string literals implicitly cast for boolean columns).
 * Both layers share this matcher's comparator, so they can agree while both being wrong; each pruned folder is
 * therefore also checked against the engine, by folding the same filter over the folder's value.
 */
public class PartitionValueMatcherTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    // -- the kind guard: a literal of a different kind than the typed folder value must never prune --

    public void testTextLiteralsAgainstBooleanValuesAreUndecidable() {
        // WHERE flag IN ("True", "false"): the analyzer casts these for the read layer; the raw hint must not prune.
        List<String> values = Arrays.asList("True", "False");
        boolean[] keep = PartitionValueMatcher.matchesFolders(values, List.of(hint("flag", Operator.IN, "True", "false")));
        assertTrue(keep[0]);
        assertTrue(keep[1]);

        keep = PartitionValueMatcher.matchesFolders(values, List.of(hint("flag", Operator.EQUALS, "false")));
        assertTrue(keep[0]);
        assertTrue(keep[1]);
    }

    public void testBooleanLiteralAgainstBooleanValuesPrunes() {
        boolean[] keep = PartitionValueMatcher.matchesFolders(
            Arrays.asList("True", "False"),
            List.of(hint("flag", Operator.EQUALS, Boolean.TRUE))
        );
        assertTrue(keep[0]);
        assertFalse(keep[1]);
    }

    public void testNumberLiteralAgainstKeywordValuesIsUndecidable() {
        // The abc sibling types the level keyword; an integer literal against keyword is a verification error in the
        // full-world reading, so the walk must not guess.
        boolean[] keep = PartitionValueMatcher.matchesFolders(Arrays.asList("06", "6", "abc"), List.of(hint("month", Operator.EQUALS, 6)));
        assertTrue(keep[0]);
        assertTrue(keep[1]);
        assertTrue(keep[2]);
    }

    public void testMixedKindInListPrunesOnlyWhenDecidedByMatchingKinds() {
        // IN (true, "false"): the boolean candidate decides flag=True (kept); flag=False is undecidable because the
        // only remaining candidate is text — kept, since the analyzer would cast it and keep the folder.
        boolean[] keep = PartitionValueMatcher.matchesFolders(
            Arrays.asList("True", "False"),
            List.of(hint("flag", Operator.IN, Boolean.TRUE, "false"))
        );
        assertTrue(keep[0]);
        assertTrue(keep[1]);
    }

    public void testNullPartitionIsNeverPruned() {
        boolean[] keep = PartitionValueMatcher.matchesFolders(Arrays.asList("2024", null), List.of(hint("year", Operator.EQUALS, 2025)));
        assertFalse(keep[0]);
        assertTrue(keep[1]);
    }

    // -- signed zero: the engine compares doubles with ==, under which -0.0 and 0.0 are equal --

    public void testNegativeZeroFolderIsNotPrunedByEqualsZero() {
        List<String> values = Arrays.asList("-0e0", "1e5");
        assertEquals(DataType.DOUBLE, HivePartitionDetector.inferType(values));
        boolean[] keep = PartitionValueMatcher.matchesFolders(values, List.of(hint("d", Operator.EQUALS, 0.0)));
        assertTrue("a d=-0e0 folder holds rows that WHERE d == 0.0 keeps", keep[0]);
        assertFalse(keep[1]);
    }

    public void testSignedZeroFolderIsDecidedAsTheEngineDecidesIt() {
        // Either zero folder against either zero literal: kept by every operator that is true for equal values, pruned
        // by every operator that is false for them. Separating the zeros flips both halves.
        for (String zeroFolder : List.of("-0e0", "0e0")) {
            List<String> values = Arrays.asList(zeroFolder, "1e5");
            assertEquals(DataType.DOUBLE, HivePartitionDetector.inferType(values));
            for (double zero : new double[] { 0.0, -0.0 }) {
                for (PartitionFilterHint hint : List.of(
                    hint("d", Operator.EQUALS, zero),
                    hint("d", Operator.GREATER_THAN_OR_EQUAL, zero),
                    hint("d", Operator.LESS_THAN_OR_EQUAL, zero)
                )) {
                    boolean[] keep = PartitionValueMatcher.matchesFolders(values, List.of(hint));
                    assertTrue("d=" + zeroFolder + " holds rows that " + hint + " keeps", keep[0]);
                }
                for (PartitionFilterHint hint : List.of(
                    hint("d", Operator.NOT_EQUALS, zero),
                    hint("d", Operator.GREATER_THAN, zero),
                    hint("d", Operator.LESS_THAN, zero)
                )) {
                    boolean[] keep = PartitionValueMatcher.matchesFolders(values, List.of(hint));
                    assertFalse("d=" + zeroFolder + " holds no rows that " + hint + " keeps", keep[0]);
                }
            }
        }
    }

    public void testSignedZerosCompareEqualAcrossNumericKinds() {
        // A LONG _file.size of 0 against a -0.0 literal takes the same double arm as two doubles.
        List<Number> zeros = List.of(-0.0, 0.0, -0.0f, 0.0f, 0L, 0);
        for (Number a : zeros) {
            for (Number b : zeros) {
                String pair = a + " (" + a.getClass().getSimpleName() + ") vs " + b + " (" + b.getClass().getSimpleName() + ")";
                assertTrue(pair, PartitionValueMatcher.compareEquals(a, b));
                assertEquals(pair, 0, PartitionValueMatcher.compareValues(a, b));
            }
        }
        // Equating the zeros leaves the order around them alone.
        assertThat(PartitionValueMatcher.compareValues(-Double.MIN_VALUE, -0.0), lessThan(0));
        assertThat(PartitionValueMatcher.compareValues(-0.0, Double.MIN_VALUE), lessThan(0));
        assertThat(PartitionValueMatcher.compareValues(0L, Double.MIN_VALUE), lessThan(0));
    }

    // -- differential: walk-pruned implies read-layer-excluded --

    /** Folder-value spellings, including the mixed-type and padded shapes that forced the walk's guards. */
    private static final List<List<String>> VALUE_SETS = List.of(
        Arrays.asList("06", "6"),
        Arrays.asList("2023", "2024", "2025"),
        Arrays.asList("007", "10"),
        Arrays.asList("True", "False"),
        Arrays.asList("true", "false"),
        Arrays.asList("06", "6", "abc"),
        Arrays.asList("6", "true"),
        Arrays.asList("a", "b", "x y"),
        Arrays.asList("6.5", "2.25"),
        Arrays.asList("2024", null),
        Arrays.asList("-1", "42"),
        Arrays.asList("-0e0", "1e5"),
        Arrays.asList("0e0", "-1e5"),
        Arrays.asList("0", "-0e0")
    );

    private static final List<PartitionFilterHint> HINTS = List.of(
        hint("k", Operator.EQUALS, 6),
        hint("k", Operator.EQUALS, "6"),
        hint("k", Operator.EQUALS, "True"),
        hint("k", Operator.EQUALS, "abc"),
        hint("k", Operator.EQUALS, Boolean.TRUE),
        hint("k", Operator.EQUALS, Boolean.FALSE),
        hint("k", Operator.EQUALS, 6.5),
        hint("k", Operator.NOT_EQUALS, 2024),
        hint("k", Operator.NOT_EQUALS, "b"),
        hint("k", Operator.GREATER_THAN_OR_EQUAL, 2024),
        hint("k", Operator.GREATER_THAN, "1"),
        hint("k", Operator.LESS_THAN, 7),
        hint("k", Operator.LESS_THAN_OR_EQUAL, 6.5),
        hint("k", Operator.IN, 2023, 2025),
        hint("k", Operator.IN, "True", "false"),
        hint("k", Operator.IN, Boolean.TRUE, Boolean.FALSE),
        hint("k", Operator.IN, "06", "6"),
        hint("k", Operator.EQUALS, 0.0),
        hint("k", Operator.EQUALS, -0.0),
        hint("k", Operator.NOT_EQUALS, 0.0),
        hint("k", Operator.GREATER_THAN, -0.0),
        hint("k", Operator.GREATER_THAN_OR_EQUAL, 0.0),
        hint("k", Operator.LESS_THAN, 0.0),
        hint("k", Operator.LESS_THAN_OR_EQUAL, -0.0)
    );

    public void testWalkPruneImpliesReadLayerExclusion() {
        for (List<String> values : VALUE_SETS) {
            DataType fullType = HivePartitionDetector.inferType(values);
            for (PartitionFilterHint hint : HINTS) {
                boolean[] keep = PartitionValueMatcher.matchesFolders(values, List.of(hint));
                for (int i = 0; i < values.size(); i++) {
                    if (keep[i]) {
                        continue; // keeping too much is always safe — the read layer re-prunes per file
                    }
                    String raw = values.get(i);
                    String description = "values=" + values + " hint=" + hint + " folder=" + raw;
                    assertNotNull("a NULL partition must never be pruned: " + description, raw);
                    Object typed = HivePartitionDetector.castValue(raw, fullType);
                    Expression readFilter = readLayerFilter(hint, fullType);
                    assertNotNull("walk pruned a folder the full-world query cannot even evaluate: " + description, readFilter);
                    assertFalse(
                        "walk pruned a folder whose files the read layer keeps: " + description,
                        FileSplitProvider.matchesPartitionFilters(Map.of("k", typed), List.of(readFilter))
                    );
                    // The read layer shares the walk's comparator, so the two agree even when both are wrong. The
                    // engine's own answer is the one the pruned rows would have been filtered by.
                    assertNotEquals(
                        "walk pruned a folder whose rows the engine keeps: " + description,
                        Boolean.TRUE,
                        engineAnswer(readFilter, typed, fullType)
                    );
                }
            }
        }
    }

    public void testReadLayerNeverExcludesAFileTheEngineKeeps() {
        for (List<String> values : VALUE_SETS) {
            DataType fullType = HivePartitionDetector.inferType(values);
            for (PartitionFilterHint hint : HINTS) {
                Expression readFilter = readLayerFilter(hint, fullType);
                if (readFilter == null) {
                    continue; // the full-world query fails analysis; there is no engine answer to agree with
                }
                // The negation is where a wrong equality turns into a wrong exclusion for !=, < and >.
                for (Expression filter : List.of(readFilter, new Not(SRC, readFilter))) {
                    for (String raw : values) {
                        if (raw == null) {
                            continue;
                        }
                        Object typed = HivePartitionDetector.castValue(raw, fullType);
                        if (Boolean.FALSE.equals(FileSplitProvider.evaluateFilter(filter, Map.of("k", typed)))) {
                            assertNotEquals(
                                "read layer excluded a file whose rows the engine keeps: values="
                                    + values
                                    + " filter="
                                    + filter.nodeString()
                                    + " folder="
                                    + raw,
                                Boolean.TRUE,
                                engineAnswer(filter, typed, fullType)
                            );
                        }
                    }
                }
            }
        }
    }

    /** What the compute engine answers for {@code filter} on a row whose {@code k} holds {@code value}. */
    private static Object engineAnswer(Expression filter, Object value, DataType columnType) {
        Literal row = new Literal(SRC, columnType == DataType.KEYWORD ? new BytesRef((String) value) : value, columnType);
        return filter.transformUp(FieldAttribute.class, column -> row).fold(FoldContext.small());
    }

    /**
     * The filter as the read layer sees it after analysis: string literals implicitly cast for a boolean column,
     * keyword literals as {@code BytesRef}. Returns {@code null} when analysis would reject the comparison (kind
     * mismatch with no implicit cast, or an uncastable literal) — the full-world query errors, so a pruned folder
     * cannot be justified by it.
     */
    private static Expression readLayerFilter(PartitionFilterHint hint, DataType columnType) {
        FieldAttribute column = new FieldAttribute(
            SRC,
            "k",
            new EsField("k", columnType, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        if (hint.operator() == Operator.IN) {
            List<Expression> candidates = new ArrayList<>();
            for (Object value : hint.values()) {
                Literal literal = readLayerLiteral(value, columnType);
                if (literal == null) {
                    return null;
                }
                candidates.add(literal);
            }
            return new In(SRC, column, candidates);
        }
        Literal literal = readLayerLiteral(hint.values().get(0), columnType);
        if (literal == null) {
            return null;
        }
        return switch (hint.operator()) {
            case EQUALS -> new Equals(SRC, column, literal);
            case NOT_EQUALS -> new NotEquals(SRC, column, literal, null);
            case GREATER_THAN -> new GreaterThan(SRC, column, literal, null);
            case GREATER_THAN_OR_EQUAL -> new GreaterThanOrEqual(SRC, column, literal, null);
            case LESS_THAN -> new LessThan(SRC, column, literal, null);
            case LESS_THAN_OR_EQUAL -> new LessThanOrEqual(SRC, column, literal, null);
            case IN -> throw new AssertionError("handled above");
        };
    }

    private static Literal readLayerLiteral(Object value, DataType columnType) {
        if (value instanceof String s) {
            if (columnType == DataType.BOOLEAN) {
                // the analyzer's implicit string cast for boolean columns; an unparseable value fails analysis
                if (s.equalsIgnoreCase("true")) {
                    return new Literal(SRC, Boolean.TRUE, DataType.BOOLEAN);
                }
                if (s.equalsIgnoreCase("false")) {
                    return new Literal(SRC, Boolean.FALSE, DataType.BOOLEAN);
                }
                return null;
            }
            if (columnType == DataType.KEYWORD) {
                return new Literal(SRC, new BytesRef(s), DataType.KEYWORD);
            }
            return null; // text vs numeric column: verification error, no implicit cast
        }
        if (value instanceof Boolean b) {
            return columnType == DataType.BOOLEAN ? new Literal(SRC, b, DataType.BOOLEAN) : null;
        }
        if (value instanceof Number n) {
            if (columnType == DataType.INTEGER
                || columnType == DataType.LONG
                || columnType == DataType.UNSIGNED_LONG
                || columnType == DataType.DOUBLE) {
                DataType literalType = n instanceof Integer ? DataType.INTEGER : n instanceof Long ? DataType.LONG : DataType.DOUBLE;
                return new Literal(SRC, n, literalType);
            }
            return null; // numeric vs keyword/boolean column: verification error
        }
        return null;
    }

    private static PartitionFilterHint hint(String column, Operator operator, Object... values) {
        return new PartitionFilterHint(column, operator, List.of(values));
    }
}
