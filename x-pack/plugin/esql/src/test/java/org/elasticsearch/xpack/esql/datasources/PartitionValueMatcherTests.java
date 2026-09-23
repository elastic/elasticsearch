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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
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

/**
 * The walk's folder matching against the read layer's file matching. The safety claim of listing-time pruning is
 * that a folder the walk drops can hold no file the read layer would keep — a subset relation, not equality: the
 * walk may keep too much (the read layer re-prunes), never too little. {@link #testWalkPruneImpliesReadLayerExclusion}
 * pins that relation over a grid of folder spellings and hints, evaluating the read side through
 * {@link FileSplitProvider#matchesPartitionFilters} with the column typed over the FULL value set (the flat-listing
 * world) and the literal as the analyzer would deliver it (string literals implicitly cast for boolean columns).
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
        Arrays.asList("-1", "42")
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
        hint("k", Operator.IN, "06", "6")
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
                }
            }
        }
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
