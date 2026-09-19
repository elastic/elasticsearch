/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

/**
 * Structural recognition of the multivalue comparison functions for dataset pushdown.
 *
 * <p>These are the shapes the out-of-band request {@code filter} translates into, so what is pinned here is mostly
 * what must <em>not</em> push: a case-insensitive term arrives as {@code mv_contains(TO_LOWER(f), lowered)} and
 * pruning it against original-case statistics would under-match, and a pruned unit cannot be recovered by the
 * retained filter.
 */
public class PushdownPredicatesTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    /** A format that handles the four types these tests use, so a decline is never the type check firing by accident. */
    private static final Predicate<DataType> SUPPORTED = dt -> dt == DataType.KEYWORD
        || dt == DataType.LONG
        || dt == DataType.INTEGER
        || dt == DataType.DATETIME;

    private static final Predicate<DataType> NOTHING_SUPPORTED = dt -> false;

    public void testMvContainsOnPlainField() {
        assertTrue(PushdownPredicates.isMvContains(new MvContains(SRC, field("host", DataType.KEYWORD), keyword("h1")), SUPPORTED));
    }

    public void testMvContainsDeclinesToLowerWrappedField() {
        // The case-insensitive DSL term. The field is a function, not a column: statistics and partition values hold
        // original-case values, so pruning against the lowered literal would under-match.
        MvContains caseInsensitive = new MvContains(
            SRC,
            new ToLower(SRC, field("host", DataType.KEYWORD), TEST_CFG),
            keyword("h1")
        );
        assertFalse(PushdownPredicates.isMvContains(caseInsensitive, SUPPORTED));
    }

    public void testMvContainsDeclinesVirtualColumn() {
        Expression virtual = new ExternalMetadataAttribute(SRC, "_file.name", MetadataAttribute.dataType("_file.name"));
        assertFalse(PushdownPredicates.isMvContains(new MvContains(SRC, virtual, keyword("x")), dt -> true));
    }

    public void testMvContainsDeclinesUnsupportedType() {
        assertFalse(
            PushdownPredicates.isMvContains(new MvContains(SRC, field("host", DataType.KEYWORD), keyword("h1")), NOTHING_SUPPORTED)
        );
    }

    public void testMvContainsDeclinesListLiteral() {
        // Legal, but "contains all of these" is not the equality bound, and the DSL translator never emits it.
        Literal list = new Literal(SRC, List.of(new BytesRef("a"), new BytesRef("b")), DataType.KEYWORD);
        assertFalse(PushdownPredicates.isMvContains(new MvContains(SRC, field("host", DataType.KEYWORD), list), SUPPORTED));
    }

    public void testMvContainsDeclinesNullLiteral() {
        Literal nullLiteral = new Literal(SRC, null, DataType.KEYWORD);
        assertFalse(PushdownPredicates.isMvContains(new MvContains(SRC, field("host", DataType.KEYWORD), nullLiteral), SUPPORTED));
    }

    public void testMvContainsDeclinesMismatchedDateLiteral() {
        // ES|QL reconciles date/date_nanos in the evaluator; a reader would read the raw number in the column's unit.
        Literal nanos = new Literal(SRC, 1_700_000_000_000L, DataType.DATE_NANOS);
        assertFalse(PushdownPredicates.isMvContains(new MvContains(SRC, field("@timestamp", DataType.DATETIME), nanos), SUPPORTED));
    }

    public void testMvIntersectsAcceptsListLiteral() {
        assertTrue(PushdownPredicates.isMvIntersects(new MvIntersects(SRC, field("host", DataType.KEYWORD), list("a", "b")), SUPPORTED));
    }

    public void testMvIntersectsDeclinesAllNullList() {
        Literal allNull = new Literal(SRC, Arrays.asList(null, null), DataType.KEYWORD);
        assertFalse(PushdownPredicates.isMvIntersects(new MvIntersects(SRC, field("host", DataType.KEYWORD), allNull), SUPPORTED));
    }

    public void testMvIntersectsDeclinesToLowerWrappedField() {
        MvIntersects wrapped = new MvIntersects(SRC, new ToLower(SRC, field("host", DataType.KEYWORD), TEST_CFG), list("a", "b"));
        assertFalse(PushdownPredicates.isMvIntersects(wrapped, SUPPORTED));
    }

    public void testMvInRangeOnDateField() {
        assertTrue(PushdownPredicates.isMvInRange(dateRange("@timestamp", 1_000L, 2_000L), SUPPORTED));
    }

    public void testMvInRangeDeclinesMismatchedBound() {
        MvInRange mixed = new MvInRange(
            SRC,
            field("@timestamp", DataType.DATETIME),
            new Literal(SRC, 1_000L, DataType.DATETIME),
            new Literal(SRC, 2_000L, DataType.DATE_NANOS)
        );
        assertFalse(PushdownPredicates.isMvInRange(mixed, SUPPORTED));
    }

    public void testMvInRangeDeclinesNonFoldableBound() {
        MvInRange columnBound = new MvInRange(
            SRC,
            field("@timestamp", DataType.DATETIME),
            new Literal(SRC, 1_000L, DataType.DATETIME),
            field("other", DataType.DATETIME)
        );
        assertFalse(PushdownPredicates.isMvInRange(columnBound, SUPPORTED));
    }

    public void testMvGreaterAndMvLessAreRecognisedByTheSameHelper() {
        Literal bound = new Literal(SRC, 1_000L, DataType.DATETIME);
        assertTrue(PushdownPredicates.isMvCompare(new MvGreater(SRC, field("@timestamp", DataType.DATETIME), bound), SUPPORTED));
        assertTrue(PushdownPredicates.isMvCompare(new MvLess(SRC, field("@timestamp", DataType.DATETIME), bound), SUPPORTED));
    }

    public void testMvCompareDeclinesToLowerWrappedField() {
        MvGreater wrapped = new MvGreater(SRC, new ToLower(SRC, field("host", DataType.KEYWORD), TEST_CFG), keyword("h1"));
        assertFalse(PushdownPredicates.isMvCompare(wrapped, SUPPORTED));
    }

    public void testNestedMvLeafWithMismatchedLiteralIsCaught() {
        // The entry-point walk must see an mv_ leaf under a connective, or a mixed leaf attaches through the back door.
        Literal nanos = new Literal(SRC, 1L, DataType.DATE_NANOS);
        Expression mixedLeaf = new MvContains(SRC, field("@timestamp", DataType.DATETIME), nanos);
        Expression agreeingLeaf = new MvContains(SRC, field("host", DataType.KEYWORD), keyword("h1"));

        assertFalse(PushdownPredicates.allPushdownLiteralsAgree(new And(SRC, agreeingLeaf, mixedLeaf)));
        assertFalse(PushdownPredicates.allPushdownLiteralsAgree(new Or(SRC, agreeingLeaf, mixedLeaf)));
        assertFalse(PushdownPredicates.allPushdownLiteralsAgree(new Not(SRC, mixedLeaf)));
        assertTrue(PushdownPredicates.allPushdownLiteralsAgree(new And(SRC, agreeingLeaf, agreeingLeaf)));
    }

    public void testNestedMvInRangeAndMvCompareLeavesAreWalked() {
        Expression mixedRange = new MvInRange(
            SRC,
            field("@timestamp", DataType.DATETIME),
            new Literal(SRC, 1L, DataType.DATETIME),
            new Literal(SRC, 2L, DataType.DATE_NANOS)
        );
        Expression mixedCompare = new MvGreater(SRC, field("@timestamp", DataType.DATETIME), new Literal(SRC, 1L, DataType.DATE_NANOS));

        assertFalse(PushdownPredicates.allPushdownLiteralsAgree(mixedRange));
        assertFalse(PushdownPredicates.allPushdownLiteralsAgree(mixedCompare));
        assertTrue(PushdownPredicates.allPushdownLiteralsAgree(dateRange("@timestamp", 1L, 2L)));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal keyword(String value) {
        return new Literal(SRC, new BytesRef(value), DataType.KEYWORD);
    }

    private static Literal list(String... values) {
        return new Literal(SRC, Arrays.stream(values).map(BytesRef::new).map(Object.class::cast).toList(), DataType.KEYWORD);
    }

    private static MvInRange dateRange(String name, long lower, long upper) {
        return new MvInRange(
            SRC,
            field(name, DataType.DATETIME),
            new Literal(SRC, lower, DataType.DATETIME),
            new Literal(SRC, upper, DataType.DATETIME)
        );
    }
}
