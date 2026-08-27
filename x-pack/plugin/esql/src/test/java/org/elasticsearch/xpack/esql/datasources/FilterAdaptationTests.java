/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterAdaptationTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testAdaptFilter_leafComparison_missingColumn() {
        Expression filter = gt("price", 100);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("name", "id"));
        assertTrue("Comparison on missing column should be removed (entire AND is false)", result.isEmpty());
    }

    public void testAdaptFilter_leafComparison_presentColumn() {
        Expression filter = gt("price", 100);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price", "name"));
        assertEquals("Comparison on present column should be unchanged", 1, result.size());
        assertSame(filter, result.get(0));
    }

    public void testAdaptFilter_isNull_missingColumn() {
        Expression filter = new IsNull(SRC, fieldAttr("price"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("name"));
        assertTrue("IS NULL on missing column is TRUE — conjunct removed (always true)", result.isEmpty());
    }

    public void testAdaptFilter_isNotNull_missingColumn() {
        Expression filter = new IsNotNull(SRC, fieldAttr("price"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("name"));
        assertTrue("IS NOT NULL on missing column is FALSE — entire AND is false", result.isEmpty());
    }

    public void testAdaptFilter_and_oneMissing() {
        // price > 100 AND name = 'x' — price is missing
        Expression and = new And(SRC, gt("price", 100), eq("name", "x"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(and), Set.of("name"));
        // price > 100 is UNKNOWN → AND(UNKNOWN, X) = UNKNOWN → entire conjunct is false
        assertTrue("AND with one missing-column child should be removed", result.isEmpty());
    }

    public void testAdaptFilter_and_bothMissing() {
        Expression and = new And(SRC, gt("price", 100), gt("quantity", 5));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(and), Set.of("name"));
        assertTrue("AND with both children missing should be removed", result.isEmpty());
    }

    public void testAdaptFilter_or_oneMissing() {
        // price > 100 OR name = 'x' — price is missing
        Expression or = new Or(SRC, gt("price", 100), eq("name", "x"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(or), Set.of("name"));
        assertEquals("OR with one missing child should keep the other side", 1, result.size());
        assertThat(result.get(0), org.hamcrest.Matchers.instanceOf(Equals.class));
    }

    public void testAdaptFilter_or_isNullMissing() {
        // price IS NULL OR name = 'x' — price is missing → IS NULL is TRUE → OR(TRUE, X) = TRUE
        Expression or = new Or(SRC, new IsNull(SRC, fieldAttr("price")), eq("name", "x"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(or), Set.of("name"));
        assertTrue("OR with IS NULL on missing column → TRUE → entire conjunct is always true", result.isEmpty());
    }

    public void testAdaptFilter_not_missingColumn() {
        // NOT(price > 100) — price is missing → NOT(UNKNOWN) = UNKNOWN → FALSE
        Expression not = new Not(SRC, gt("price", 100));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(not), Set.of("name"));
        // NOT(null) → TRUE_SENTINEL (always true), so it should be removed from conjuncts
        assertTrue("NOT on missing column comparison should be removed (always true)", result.isEmpty());
    }

    public void testAdaptFilter_allPresent_unchanged() {
        Expression f1 = gt("price", 100);
        Expression f2 = eq("name", "x");
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(f1, f2), Set.of("price", "name"));
        assertEquals(2, result.size());
        assertSame(f1, result.get(0));
        assertSame(f2, result.get(1));
    }

    public void testAdaptFilter_multipleConjuncts_mixedMissing() {
        // [price > 100, name = 'x'] — price is missing
        Expression f1 = gt("price", 100);
        Expression f2 = eq("name", "x");
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(f1, f2), Set.of("name"));
        // price > 100 on missing column → null → entire AND is false → empty
        assertTrue("Any conjunct on missing column makes entire AND false", result.isEmpty());
    }

    public void testAdaptFilter_inExpression_missingColumn() {
        Expression filter = new In(SRC, fieldAttr("status"), List.of(intLiteral(1), intLiteral(2)));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("name"));
        assertTrue("IN on missing column should make entire AND false", result.isEmpty());
    }

    public void testAdaptFilter_or_bothMissing() {
        // price > 100 OR quantity > 5 — both missing
        Expression or = new Or(SRC, gt("price", 100), gt("quantity", 5));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(or), Set.of("name"));
        assertTrue("OR with both sides missing should be removed (entire AND is false)", result.isEmpty());
    }

    public void testAdaptFilter_nestedAndOr() {
        // AND(OR(price > 100, name = 'x'), id = 1) — price is missing, name and id present
        Expression or = new Or(SRC, gt("price", 100), eq("name", "x"));
        Expression and = new And(SRC, or, eq("id", "1"));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(and), Set.of("name", "id"));
        assertEquals("Nested AND(OR(missing, present), present) should simplify", 1, result.size());
        assertThat(result.get(0), org.hamcrest.Matchers.instanceOf(And.class));
    }

    public void testAdaptFilter_notIsNull_missingColumn() {
        // NOT(IS NULL price) — price is missing → IS NULL is TRUE → NOT(TRUE) → FALSE
        Expression not = new Not(SRC, new IsNull(SRC, fieldAttr("price")));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(not), Set.of("name"));
        assertTrue("NOT(IS NULL missing) should be unsatisfiable", result.isEmpty());
    }

    public void testAdaptFilter_notIsNotNull_missingColumn() {
        // NOT(IS NOT NULL price) — price is missing → IS NOT NULL is FALSE → NOT(FALSE) → TRUE
        Expression not = new Not(SRC, new IsNotNull(SRC, fieldAttr("price")));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(not), Set.of("name"));
        assertTrue("NOT(IS NOT NULL missing) should be always true (removed from conjuncts)", result.isEmpty());
    }

    public void testAdaptFilter_emptyConjuncts() {
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(), Set.of("name"));
        assertTrue("Empty conjuncts should return empty list", result.isEmpty());
    }

    // -- type adaptation tests --

    public void testAdaptType_longToInt_literalFits() {
        Expression filter = gtLong("price", 100L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertEquals(1, result.size());
        assertThat(result.get(0), org.hamcrest.Matchers.instanceOf(GreaterThan.class));
        GreaterThan adapted = (GreaterThan) result.get(0);
        assertEquals(DataType.INTEGER, ((FieldAttribute) adapted.left()).dataType());
        assertEquals(100, ((Literal) adapted.right()).value());
        assertEquals(DataType.INTEGER, adapted.right().dataType());
    }

    public void testAdaptType_longToInt_overflowHigh_greaterThan() {
        Expression filter = gtLong("price", 3_000_000_000L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price > 3B with INT32 column → always FALSE → empty", result.isEmpty());
    }

    public void testAdaptType_longToInt_overflowHigh_lessThan() {
        Expression filter = ltLong("price", 3_000_000_000L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price < 3B with INT32 column → always TRUE → removed from conjuncts", result.isEmpty());
    }

    public void testAdaptType_longToInt_overflowHigh_equals() {
        Expression filter = eqLong("price", 3_000_000_000L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price = 3B with INT32 column → always FALSE → empty", result.isEmpty());
    }

    public void testAdaptType_longToInt_underflowLow_greaterThan() {
        Expression filter = gtLong("price", -3_000_000_000L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price > -3B with INT32 column → always TRUE → removed from conjuncts", result.isEmpty());
    }

    public void testAdaptType_longToInt_underflowLow_lessThan() {
        Expression filter = ltLong("price", -3_000_000_000L);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price < -3B with INT32 column → always FALSE → empty", result.isEmpty());
    }

    public void testAdaptType_noTypeChange_unchanged() {
        Expression filter = gt("price", 100);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of());
        assertEquals(1, result.size());
        assertSame("No type info → expression unchanged", filter, result.get(0));
    }

    public void testAdaptType_longToInt_intBoundaryMax() {
        Expression filter = eqLong("price", (long) Integer.MAX_VALUE);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertEquals(1, result.size());
        Equals adapted = (Equals) result.get(0);
        assertEquals(Integer.MAX_VALUE, ((Literal) adapted.right()).value());
        assertEquals(DataType.INTEGER, adapted.right().dataType());
    }

    public void testAdaptType_longToInt_intBoundaryMin() {
        Expression filter = eqLong("price", (long) Integer.MIN_VALUE);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertEquals(1, result.size());
        Equals adapted = (Equals) result.get(0);
        assertEquals(Integer.MIN_VALUE, ((Literal) adapted.right()).value());
        assertEquals(DataType.INTEGER, adapted.right().dataType());
    }

    public void testAdaptType_longToInt_justAboveMax() {
        Expression filter = eqLong("price", (long) Integer.MAX_VALUE + 1);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("Literal just above Integer.MAX_VALUE → overflow → FALSE for equals", result.isEmpty());
    }

    public void testAdaptType_longToInt_overflowHigh_greaterThanOrEqual() {
        Expression filter = new GreaterThanOrEqual(SRC, longFieldAttr("price"), longLiteral(3_000_000_000L), null);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price >= 3B with INT32 → always FALSE → empty", result.isEmpty());
    }

    public void testAdaptType_longToInt_overflowHigh_lessThanOrEqual() {
        Expression filter = new LessThanOrEqual(SRC, longFieldAttr("price"), longLiteral(3_000_000_000L), null);
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price <= 3B with INT32 → always TRUE → removed from conjuncts", result.isEmpty());
    }

    public void testAdaptType_longToInt_overflowHigh_notEquals() {
        Expression filter = new NotEquals(SRC, longFieldAttr("price"), longLiteral(3_000_000_000L));
        List<Expression> result = FilterAdaptation.adaptFilterForFile(List.of(filter), Set.of("price"), Map.of("price", DataType.INTEGER));
        assertTrue("price != 3B with INT32 → always TRUE → removed from conjuncts", result.isEmpty());
    }

    // -- helpers --

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Expression gt(String column, int value) {
        return new GreaterThan(SRC, fieldAttr(column), intLiteral(value), null);
    }

    private static Expression eq(String column, String value) {
        return new Equals(SRC, fieldAttr(column), new Literal(SRC, new org.apache.lucene.util.BytesRef(value), DataType.KEYWORD));
    }

    private static FieldAttribute longFieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal longLiteral(long value) {
        return new Literal(SRC, value, DataType.LONG);
    }

    private static Expression gtLong(String column, long value) {
        return new GreaterThan(SRC, longFieldAttr(column), longLiteral(value), null);
    }

    private static Expression ltLong(String column, long value) {
        return new LessThan(SRC, longFieldAttr(column), longLiteral(value), null);
    }

    private static Expression eqLong(String column, long value) {
        return new Equals(SRC, longFieldAttr(column), longLiteral(value));
    }
}
