/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.util.List;
import java.util.Map;

public class PhysicalNamesTests extends ESTestCase {

    private static final Map<String, String> RENAMES = Map.of("id", "emp_no", "when", "ts");

    public void testTranslateAndInverseAreOpaqueWholeStringLookups() {
        assertEquals("emp_no", PhysicalNames.translate("id", RENAMES));
        assertEquals("dept", PhysicalNames.translate("dept", RENAMES)); // not renamed -> unchanged
        // Dotted names are opaque: a physical path with dots is not split.
        Map<String, String> dotted = Map.of("city", "user.address.city");
        assertEquals("user.address.city", PhysicalNames.translate("city", dotted));

        Map<String, String> inverse = PhysicalNames.inverse(RENAMES);
        assertEquals("id", inverse.get("emp_no"));
        assertEquals("when", inverse.get("ts"));
        assertEquals(2, inverse.size());
    }

    public void testEmptyRenamesAreNoOps() {
        assertEquals("id", PhysicalNames.translate("id", Map.of()));
        List<String> names = List.of("a", "b");
        assertSame(names, PhysicalNames.translateNames(names, Map.of())); // returns the input unchanged, no copy
        assertTrue(PhysicalNames.inverse(Map.of()).isEmpty());
    }

    public void testTranslateNamesPreservesOrderAndCardinality() {
        assertEquals(List.of("emp_no", "dept", "ts"), PhysicalNames.translateNames(List.of("id", "dept", "when"), RENAMES));
    }

    public void testTranslateSchemaRenamesAndPreservesType() {
        List<Attribute> logical = List.of(ref("id", DataType.LONG), ref("dept", DataType.KEYWORD));
        List<Attribute> physical = PhysicalNames.translateSchema(logical, RENAMES);
        assertEquals("emp_no", physical.get(0).name());
        assertEquals(DataType.LONG, physical.get(0).dataType());
        assertEquals("dept", physical.get(1).name()); // untouched instance
        assertSame(logical.get(1), physical.get(1));
    }

    /**
     * The load-bearing property for the filter surface: physicalize then inverse restores the original attribute
     * identity ({@code NameId}), so the de-physicalized remainder/pushed expressions still resolve against the source
     * output. Only the column names round-trip; the tree shape and attribute ids are preserved.
     */
    public void testExpressionNameRoundTripPreservesNameId() {
        ReferenceAttribute id = ref("id", DataType.LONG);
        Expression predicate = new GreaterThan(Source.EMPTY, id, new Literal(Source.EMPTY, 5, DataType.INTEGER));

        List<Expression> physical = PhysicalNames.translateExpressionNames(List.of(predicate), RENAMES);
        Attribute physicalAttr = (Attribute) ((GreaterThan) physical.get(0)).left();
        assertEquals("emp_no", physicalAttr.name());
        assertEquals(id.id(), physicalAttr.id()); // NameId preserved through the physical rewrite

        List<Expression> backToLogical = PhysicalNames.translateExpressionNames(physical, PhysicalNames.inverse(RENAMES));
        Attribute logicalAttr = (Attribute) ((GreaterThan) backToLogical.get(0)).left();
        assertEquals("id", logicalAttr.name());
        assertEquals(id.id(), logicalAttr.id()); // original identity restored
    }

    public void testNoLogicalNamesRemainInvariant() {
        assertTrue(PhysicalNames.noLogicalNamesRemain(List.of("emp_no", "ts", "dept"), RENAMES));
        // A leaked logical rename-source name trips the invariant.
        assertFalse(PhysicalNames.noLogicalNamesRemain(List.of("emp_no", "id"), RENAMES));
        assertTrue(PhysicalNames.noLogicalNamesRemain(List.of("id"), Map.of())); // no renames -> always holds
    }

    public void testFanOutMoveIsIdentity() {
        // A move/rename: every projected column has a distinct physical, so the plan is identity (no fan-out).
        PhysicalNames.FanOut f = PhysicalNames.fanOut(List.of("id", "ts", "dept"), RENAMES);
        assertTrue(f.isIdentity());
        assertEquals(List.of("id", "ts", "dept"), f.base());
        assertArrayEquals(new int[] { 0, 1, 2 }, f.index());
    }

    public void testFanOutCopyDedupsAndFansOut() {
        // ts kept and @timestamp copied from it: both physicalize to "ts" (via a copy rename), so the reader reads "ts"
        // once (base position 0) and both output columns point at it.
        Map<String, String> copyRenames = Map.of("@timestamp", "ts"); // ts -> ts (identity, kept), @timestamp -> ts
        PhysicalNames.FanOut f = PhysicalNames.fanOut(List.of("ts", "other", "@timestamp"), copyRenames);
        assertFalse(f.isIdentity());
        assertEquals(List.of("ts", "other"), f.base()); // @timestamp deduped away — its physical (ts) is base 0
        assertArrayEquals(new int[] { 0, 1, 0 }, f.index()); // @timestamp fans out from base 0 (ts)
    }

    public void testFanOutMovePlusCopyBaseIsLogicalAndMustPhysicalize() {
        // Column b is MOVED from physical p AND copied to c. base holds the LOGICAL name b (not the physical p), and
        // MUST be run through translateNames before reaching a reader — this is the contract the correctness gate flagged.
        Map<String, String> renames = Map.of("b", "p", "c", "p"); // b -> p (move), c -> p (copy of b)
        PhysicalNames.FanOut f = PhysicalNames.fanOut(List.of("b", "c"), renames);
        assertFalse(f.isIdentity());
        assertEquals(List.of("b"), f.base()); // logical b, deduped (c shares physical p) — NOT physical p
        assertArrayEquals(new int[] { 0, 0 }, f.index());
        assertEquals(List.of("p"), PhysicalNames.translateNames(f.base(), renames)); // physicalizes to p for the reader
    }

    private static ReferenceAttribute ref(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
