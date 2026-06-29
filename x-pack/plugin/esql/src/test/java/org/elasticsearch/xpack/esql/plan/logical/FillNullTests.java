/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;

/**
 * Direct unit coverage for the novel {@link FillNull} machinery (materialize / resolveDefaultValue / defaultForType /
 * expressionsResolved). These exercise the alias-building logic against a synthetic child output without going through the
 * full analyzer, which lets us pin behaviors that are otherwise only reachable indirectly: incremental re-materialization
 * NameId stability, the type-mismatch alias rebuild, and the resolved-aliases guard in expressionsResolved.
 */
public class FillNullTests extends ESTestCase {

    private static LocalRelation childWith(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }

    private static FillNull allFields(List<Attribute> childOutput) {
        return new FillNull(Source.EMPTY, childWith(childOutput), null, List.of());
    }

    private static Alias aliasFor(FillNull fillNull, String name) {
        for (Alias a : fillNull.fields()) {
            if (a.name().equals(name)) {
                return a;
            }
        }
        throw new AssertionError("no fill alias for [" + name + "] in " + fillNull.fields());
    }

    private static Set<String> filledNames(FillNull fillNull) {
        Set<String> names = new HashSet<>();
        for (Alias a : fillNull.fields()) {
            names.add(a.name());
        }
        return names;
    }

    public void testAllFieldsMaterializeFillsSupportedTypesOnly() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        Attribute d = getFieldAttribute("d", DataType.DATETIME); // unsupported default -> skipped
        List<Attribute> output = List.of(i, s, d);

        FillNull materialized = allFields(output).materialize(output);

        assertEquals("only the numeric and string columns should be filled", Set.of("i", "s"), filledNames(materialized));
        assertTrue("the fill alias wraps a COALESCE", aliasFor(materialized, "i").child() instanceof Coalesce);
    }

    public void testIncrementalMaterializePreservesNameIds() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        // A TEXT column's fill alias reports KEYWORD (Coalesce normalizes via noText): it must still be recognized as
        // unchanged and reused, otherwise re-materialization would churn its attribute id.
        Attribute t = getFieldAttribute("t", DataType.TEXT);
        List<Attribute> firstPass = List.of(i, s, t);

        FillNull first = allFields(firstPass).materialize(firstPass);
        NameId iId = aliasFor(first, "i").id();
        NameId sId = aliasFor(first, "s").id();
        NameId tId = aliasFor(first, "t").id();

        // Simulate unmapped_fields="load" appending a fillable column on a later pass.
        Attribute injected = getFieldAttribute("loaded", DataType.LONG);
        List<Attribute> secondPass = List.of(i, s, t, injected);
        FillNull second = first.materialize(secondPass);

        assertEquals("re-materialization must cover the newly injected column", 4, second.fields().size());
        assertEquals("already-filled column [i] must keep its attribute id", iId, aliasFor(second, "i").id());
        assertEquals("already-filled column [s] must keep its attribute id", sId, aliasFor(second, "s").id());
        assertEquals("TEXT column [t] must keep its attribute id across re-materialization", tId, aliasFor(second, "t").id());
        assertSame("the existing alias instance is reused verbatim", aliasFor(first, "i"), aliasFor(second, "i"));
    }

    public void testReMaterializeRebuildsAliasWhenColumnTypeChanges() {
        Attribute asLong = getFieldAttribute("c", DataType.LONG);
        List<Attribute> firstPass = List.of(asLong);
        FillNull first = allFields(firstPass).materialize(firstPass);
        Alias longAlias = aliasFor(first, "c");
        assertEquals(DataType.LONG, longAlias.dataType());

        // The same column name now resolves to a different type: the stale LONG alias must not be reused.
        Attribute asInt = getFieldAttribute("c", DataType.INTEGER);
        FillNull second = first.materialize(List.of(asInt));
        Alias intAlias = aliasFor(second, "c");

        assertEquals("alias type must follow the current column type", DataType.INTEGER, intAlias.dataType());
        assertNotEquals("a type change must produce a fresh alias, not reuse the stale one", longAlias.id(), intAlias.id());
    }

    public void testNullTypedColumnIsSkipped() {
        Attribute nullCol = getFieldAttribute("n", DataType.NULL);
        List<Attribute> output = List.of(nullCol);
        FillNull materialized = allFields(output).materialize(output);
        assertTrue("NULL-typed columns cannot be promoted and must be left unchanged", materialized.fields().isEmpty());
    }

    public void testFillWithNullLiteralIsNoOp() {
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        Literal nullFill = new Literal(Source.EMPTY, null, DataType.KEYWORD);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(s)), nullFill, List.of(s));
        FillNull materialized = fillNull.materialize(List.of(s));
        assertTrue("FILLNULL WITH null must not rewrite any column", materialized.fields().isEmpty());
    }

    public void testAllFieldsOutOfRangeFillIsSilentlySkipped() {
        Attribute intCol = getFieldAttribute("i", DataType.INTEGER);
        Literal outOfRange = new Literal(Source.EMPTY, (long) Integer.MAX_VALUE + 1, DataType.LONG);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(intCol)), outOfRange, List.of());
        FillNull materialized = fillNull.materialize(List.of(intCol));
        assertTrue("an out-of-range fill value must be silently skipped in all-fields mode", materialized.fields().isEmpty());
    }

    public void testAllFieldsInRangeFillConvertsLiteralToColumnType() {
        Attribute intCol = getFieldAttribute("i", DataType.INTEGER);
        Literal inRange = new Literal(Source.EMPTY, 7L, DataType.LONG);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(intCol)), inRange, List.of());
        FillNull materialized = fillNull.materialize(List.of(intCol));
        assertEquals(1, materialized.fields().size());
        assertEquals("the fill literal must be converted to the column type", DataType.INTEGER, aliasFor(materialized, "i").dataType());
    }

    public void testExpressionsResolvedRequiresMaterializedAliases() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull beforeMaterialize = new FillNull(Source.EMPTY, childWith(List.of(i)), null, List.of(i));
        assertFalse("a non-materialized node must report unresolved so ResolveRefs runs", beforeMaterialize.expressionsResolved());

        FillNull materialized = beforeMaterialize.materialize(List.of(i));
        assertTrue("a fully materialized node with resolved aliases is resolved", materialized.expressionsResolved());
    }

    public void testExpressionsResolvedRejectsUnresolvedAlias() {
        // Even with resolved inputs and a non-null fields list, an unresolved alias must keep the node unresolved so the
        // Verifier still validates the Coalesce expressions instead of skipping the subtree.
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Alias unresolved = new Alias(Source.EMPTY, "i", new UnresolvedAttribute(Source.EMPTY, "i"));
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), null, List.of(i), List.of(unresolved));
        assertFalse("an unresolved fill alias must not be reported as resolved", fillNull.expressionsResolved());
    }

    public void testDefaultForTypeCoversNumericStringBoolean() {
        assertEquals(0, ((Literal) FillNull.defaultForType(DataType.INTEGER)).value());
        assertEquals(0L, ((Literal) FillNull.defaultForType(DataType.LONG)).value());
        assertEquals(0.0, ((Literal) FillNull.defaultForType(DataType.DOUBLE)).value());
        assertEquals(false, ((Literal) FillNull.defaultForType(DataType.BOOLEAN)).value());
        assertEquals(DataType.KEYWORD, FillNull.defaultForType(DataType.KEYWORD).dataType());
        assertEquals("a TEXT column takes a KEYWORD default literal", DataType.KEYWORD, FillNull.defaultForType(DataType.TEXT).dataType());
    }

    public void testDefaultForTypeSkipsUnsupportedTypes() {
        assertNull(FillNull.defaultForType(DataType.NULL));
        assertNull(FillNull.defaultForType(DataType.UNSIGNED_LONG));
        assertNull(FillNull.defaultForType(DataType.DATETIME));
        assertNull(FillNull.defaultForType(DataType.IP));
        assertNull(FillNull.defaultForType(DataType.VERSION));
    }
}
