/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;

/**
 * Direct unit coverage for the {@link FillNull} alias machinery (alias derivation / resolveDefaultValue / defaultForType /
 * expressionsResolved / unfillableTargets) against a synthetic child output, pinning behaviors only reachable indirectly
 * through the analyzer.
 */
public class FillNullTests extends ESTestCase {

    private static LocalRelation childWith(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }

    private static FillNull allFields(List<Attribute> childOutput) {
        return new FillNull(Source.EMPTY, childWith(childOutput), null, List.of());
    }

    private static Literal keyword(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
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

    private static Set<String> unfillableNames(FillNull fillNull) {
        Set<String> names = new HashSet<>();
        for (Attribute a : fillNull.unfillableTargets()) {
            names.add(a.name());
        }
        return names;
    }

    private static Configuration cfgWithZone(String zone) {
        return TEST_CFG.withSetting(QuerySettings.TIME_ZONE, ZoneId.of(zone));
    }

    /** The converted fill literal a derived alias will COALESCE the column with, looking through any TO_TEXT wrapper. */
    private static Literal fillLiteralFor(FillNull fillNull, String name) {
        Expression fill = aliasFor(fillNull, name).child();
        Coalesce coalesce = (Coalesce) (fill instanceof ToText toText ? toText.field() : fill);
        return (Literal) coalesce.children().get(1);
    }

    /**
     * Derives the fill aliases against {@code childOutput}, the way the analyzer does: re-child the node, then supply
     * the {@link Configuration}. {@code FillNull} has no public materialize step - the aliases are a pure function of
     * the child output, re-derived by every rebuild.
     */
    private static FillNull materialize(FillNull fillNull, List<Attribute> childOutput, Configuration configuration) {
        return fillNull.replaceChild(childWith(childOutput)).withConfiguration(configuration);
    }

    public void testAllFieldsMaterializeFillsSupportedTypesOnly() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        Attribute d = getFieldAttribute("d", DataType.DATETIME); // unsupported default -> skipped
        List<Attribute> output = List.of(i, s, d);

        FillNull materialized = materialize(allFields(output), output, TEST_CFG);

        assertEquals("only the numeric and string columns should be filled", Set.of("i", "s"), filledNames(materialized));
        assertTrue("the fill alias wraps a COALESCE", aliasFor(materialized, "i").child() instanceof Coalesce);
    }

    public void testIncrementalMaterializePreservesNameIds() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        // A TEXT column keeps TEXT (the COALESCE is wrapped in TO_TEXT); its alias must still be reused.
        Attribute t = getFieldAttribute("t", DataType.TEXT);
        List<Attribute> firstPass = List.of(i, s, t);

        FillNull first = materialize(allFields(firstPass), firstPass, TEST_CFG);
        NameId iId = aliasFor(first, "i").id();
        NameId sId = aliasFor(first, "s").id();
        NameId tId = aliasFor(first, "t").id();

        // Simulate unmapped_fields="load" appending a fillable column on a later pass.
        Attribute injected = getFieldAttribute("loaded", DataType.LONG);
        List<Attribute> secondPass = List.of(i, s, t, injected);
        FillNull second = materialize(first, secondPass, TEST_CFG);

        assertEquals("re-materialization must cover the newly injected column", 4, second.fields().size());
        assertEquals("already-filled column [i] must keep its attribute id", iId, aliasFor(second, "i").id());
        assertEquals("already-filled column [s] must keep its attribute id", sId, aliasFor(second, "s").id());
        assertEquals("TEXT column [t] must keep its attribute id across re-derivation", tId, aliasFor(second, "t").id());
        assertSame("the existing alias instance is reused verbatim", aliasFor(first, "i"), aliasFor(second, "i"));
    }

    public void testReMaterializeRebuildsAliasWhenColumnTypeChanges() {
        Attribute asLong = getFieldAttribute("c", DataType.LONG);
        List<Attribute> firstPass = List.of(asLong);
        FillNull first = materialize(allFields(firstPass), firstPass, TEST_CFG);
        Alias longAlias = aliasFor(first, "c");
        assertEquals(DataType.LONG, longAlias.dataType());

        // The same column name now resolves to a different type: the stale LONG alias must not be reused.
        Attribute asInt = getFieldAttribute("c", DataType.INTEGER);
        FillNull second = materialize(first, List.of(asInt), TEST_CFG);
        Alias intAlias = aliasFor(second, "c");

        assertEquals("alias type must follow the current column type", DataType.INTEGER, intAlias.dataType());
        assertNotEquals("a type change must produce a fresh alias, not reuse the stale one", longAlias.id(), intAlias.id());
    }

    /**
     * FILLNULL changes a column's null positions, never its type. {@link Coalesce} normalizes its own type to
     * {@code keyword}, so a {@code text} column would be silently re-typed without the {@code TO_TEXT} wrapper.
     */
    public void testFillingATextColumnKeepsItText() {
        Attribute t = getFieldAttribute("t", DataType.TEXT);
        Attribute k = getFieldAttribute("k", DataType.KEYWORD);
        List<Attribute> output = List.of(t, k);

        FillNull materialized = materialize(allFields(output), output, TEST_CFG);
        assertEquals("a text column must stay text", DataType.TEXT, aliasFor(materialized, "t").dataType());
        assertEquals("a keyword column stays keyword", DataType.KEYWORD, aliasFor(materialized, "k").dataType());
        assertTrue("the text fill is wrapped in TO_TEXT", aliasFor(materialized, "t").child() instanceof ToText);
        assertTrue("the keyword fill is not wrapped", aliasFor(materialized, "k").child() instanceof Coalesce);
        // The output schema the rest of the plan sees must carry the original types too.
        assertEquals(DataType.TEXT, materialized.output().get(0).dataType());
        assertEquals(DataType.KEYWORD, materialized.output().get(1).dataType());
    }

    public void testNullTypedColumnIsSkipped() {
        Attribute nullCol = getFieldAttribute("n", DataType.NULL);
        List<Attribute> output = List.of(nullCol);
        FillNull materialized = materialize(allFields(output), output, TEST_CFG);
        assertTrue("NULL-typed columns cannot be promoted and must be left unchanged", materialized.fields().isEmpty());
    }

    public void testFillWithNullLiteralIsNoOp() {
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        Literal nullFill = new Literal(Source.EMPTY, null, DataType.KEYWORD);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(s)), nullFill, List.of(s));
        FillNull materialized = materialize(fillNull, List.of(s), TEST_CFG);
        assertTrue("FILLNULL WITH null must not rewrite any column", materialized.fields().isEmpty());
    }

    public void testAllFieldsOutOfRangeFillIsSkipped() {
        Attribute intCol = getFieldAttribute("i", DataType.INTEGER);
        Literal outOfRange = new Literal(Source.EMPTY, (long) Integer.MAX_VALUE + 1, DataType.LONG);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(intCol)), outOfRange, List.of());
        FillNull materialized = materialize(fillNull, List.of(intCol), TEST_CFG);
        assertTrue("an out-of-range fill value must not be applied", materialized.fields().isEmpty());
        assertEquals("and the column is reported", Set.of("i"), unfillableNames(materialized));
    }

    public void testAllFieldsInRangeFillConvertsLiteralToColumnType() {
        Attribute intCol = getFieldAttribute("i", DataType.INTEGER);
        Literal inRange = new Literal(Source.EMPTY, 7L, DataType.LONG);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(intCol)), inRange, List.of());
        FillNull materialized = materialize(fillNull, List.of(intCol), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals("the fill literal must be converted to the column type", DataType.INTEGER, aliasFor(materialized, "i").dataType());
    }

    public void testStringFillIsImplicitlyCastToDatetime() {
        Attribute d = getFieldAttribute("d", DataType.DATETIME);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("2025-04-11T00:00:00.000Z"), List.of(d));
        FillNull materialized = materialize(fillNull, List.of(d), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals("a string fill must be cast to the DATETIME column type", DataType.DATETIME, aliasFor(materialized, "d").dataType());
    }

    public void testStringFillIsImplicitlyCastToDateNanos() {
        Attribute d = getFieldAttribute("d", DataType.DATE_NANOS);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("2025-04-11T00:00:00.000Z"), List.of(d));
        FillNull materialized = materialize(fillNull, List.of(d), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals(
            "a string fill must be cast to the DATE_NANOS column type",
            DataType.DATE_NANOS,
            aliasFor(materialized, "d").dataType()
        );
    }

    public void testStringFillIsImplicitlyCastToBoolean() {
        Attribute b = getFieldAttribute("b", DataType.BOOLEAN);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(b)), keyword("true"), List.of(b));
        FillNull materialized = materialize(fillNull, List.of(b), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals("a string fill must be cast to the BOOLEAN column type", DataType.BOOLEAN, aliasFor(materialized, "b").dataType());
    }

    public void testStringFillIsImplicitlyCastToIp() {
        Attribute ip = getFieldAttribute("addr", DataType.IP);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(ip)), keyword("1.2.3.4"), List.of(ip));
        FillNull materialized = materialize(fillNull, List.of(ip), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals("a string fill must be cast to the IP column type", DataType.IP, aliasFor(materialized, "addr").dataType());
    }

    public void testStringFillIsImplicitlyCastToVersion() {
        Attribute v = getFieldAttribute("ver", DataType.VERSION);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(v)), keyword("1.2.3"), List.of(v));
        FillNull materialized = materialize(fillNull, List.of(v), TEST_CFG);
        assertEquals(1, materialized.fields().size());
        assertEquals("a string fill must be cast to the VERSION column type", DataType.VERSION, aliasFor(materialized, "ver").dataType());
    }

    public void testUnparsableStringDateFillIsSkipped() {
        Attribute d = getFieldAttribute("d", DataType.DATETIME);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("not-a-date"), List.of(d));
        FillNull materialized = materialize(fillNull, List.of(d), TEST_CFG);
        assertTrue("an unparsable date fill must not produce an alias", materialized.fields().isEmpty());
    }

    public void testUnfillableTargetsForExplicitDateFieldWithoutWith() {
        Attribute d = getFieldAttribute("d", DataType.DATETIME);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), null, List.of(d));
        FillNull materialized = materialize(fillNull, List.of(d), TEST_CFG);
        assertEquals("a targeted date field without WITH is un-fillable", Set.of("d"), unfillableNames(materialized));
    }

    public void testUnfillableTargetsForAllFieldsFormListsUnfillableColumns() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute d = getFieldAttribute("d", DataType.DATETIME);
        Attribute ip = getFieldAttribute("addr", DataType.IP);
        List<Attribute> output = List.of(i, d, ip);
        FillNull materialized = materialize(allFields(output), output, TEST_CFG);
        assertEquals("only the un-fillable columns are reported", Set.of("d", "addr"), unfillableNames(materialized));
    }

    public void testUnfillableTargetsForNullTypedField() {
        Attribute n = getFieldAttribute("n", DataType.NULL);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(n)), null, List.of(n));
        FillNull materialized = materialize(fillNull, List.of(n), TEST_CFG);
        assertEquals("a NULL-typed targeted field is un-fillable", Set.of("n"), unfillableNames(materialized));
    }

    public void testFillableTargetHasNoUnfillableEntry() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), null, List.of(i));
        FillNull materialized = materialize(fillNull, List.of(i), TEST_CFG);
        assertTrue("a fillable target must not be reported as un-fillable", unfillableNames(materialized).isEmpty());
    }

    public void testExpressionsResolvedRequiresMaterializedAliases() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull beforeMaterialize = new FillNull(Source.EMPTY, childWith(List.of(i)), null, List.of(i));
        assertFalse("a non-materialized node must report unresolved so ResolveRefs runs", beforeMaterialize.expressionsResolved());

        FillNull materialized = materialize(beforeMaterialize, List.of(i), TEST_CFG);
        assertTrue("a fully materialized node with resolved aliases is resolved", materialized.expressionsResolved());
    }

    public void testReMaterializeIsIdempotent() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        // A TEXT column's alias reports KEYWORD, the shape most at risk of being rebuilt with a fresh id every pass.
        Attribute t = getFieldAttribute("t", DataType.TEXT);
        // An un-fillable column never gets an alias and must not make the node churn either.
        Attribute d = getFieldAttribute("d", DataType.DATETIME);
        List<Attribute> output = List.of(i, s, t, d);

        FillNull first = materialize(allFields(output), output, TEST_CFG);
        FillNull second = materialize(first, output, TEST_CFG);
        assertEquals("re-materializing against an unchanged child output must be a no-op", first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    public void testReMaterializeIsIdempotentForTargetedFields() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        List<Attribute> output = List.of(i, s);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(output), null, List.of(i));

        FillNull first = materialize(fillNull, output, TEST_CFG);
        assertEquals(Set.of("i"), filledNames(first));
        assertEquals("a targeted FILLNULL must also be idempotent", first, materialize(first, output, TEST_CFG));

        // an unrelated column appearing later changes nothing for a targeted FILLNULL, so re-materializing must leave the node untouched
        Attribute injected = getFieldAttribute("loaded", DataType.LONG);
        FillNull afterInjection = materialize(first, List.of(i, s, injected), TEST_CFG);
        assertEquals("an injected non-target column must not alter the fill aliases", Set.of("i"), filledNames(afterInjection));
        assertEquals(first.fields(), afterInjection.fields());
        assertTrue(afterInjection.expressionsResolved());
    }

    public void testReMaterializeRebuildsARetypedTargetedColumn() {
        Attribute asLong = getFieldAttribute("c", DataType.LONG);
        FillNull first = materialize(allFields(List.of(asLong)), List.of(asLong), TEST_CFG);
        assertEquals(DataType.LONG, aliasFor(first, "c").dataType());

        Attribute asInt = getFieldAttribute("c", DataType.INTEGER);
        FillNull second = materialize(first, List.of(asInt), TEST_CFG);
        assertEquals(DataType.INTEGER, aliasFor(second, "c").dataType());
        assertNotEquals(first, second);
    }

    public void testReplaceChildReDerivesForAnInjectedFillableColumn() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull materialized = materialize(allFields(List.of(i)), List.of(i), TEST_CFG);
        assertEquals(Set.of("i"), filledNames(materialized));

        // SET unmapped_fields="load" injects a KEYWORD column after the aliases were first derived.
        Attribute injected = getFieldAttribute("loaded", DataType.KEYWORD);
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(i, injected)));
        assertEquals("replaceChild must pick the new column up on its own", Set.of("i", "loaded"), filledNames(rebuilt));
        assertTrue(rebuilt.expressionsResolved());
    }

    /**
     * SET unmapped_fields="nullify" injects a NULL-typed column, which can never be filled.
     */
    public void testReplaceChildWithAnInjectedNullTypedColumnStaysResolved() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull materialized = materialize(allFields(List.of(i)), List.of(i), TEST_CFG);

        Attribute nullified = getFieldAttribute("nullified", DataType.NULL);
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(i, nullified)));
        assertEquals("an un-fillable NULL column adds no alias", Set.of("i"), filledNames(rebuilt));
        assertTrue("and must not wedge the node", rebuilt.expressionsResolved());
    }

    public void testReplaceChildWithAnInjectedUnfillableColumnStaysResolved() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Literal zero = new Literal(Source.EMPTY, 0, DataType.INTEGER);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), zero, List.of(), true);
        FillNull materialized = materialize(fillNull, List.of(i), TEST_CFG);
        assertEquals(Set.of("i"), filledNames(materialized));

        Attribute injected = getFieldAttribute("loaded", DataType.KEYWORD);
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(i, injected)));
        assertEquals("an INTEGER value cannot fill a KEYWORD column, so no alias is added", Set.of("i"), filledNames(rebuilt));
        assertTrue("and the node must still be resolved", rebuilt.expressionsResolved());
    }

    /** A targeted FILLNULL does not care about a column it never fills. */
    public void testReplaceChildWithAnInjectedNonTargetColumnStaysResolved() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i, s)), null, List.of(i));
        FillNull materialized = materialize(fillNull, List.of(i, s), TEST_CFG);

        Attribute injected = getFieldAttribute("loaded", DataType.KEYWORD);
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(i, s, injected)));
        assertEquals(Set.of("i"), filledNames(rebuilt));
        assertTrue(rebuilt.expressionsResolved());
    }

    /** A retyped target is rebuilt by replaceChild, with no intermediate unresolved state. */
    public void testReplaceChildRebuildsARetypedTarget() {
        Attribute asLong = getFieldAttribute("c", DataType.LONG);
        FillNull materialized = materialize(
            new FillNull(Source.EMPTY, childWith(List.of(asLong)), null, List.of(asLong)),
            List.of(asLong),
            TEST_CFG
        );
        assertEquals(DataType.LONG, aliasFor(materialized, "c").dataType());

        Attribute asInt = getFieldAttribute("c", DataType.INTEGER);
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(asInt)));
        assertEquals("the alias must follow the new column type", DataType.INTEGER, aliasFor(rebuilt, "c").dataType());
        assertTrue(rebuilt.expressionsResolved());
    }

    /**
     * An attribute re-created with a fresh id but the same name and type must not keep the old alias, or its COALESCE
     * would reference an attribute that is no longer in the child output.
     */
    public void testReplaceChildRebuildsAnAliasWhoseAttributeWasRecreated() {
        Attribute original = getFieldAttribute("c", DataType.LONG);
        FillNull materialized = materialize(allFields(List.of(original)), List.of(original), TEST_CFG);
        Alias before = aliasFor(materialized, "c");

        Attribute recreated = getFieldAttribute("c", DataType.LONG); // same name and type, new NameId
        FillNull rebuilt = materialized.replaceChild(childWith(List.of(recreated)));
        Coalesce coalesce = (Coalesce) aliasFor(rebuilt, "c").child();
        assertEquals("the COALESCE must reference the current attribute", recreated, coalesce.children().get(0));
        assertNotEquals(before.id(), aliasFor(rebuilt, "c").id());
    }

    /** {@code ON *} means every user column, not internal ones. */
    public void testAllFieldsSkipsMetadataAndSyntheticColumns() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, true);
        Attribute synthetic = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "$$c$converted_to$keyword",
            DataType.KEYWORD,
            Nullability.TRUE,
            null,
            true
        );
        List<Attribute> output = List.of(i, index, synthetic);

        FillNull materialized = materialize(allFields(output), output, TEST_CFG);
        assertEquals("only the user column is filled", Set.of("i"), filledNames(materialized));
    }

    /** An explicitly named metadata column is still honoured, matching how the language treats metadata elsewhere. */
    public void testExplicitlyTargetedMetadataColumnIsFilled() {
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, true);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(index)), keyword("unknown"), List.of(index));
        assertEquals(Set.of("_index"), filledNames(materialize(fillNull, List.of(index), TEST_CFG)));
    }

    /**
     * Naming a metadata column alongside a {@code *} must fill it exactly as naming it alone does. What a column does
     * cannot depend on whether a {@code *} happens to share the target list - that is the whole point of the contract.
     * It must also not be silently dropped: unfilled and unreported is the one outcome FILLNULL must never produce.
     */
    public void testMetadataColumnNamedAlongsideStarIsStillFilled() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, true);
        List<Attribute> output = List.of(i, index);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(output), keyword("unknown"), List.of(index), true);

        FillNull materialized = materialize(fillNull, output, TEST_CFG);
        assertEquals("the named metadata column is filled; `i` cannot take a keyword", Set.of("_index"), filledNames(materialized));
        assertEquals("and `i` is reported, not dropped", Set.of("i"), unfillableNames(materialized));
    }

    /** A metadata column that is only swept up by `*` stays out of scope entirely - neither filled nor reported. */
    public void testMetadataColumnIsUntouchedByABareStar() {
        Attribute s = getFieldAttribute("s", DataType.KEYWORD);
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, true);
        List<Attribute> output = List.of(s, index);

        FillNull materialized = materialize(allFields(output), output, TEST_CFG);
        assertEquals(Set.of("s"), filledNames(materialized));
        assertTrue("_index was never in scope, so it is not reported either", unfillableNames(materialized).isEmpty());
    }

    /**
     * Re-materializing against a grown child output does pick up the new column in the all-fields form, and yields a
     * different node so the rule batch iterates again.
     */
    public void testReMaterializePicksUpAnInjectedColumn() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        FillNull first = materialize(allFields(List.of(i)), List.of(i), TEST_CFG);
        assertEquals(Set.of("i"), filledNames(first));

        Attribute injected = getFieldAttribute("loaded", DataType.LONG);
        FillNull second = materialize(first, List.of(i, injected), TEST_CFG);
        assertEquals(Set.of("i", "loaded"), filledNames(second));
        assertNotEquals("picking up a new column must produce a different node so the rule batch iterates", first, second);
    }

    public void testExpressionsResolvedRejectsUnresolvedAlias() {
        // An unresolved alias must keep the node unresolved so the Verifier still validates the Coalesce expressions.
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Alias unresolved = new Alias(Source.EMPTY, "i", new UnresolvedAttribute(Source.EMPTY, "i"));
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), null, List.of(i), false, List.of(unresolved));
        assertFalse("an unresolved fill alias must not be reported as resolved", fillNull.expressionsResolved());
    }

    /**
     * {@code SET time_zone = "+10:00"; ROW foo_date = NULL::date_nanos | FILLNULL "1970-01-01T05:00:00" ON foo_date}.
     * At +10:00 the zone-less literal is 1969-12-31T19:00:00Z, which {@code DateUtils.toLong} rejects as before the
     * date_nanos epoch, so nothing is filled and the explicitly targeted column is reported. The same literal fits
     * under UTC - the verdict follows the query time zone, so the alias derivation must use the query Configuration
     * rather than deriving fillability without one.
     */
    public void testDateNanosFillRejectedOnlyUnderQueryTimeZone() {
        Attribute d = getFieldAttribute("foo_date", DataType.DATE_NANOS);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("1970-01-01T05:00:00"), List.of(d));

        FillNull atPlusTen = materialize(fillNull, List.of(d), cfgWithZone("+10:00"));
        assertTrue("1969-12-31T19:00:00Z is out of date_nanos range, so no fill alias is built", atPlusTen.fields().isEmpty());
        assertEquals("the column is reported as un-fillable, and warned about", Set.of("foo_date"), unfillableNames(atPlusTen));

        FillNull atUtc = materialize(fillNull, List.of(d), cfgWithZone("Z"));
        assertEquals("the very same literal is in range under UTC", 1, atUtc.fields().size());
        assertEquals(5L * 3600 * 1_000_000_000L, fillLiteralFor(atUtc, "foo_date").value());
        assertTrue("a filled column must not be reported", unfillableNames(atUtc).isEmpty());
    }

    /**
     * {@code SET time_zone = "-10:00"; ROW foo_date = NULL::date_nanos | FILLNULL "1969-12-31T20:00:00" ON foo_date}.
     * The mirror image of {@link #testDateNanosFillRejectedOnlyUnderQueryTimeZone}: at -10:00 the literal is
     * 1970-01-01T06:00:00Z and fills, while under UTC it is pre-epoch and is not applied. Ignoring the time zone here
     * would silently leave the column unfilled.
     */
    public void testDateNanosFillAcceptedOnlyUnderQueryTimeZone() {
        Attribute d = getFieldAttribute("foo_date", DataType.DATE_NANOS);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("1969-12-31T20:00:00"), List.of(d));

        FillNull atMinusTen = materialize(fillNull, List.of(d), cfgWithZone("-10:00"));
        assertEquals("1970-01-01T06:00:00Z is in date_nanos range", 1, atMinusTen.fields().size());
        assertEquals(DataType.DATE_NANOS, aliasFor(atMinusTen, "foo_date").dataType());
        assertEquals(6L * 3600 * 1_000_000_000L, fillLiteralFor(atMinusTen, "foo_date").value());
        assertTrue(unfillableNames(atMinusTen).isEmpty());

        FillNull atUtc = materialize(fillNull, List.of(d), cfgWithZone("Z"));
        assertTrue("the very same literal is pre-epoch under UTC", atUtc.fields().isEmpty());
        assertEquals("so it is reported and warned about instead", Set.of("foo_date"), unfillableNames(atUtc));
    }

    /**
     * A DATETIME column accepts the literal under either zone (its range spans the epoch), so only the resulting
     * instant differs. This pins that the zone is threaded through for datetime too, not just date_nanos.
     */
    public void testDatetimeFillUsesQueryTimeZone() {
        Attribute d = getFieldAttribute("foo_date", DataType.DATETIME);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(d)), keyword("1970-01-01T05:00:00"), List.of(d));

        assertEquals(-5L * 3600 * 1000, fillLiteralFor(materialize(fillNull, List.of(d), cfgWithZone("+10:00")), "foo_date").value());
        assertEquals(5L * 3600 * 1000, fillLiteralFor(materialize(fillNull, List.of(d), cfgWithZone("Z")), "foo_date").value());
    }

    /**
     * {@code RATIONAL_TO_INT}/{@code RATIONAL_TO_LONG} round through {@code Math.round} rather than failing, so a
     * fractional fill would be silently altered on the way into an integral column. The conversion must be exactly
     * reversible.
     */
    public void testFractionalFillIntoIntegralColumnIsNotRounded() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Literal fractional = new Literal(Source.EMPTY, 2.7, DataType.DOUBLE);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), fractional, List.of(i));

        FillNull materialized = materialize(fillNull, List.of(i), TEST_CFG);
        assertTrue("2.7 must not be rounded to 3 behind the user's back", materialized.fields().isEmpty());
        assertEquals("and is reported so the user is warned, not failed", Set.of("i"), unfillableNames(materialized));
    }

    public void testWholeDoubleFillIntoIntegralColumnIsAccepted() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Literal whole = new Literal(Source.EMPTY, 2.0, DataType.DOUBLE);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(i)), whole, List.of(i));

        FillNull materialized = materialize(fillNull, List.of(i), TEST_CFG);
        assertEquals("2.0 round-trips exactly, so it fills", 1, materialized.fields().size());
        assertEquals(2, fillLiteralFor(materialized, "i").value());
        assertTrue(unfillableNames(materialized).isEmpty());
    }

    /**
     * 2^53+1 is not representable as a double, so LONG -&gt; DOUBLE loses it. The round trip catches that even though
     * the forward conversion itself never throws.
     */
    public void testLongFillNotExactlyRepresentableAsDoubleIsRejected() {
        Attribute dbl = getFieldAttribute("d", DataType.DOUBLE);
        Literal notRepresentable = new Literal(Source.EMPTY, 9007199254740993L, DataType.LONG);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(List.of(dbl)), notRepresentable, List.of(dbl));

        FillNull materialized = materialize(fillNull, List.of(dbl), TEST_CFG);
        assertTrue("a long that does not survive the round trip to double must not be filled", materialized.fields().isEmpty());
    }

    /** The all-fields form skips the same fractional value and reports it, exactly as a named target does. */
    public void testFractionalFillIntoIntegralColumnIsReportedForAllFields() {
        Attribute i = getFieldAttribute("i", DataType.INTEGER);
        Attribute dbl = getFieldAttribute("d", DataType.DOUBLE);
        List<Attribute> output = List.of(i, dbl);
        Literal fractional = new Literal(Source.EMPTY, 2.7, DataType.DOUBLE);
        FillNull fillNull = new FillNull(Source.EMPTY, childWith(output), fractional, List.of(), true);

        FillNull materialized = materialize(fillNull, output, TEST_CFG);
        assertEquals("only the double column takes the fractional value", Set.of("d"), filledNames(materialized));
        assertEquals("the skipped column is still reported, as it is for a named target", Set.of("i"), unfillableNames(materialized));
    }

    public void testDefaultForTypeCoversNumericStringBoolean() {
        assertEquals(0, ((Literal) FillNull.defaultForType(DataType.INTEGER)).value());
        assertEquals(0L, ((Literal) FillNull.defaultForType(DataType.LONG)).value());
        // Held as the unsigned-long encoding of 0, not a plain 0L - a plain 0L would render as 9223372036854775808.
        assertEquals(NumericUtils.ZERO_AS_UNSIGNED_LONG, ((Literal) FillNull.defaultForType(DataType.UNSIGNED_LONG)).value());
        assertEquals(0.0, ((Literal) FillNull.defaultForType(DataType.DOUBLE)).value());
        assertEquals(false, ((Literal) FillNull.defaultForType(DataType.BOOLEAN)).value());
        assertEquals(DataType.KEYWORD, FillNull.defaultForType(DataType.KEYWORD).dataType());
        assertEquals("a TEXT column takes a KEYWORD default literal", DataType.KEYWORD, FillNull.defaultForType(DataType.TEXT).dataType());
    }

    public void testDefaultForTypeSkipsUnsupportedTypes() {
        assertNull(FillNull.defaultForType(DataType.NULL));
        assertNull(FillNull.defaultForType(DataType.DATETIME));
        assertNull(FillNull.defaultForType(DataType.IP));
        assertNull(FillNull.defaultForType(DataType.VERSION));
    }
}
