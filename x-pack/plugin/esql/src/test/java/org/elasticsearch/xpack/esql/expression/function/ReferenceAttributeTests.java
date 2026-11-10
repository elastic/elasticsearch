/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;

public class ReferenceAttributeTests extends AbstractNamedExpressionSerializationTests<ReferenceAttribute> {
    @Override
    protected ReferenceAttribute createTestInstance() {
        return randomReferenceAttribute(false);
    }

    @Override
    protected ReferenceAttribute mutateInstance(ReferenceAttribute instance) {
        Source source = instance.source();
        String qualifier = instance.qualifier();
        String name = instance.name();
        DataType type = instance.dataType();
        Nullability nullability = instance.nullable();
        NameId id = instance.id();
        boolean synthetic = instance.synthetic();
        String valuesAnalyzer = instance.valuesAnalyzer();
        switch (between(0, 6)) {
            case 0 -> qualifier = randomAlphaOfLength(qualifier == null ? 3 : qualifier.length() + 1);
            case 1 -> name = randomAlphaOfLength(name.length() + 1);
            case 2 -> type = randomValueOtherThan(type, () -> randomFrom(DataType.types()));
            case 3 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 4 -> id = new NameId();
            case 5 -> synthetic = false == synthetic;
            case 6 -> valuesAnalyzer = randomAlphaOfLength(valuesAnalyzer == null ? 3 : valuesAnalyzer.length() + 1);
        }
        // The leg types have to follow the type: the constructor refuses a union without them and anything else with
        // them, and case 2 above draws from every type including UNION.
        return new ReferenceAttribute(
            source,
            qualifier,
            name,
            type,
            nullability,
            id,
            synthetic,
            valuesAnalyzer,
            ReferenceAttributeTestUtils.randomLegTypes(type)
        );
    }

    @Override
    protected ReferenceAttribute mutateNameId(ReferenceAttribute instance) {
        return (ReferenceAttribute) instance.withId(new NameId());
    }

    /**
     * Ensures {@code withQualifier} applies the new qualifier rather than returning
     * the unchanged attribute, and still avoids cloning when the qualifier is equal.
     */
    public void testWithQualifier() {
        ReferenceAttribute attribute = randomReferenceAttribute(false);
        String newQualifier = randomValueOtherThan(attribute.qualifier(), () -> randomBoolean() ? null : randomAlphaOfLength(5));
        assertEquals(newQualifier, attribute.withQualifier(newQualifier).qualifier());
        assertSame(attribute, attribute.withQualifier(attribute.qualifier()));
    }

    private static ReferenceAttribute analyzedReference(String valuesAnalyzer) {
        return new ReferenceAttribute(Source.EMPTY, null, "t", DataType.TEXT, Nullability.FALSE, new NameId(), false, valuesAnalyzer);
    }

    public void testSerializationPreservesValuesAnalyzer() throws IOException {
        ReferenceAttribute attribute = analyzedReference("whitespace");
        ReferenceAttribute copy = copyInstance(attribute);
        assertEquals("whitespace", copy.valuesAnalyzer());
        assertEquals(attribute, copy);
    }

    public void testBackcompatSerializationWithoutValuesAnalyzer() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(ToText.ESQL_TO_TEXT_VALUES_ANALYZER);
        ReferenceAttribute attribute = analyzedReference(null);
        ReferenceAttribute copy = copyInstance(attribute, oldVersion);
        assertNull(copy.valuesAnalyzer());
        assertEquals(attribute, copy);
    }

    public void testBackcompatSerializationRejectsValuesAnalyzer() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(ToText.ESQL_TO_TEXT_VALUES_ANALYZER);
        ReferenceAttribute attribute = analyzedReference("whitespace");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(attribute, oldVersion));
        assertThat(e.getMessage(), containsString("with a values analyzer is not supported in peer node's version [" + oldVersion + "]"));
    }

    /**
     * Every {@code withName}/{@code withId}/... helper routes through {@code clone}, whose signature does not
     * include the values analyzer; dropping it there would silently change how the column's values are analyzed.
     */
    public void testCloneHelpersPreserveValuesAnalyzer() {
        ReferenceAttribute attribute = analyzedReference("whitespace");
        assertEquals("whitespace", ((ReferenceAttribute) attribute.withName("renamed")).valuesAnalyzer());
        assertEquals("whitespace", ((ReferenceAttribute) attribute.withId(new NameId())).valuesAnalyzer());
        assertEquals("whitespace", ((ReferenceAttribute) attribute.withQualifier("q")).valuesAnalyzer());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }

    /**
     * A union's leg types are half of what its type means - a value's tag indexes them - so a reference carrying the
     * type without them is a column nothing can read. The constructor is where that is made unconstructable, which is
     * the nearest an enum type can come to making it unrepresentable.
     */
    public void testAUnionReferenceMustCarryItsLegsAndOthersMustNot() {
        List<DataType> legs = List.of(DataType.KEYWORD, DataType.LONG);
        IllegalArgumentException noLegs = expectThrows(
            IllegalArgumentException.class,
            () -> new ReferenceAttribute(Source.EMPTY, null, "u", DataType.UNION, Nullability.TRUE, new NameId(), false, null, null)
        );
        assertThat(noLegs.getMessage(), containsString("[u] is a union and must carry its leg types"));

        IllegalArgumentException wrongType = expectThrows(
            IllegalArgumentException.class,
            () -> new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG, Nullability.TRUE, new NameId(), false, null, legs)
        );
        assertThat(wrongType.getMessage(), containsString("[n] is [long] and cannot carry leg types"));

        ReferenceAttribute union = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "u",
            DataType.UNION,
            Nullability.TRUE,
            new NameId(),
            false,
            null,
            legs
        );
        assertEquals(legs, union.unionLegTypes());
    }

    /**
     * {@code withName} and the other witherers route through {@code clone}, whose signature cannot take the legs - so
     * they are carried from the instance. A rename that dropped them would turn a readable column into an unreadable
     * one, silently, which is exactly what renaming a union used to do.
     */
    public void testARenamedUnionReferenceKeepsItsLegs() {
        List<DataType> legs = List.of(DataType.KEYWORD, DataType.LONG);
        ReferenceAttribute union = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "u",
            DataType.UNION,
            Nullability.TRUE,
            new NameId(),
            false,
            null,
            legs
        );
        assertEquals(legs, ((ReferenceAttribute) union.withName("d")).unionLegTypes());
        assertEquals(legs, ((ReferenceAttribute) union.withId(new NameId())).unionLegTypes());
        // And the legs are part of identity, or a plan cache keyed on the attribute would serve one for another.
        assertNotEquals(
            union,
            new ReferenceAttribute(
                Source.EMPTY,
                null,
                "u",
                DataType.UNION,
                Nullability.TRUE,
                union.id(),
                false,
                null,
                List.of(DataType.DOUBLE, DataType.KEYWORD)
            )
        );
    }

}
