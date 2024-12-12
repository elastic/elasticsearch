/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import joptsimple.internal.Strings;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.Converter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.hamcrest.Matchers.equalTo;

public class LiteralTests extends AbstractNodeTestCase<Literal, Expression> {

    static class ValueAndCompatibleTypes {

        final Supplier<Object> valueSupplier;
        final List<DataType> validDataTypes;

        ValueAndCompatibleTypes(Supplier<Object> valueSupplier, DataType... validDataTypes) {
            this.valueSupplier = valueSupplier;
            this.validDataTypes = Arrays.asList(validDataTypes);
        }
    }

    /**
     * Generators for values and data types. The first valid
     * data type is special it is used when picking a generator
     * for a specific data type. So the first valid data type
     * after a generators is its "native" type.
     */
    private static final List<ValueAndCompatibleTypes> GENERATORS = Arrays.asList(
        new ValueAndCompatibleTypes(() -> randomBoolean() ? randomBoolean() : randomFrom("true", "false"), BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomByte, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomShort, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomInt, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomLong, LONG, FLOAT, DOUBLE, BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomFloat, FLOAT, LONG, DOUBLE, BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomDouble, DOUBLE, LONG, FLOAT, BOOLEAN),
        new ValueAndCompatibleTypes(() -> randomAlphaOfLength(5), KEYWORD)
    );

    public static Literal randomLiteral() {
        ValueAndCompatibleTypes gen = randomFrom(GENERATORS);
        DataType dataType = randomFrom(gen.validDataTypes);
        return new Literal(SourceTests.randomSource(), DataTypeConverter.convert(gen.valueSupplier.get(), dataType), dataType);
    }

    @Override
    protected Literal randomInstance() {
        return randomLiteral();
    }

    @Override
    protected Literal copy(Literal instance) {
        return new Literal(instance.source(), instance.value(), instance.dataType());
    }

    @Override
    protected Literal mutate(Literal instance) {
        return mutateLiteral(instance);
    }

    public static Literal mutateLiteral(Literal instance) {
        List<Function<Literal, Literal>> mutators = new ArrayList<>();
        // Changing the location doesn't count as mutation because..... it just doesn't, ok?!
        // Change the value to another valid value
        mutators.add(l -> new Literal(l.source(), randomValueOfTypeOtherThan(l.value(), l.dataType()), l.dataType()));
        // If we can change the data type then add that as an option as well
        List<DataType> validDataTypes = validReplacementDataTypes(instance.value(), instance.dataType());
        if (validDataTypes.size() > 1) {
            mutators.add(l -> new Literal(l.source(), l.value(), randomValueOtherThan(l.dataType(), () -> randomFrom(validDataTypes))));
        }
        return randomFrom(mutators).apply(instance);
    }

    @Override
    public void testTransform() {
        Literal literal = randomInstance();

        // Replace value
        Object newValue = randomValueOfTypeOtherThan(literal.value(), literal.dataType());
        assertEquals(
            new Literal(literal.source(), newValue, literal.dataType()),
            literal.transformPropertiesOnly(Object.class, p -> p == literal.value() ? newValue : p)
        );

        // Replace data type if there are more compatible data types
        List<DataType> validDataTypes = validReplacementDataTypes(literal.value(), literal.dataType());
        if (validDataTypes.size() > 1) {
            DataType newDataType = randomValueOtherThan(literal.dataType(), () -> randomFrom(validDataTypes));
            assertEquals(
                new Literal(literal.source(), literal.value(), newDataType),
                literal.transformPropertiesOnly(DataType.class, p -> newDataType)
            );
        }
    }

    @Override
    public void testReplaceChildren() {
        Exception e = expectThrows(UnsupportedOperationException.class, () -> randomInstance().replaceChildrenSameSize(emptyList()));
        assertEquals("this type of node doesn't have any children to replace", e.getMessage());
    }

    public void testToString() {
        assertThat(new Literal(Source.EMPTY, 1, LONG).toString(), equalTo("1"));
        assertThat(new Literal(Source.EMPTY, "short", KEYWORD).toString(), equalTo("short"));
        // toString should limit it's length
        String tooLong = Strings.repeat('a', 510);
        assertThat(new Literal(Source.EMPTY, tooLong, KEYWORD).toString(), equalTo(Strings.repeat('a', 500) + "..."));

        for (ValueAndCompatibleTypes g : GENERATORS) {
            Literal lit = new Literal(Source.EMPTY, g.valueSupplier.get(), randomFrom(g.validDataTypes));
            assertThat(lit.toString(), equalTo(Objects.toString(lit.value())));
        }
    }

    private static Object randomValueOfTypeOtherThan(Object original, DataType type) {
        for (ValueAndCompatibleTypes gen : GENERATORS) {
            if (gen.validDataTypes.get(0) == type) {
                return randomValueOtherThan(original, () -> DataTypeConverter.convert(gen.valueSupplier.get(), type));
            }
        }
        throw new IllegalArgumentException("No native generator for [" + type + "]");
    }

    private static List<DataType> validReplacementDataTypes(Object value, DataType type) {
        List<DataType> validDataTypes = new ArrayList<>();
        List<DataType> options = Arrays.asList(BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN);
        for (DataType candidate : options) {
            try {
                Converter c = DataTypeConverter.converterFor(type, candidate);
                c.convert(value);
                validDataTypes.add(candidate);
            } catch (InvalidArgumentException e) {
                // invalid conversion then....
            }
        }
        return validDataTypes;
    }
}
