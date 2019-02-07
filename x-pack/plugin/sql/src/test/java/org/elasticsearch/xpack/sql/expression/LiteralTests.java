/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.sql.tree.SourceTests;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

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
        new ValueAndCompatibleTypes(() -> randomBoolean() ? randomBoolean() : randomFrom("true", "false"), DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomByte, DataType.BYTE, DataType.SHORT, DataType.INTEGER, DataType.LONG,
                DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomShort, DataType.SHORT, DataType.INTEGER, DataType.LONG,
                DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomInt, DataType.INTEGER, DataType.LONG,
                DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomLong, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomFloat, DataType.FLOAT, DataType.LONG, DataType.DOUBLE, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(ESTestCase::randomDouble, DataType.DOUBLE, DataType.LONG, DataType.FLOAT, DataType.BOOLEAN),
        new ValueAndCompatibleTypes(() -> randomAlphaOfLength(5), DataType.KEYWORD));

    public static Literal randomLiteral() {
        ValueAndCompatibleTypes gen = randomFrom(GENERATORS);
        return new Literal(SourceTests.randomSource(), gen.valueSupplier.get(), randomFrom(gen.validDataTypes));
    }

    @Override
    protected Literal randomInstance() {
        return randomLiteral();
    }

    @Override
    protected Literal copy(Literal instance) {
        return new Literal(instance.source(), instance.name(), instance.value(), instance.dataType());
    }

    @Override
    protected Literal mutate(Literal instance) {
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
        assertEquals(new Literal(literal.source(), newValue, literal.dataType()),
                literal.transformPropertiesOnly(p -> p == literal.value() ? newValue : p, Object.class));

        // Replace data type if there are more compatible data types
        List<DataType> validDataTypes = validReplacementDataTypes(literal.value(), literal.dataType());
        if (validDataTypes.size() > 1) {
            DataType newDataType = randomValueOtherThan(literal.dataType(), () -> randomFrom(validDataTypes));
            assertEquals(new Literal(literal.source(), literal.value(), newDataType),
                literal.transformPropertiesOnly(p -> newDataType, DataType.class));
        }
    }

    @Override
    public void testReplaceChildren() {
        Exception e = expectThrows(UnsupportedOperationException.class, () -> randomInstance().replaceChildren(emptyList()));
        assertEquals("this type of node doesn't have any children to replace", e.getMessage());
    }

    private Object randomValueOfTypeOtherThan(Object original, DataType type) {
        for (ValueAndCompatibleTypes gen : GENERATORS) {
            if (gen.validDataTypes.get(0) == type) {
                return randomValueOtherThan(original, () -> DataTypeConversion.convert(gen.valueSupplier.get(), type));
            }
        }
        throw new IllegalArgumentException("No native generator for [" + type + "]");
    }

    private List<DataType> validReplacementDataTypes(Object value, DataType type) {
        List<DataType> validDataTypes = new ArrayList<>();
        List<DataType> options = Arrays.asList(DataType.BYTE, DataType.SHORT, DataType.INTEGER, DataType.LONG,
                DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN);
        for (DataType candidate : options) {
            try {
                DataTypeConversion.Conversion c = DataTypeConversion.conversionFor(type, candidate);
                c.convert(value);
                validDataTypes.add(candidate);
            } catch (SqlIllegalArgumentException e) {
                // invalid conversion then....
            }
        }
        return validDataTypes;
    }
}
