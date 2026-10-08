/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesUnsetTests extends AbstractScalarFunctionTestCase {
    public TimeSeriesUnsetTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType type : List.of(DataType.KEYWORD, DataType.TEXT)) {
            suppliers.add(unset("unsets a dimension", type, "{\"region\":\"us\",\"pod\":\"p1\"}", List.of("pod"), "{\"region\":\"us\"}"));
        }
        suppliers.add(unset("unsets several dimensions", "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}", List.of("c", "a"), "{\"b\":\"2\"}"));
        suppliers.add(
            unset(
                "a dotted name stored nested",
                "{\"labels\":{\"pod\":\"p1\"},\"region\":\"us\"}",
                List.of("labels.pod"),
                "{\"region\":\"us\"}"
            )
        );
        suppliers.add(
            unset(
                "a dotted name stored as one key",
                "{\"labels.pod\":\"p1\",\"region\":\"us\"}",
                List.of("labels.pod"),
                "{\"region\":\"us\"}"
            )
        );
        suppliers.add(
            unset("a dimension the value doesn't carry", "{\"b\":\"2\",\"a\":\"1\"}", List.of("pod"), "{\"a\":\"1\",\"b\":\"2\"}")
        );
        suppliers.add(
            unset(
                "no dimensions only canonicalizes",
                "{\"b\":{\"d\":\"4\",\"c\":\"3\"},\"a\":\"1\"}",
                List.of(),
                "{\"a\":\"1\",\"b\":{\"c\":\"3\",\"d\":\"4\"}}"
            )
        );
        suppliers.add(notAnObject());
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    private static TestCaseSupplier unset(String name, String value, List<String> dimensions, String expected) {
        return unset(name, DataType.KEYWORD, value, dimensions, expected);
    }

    private static TestCaseSupplier unset(String name, DataType type, String value, List<String> dimensions, String expected) {
        List<DataType> types = new ArrayList<>(List.of(type));
        dimensions.forEach(dimension -> types.add(DataType.KEYWORD));
        return new TestCaseSupplier(name + " [" + type.typeName() + "]", types, () -> {
            List<TestCaseSupplier.TypedData> data = new ArrayList<>();
            data.add(new TestCaseSupplier.TypedData(new BytesRef(value), type, "timeseries"));
            for (int i = 0; i < dimensions.size(); i++) {
                data.add(new TestCaseSupplier.TypedData(new BytesRef(dimensions.get(i)), DataType.KEYWORD, "dimension" + i).forceLiteral());
            }
            return new TestCaseSupplier.TestCase(data, expectedToString(dimensions), DataType.KEYWORD, equalTo(new BytesRef(expected)));
        });
    }

    private static TestCaseSupplier notAnObject() {
        return new TestCaseSupplier("not a JSON object", List.of(DataType.KEYWORD), () -> {
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(new BytesRef("[1]"), DataType.KEYWORD, "timeseries")
            );
            return new TestCaseSupplier.TestCase(data, expectedToString(List.of()), DataType.KEYWORD, nullValue()).withWarning(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
            ).withWarning("Line 1:1: java.lang.IllegalArgumentException: expected a JSON object");
        });
    }

    private static String expectedToString(List<String> dimensions) {
        return "TimeSeriesUnsetEvaluator[timeseries=Attribute[channel=0], unset=dimensions=" + dimensions + "]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TimeSeriesUnset(source, args.getFirst(), args.subList(1, args.size()));
    }
}
