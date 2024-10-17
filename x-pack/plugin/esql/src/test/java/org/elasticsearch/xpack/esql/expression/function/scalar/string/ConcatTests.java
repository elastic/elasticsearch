/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ConcatTests extends AbstractScalarFunctionTestCase {
    public ConcatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers(suppliers, 2);
        suppliers(suppliers, 3);
        suppliers = anyNullIsNull(true, suppliers);
        for (int length = 4; length < 100; length++) {
            suppliers(suppliers, length);
        }
        Set<DataType> supported = Set.of(DataType.NULL, DataType.KEYWORD, DataType.TEXT);
        List<Set<DataType>> supportedPerPosition = List.of(supported, supported);
        for (DataType lhs : DataType.types()) {
            if (lhs == DataType.NULL || DataType.isRepresentable(lhs) == false) {
                continue;
            }
            for (DataType rhs : DataType.types()) {
                if (rhs == DataType.NULL || DataType.isRepresentable(rhs) == false) {
                    continue;
                }
                if (DataType.isString(lhs) && DataType.isString(rhs)) {
                    continue;
                }

                suppliers.add(typeErrorSupplier(false, supportedPerPosition, List.of(lhs, rhs), (v, p) -> "string"));
            }
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void suppliers(List<TestCaseSupplier> suppliers, int length) {
        if (length > 3) {
            suppliers.add(supplier("ascii", DataType.KEYWORD, length, () -> randomAlphaOfLengthBetween(1, 10)));
            suppliers.add(supplier("unicode", DataType.TEXT, length, () -> randomRealisticUnicodeOfLengthBetween(1, 10)));
        } else {
            add(suppliers, "ascii", length, () -> randomAlphaOfLengthBetween(1, 10));
            add(suppliers, "unicode", length, () -> randomRealisticUnicodeOfLengthBetween(1, 10));
        }
    }

    private static TestCaseSupplier supplier(String name, DataType type, int length, Supplier<String> valueSupplier) {
        return new TestCaseSupplier(length + " " + name, IntStream.range(0, length).mapToObj(i -> type).toList(), () -> {
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            String expectedValue = "";
            String expectedToString = "ConcatEvaluator[values=[";
            for (int v = 0; v < length; v++) {
                String value = valueSupplier.get();
                values.add(new TestCaseSupplier.TypedData(new BytesRef(value), type, Integer.toString(v)));
                expectedValue += value;
                if (v != 0) {
                    expectedToString += ", ";
                }
                expectedToString += "Attribute[channel=" + v + "]";
            }
            expectedToString += "]]";
            return new TestCaseSupplier.TestCase(values, expectedToString, DataType.KEYWORD, equalTo(new BytesRef(expectedValue)));
        });
    }

    private static void add(List<TestCaseSupplier> suppliers, String name, int length, Supplier<String> valueSupplier) {
        Map<Integer, List<List<DataType>>> permutations = new HashMap<Integer, List<List<DataType>>>();
        List<DataType> supportedDataTypes = List.of(DataType.KEYWORD, DataType.TEXT);
        permutations.put(0, List.of(List.of(DataType.KEYWORD), List.of(DataType.TEXT)));
        for (int v = 0; v < length - 1; v++) {
            List<List<DataType>> current = permutations.get(v);
            List<List<DataType>> next = new ArrayList<>();
            for (int i = 0; i < current.size(); i++) {
                for (int j = 0; j < supportedDataTypes.size(); j++) {
                    List<DataType> n = new ArrayList<>(current.get(i));
                    n.add(supportedDataTypes.get(j));
                    next.add(n);
                }
            }
            permutations.put(v + 1, next);
        }

        for (List<DataType> types : permutations.get(length - 1)) {
            suppliers.add(new TestCaseSupplier(length + " " + name, types, () -> {
                List<TestCaseSupplier.TypedData> values = new ArrayList<>();
                String expectedValue = "";
                String expectedToString = "ConcatEvaluator[values=[";
                for (int v = 0; v < length; v++) {
                    String value = valueSupplier.get();
                    values.add(new TestCaseSupplier.TypedData(new BytesRef(value), types.get(v), Integer.toString(v)));
                    expectedValue += value;
                    if (v != 0) {
                        expectedToString += ", ";
                    }
                    expectedToString += "Attribute[channel=" + v + "]";
                }
                expectedToString += "]]";
                return new TestCaseSupplier.TestCase(values, expectedToString, DataType.KEYWORD, equalTo(new BytesRef(expectedValue)));
            }));
        }

    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Concat(source, args.get(0), args.subList(1, args.size()));
    }

    public void testSomeConstant() {
        List<Expression> fields = testCase.getDataAsFields();
        List<Expression> literals = testCase.getDataAsLiterals();
        List<Object> fieldValues = new ArrayList<>();
        List<Expression> mix = new ArrayList<>(fields.size());
        assert fields.size() == literals.size();
        for (int i = 0; i < fields.size(); i++) {
            if (randomBoolean()) {
                fieldValues.add(testCase.getData().get(i).data());
                mix.add(fields.get(i));
            } else {
                mix.add(literals.get(i));
            }
        }
        if (fieldValues.isEmpty()) {
            fieldValues.add(new BytesRef("dummy"));
        }
        Expression expression = build(testCase.getSource(), mix);
        if (testCase.getExpectedTypeError() != null) {
            assertTrue("expected unresolved", expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
            return;
        }

        int totalLength = testDataLength();
        if (totalLength >= Concat.MAX_CONCAT_LENGTH || rarely()) {
            boolean hasNulls = mix.stream().anyMatch(x -> x instanceof Literal l && l.value() == null)
                || fieldValues.stream().anyMatch(Objects::isNull);
            if (hasNulls == false) {
                testOversized(totalLength, mix, fieldValues);
                return;
            }
        }

        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(expression).get(driverContext());
            Block block = eval.eval(row(fieldValues))
        ) {
            assertThat(toJavaObject(block, 0), testCase.getMatcher());
        }
    }

    private void testOversized(int totalLen, List<Expression> mix, List<Object> fieldValues) {
        for (int len; totalLen < Concat.MAX_CONCAT_LENGTH; totalLen += len) {
            len = randomIntBetween(1, (int) Concat.MAX_CONCAT_LENGTH);
            mix.add(new Literal(Source.EMPTY, new BytesRef(randomAlphaOfLength(len)), DataType.KEYWORD));
        }
        Expression expression = build(testCase.getSource(), mix);
        Exception e = expectThrows(EsqlClientException.class, () -> {
            try (
                EvalOperator.ExpressionEvaluator eval = evaluator(expression).get(driverContext());
                Block block = eval.eval(row(fieldValues));
            ) {}
        });
        assertThat(e.getMessage(), is("concatenating more than [1048576] bytes is not supported"));
    }

    private int testDataLength() {
        int totalLength = 0;
        for (var data : testCase.getData()) {
            if (data.data() instanceof BytesRef bytesRef) {
                totalLength += bytesRef.length;
            }
        }
        return totalLength;
    }
}
