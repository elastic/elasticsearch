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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link Locate} function.
 */
public class LocateTests extends AbstractScalarFunctionTestCase {
    public LocateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static final DataType[] STRING_TYPES = new DataType[] { DataType.KEYWORD, DataType.TEXT };

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : STRING_TYPES) {
            for (DataType substrType : STRING_TYPES) {
                suppliers.add(
                    supplier(
                        "",
                        strType,
                        substrType,
                        () -> randomRealisticUnicodeOfCodepointLength(10),
                        str -> randomRealisticUnicodeOfCodepointLength(2),
                        null,
                        (str, substr, start) -> 1 + str.indexOf(substr)
                    )
                );
                suppliers.add(
                    supplier(
                        "exact match ",
                        strType,
                        substrType,
                        () -> randomRealisticUnicodeOfCodepointLength(10),
                        str -> str,
                        null,
                        (str, substr, start) -> 1
                    )
                );
                suppliers.add(
                    supplier(
                        "",
                        strType,
                        substrType,
                        () -> randomRealisticUnicodeOfCodepointLength(10),
                        str -> randomRealisticUnicodeOfCodepointLength(2),
                        () -> between(0, 3),
                        (str, substr, start) -> 1 + str.indexOf(substr, start)
                    )
                );
            }
        }

        suppliers = errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), (v, p) -> {
            if (p == 0 || p == 1) {
                return "string";
            }
            return "integer";
        });

        // Here follows some non-randomized examples that we want to cover on every run
        suppliers.add(supplier("a tiger", "a t", null, 1));
        suppliers.add(supplier("a tiger", "a", null, 1));
        suppliers.add(supplier("界世", "界", null, 1));
        suppliers.add(supplier("a tiger", "er", null, 6));
        suppliers.add(supplier("a tiger", "r", null, 7));
        suppliers.add(supplier("界世", "世", null, 2));
        suppliers.add(supplier("a tiger", "ti", null, 3));
        suppliers.add(supplier("a tiger", "ige", null, 4));
        suppliers.add(supplier("世界世", "界", null, 2));
        suppliers.add(supplier("a tiger", "tigers", null, 0));
        suppliers.add(supplier("a tiger", "ipa", null, 0));
        suppliers.add(supplier("世界世", "\uD83C\uDF0D", null, 0));

        // Extra assertions about 4-byte characters
        // some assertions about the supplementary (4-byte) character we'll use for testing
        assert "𠜎".length() == 2;
        assert "𠜎".codePointCount(0, 2) == 1;
        assert "𠜎".getBytes(StandardCharsets.UTF_8).length == 4;
        suppliers.add(supplier("a ti𠜎er", "𠜎er", null, 5));
        suppliers.add(supplier("a ti𠜎er", "i𠜎e", null, 4));
        suppliers.add(supplier("a ti𠜎er", "ti𠜎", null, 3));
        suppliers.add(supplier("a ti𠜎er", "er", null, 6));
        suppliers.add(supplier("a ti𠜎er", "r", null, 7));
        suppliers.add(supplier("a ti𠜎er", "a ti𠜎er", null, 1));
        // prefix
        suppliers.add(supplier("𠜎abc", "𠜎", null, 1));
        suppliers.add(supplier("𠜎 abc", "𠜎 ", null, 1));
        suppliers.add(supplier("𠜎𠜎𠜎abc", "𠜎𠜎𠜎", null, 1));
        suppliers.add(supplier("𠜎𠜎𠜎 abc", "𠜎𠜎𠜎 ", null, 1));
        suppliers.add(supplier(" 𠜎𠜎𠜎 abc", " 𠜎𠜎𠜎 ", null, 1));
        suppliers.add(supplier("𠜎 𠜎 𠜎 abc", "𠜎 𠜎 𠜎 ", null, 1));
        // suffix
        suppliers.add(supplier("abc𠜎", "𠜎", null, 4));
        suppliers.add(supplier("abc 𠜎", " 𠜎", null, 4));
        suppliers.add(supplier("abc𠜎𠜎𠜎", "𠜎𠜎𠜎", null, 4));
        suppliers.add(supplier("abc 𠜎𠜎𠜎", " 𠜎𠜎𠜎", null, 4));
        suppliers.add(supplier("abc𠜎𠜎𠜎 ", "𠜎𠜎𠜎 ", null, 4));
        // out of range
        suppliers.add(supplier("𠜎a ti𠜎er", "𠜎a ti𠜎ers", null, 0));
        suppliers.add(supplier("a ti𠜎er", "aa ti𠜎er", null, 0));
        suppliers.add(supplier("abc𠜎𠜎", "𠜎𠜎𠜎", null, 0));

        assert "🐱".length() == 2 && "🐶".length() == 2;
        assert "🐱".codePointCount(0, 2) == 1 && "🐶".codePointCount(0, 2) == 1;
        assert "🐱".getBytes(StandardCharsets.UTF_8).length == 4 && "🐶".getBytes(StandardCharsets.UTF_8).length == 4;
        suppliers.add(supplier("🐱Meow!🐶Woof!", "🐱Meow!🐶Woof!", null, 1));
        suppliers.add(supplier("🐱Meow!🐶Woof!", "Meow!🐶Woof!", 0, 2));
        suppliers.add(supplier("🐱Meow!🐶Woof!", "eow!🐶Woof!", 0, 3));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Locate(source, args.get(0), args.get(1), args.size() < 3 ? null : args.get(2));
    }

    private static TestCaseSupplier supplier(String str, String substr, @Nullable Integer start, @Nullable Integer expectedValue) {
        String name = String.format(Locale.ROOT, "\"%s\" in \"%s\"", substr, str);
        if (start != null) {
            name += " starting at " + start;
        }

        return new TestCaseSupplier(
            name,
            types(DataType.KEYWORD, DataType.KEYWORD, start != null),
            () -> testCase(DataType.KEYWORD, DataType.KEYWORD, str, substr, start, expectedValue)
        );
    }

    interface ExpectedValue {
        int expectedValue(String str, String substr, Integer start);
    }

    private static TestCaseSupplier supplier(
        String name,
        DataType strType,
        DataType substrType,
        Supplier<String> strValueSupplier,
        Function<String, String> substrValueSupplier,
        @Nullable Supplier<Integer> startSupplier,
        ExpectedValue expectedValue
    ) {
        List<DataType> types = types(strType, substrType, startSupplier != null);
        return new TestCaseSupplier(name + TestCaseSupplier.nameFromTypes(types), types, () -> {
            String str = strValueSupplier.get();
            String substr = substrValueSupplier.apply(str);
            Integer start = startSupplier == null ? null : startSupplier.get();
            return testCase(strType, substrType, str, substr, start, expectedValue.expectedValue(str, substr, start));
        });
    }

    private static String expectedToString(boolean hasStart) {
        if (hasStart) {
            return "LocateEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1], start=Attribute[channel=2]]";
        }
        return "LocateNoStartEvaluator[str=Attribute[channel=0], substr=Attribute[channel=1]]";
    }

    private static List<DataType> types(DataType firstType, DataType secondType, boolean hasStart) {
        List<DataType> types = new ArrayList<>();
        types.add(firstType);
        types.add(secondType);
        if (hasStart) {
            types.add(DataType.INTEGER);
        }
        return types;
    }

    private static TestCaseSupplier.TestCase testCase(
        DataType strType,
        DataType substrType,
        String str,
        String substr,
        Integer start,
        Integer expectedValue
    ) {
        List<TestCaseSupplier.TypedData> values = new ArrayList<>();
        values.add(new TestCaseSupplier.TypedData(str == null ? null : new BytesRef(str), strType, "str"));
        values.add(new TestCaseSupplier.TypedData(substr == null ? null : new BytesRef(substr), substrType, "substr"));
        if (start != null) {
            values.add(new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"));
        }
        return new TestCaseSupplier.TestCase(values, expectedToString(start != null), DataType.INTEGER, equalTo(expectedValue));
    }
}
