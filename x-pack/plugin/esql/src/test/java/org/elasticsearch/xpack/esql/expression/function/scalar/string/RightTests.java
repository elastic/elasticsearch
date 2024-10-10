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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class RightTests extends AbstractScalarFunctionTestCase {
    public RightTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(new TestCaseSupplier("empty string", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            int length = between(-64, 64);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(""), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        suppliers.add(new TestCaseSupplier("ascii", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLengthBetween(1, 64);
            int length = between(1, text.length());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(unicodeRightSubstring(text, length)))
            );
        }));
        suppliers.add(new TestCaseSupplier("ascii longer than string", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLengthBetween(1, 64);
            int length = between(text.length(), 128);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(text))
            );
        }));
        suppliers.add(new TestCaseSupplier("ascii zero length", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLengthBetween(1, 64);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(0, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));
        suppliers.add(new TestCaseSupplier("ascii negative length", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLengthBetween(1, 64);
            int length = between(-128, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        suppliers.add(new TestCaseSupplier("unicode", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomUnicodeOfLengthBetween(1, 64);
            int length = between(1, text.length());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(unicodeRightSubstring(text, length)))
            );
        }));
        suppliers.add(new TestCaseSupplier("unicode longer than string", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomUnicodeOfLengthBetween(1, 64);
            int length = between(text.length(), 128);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(text))
            );
        }));
        suppliers.add(new TestCaseSupplier("unicode zero length", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomUnicodeOfLengthBetween(1, 64);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(0, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));
        suppliers.add(new TestCaseSupplier("unicode negative length", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomUnicodeOfLengthBetween(1, 64);
            int length = between(-128, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));
        suppliers.add(new TestCaseSupplier("ascii as text", List.of(DataType.TEXT, DataType.INTEGER), () -> {
            String text = randomAlphaOfLengthBetween(1, 64);
            int length = between(1, text.length());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.TEXT, "str"),
                    new TestCaseSupplier.TypedData(length, DataType.INTEGER, "length")
                ),
                "RightEvaluator[str=Attribute[channel=0], length=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(unicodeRightSubstring(text, length)))
            );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> switch (p) {
            case 0 -> "string";
            case 1 -> "integer";
            default -> throw new IllegalStateException("bad parameter number");
        });
    }

    private static String unicodeRightSubstring(String str, int length) {
        int codepointCount = str.codePointCount(0, str.length());
        int codePointsToSkip = codepointCount - length;
        if (codePointsToSkip < 0) {
            return str;
        } else {
            return str.codePoints()
                .skip(codePointsToSkip)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Right(source, args.get(0), args.get(1));
    }

    public Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        int length = (Integer) typedData.get(1).data();
        return equalTo(new BytesRef(str.substring(str.length() - length)));
    }

    public void testUnicode() {
        final String s = "a\ud83c\udf09tiger";
        assert s.codePointCount(0, s.length()) == 7;
        assertThat(process(s, 6), equalTo("\ud83c\udf09tiger"));
    }

    private String process(String str, int length) {
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                new Right(Source.EMPTY, field("str", DataType.KEYWORD), new Literal(Source.EMPTY, length, DataType.INTEGER))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            if (block.isNull(0)) {
                return null;
            }
            BytesRef resultByteRef = ((BytesRef) toJavaObject(block, 0));
            return resultByteRef == null ? null : resultByteRef.utf8ToString();
        }
    }
}
