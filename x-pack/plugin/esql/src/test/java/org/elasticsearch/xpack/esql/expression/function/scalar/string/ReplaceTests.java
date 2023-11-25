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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ReplaceTests extends AbstractFunctionTestCase {
    public ReplaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : EsqlDataTypes.types()) {
            if (DataTypes.isString(strType) == false) {
                continue;
            }
            for (DataType oldStrType : EsqlDataTypes.types()) {
                if (DataTypes.isString(oldStrType) == false) {
                    continue;
                }
                for (DataType newStrType : EsqlDataTypes.types()) {
                    if (DataTypes.isString(newStrType) == false) {
                        continue;
                    }
                    suppliers.add(new TestCaseSupplier(List.of(strType, oldStrType, newStrType), () -> {
                        String str = randomAlphaOfLength(10);
                        String oldStr = str.substring(1, 2);
                        String newStr = randomAlphaOfLength(5);
                        return testCase(strType, oldStrType, newStrType, str, oldStr, newStr, str.replaceAll(oldStr, newStr));
                    }));
                }
            }
        }
        /*
        final String s = "a\ud83c\udf09tiger";
        assertThat(process(s, "a\ud83c\udf09t", "pp"), equalTo("ppiger"));
        assertThat(process(s, "\ud83c\udf09", "\ud83c\udf09\ud83c\udf09"), equalTo("a\ud83c\udf09\ud83c\udf09tiger"));

         */
        suppliers.add(fixedCase("replace one with two", "a tiger", "a", "pp", "pp tiger"));
        suppliers.add(fixedCase("replace many times", "a tiger is always a tiger", "a", "pp", "pp tiger is pplwppys pp tiger"));
        suppliers.add(fixedCase("replace no times", "a tiger", "ti ", "", "a tiger"));
        suppliers.add(fixedCase("replace with none", "a tiger", " ti", "", "ager"));
        suppliers.add(fixedCase("regex", "what a nice day", "\\s+", "-", "what-a-nice-day"));
        suppliers.add(
            fixedCase("more complex regex", "I love cats and cats are amazing.", "\\bcats\\b", "dogs", "I love dogs and dogs are amazing.")
        );
        suppliers.add(fixedCase("match unicode", "a\ud83c\udf09tiger", "a\ud83c\udf09t", "pp", "ppiger"));
        suppliers.add(
            fixedCase(
                "replace with unicode",
                "a\ud83c\udf09tiger",
                "\ud83c\udf09",
                "\ud83c\udf09\ud83c\udf09",
                "a\ud83c\udf09\ud83c\udf09tiger"
            )
        );

        // a syntactically wrong regex should yield null. And a warning header
        // but for now we are letting the exception pass through. See also https://github.com/elastic/elasticsearch/issues/100038
        // suppliers.add(new TestCaseSupplier("invalid_regex", () -> {
        // String text = randomAlphaOfLength(10);
        // String invalidRegex = "[";
        // String newStr = randomAlphaOfLength(5);
        // return new TestCaseSupplier.TestCase(
        // List.of(
        // new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
        // new TestCaseSupplier.TypedData(new BytesRef(invalidRegex), DataTypes.KEYWORD, "oldStr"),
        // new TestCaseSupplier.TypedData(new BytesRef(newStr), DataTypes.KEYWORD, "newStr")
        // ),
        // "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
        // DataTypes.KEYWORD,
        // equalTo(null)
        // ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
        // .withWarning("java.util.regex.PatternSyntaxException: Unclosed character class near index 0\r\n[\r\n^");
        // }));
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, suppliers)));
    }

    private static TestCaseSupplier fixedCase(String name, String str, String oldStr, String newStr, String result) {
        return new TestCaseSupplier(
            name,
            List.of(DataTypes.KEYWORD, DataTypes.KEYWORD, DataTypes.KEYWORD),
            () -> testCase(DataTypes.KEYWORD, DataTypes.KEYWORD, DataTypes.KEYWORD, str, oldStr, newStr, result)
        );
    }

    private static TestCaseSupplier.TestCase testCase(
        DataType strType,
        DataType oldStrType,
        DataType newStrType,
        String str,
        String oldStr,
        String newStr,
        String result
    ) {
        return new TestCaseSupplier.TestCase(
            List.of(
                new TestCaseSupplier.TypedData(new BytesRef(str), strType, "str"),
                new TestCaseSupplier.TypedData(new BytesRef(oldStr), oldStrType, "oldStr"),
                new TestCaseSupplier.TypedData(new BytesRef(newStr), newStrType, "newStr")
            ),
            "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
            DataTypes.KEYWORD,
            equalTo(new BytesRef(result))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Replace(source, args.get(0), args.get(1), args.get(2));
    }
}
