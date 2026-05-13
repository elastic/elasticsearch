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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.Matchers.equalTo;

public class ReplaceTests extends AbstractScalarFunctionTestCase {
    public ReplaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : DataType.types()) {
            if (DataType.isString(strType) == false) {
                continue;
            }
            for (DataType oldStrType : DataType.types()) {
                if (DataType.isString(oldStrType) == false) {
                    continue;
                }
                for (DataType newStrType : DataType.types()) {
                    if (DataType.isString(newStrType) == false) {
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

        // Groups
        suppliers.add(fixedCase("Full group", "Cats are awesome", ".+", "<$0>", "<Cats are awesome>"));
        suppliers.add(
            fixedCase("Nested groups", "A cat is great, a cat is awesome", "\\b([Aa] (\\w+)) is (\\w+)\\b", "$1$2", "A catcat, a catcat")
        );
        suppliers.add(
            fixedCase(
                "Multiple groups",
                "Cats are awesome",
                "(\\w+) (.+)",
                "$0 -> $1 and dogs $2",
                "Cats are awesome -> Cats and dogs are awesome"
            )
        );

        // Errors
        suppliers.add(new TestCaseSupplier("syntax error", List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD), () -> {
            String text = randomAlphaOfLength(10);
            String invalidRegex = "[";
            String newStr = randomAlphaOfLength(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(invalidRegex), DataType.KEYWORD, "regex"),
                    new TestCaseSupplier.TypedData(new BytesRef(newStr), DataType.KEYWORD, "newStr")
                ),
                "ReplaceEvaluator[str=Attribute[channel=0], regex=Attribute[channel=1], newStr=Attribute[channel=2]]",
                DataType.KEYWORD,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning(
                    "Line 1:1: java.util.regex.PatternSyntaxException: Unclosed character class near index 0\n[\n^".replaceAll(
                        "\n",
                        System.lineSeparator()
                    )
                )
                .withFoldingException(
                    PatternSyntaxException.class,
                    "Unclosed character class near index 0\n[\n^".replaceAll("\n", System.lineSeparator())
                );
        }));
        // URL-domain byte-scan fast path: when the regex literal matches the
        // canonical ClickBench "extract referer host" shape and the replacement
        // literal is exactly "$1", Replace#toEvaluator dispatches to
        // ReplaceUrlDomainConstantEvaluator. These cases verify dispatch +
        // bit-for-bit correctness against the regex on common URL shapes and
        // on the no-match inputs the fast path is required to leave unchanged.
        suppliers.add(urlDomainCase("plain http", "http://example.com/path", "example.com"));
        suppliers.add(urlDomainCase("https with www", "https://www.example.com/path/to/page", "example.com"));
        suppliers.add(urlDomainCase("https no www", "https://example.org/", "example.org"));
        suppliers.add(urlDomainCase("http no www no path", "http://example.com", "http://example.com"));
        suppliers.add(urlDomainCase("no scheme", "ftp://example.com/", "ftp://example.com/"));
        suppliers.add(urlDomainCase("empty host", "http:///foo", "http:///foo"));
        suppliers.add(urlDomainCase("contains newline", "http://example.com/p\nq", "http://example.com/p\nq"));
        // Regex backtracking on (?:www\.)? — when the bytes immediately after
        // the scheme are "www.<slash>", the regex engine backtracks to let
        // "www." itself be the host capture. The fast path must mirror that.
        suppliers.add(urlDomainCase("www. is host, no path", "http://www./", "www."));
        suppliers.add(urlDomainCase("www. is host, with path", "http://www./foo", "www."));
        suppliers.add(urlDomainCase("www. is host, doubled", "http://www./www./foo", "www."));
        suppliers.add(urlDomainCase("www.www.host", "http://www.www.example.com/", "www.example.com"));
        // Documented divergence (see processUrlDomain Javadoc): a URL that
        // ends in a single line terminator. The regex engine matches up to
        // before the trailing terminator and returns host-plus-terminator,
        // while the fast path's up-front terminator gate returns the input
        // unchanged. Pinned here so the contract is visible.
        suppliers.add(
            urlDomainDivergenceCase(
                "trailing newline (documented divergence)",
                "http://example.com/path\n",
                "http://example.com/path\n",
                "example.com\n"
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    /**
     * Build a test case that forces the regex + replacement to literals so that
     * the URL-domain fast path in {@link Replace#toEvaluator} is selected, and
     * cross-checks the hand-written {@code expected} against what
     * {@link String#replaceAll} produces, so drift between the fast path and
     * the regex it stands in for surfaces at parameter-construction time.
     */
    private static TestCaseSupplier urlDomainCase(String name, String input, String expected) {
        String regexOutput = input.replaceAll(Replace.URL_DOMAIN_REGEX, "$1");
        if (regexOutput.equals(expected) == false) {
            throw new AssertionError(
                "URL-domain test expectation drifted from java.util.regex for input ["
                    + input
                    + "]: expected ["
                    + expected
                    + "] but regex produces ["
                    + regexOutput
                    + "]"
            );
        }
        return urlDomainCaseImpl(name, input, expected);
    }

    /**
     * Build a test case that pins a documented divergence between the fast
     * path and the regex: the fast path returns {@code expected} while the
     * regex returns {@code regexOutput}. The regex side is still cross-checked
     * so {@code regexOutput} stays accurate; only the cross-check against
     * {@code expected} is skipped.
     */
    private static TestCaseSupplier urlDomainDivergenceCase(String name, String input, String expected, String regexOutput) {
        String actualRegex = input.replaceAll(Replace.URL_DOMAIN_REGEX, "$1");
        if (actualRegex.equals(regexOutput) == false) {
            throw new AssertionError(
                "URL-domain divergence test regex-side expectation drifted for input ["
                    + input
                    + "]: declared regex output ["
                    + regexOutput
                    + "] but actual ["
                    + actualRegex
                    + "]"
            );
        }
        return urlDomainCaseImpl(name, input, expected);
    }

    private static TestCaseSupplier urlDomainCaseImpl(String name, String input, String expected) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(input), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(Replace.URL_DOMAIN_REGEX), DataType.KEYWORD, "regex").forceLiteral(),
                    new TestCaseSupplier.TypedData(new BytesRef("$1"), DataType.KEYWORD, "newStr").forceLiteral()
                ),
                "ReplaceUrlDomainConstantEvaluator[str=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expected))
            )
        );
    }

    private static TestCaseSupplier fixedCase(String name, String str, String oldStr, String newStr, String result) {
        return new TestCaseSupplier(
            name,
            List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD),
            () -> testCase(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD, str, oldStr, newStr, result)
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
            DataType.KEYWORD,
            equalTo(new BytesRef(result))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Replace(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: This function doesn't serialize the Source, and must be fixed.
        return expression;
    }
}
