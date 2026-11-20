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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Re2LikeTests extends AbstractScalarFunctionTestCase {
    public Re2LikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final Function<String, String> escapeString = str -> {
            for (String syntax : new String[] { "\\", ".", "+", "*", "?", "(", ")", "|", "[", "]", "{", "}", "^", "$" }) {
                str = str.replace(syntax, "\\" + syntax);
            }
            return str;
        };
        // Re2Like has no case-insensitive argument, an RE2 expression folds case with (?i) instead, and RE2's case folding
        // isn't the Java case mapping these cases build their expectations with
        List<TestCaseSupplier> cases = RegexMatchTestCases.buildCases(
            escapeString,
            () -> randomAlphaOfLength(1) + "?",
            RegexMatchTestCases.AUTOMATA_MATCH_EVALUATOR
        ).stream().filter(c -> c.name().contains("case insensitive") == false).toList();
        return parameterSuppliersFromTypedData(cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        String pattern = ((BytesRef) args.get(1).fold(FoldContext.small())).utf8ToString();
        return new Re2Like(source, args.get(0), new Re2Pattern(pattern));
    }
}
