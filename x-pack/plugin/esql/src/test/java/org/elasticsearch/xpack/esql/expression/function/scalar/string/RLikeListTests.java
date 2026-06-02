/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePatternList;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class RLikeListTests extends AbstractScalarFunctionTestCase {
    public RLikeListTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final Function<String, String> escapeString = str -> {
            for (String syntax : new String[] { "\\", ".", "?", "+", "*", "|", "{", "}", "[", "]", "(", ")", "\"", "<", ">", "#", "&" }) {
                str = str.replace(syntax, "\\" + syntax);
            }
            return str;
        };
        return parameterSuppliersFromTypedData(
            RegexMatchTestCases.buildCases(escapeString, () -> randomAlphaOfLength(1) + "?", RegexMatchTestCases.AUTOMATA_MATCH_EVALUATOR)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return buildRLikeList(logger, source, args);
    }

    static Expression buildRLikeList(Logger logger, Source source, List<Expression> args) {
        Expression expression = args.get(0);
        Literal pattern = (Literal) args.get(1);
        Literal caseInsensitive = args.size() > 2 ? (Literal) args.get(2) : null;
        String patternString = ((BytesRef) pattern.fold(FoldContext.small())).utf8ToString();
        boolean caseInsensitiveBool = caseInsensitive != null ? (boolean) caseInsensitive.fold(FoldContext.small()) : false;
        logger.info("pattern={} caseInsensitive={}", patternString, caseInsensitiveBool);

        return caseInsensitiveBool
            ? new RLikeList(source, expression, new RLikePatternList(List.of(new RLikePattern(patternString))), true)
            : (randomBoolean()
                ? new RLikeList(source, expression, new RLikePatternList(List.of(new RLikePattern(patternString))))
                : new RLikeList(source, expression, new RLikePatternList(List.of(new RLikePattern(patternString))), false));
    }

    @Override
    protected void filterCoAndContraVarianceNarrowing(Map<Integer, DataType> positionNarrowing, List<TestCaseSupplier.TypedData> data) {
        positionNarrowing.entrySet().removeIf(e -> e.getKey() > 0 && e.getValue() == DataType.NULL);
    }
}
