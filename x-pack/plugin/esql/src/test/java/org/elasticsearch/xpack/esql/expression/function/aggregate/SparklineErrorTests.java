/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.validFunctionParameters;
import static org.hamcrest.Matchers.equalTo;

public class SparklineErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(SparklineTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sparkline(source, new Values(Source.EMPTY, args.get(0)), args.get(1), args.get(2), args.get(3), args.get(4));
    }

    public void testValidSignaturesResolve() {
        for (TestCaseSupplier supplier : cases()) {
            List<DataType> signature = supplier.types();
            if (signature.contains(DataType.NULL)) {
                continue;
            }
            List<Expression> args = signature.stream().<Expression>map(type -> randomLiteral(type)).toList();
            Expression expression = build(Source.synthetic(sourceForSignature(signature)), args);
            assertTrue("expected resolved " + signature + " but got " + expression.typeResolved(), expression.typeResolved().resolved());
        }
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Five parameters are too many for all permutations, so every valid signature is checked with one argument replaced by each type.
        return valid.stream()
            .flatMap(
                signature -> IntStream.range(0, signature.size())
                    .boxed()
                    .flatMap(position -> validFunctionParameters().map(type -> replace(signature, position, type)))
            )
            .distinct()
            .filter(signature -> valid.contains(signature) == false)
            .filter(signature -> new Values(Source.EMPTY, randomLiteral(signature.get(0))).typeResolved().resolved())
            // TODO long and unsigned_long buckets pass resolveType but are not published
            // https://github.com/elastic/elasticsearch/issues/163944
            .filter(signature -> signature.get(2) != DataType.LONG && signature.get(2) != DataType.UNSIGNED_LONG);
    }

    private static List<DataType> replace(List<DataType> signature, int position, DataType type) {
        List<DataType> replaced = new ArrayList<>(signature);
        replaced.set(position, type);
        return replaced;
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (validPerPosition.get(0).contains(signature.get(0)) == false) {
            return equalTo(
                "first argument of ["
                    + sourceForSignature(signature)
                    + "] must be [integer or long or double], found value [] type ["
                    + signature.get(0).noText().typeName()
                    + "]"
            );
        }
        String[] expectedTypes = { null, "date", "integer", "date or keyword or text", "date or keyword or text" };
        for (int i = 1; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i))) {
                continue;
            }
            if (signature.get(i) == DataType.NULL) {
                return equalTo(
                    TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT)
                        + " argument of ["
                        + sourceForSignature(signature)
                        + "] cannot be null, received []"
                );
            }
            return typeErrorMessage(signature, i, expectedTypes[i]);
        }
        throw new IllegalStateException("can't make error message for " + signature);
    }
}
