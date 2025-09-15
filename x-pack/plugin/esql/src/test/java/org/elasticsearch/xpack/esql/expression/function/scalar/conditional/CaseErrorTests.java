/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CaseErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(CaseTests.parameters()).stream()
            // Take only the shorter signatures because we don't have error generation for longer cases
            // TODO handle longer signatures
            .filter(c -> c.types().size() < 4)
            .toList();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Case(source, args.get(0), args.subList(1, args.size()));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.get(0) != DataType.BOOLEAN && signature.get(0) != DataType.NULL) {
            return typeErrorMessage(signature, 0, "boolean");
        }
        DataType mainType = signature.get(1).noText();
        for (int i = 2; i < signature.size(); i++) {
            if (i % 2 == 0 && i != signature.size() - 1) {
                // condition
                if (signature.get(i) != DataType.BOOLEAN && signature.get(i) != DataType.NULL) {
                    return typeErrorMessage(signature, i, "boolean");
                }
            } else {
                // value
                if (signature.get(i).noText() != mainType) {
                    return typeErrorMessage(signature, i, mainType.typeName());
                }
            }
        }
        throw new IllegalStateException("can't find bad arg for " + signature);
    }

    private static Matcher<String> typeErrorMessage(List<DataType> signature, int badArgPosition, String expectedTypeString) {
        String ordinal = TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT);
        String sig = sourceForSignature(signature);
        String name = signature.get(badArgPosition).typeName();
        return equalTo(ordinal + " argument of [" + sig + "] must be [" + expectedTypeString + "], found value [] type [" + name + "]");
    }
}
