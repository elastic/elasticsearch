/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MvInRangeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private static final String SUPPORTED_TYPES = "a numeric, date, ip, version or string type";

    @Override
    protected List<TestCaseSupplier> cases() {
        // MvInRangeTests.parameters() already includes the null-typed signatures (a null field or bound resolves — it
        // folds to false — so those are valid, not type errors), which keeps the error harness from flagging them.
        return paramsToSuppliers(MvInRangeTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvInRange(source, args.get(0), args.get(1), args.get(2), args.size() > 3 ? args.get(3) : null);
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Don't fuzz the options position by type — options are validated by Options.resolve, not by positional type.
        return super.testCandidates(cases, valid).filter(sig -> sig.size() == 3 || sig.get(3) == DataType.UNSUPPORTED);
    }

    /**
     * Mirrors {@link MvInRange}'s resolveType: the field must be an ordered type; each bound must share the field's type
     * (a null field or a null bound resolves — it folds to {@code false} at runtime — so those signatures carry no
     * error message).
     */
    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType field = signature.get(0);
        if (field == DataType.NULL) {
            for (int p = 1; p <= 2; p++) {
                DataType bound = signature.get(p);
                if (bound != DataType.NULL && isSupported(bound) == false) {
                    return typeErrorMessage(signature, p, SUPPORTED_TYPES);
                }
            }
            // With a null field the bounds must agree with each other (keyword and text agree).
            DataType lower = signature.get(1);
            DataType upper = signature.get(2);
            if (lower != DataType.NULL && upper != DataType.NULL && lower.noText() != upper.noText()) {
                return typeErrorMessage(signature, 2, lower.noText().typeName());
            }
            return equalTo("");
        }
        if (isSupported(field) == false) {
            return typeErrorMessage(signature, 0, SUPPORTED_TYPES);
        }
        for (int p = 1; p <= 2; p++) {
            DataType bound = signature.get(p);
            if (bound != DataType.NULL && bound.noText() != field.noText()) {
                return typeErrorMessage(signature, p, field.noText().typeName());
            }
        }
        return equalTo("");
    }

    private static boolean isSupported(DataType dt) {
        return dt == DataType.INTEGER
            || dt == DataType.LONG
            || dt == DataType.DOUBLE
            || dt == DataType.UNSIGNED_LONG
            || dt == DataType.DATETIME
            || dt == DataType.DATE_NANOS
            || dt == DataType.IP
            || dt == DataType.VERSION
            || dt == DataType.KEYWORD
            || dt == DataType.TEXT;
    }
}
