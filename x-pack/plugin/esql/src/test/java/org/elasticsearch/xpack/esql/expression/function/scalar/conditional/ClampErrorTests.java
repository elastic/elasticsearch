/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ClampErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(ClampTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // TODO unsigned_long mixed with other numeric types resolves, but isn't declared until we decide how to support it
        return super.testCandidates(cases, valid).filter(signature -> isUnsignedLongNumericMix(signature) == false);
    }

    private static boolean isUnsignedLongNumericMix(List<DataType> signature) {
        boolean hasUnsignedLong = false;
        for (DataType type : signature) {
            if (type.isNumeric() == false) {
                return false;
            }
            hasUnsignedLong |= type == DataType.UNSIGNED_LONG;
        }
        return hasUnsignedLong;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Clamp(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        DataType field = signature.get(0);
        if (field == DataType.NULL) {
            return equalTo("'field' must not be null in clamp()");
        }
        if (isValidField(field) == false) {
            return typeErrorMessage(signature, 0, "boolean, date, date_nanos, ip, numeric, string or version");
        }
        for (int position = 1; position < signature.size(); position++) {
            if (isValidBound(field, signature.get(position)) == false) {
                return typeErrorMessage(signature, position, boundType(field));
            }
        }
        throw new IllegalStateException("Can't generate error message for these types, signature = " + signature);
    }

    private static boolean isValidField(DataType field) {
        return field.isNumeric()
            || field == DataType.BOOLEAN
            || field.isDate()
            || DataType.isString(field)
            || field == DataType.IP
            || field == DataType.VERSION;
    }

    private static boolean isValidBound(DataType field, DataType bound) {
        if (bound == DataType.NULL) {
            return true;
        }
        return bound.isNumeric() ? field.isNumeric() : bound.noText() == field.noText();
    }

    private static String boundType(DataType field) {
        if (field.isNumeric()) {
            return "numeric";
        }
        if (DataType.isString(field)) {
            return "string";
        }
        return field.noText().typeName();
    }
}
