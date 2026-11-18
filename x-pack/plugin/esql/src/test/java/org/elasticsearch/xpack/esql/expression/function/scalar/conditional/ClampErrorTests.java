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
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ClampErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(ClampTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Clamp(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return expectedClampTypeErrorMatcher(signature, "clamp");
    }

    /**
     * The expected type error of {@code CLAMP}, {@code CLAMP_MIN} and {@code CLAMP_MAX}, which check the field and then
     * each bound in order.
     */
    static Matcher<String> expectedClampTypeErrorMatcher(List<DataType> signature, String functionName) {
        DataType field = signature.get(0);
        if (field == DataType.NULL) {
            return equalTo("'field' must not be null in " + functionName + "()");
        }
        if (isValidField(field) == false) {
            return typeErrorMessage(signature, 0, "boolean, date, date_nanos, ip, numeric, string or version");
        }
        for (int position = 1; position < signature.size(); position++) {
            DataType bound = signature.get(position);
            if (isValidBound(field, bound) == false) {
                return typeErrorMessage(signature, position, boundType(field));
            }
            if (bound.isNumeric() && (field == DataType.UNSIGNED_LONG) != (bound == DataType.UNSIGNED_LONG)) {
                return equalTo(
                    "first argument of ["
                        + sourceForSignature(signature)
                        + "] is ["
                        + field.typeName()
                        + "] and "
                        + TypeResolutions.ParamOrdinal.fromIndex(position).name().toLowerCase(Locale.ROOT)
                        + " argument is ["
                        + bound.typeName()
                        + "]. [unsigned_long] can only be operated on together with another [unsigned_long]"
                );
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
