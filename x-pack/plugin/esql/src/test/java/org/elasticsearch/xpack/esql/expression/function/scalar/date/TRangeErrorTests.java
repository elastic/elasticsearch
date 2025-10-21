/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TRangeErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    private static final String ONE_PARAM_TYPE_ERROR_STRING = "time_duration or date_period";
    private static final String TWO_PARAM_TYPE_ERROR_STRING = "string, long, date or date_nanos";

    @Override
    protected List<TestCaseSupplier> cases() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // one parameter
        suppliers.add(new TestCaseSupplier(List.of(DataType.TIME_DURATION), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_PERIOD), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.KEYWORD), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS), () -> null));

        // two parameters
        suppliers.add(new TestCaseSupplier(List.of(DataType.KEYWORD, DataType.KEYWORD), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, DataType.LONG), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, DataType.DATETIME), () -> null));
        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS, DataType.DATE_NANOS), () -> null));

        return suppliers;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        String fieldName = "@timestamp";
        FieldAttribute timestamp = new FieldAttribute(
            source,
            fieldName,
            DateEsField.dateEsField(fieldName, Collections.emptyMap(), true, DateEsField.TimeSeriesFieldType.NONE)
        );

        if (args.size() == 1) {
            return new TRange(source, timestamp, args.getFirst(), null, EsqlTestUtils.TEST_CFG);
        }

        return new TRange(source, timestamp, args.get(0), args.get(1), EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(errorMessageStringForTRange(signature, validPerPosition, (l, p) -> {
            if (signature.size() == 1 && p == 0) {
                return ONE_PARAM_TYPE_ERROR_STRING;
            }

            if (p == 0) {
                return TWO_PARAM_TYPE_ERROR_STRING;
            }

            return signature.getFirst().esType();
        }));
    }

    private String errorMessageStringForTRange(
        List<DataType> signature,
        List<Set<DataType>> validPerPosition,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        for (int i = 0; i < signature.size(); i++) {
            if (signature.get(i) == DataType.NULL) {
                return TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT)
                    + " argument of ["
                    + sourceForSignature(signature)
                    + "] cannot be null, received []";
            }
        }

        if (signature.size() == 1) {
            if (validPerPosition.getFirst().contains(signature.getFirst()) == false) {
                return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
            }
        } else {
            // 2nd parameter must have the same type as the first (the 1st one is taken from signature to compare)
            validPerPosition = List.of(
                Set.of(DataType.KEYWORD, DataType.LONG, DataType.DATETIME, DataType.DATE_NANOS),
                Set.of(signature.getFirst())
            );
            for (int i = 0; i < signature.size(); i++) {
                if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                    return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
                }
            }
        }
        return "";
    }
}
