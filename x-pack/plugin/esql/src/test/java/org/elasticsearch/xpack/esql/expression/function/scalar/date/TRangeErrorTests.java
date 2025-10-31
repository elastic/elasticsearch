/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.time.Instant;
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

    public void testGetRangeExceptions() {
        // Invalid offset type in single parameter mode
        expectThrows(InvalidArgumentException.class, equalTo("invalid time range for []: Unsupported offset type [BytesRef]"), () -> {
            TRange trange = new TRange(
                Source.EMPTY,
                Literal.keyword(Source.EMPTY, "invalid_offset"),
                null,
                new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME),
                EsqlTestUtils.configuration(StringUtils.EMPTY)
            );
            trange.surrogate();
        });

        // Invalid datetime string in two parameter mode
        expectThrows(
            InvalidArgumentException.class,
            equalTo(
                "invalid time range for []: TRANGE start_time_or_offset parameter must be a valid datetime string, got: "
                    + BytesRefs.toBytesRef("invalid_offset")
            ),
            () -> {
                TRange trange = new TRange(
                    Source.EMPTY,
                    Literal.keyword(Source.EMPTY, "invalid_offset"),
                    Literal.keyword(Source.EMPTY, "2024-01-01T12:00:00Z"),
                    new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME),
                    EsqlTestUtils.configuration(StringUtils.EMPTY)
                );
                trange.surrogate();
            }
        );

        // Start time after end time in two parameter mode
        expectThrows(
            InvalidArgumentException.class,
            equalTo("TRANGE rangeStart time [2024-01-01T12:00:00Z] must be before rangeEnd time [2024-01-01T10:00:00Z]"),
            () -> {
                TRange trange = new TRange(
                    Source.EMPTY,
                    Literal.keyword(Source.EMPTY, "2024-01-01T12:00:00Z"),
                    Literal.keyword(Source.EMPTY, "2024-01-01T10:00:00Z"),
                    new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME),
                    EsqlTestUtils.configuration(StringUtils.EMPTY)
                );
                trange.surrogate();
            }
        );

        // Unsupported value type in parseToInstant
        expectThrows(
            InvalidArgumentException.class,
            equalTo("invalid time range for []: Unsupported time value type [Double] for parameter [start_time_or_offset]"),
            () -> {
                TRange trange = new TRange(
                    Source.EMPTY,
                    Literal.fromDouble(Source.EMPTY, 123.45),
                    Literal.keyword(Source.EMPTY, "2024-01-01T12:00:00Z"),
                    new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME),
                    EsqlTestUtils.configuration(StringUtils.EMPTY)
                );
                trange.surrogate();
            }
        );

        // Invalid offset type in timeWithOffset
        expectThrows(InvalidArgumentException.class, equalTo("invalid time range for []: Unsupported offset type [Double]"), () -> {
            TRange trange = new TRange(
                Source.EMPTY,
                Literal.fromDouble(Source.EMPTY, 123.45),
                null,
                new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME),
                EsqlTestUtils.configuration(StringUtils.EMPTY)
            );
            trange.surrogate();
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        String fieldName = "@timestamp";
        FieldAttribute timestamp = new FieldAttribute(
            source,
            fieldName,
            DateEsField.dateEsField(fieldName, Collections.emptyMap(), true, DateEsField.TimeSeriesFieldType.NONE)
        );

        return new TRange(source, args.getFirst(), args.size() == 1 ? null : args.get(1), timestamp, EsqlTestUtils.TEST_CFG);
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
