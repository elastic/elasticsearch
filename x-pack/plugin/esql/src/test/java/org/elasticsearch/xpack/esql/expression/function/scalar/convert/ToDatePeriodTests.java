/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@FunctionName("to_dateperiod")
public class ToDatePeriodTests extends AbstractFunctionTestCase {
    public ToDatePeriodTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.unary(
            suppliers,
            read,
            validDatePeriod(DataType.DATE_PERIOD),
            DataType.DATE_PERIOD,
            bytesRef -> EsqlDataTypeConverter.parseTemporalAmount(((BytesRef) bytesRef).utf8ToString(), DataType.DATE_PERIOD),
            List.of()
        );

        for (DataType inputType : AbstractConvertFunction.STRING_TYPES) {
            TestCaseSupplier.unary(
                suppliers,
                read,
                validDatePeriod(inputType),
                DataType.DATE_PERIOD,
                bytesRef -> EsqlDataTypeConverter.parseTemporalAmount(((BytesRef) bytesRef).utf8ToString(), DataType.DATE_PERIOD),
                List.of()
            );
            suppliers.add(new TestCaseSupplier(List.of(inputType), () -> {
                BytesRef l = (BytesRef) randomLiteral(inputType).value();
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(l, inputType, "invalid date_period")),
                    read,
                    DataType.DATE_PERIOD,
                    is(nullValue())
                ).withFoldingException(ParsingException.class, "line -1:0: Cannot parse [" + l.utf8ToString() + "] to DATE_PERIOD");
            }));
        }

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDatePeriod(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> validDatePeriod(DataType dataType) {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("1 day", () -> new BytesRef("1 day"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 d", () -> new BytesRef("1 d"), dataType),
            new TestCaseSupplier.TypedDataSupplier("10 days", () -> new BytesRef("10 days"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 week", () -> new BytesRef("1 week"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 w", () -> new BytesRef("1 w"), dataType),
            new TestCaseSupplier.TypedDataSupplier("5 weeks", () -> new BytesRef("5 weeks"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 month", () -> new BytesRef("1 month"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 mo", () -> new BytesRef("1 mo"), dataType),
            new TestCaseSupplier.TypedDataSupplier("12 months", () -> new BytesRef("12 months"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 quarter", () -> new BytesRef("1 quarter"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 q", () -> new BytesRef("1 q"), dataType),
            new TestCaseSupplier.TypedDataSupplier("9 quarters", () -> new BytesRef("9 quarters"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 year", () -> new BytesRef("1 year"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 y", () -> new BytesRef("1 y"), dataType),
            new TestCaseSupplier.TypedDataSupplier("8 years", () -> new BytesRef("8 years"), dataType)
        );
    }
}
