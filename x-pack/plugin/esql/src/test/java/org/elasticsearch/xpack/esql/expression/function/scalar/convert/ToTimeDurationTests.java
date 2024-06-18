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

@FunctionName("to_timeduration")
public class ToTimeDurationTests extends AbstractFunctionTestCase {
    public ToTimeDurationTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
            validTimeDuration(DataType.TIME_DURATION),
            DataType.TIME_DURATION,
            bytesRef -> EsqlDataTypeConverter.parseTemporalAmount(((BytesRef) bytesRef).utf8ToString(), DataType.TIME_DURATION),
            List.of()
        );

        for (DataType inputType : AbstractConvertFunction.STRING_TYPES) {
            TestCaseSupplier.unary(
                suppliers,
                read,
                validTimeDuration(inputType),
                DataType.TIME_DURATION,
                bytesRef -> EsqlDataTypeConverter.parseTemporalAmount(((BytesRef) bytesRef).utf8ToString(), DataType.TIME_DURATION),
                List.of()
            );
            suppliers.add(new TestCaseSupplier(List.of(inputType), () -> {
                BytesRef l = (BytesRef) randomLiteral(inputType).value();
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(l, inputType, "invalid time_duration")),
                    read,
                    DataType.TIME_DURATION,
                    is(nullValue())
                ).withFoldingException(ParsingException.class, "line -1:0: Cannot parse [" + l.utf8ToString() + "] to TIME_DURATION");
            }));
        }

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToTimeDuration(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> validTimeDuration(DataType dataType) {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("1 millisecond", () -> new BytesRef("1 millisecond"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 ms", () -> new BytesRef("1 ms"), dataType),
            new TestCaseSupplier.TypedDataSupplier("10 milliseconds", () -> new BytesRef("10 milliseconds"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 second", () -> new BytesRef("1 second"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 s", () -> new BytesRef("1 s"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 sec", () -> new BytesRef("1 sec"), dataType),
            new TestCaseSupplier.TypedDataSupplier("5 seconds", () -> new BytesRef("5 seconds"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 minute", () -> new BytesRef("1 minute"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 min", () -> new BytesRef("1 min"), dataType),
            new TestCaseSupplier.TypedDataSupplier("12 minutes", () -> new BytesRef("12 minutes"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 hour", () -> new BytesRef("1 hour"), dataType),
            new TestCaseSupplier.TypedDataSupplier("1 h", () -> new BytesRef("1 h"), dataType),
            new TestCaseSupplier.TypedDataSupplier("9 hours", () -> new BytesRef("9 hours"), dataType)
        );
    }
}
