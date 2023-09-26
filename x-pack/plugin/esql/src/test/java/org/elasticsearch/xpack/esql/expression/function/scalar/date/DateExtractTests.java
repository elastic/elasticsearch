/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DateExtractTests extends AbstractScalarFunctionTestCase {
    public DateExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Date Extract Year", () -> {
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("YEAR"), DataTypes.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(1687944333000L, DataTypes.DATETIME, "date")
                ),
                "DateExtractEvaluator[value=Attribute[channel=1], chronoField=Attribute[channel=0], zone=Z]",
                DataTypes.LONG,
                equalTo(2023L)
            );
        })));
    }

    public void testAllChronoFields() {
        long epochMilli = 1687944333123L;
        ZonedDateTime date = Instant.ofEpochMilli(epochMilli).atZone(EsqlTestUtils.TEST_CFG.zoneId());
        for (ChronoField value : ChronoField.values()) {
            DateExtract instance = new DateExtract(
                Source.EMPTY,
                new Literal(Source.EMPTY, new BytesRef(value.name()), DataTypes.KEYWORD),
                new Literal(Source.EMPTY, epochMilli, DataTypes.DATETIME),
                EsqlTestUtils.TEST_CFG
            );

            assertThat(instance.fold(), is(date.getLong(value)));
            assertThat(
                DateExtract.process(epochMilli, new BytesRef(value.name()), EsqlTestUtils.TEST_CFG.zoneId()),
                is(date.getLong(value))
            );
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateExtract(source, args.get(0), args.get(1), EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(DataTypes.DATETIME));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.LONG;
    }
}
