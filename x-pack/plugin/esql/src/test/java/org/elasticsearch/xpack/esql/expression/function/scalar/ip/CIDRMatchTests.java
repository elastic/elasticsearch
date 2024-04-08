/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class CIDRMatchTests extends AbstractScalarFunctionTestCase {
    public CIDRMatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        var suppliers = List.of(
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("192.168.0.0/16"), DataTypes.KEYWORD, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataTypes.BOOLEAN,
                    equalTo(true)
                )
            ),
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("192.168.0.0/16"), DataTypes.TEXT, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataTypes.BOOLEAN,
                    equalTo(true)
                )
            ),
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("10.0.0.0/16"), DataTypes.KEYWORD, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataTypes.BOOLEAN,
                    equalTo(false)
                )
            ),
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("10.0.0.0/16"), DataTypes.TEXT, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataTypes.BOOLEAN,
                    equalTo(false)
                )
            )
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new CIDRMatch(source, args.get(0), List.of(args.get(1)));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(DataTypes.IP), required(strings()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }
}
