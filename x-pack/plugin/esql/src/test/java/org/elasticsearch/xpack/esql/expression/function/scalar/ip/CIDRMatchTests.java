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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@FunctionName("cidr_match")
public class CIDRMatchTests extends AbstractScalarFunctionTestCase {
    public CIDRMatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        var suppliers = List.of(
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataType.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("192.168.0.0/16"), DataType.KEYWORD, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataType.BOOLEAN,
                    equalTo(true)
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataType.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("192.168.0.0/16"), DataType.TEXT, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataType.BOOLEAN,
                    equalTo(true)
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataType.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("10.0.0.0/16"), DataType.KEYWORD, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataType.BOOLEAN,
                    equalTo(false)
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.0.10"), DataType.IP, "ip"),
                        new TestCaseSupplier.TypedData(new BytesRef("10.0.0.0/16"), DataType.TEXT, "cidrs")
                    ),
                    "CIDRMatchEvaluator[ip=Attribute[channel=0], cidrs=[Attribute[channel=1]]]",
                    DataType.BOOLEAN,
                    equalTo(false)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new CIDRMatch(source, args.get(0), List.of(args.get(1)));
    }
}
