/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@FunctionName("ip_prefix")
public class IpPrefixTests extends AbstractFunctionTestCase {
    public IpPrefixTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        var suppliers = List.of(
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("1.2.3.4"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(24, DataTypes.INTEGER, "prefixLength")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLength=Attribute[channel=1]]",
                    DataTypes.IP,
                    equalTo(EsqlDataTypeConverter.stringToIP("1.2.3.0"))
                )
            ),
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("::ff"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(127, DataTypes.INTEGER, "prefixLength")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLength=Attribute[channel=1]]",
                    DataTypes.IP,
                    equalTo(EsqlDataTypeConverter.stringToIP("::fe"))
                )
            )
        );

        return parameterSuppliersFromTypedData(anyNullIsNull(true, suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new IpPrefix(source, args.get(0), args.get(1));
    }
}
