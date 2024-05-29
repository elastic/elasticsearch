/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class IpPrefixTests extends AbstractFunctionTestCase {
    public IpPrefixTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = List.of(
            // V4
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.INTEGER, DataTypes.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("1.2.3.4"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(24, DataTypes.INTEGER, "prefixLengthV4"),
                        new TestCaseSupplier.TypedData(ESTestCase.randomIntBetween(0, 128), DataTypes.INTEGER, "prefixLengthV6")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLengthV4=Attribute[channel=1], prefixLengthV6=Attribute[channel=2]]",
                    DataTypes.IP,
                    equalTo(EsqlDataTypeConverter.stringToIP("1.2.3.0"))
                )
            ),
            new TestCaseSupplier(List.of(DataTypes.IP, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
                var randomIp = randomIp(true);
                var randomPrefix = randomIntBetween(0, 32);
                var cidrString = InetAddresses.toCidrString(randomIp, randomPrefix);

                var ipParameter = EsqlDataTypeConverter.stringToIP(randomIp.getHostAddress());
                var expectedPrefix = EsqlDataTypeConverter.stringToIP(
                    InetAddresses.parseIpRangeFromCidr(cidrString).lowerBound().getHostAddress()
                );

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(ipParameter, DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(randomPrefix, DataTypes.INTEGER, "prefixLengthV4"),
                        new TestCaseSupplier.TypedData(ESTestCase.randomIntBetween(0, 128), DataTypes.INTEGER, "prefixLengthV6")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLengthV4=Attribute[channel=1], prefixLengthV6=Attribute[channel=2]]",
                    DataTypes.IP,
                    equalTo(expectedPrefix)
                );
            }),

            // V6
            new TestCaseSupplier(
                List.of(DataTypes.IP, DataTypes.INTEGER, DataTypes.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("::ff"), DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(ESTestCase.randomIntBetween(0, 32), DataTypes.INTEGER, "prefixLengthV4"),
                        new TestCaseSupplier.TypedData(127, DataTypes.INTEGER, "prefixLengthV6")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLengthV4=Attribute[channel=1], prefixLengthV6=Attribute[channel=2]]",
                    DataTypes.IP,
                    equalTo(EsqlDataTypeConverter.stringToIP("::fe"))
                )
            ),
            new TestCaseSupplier(List.of(DataTypes.IP, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
                var randomIp = randomIp(false);
                var randomPrefix = randomIntBetween(0, 128);
                var cidrString = InetAddresses.toCidrString(randomIp, randomPrefix);

                var ipParameter = EsqlDataTypeConverter.stringToIP(randomIp.getHostAddress());
                var expectedPrefix = EsqlDataTypeConverter.stringToIP(
                    InetAddresses.parseIpRangeFromCidr(cidrString).lowerBound().getHostAddress()
                );

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(ipParameter, DataTypes.IP, "ip"),
                        new TestCaseSupplier.TypedData(ESTestCase.randomIntBetween(0, 32), DataTypes.INTEGER, "prefixLengthV4"),
                        new TestCaseSupplier.TypedData(randomPrefix, DataTypes.INTEGER, "prefixLengthV6")
                    ),
                    "IpPrefixEvaluator[ip=Attribute[channel=0], prefixLengthV4=Attribute[channel=1], prefixLengthV6=Attribute[channel=2]]",
                    DataTypes.IP,
                    equalTo(expectedPrefix)
                );
            })
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new IpPrefix(source, args.get(0), args.get(1), args.size() == 3 ? args.get(2) : null);
    }
}
