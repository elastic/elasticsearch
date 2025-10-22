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
import org.elasticsearch.common.network.NetworkDirectionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class NetworkDirectionTests extends AbstractScalarFunctionTestCase {
    public NetworkDirectionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // These tests copy the data from the NetworkDirectionUtils tests
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (var stringType : DataType.stringTypes()) {
            suppliers.addAll(
                List.of(
                    // CIDR tests
                    createTestCase("CIDR1", "10.0.1.1", "192.168.1.2", "10.0.0.0/8", stringType, NetworkDirectionUtils.DIRECTION_OUTBOUND),
                    createTestCase("CIDR2", "192.168.1.2", "10.0.1.1", "10.0.0.0/8", stringType, NetworkDirectionUtils.DIRECTION_INBOUND),
                    // Unspecified tests
                    createTestCase(
                        "Unspecified1",
                        "0.0.0.0",
                        "0.0.0.0",
                        "unspecified",
                        stringType,
                        NetworkDirectionUtils.DIRECTION_INTERNAL
                    ),
                    createTestCase("Unspecified2", "::", "::", "unspecified", stringType, NetworkDirectionUtils.DIRECTION_INTERNAL),
                    // Private network tests
                    createTestCase(
                        "Private1",
                        "192.168.1.1",
                        "192.168.1.2",
                        "private",
                        stringType,
                        NetworkDirectionUtils.DIRECTION_INTERNAL
                    ),
                    createTestCase("Private2", "10.0.1.1", "192.168.1.2", "private", stringType, NetworkDirectionUtils.DIRECTION_INTERNAL),
                    createTestCase(
                        "Private3",
                        "192.168.1.1",
                        "172.16.0.1",
                        "private",
                        stringType,
                        NetworkDirectionUtils.DIRECTION_INTERNAL
                    ),
                    createTestCase(
                        "Private4",
                        "192.168.1.1",
                        "fd12:3456:789a:1::1",
                        "private",
                        stringType,
                        NetworkDirectionUtils.DIRECTION_INTERNAL
                    ),
                    // Public tests
                    createTestCase("Public1", "192.168.1.1", "192.168.1.2", "public", stringType, NetworkDirectionUtils.DIRECTION_EXTERNAL),
                    createTestCase("Public2", "10.0.1.1", "192.168.1.2", "public", stringType, NetworkDirectionUtils.DIRECTION_EXTERNAL),
                    createTestCase("Public3", "192.168.1.1", "172.16.0.1", "public", stringType, NetworkDirectionUtils.DIRECTION_EXTERNAL),
                    createTestCase(
                        "Public4",
                        "192.168.1.1",
                        "fd12:3456:789a:1::1",
                        "public",
                        stringType,
                        NetworkDirectionUtils.DIRECTION_EXTERNAL
                    )
                )
            );
        }
        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    private static TestCaseSupplier createTestCase(
        String testName,
        String sourceIp,
        String destinationIp,
        String internalNetworks,
        DataType stringType,
        String expectedDirection
    ) {
        return new TestCaseSupplier(
            testName,
            List.of(DataType.IP, DataType.IP, stringType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP(sourceIp), DataType.IP, "source_ip"),
                    new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP(destinationIp), DataType.IP, "destination_ip"),
                    new TestCaseSupplier.TypedData(new BytesRef(internalNetworks), stringType, "internal_networks")
                ),
                "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1],"
                    + " networks=Attribute[channel=2]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(expectedDirection))
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new NetworkDirection(source, args.get(0), args.get(1), args.get(2));
    }
}
