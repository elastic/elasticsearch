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

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.core.IsEqual.equalTo;

public class NetworkDirectionTests extends AbstractScalarFunctionTestCase {
    public NetworkDirectionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // These tests copy the data from the NetworkDirectionUtils tests
        var suppliers = List.of(
            // CIDR tests
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("10.0.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("10.0.0.0/8"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INBOUND))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("10.0.1.1"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("10.0.0.0/8"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_OUTBOUND))
                )
            ),
            // Unspecified tests
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("0.0.0.0"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("0.0.0.0"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("unspecified"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("::"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("::"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("unspecified"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            // Private network tests
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("private"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("10.0.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("private"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("172.16.0.1"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("private"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("fd12:3456:789a:1::1"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("private"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_INTERNAL))
                )
            ),
            // Public tests
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("public"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_EXTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("10.0.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.2"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("public"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_EXTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("172.16.0.1"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("public"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_EXTERNAL))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.IP, DataType.IP, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("192.168.1.1"), DataType.IP, "source_ip"),
                        new TestCaseSupplier.TypedData(EsqlDataTypeConverter.stringToIP("fd12:3456:789a:1::1"), DataType.IP, "destination_ip"),
                        new TestCaseSupplier.TypedData(List.of("public"), DataType.KEYWORD, "internal_networks")
                    ),
                    "NetworkDirectionEvaluator[sourceIp=Attribute[channel=0], destinationIp=Attribute[channel=1], networks=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef(NetworkDirectionUtils.DIRECTION_EXTERNAL))
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new NetworkDirection(source, args.get(0), args.get(1), args.get(2));
    }
}
