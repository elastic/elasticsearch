/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

// TODO: integration test case without options
public class DecayTests extends AbstractScalarFunctionTestCase {

    public DecayTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> testCaseSuppliers = new ArrayList<>();

        // Int Linear
        testCaseSuppliers.addAll(intTestCase(0, 0, 10, 5, 0.5, "linear", 1.0));
        testCaseSuppliers.addAll(intTestCase(10, 0, 10, 5, 0.5, "linear", 0.75));
        testCaseSuppliers.addAll(intTestCase(50, 5, 100, 10, 0.25, "linear", 0.7375));
        testCaseSuppliers.addAll(intTestCase(100, 17, 156, 23, 0.123, "linear", 0.6626923076923077));
        testCaseSuppliers.addAll(intTestCase(2500, 0, 10, 0, 0.5, "linear", 0.0));

        // Int Exponential
        testCaseSuppliers.addAll(intTestCase(0, 0, 10, 5, 0.5, "exp", 1.0));
        testCaseSuppliers.addAll(intTestCase(10, 0, 10, 5, 0.5, "exp", 0.7071067811865475));
        testCaseSuppliers.addAll(intTestCase(50, 5, 100, 10, 0.25, "exp", 0.6155722066724582));
        testCaseSuppliers.addAll(intTestCase(100, 17, 156, 23, 0.123, "exp", 0.4466460570185927));
        testCaseSuppliers.addAll(intTestCase(2500, 0, 10, 0, 0.5, "exp", 5.527147875260539E-76));

        // Int Gaussian
        testCaseSuppliers.addAll(intTestCase(0, 0, 10, 5, 0.5, "gauss", 1.0));
        testCaseSuppliers.addAll(intTestCase(10, 0, 10, 5, 0.5, "gauss", 0.8408964152537146));
        testCaseSuppliers.addAll(intTestCase(50, 5, 100, 10, 0.25, "gauss", 0.8438157961300179));
        testCaseSuppliers.addAll(intTestCase(100, 17, 156, 23, 0.123, "gauss", 0.7334501109633149));
        testCaseSuppliers.addAll(intTestCase(2500, 0, 10, 0, 0.5, "gauss", 0.0));

        // Long Linear
        testCaseSuppliers.addAll(longTestCase(0L, 10L, 10000000L, 200L, 0.33, "linear", 1.0));
        testCaseSuppliers.addAll(longTestCase(10L, 10L, 10000000L, 200L, 0.33, "linear", 1.0));
        testCaseSuppliers.addAll(longTestCase(50000L, 10L, 10000000L, 200L, 0.33, "linear", 0.99666407));
        testCaseSuppliers.addAll(longTestCase(300000L, 10L, 10000000L, 200L, 0.33, "linear", 0.97991407));
        testCaseSuppliers.addAll(longTestCase(123456789112123L, 10L, 10000000L, 200L, 0.33, "linear", 0.0));

        // Long Exponential
        testCaseSuppliers.addAll(longTestCase(0L, 10L, 10000000L, 200L, 0.33, "exp", 1.0));
        testCaseSuppliers.addAll(longTestCase(10L, 10L, 10000000L, 200L, 0.33, "exp", 1.0));
        testCaseSuppliers.addAll(longTestCase(50000L, 10L, 10000000L, 200L, 0.33, "exp", 0.9944951761701727));
        testCaseSuppliers.addAll(longTestCase(300000L, 10L, 10000000L, 200L, 0.33, "exp", 0.9673096701204178));
        testCaseSuppliers.addAll(longTestCase(123456789112123L, 10L, 10000000L, 200L, 0.33, "exp", 0.0));

        // Long Gaussian
        testCaseSuppliers.addAll(longTestCase(0L, 10L, 10000000L, 200L, 0.33, "gauss", 1.0));
        testCaseSuppliers.addAll(longTestCase(10L, 10L, 10000000L, 200L, 0.33, "gauss", 1.0));
        testCaseSuppliers.addAll(longTestCase(50000L, 10L, 10000000L, 200L, 0.33, "gauss", 0.999972516142306));
        testCaseSuppliers.addAll(longTestCase(300000L, 10L, 10000000L, 200L, 0.33, "gauss", 0.9990040963055015));
        testCaseSuppliers.addAll(longTestCase(123456789112123L, 10L, 10000000L, 200L, 0.33, "gauss", 0.0));

        // Double Linear
        testCaseSuppliers.addAll(doubleTestCase(0.0, 10.0, 10000000.0, 200.0, 0.25, "linear", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(10.0, 10.0, 10000000.0, 200.0, 0.25, "linear", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(50000.0, 10.0, 10000000.0, 200.0, 0.25, "linear", 0.99626575));
        testCaseSuppliers.addAll(doubleTestCase(300000.0, 10.0, 10000000.0, 200.0, 0.25, "linear", 0.97751575));
        testCaseSuppliers.addAll(doubleTestCase(123456789112.123, 10.0, 10000000.0, 200.0, 0.25, "linear", 0.0));

        // Double Exponential
        testCaseSuppliers.addAll(doubleTestCase(0.0, 10.0, 10000000.0, 200.0, 0.25, "exp", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(10.0, 10.0, 10000000.0, 200.0, 0.25, "exp", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(50000.0, 10.0, 10000000.0, 200.0, 0.25, "exp", 0.9931214069469289));
        testCaseSuppliers.addAll(doubleTestCase(300000.0, 10.0, 10000000.0, 200.0, 0.25, "exp", 0.959292046002994));
        testCaseSuppliers.addAll(doubleTestCase(123456789112.123, 10.0, 10000000.0, 200.0, 0.25, "exp", 0.0));

        // Double Gaussian
        testCaseSuppliers.addAll(doubleTestCase(0.0, 10.0, 10000000.0, 200.0, 0.25, "gauss", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(10.0, 10.0, 10000000.0, 200.0, 0.25, "gauss", 1.0));
        testCaseSuppliers.addAll(doubleTestCase(50000.0, 10.0, 10000000.0, 200.0, 0.25, "gauss", 0.9999656337419655));
        testCaseSuppliers.addAll(doubleTestCase(300000.0, 10.0, 10000000.0, 200.0, 0.25, "gauss", 0.9987548570291238));
        testCaseSuppliers.addAll(doubleTestCase(123456789112.123, 10.0, 10000000.0, 200.0, 0.25, "gauss", 0.0));

        // GeoPoint Linear
        testCaseSuppliers.addAll(geoPointTestCase("POINT (1.0 1.0)", "POINT (1 1)", "10000km", "10km", 0.33, "linear", 1.0));
        testCaseSuppliers.addAll(geoPointTestCase("POINT (0 0)", "POINT (1 1)", "10000km", "10km", 0.33, "linear", 0.9901342769495362));
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (12.3 45.6)", "POINT (1 1)", "10000km", "10km", 0.33, "linear", 0.6602313771587869)
        );
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (180.0 90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "linear", 0.33761373954395957)
        );
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (-180.0 -90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "linear", 0.32271359885955425)
        );

        // GeoPoint Exponential
        testCaseSuppliers.addAll(geoPointTestCase("POINT (1.0 1.0)", "POINT (1 1)", "10000km", "10km", 0.33, "exp", 1.0));
        testCaseSuppliers.addAll(geoPointTestCase("POINT (0 0)", "POINT (1 1)", "10000km", "10km", 0.33, "exp", 0.983807518295976));
        testCaseSuppliers.addAll(geoPointTestCase("POINT (12.3 45.6)", "POINT (1 1)", "10000km", "10km", 0.33, "exp", 0.5699412181941212));
        testCaseSuppliers.addAll(geoPointTestCase("POINT (180.0 90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "exp", 0.3341838411351592));
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (-180.0 -90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "exp", 0.32604509444656576)
        );

        // GeoPoint Gaussian
        testCaseSuppliers.addAll(geoPointTestCase("POINT (1.0 1.0)", "POINT (1 1)", "10000km", "10km", 0.33, "gauss", 1.0));
        testCaseSuppliers.addAll(geoPointTestCase("POINT (0 0)", "POINT (1 1)", "10000km", "10km", 0.33, "gauss", 0.9997596437370099));
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (12.3 45.6)", "POINT (1 1)", "10000km", "10km", 0.33, "gauss", 0.7519296165431535)
        );
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (180.0 90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "gauss", 0.33837227875395753)
        );
        testCaseSuppliers.addAll(
            geoPointTestCase("POINT (-180.0 -90.0)", "POINT (1 1)", "10000km", "10km", 0.33, "gauss", 0.3220953501115956)
        );

        // GeoPoint offset & scale as keywords
        testCaseSuppliers.addAll(geoPointTestCaseKeywordScale("POINT (1 1)", "POINT (1 1)", "200km", "0km", 0.5, "linear", 1.0));
        testCaseSuppliers.addAll(geoPointOffsetKeywordTestCase("POINT (1 1)", "POINT (1 1)", "200km", "0km", 0.5, "linear", 1.0));

        // CartesianPoint Linear
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (0 0)", "POINT (1 1)", 10000.0, 10.0, 0.33, "linear", 1.0));
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (1 1)", "POINT (1 1)", 10000.0, 10.0, 0.33, "linear", 1.0));
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (1000 2000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "linear", 0.8509433324420796)
        );
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (-2000 1000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "linear", 0.8508234552350306)
        );
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (10000 20000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "linear", 0.0));

        // CartesianPoint Exponential
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (0 0)", "POINT (1 1)", 10000.0, 10.0, 0.33, "exp", 1.0));
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (1 1)", "POINT (1 1)", 10000.0, 10.0, 0.33, "exp", 1.0));
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (1000 2000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "exp", 0.7814164075951677)
        );
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (-2000 1000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "exp", 0.7812614186677811)
        );
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (10000 20000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "exp", 0.0839287052363121)
        );

        // CartesianPoint Gaussian
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (0 0)", "POINT (1 1)", 10000.0, 10.0, 0.33, "gauss", 1.0));
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (1 1)", "POINT (1 1)", 10000.0, 10.0, 0.33, "gauss", 1.0));
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (1000 2000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "gauss", 0.9466060873472042)
        );
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (-2000 1000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "gauss", 0.9465225092376659)
        );
        testCaseSuppliers.addAll(
            cartesianPointTestCase("POINT (10000 20000)", "POINT (1 1)", 10000.0, 10.0, 0.33, "gauss", 0.003935602627423666)
        );

        // Datetime Linear
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.49569100000000005
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.37334900000000004
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.28202800000000006
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.0
            )
        );

        // Datetime Exponential
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.4340956586740692
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.3545406919498116
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.30481724812400407
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.01813481247808857
            )
        );

        // Datetime Gaussian
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.5335935393743785
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.3791426943809958
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.27996050542437345
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                5.025924031342025E-7
            )
        );

        // Datenanos Linear
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.49569100000000005
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.37334900000000004
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.28202800000000006
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "linear",
                0.0
            )
        );

        // Datenanos Exponential
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.4340956586740692
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.3545406919498116
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.30481724812400407
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "exp",
                0.01813481247808857
            )
        );

        // Datenanos Gaussian
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                1.0
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2020, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.5335935393743785
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(2025, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.3791426943809958
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1970, 8, 20, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                0.27996050542437345
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                LocalDateTime.of(1900, 12, 12, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2000, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                Duration.ofDays(10000),
                Duration.ofDays(10),
                0.33,
                "gauss",
                5.025924031342025E-7
            )
        );

        return parameterSuppliersFromTypedData(testCaseSuppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Decay(source, args.get(0), args.get(1), args.get(2), args.get(3) != null ? args.get(3) : null);
    }

    @Override
    public void testFold() {
        // Decay cannot be folded
    }

    private static List<TestCaseSupplier> intTestCase(
        int value,
        int origin,
        int scale,
        Integer offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.INTEGER, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.INTEGER, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.INTEGER, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayIntEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> longTestCase(
        long value,
        long origin,
        long scale,
        Long offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.LONG, DataType.LONG, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.LONG, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.LONG, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.LONG, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayLongEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> doubleTestCase(
        double value,
        double origin,
        double scale,
        Double offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DOUBLE, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DOUBLE, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.DOUBLE, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayDoubleEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> geoPointTestCase(
        String valueWkt,
        String originWkt,
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.GEO_POINT, DataType.GEO_POINT, DataType.TEXT, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(valueWkt), DataType.GEO_POINT, "value"),
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(originWkt), DataType.GEO_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayGeoPointEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> geoPointTestCaseKeywordScale(
        String valueWkt,
        String originWkt,
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.GEO_POINT, DataType.GEO_POINT, DataType.KEYWORD, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(valueWkt), DataType.GEO_POINT, "value"),
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(originWkt), DataType.GEO_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.KEYWORD, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayGeoPointEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> geoPointOffsetKeywordTestCase(
        String valueWkt,
        String originWkt,
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.GEO_POINT, DataType.GEO_POINT, DataType.TEXT, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(valueWkt), DataType.GEO_POINT, "value"),
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(originWkt), DataType.GEO_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayGeoPointEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> cartesianPointTestCase(
        String valueWkt,
        String originWkt,
        double scale,
        double offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.CARTESIAN_POINT, DataType.CARTESIAN_POINT, DataType.DOUBLE, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(CARTESIAN.wktToWkb(valueWkt), DataType.CARTESIAN_POINT, "value"),
                        new TestCaseSupplier.TypedData(CARTESIAN.wktToWkb(originWkt), DataType.CARTESIAN_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.DOUBLE, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayCartesianPointEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> datetimeTestCase(
        long value,
        long origin,
        Duration scale,
        Duration offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATETIME, DataType.DATETIME, DataType.TIME_DURATION, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATETIME, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TIME_DURATION, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayDatetimeEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                ).withoutEvaluator()
            )
        );
    }

    private static List<TestCaseSupplier> dateNanosTestCase(
        long value,
        long origin,
        Duration scale,
        Duration offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS, DataType.TIME_DURATION, DataType.SOURCE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATE_NANOS, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATE_NANOS, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TIME_DURATION, "scale"),
                        new TestCaseSupplier.TypedData(createOptionsMap(offset, decay, functionType), DataType.SOURCE, "options")
                            .forceLiteral()
                    ),
                    startsWith("DecayDateNanosEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                ).withoutEvaluator()
            )
        );
    }

    private static MapExpression createOptionsMap(Object offset, double decay, String functionType) {
        List<Expression> keyValuePairs = new ArrayList<>();

        // Offset
        keyValuePairs.add(Literal.keyword(Source.EMPTY, "offset"));
        if (offset instanceof Integer) {
            keyValuePairs.add(Literal.integer(Source.EMPTY, (Integer) offset));
        } else if (offset instanceof Long) {
            keyValuePairs.add(Literal.fromLong(Source.EMPTY, (Long) offset));
        } else if (offset instanceof Double) {
            keyValuePairs.add(Literal.fromDouble(Source.EMPTY, (Double) offset));
        } else if (offset instanceof String) {
            keyValuePairs.add(Literal.text(Source.EMPTY, (String) offset));
        } else if (offset instanceof Duration) {
            keyValuePairs.add(Literal.timeDuration(Source.EMPTY, (Duration) offset));
        }

        // Decay
        keyValuePairs.add(Literal.keyword(Source.EMPTY, "decay"));
        keyValuePairs.add(Literal.fromDouble(Source.EMPTY, decay));

        // Type
        keyValuePairs.add(Literal.keyword(Source.EMPTY, "type"));
        keyValuePairs.add(Literal.keyword(Source.EMPTY, functionType));

        return new MapExpression(Source.EMPTY, keyValuePairs);
    }
}
