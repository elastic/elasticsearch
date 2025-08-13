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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class DecayTests extends AbstractScalarFunctionTestCase {

    public DecayTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> testCaseSuppliers = new ArrayList<>();

        // Int
        testCaseSuppliers.addAll(intTestCase(5, 10, 10, 0, 0.5, "linear", 0.75));

        // Long
        testCaseSuppliers.addAll(longTestCase(5L, 10L, 10L, 0L, 0.5, "linear", 0.75));

        // Double
        testCaseSuppliers.addAll(doubleTestCase(5.0, 10.0, 10.0, 0.0, 0.5, "linear", 0.75));

        // GeoPoint
        testCaseSuppliers.addAll(geoPointTestCase("POINT (1 1)", "POINT (1 1)", "200km", "0km", 0.5, "linear", 1.0));
        testCaseSuppliers.addAll(geoPointOffsetKeywordTestCase("POINT (1 1)", "POINT (1 1)", "200km", "0km", 0.5, "linear", 1.0));

        // CartesianPoint
        testCaseSuppliers.addAll(cartesianPointTestCase("POINT (5 5)", "POINT (0 0)", "10m", "0m", 0.25, "linear", 0.46966991411008935));

        // Datetime
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2023, 1, 1, 12, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2023, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                "24 hours",
                "0 seconds",
                0.5,
                "linear",
                0.75
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2023, 1, 1, 12, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2023, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                "24 hours",
                Period.ofDays(0),
                0.5,
                "linear",
                0.75
            )
        );
        testCaseSuppliers.addAll(
            datetimeTestCase(
                LocalDateTime.of(2023, 1, 1, 12, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                LocalDateTime.of(2023, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                "24 hours",
                Duration.ofDays(0),
                0.5,
                "linear",
                0.75
            )
        );

        // DateNanos
        var dateOne = LocalDateTime.of(2023, 1, 1, 12, 0, 0).atZone(ZoneId.systemDefault()).toInstant();
        var dateTwo = LocalDateTime.of(2023, 1, 1, 0, 0, 0).atZone(ZoneId.systemDefault()).toInstant();

        testCaseSuppliers.addAll(
            dateNanosScaleTextTestCase(
                dateOne.getEpochSecond() * 1_000_000_000L + dateOne.getNano(),
                dateTwo.getEpochSecond() * 1_000_000_000L + dateTwo.getNano(),
                "24 hours",
                "0 seconds",
                0.5,
                "linear",
                0.75
            )
        );
        testCaseSuppliers.addAll(
            dateNanosScaleKeywordTestCase(
                dateOne.getEpochSecond() * 1_000_000_000L + dateOne.getNano(),
                dateTwo.getEpochSecond() * 1_000_000_000L + dateTwo.getNano(),
                "24 hours",
                "0 seconds",
                0.5,
                "linear",
                0.75
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                dateOne.getEpochSecond() * 1_000_000_000L + dateOne.getNano(),
                dateTwo.getEpochSecond() * 1_000_000_000L + dateTwo.getNano(),
                Period.ofDays(1),
                "0 seconds",
                0.5,
                "linear",
                0.75
            )
        );
        testCaseSuppliers.addAll(
            dateNanosTestCase(
                dateOne.getEpochSecond() * 1_000_000_000L + dateOne.getNano(),
                dateTwo.getEpochSecond() * 1_000_000_000L + dateTwo.getNano(),
                Duration.ofDays(1),
                "0 seconds",
                0.5,
                "linear",
                0.75
            )
        );

        return parameterSuppliersFromTypedData(testCaseSuppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Decay(source, args.get(0), args.get(1), args.get(2), args.get(3), args.get(4), args.get(5));
    }

    @Override
    public void testFold() {
        // TODO: double-check
        // Decay cannot be folded
    }

    private static List<TestCaseSupplier> intTestCase(
        int value,
        int origin,
        int scale,
        double offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.INTEGER, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.INTEGER, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.INTEGER, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.DOUBLE, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayIntEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> intTestCase(
        int value,
        int origin,
        int scale,
        int offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER, DataType.INTEGER, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.INTEGER, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.INTEGER, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.INTEGER, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.INTEGER, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
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
        double offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.LONG, DataType.LONG, DataType.DOUBLE, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.LONG, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.LONG, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.LONG, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.DOUBLE, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayLongEvaluator["),
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
        long offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.LONG, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.LONG, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.LONG, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.LONG, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
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
        double offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DOUBLE, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DOUBLE, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.DOUBLE, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.DOUBLE, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDoubleEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    // TODO: geo point
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
                List.of(DataType.GEO_POINT, DataType.GEO_POINT, DataType.TEXT, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(valueWkt), DataType.GEO_POINT, "value"),
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(originWkt), DataType.GEO_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
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
                List.of(DataType.GEO_POINT, DataType.GEO_POINT, DataType.TEXT, DataType.KEYWORD, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(valueWkt), DataType.GEO_POINT, "value"),
                        new TestCaseSupplier.TypedData(GEO.wktToWkb(originWkt), DataType.GEO_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.KEYWORD, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
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
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(
                    DataType.CARTESIAN_POINT,
                    DataType.CARTESIAN_POINT,
                    DataType.TEXT,
                    DataType.TEXT,
                    DataType.DOUBLE,
                    DataType.KEYWORD
                ),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(CARTESIAN.wktToWkb(valueWkt), DataType.CARTESIAN_POINT, "value"),
                        new TestCaseSupplier.TypedData(CARTESIAN.wktToWkb(originWkt), DataType.CARTESIAN_POINT, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
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
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATETIME, DataType.DATETIME, DataType.TEXT, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATETIME, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDatetimeEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> datetimeTestCase(
        long value,
        long origin,
        String scale,
        Period offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATETIME, DataType.DATETIME, DataType.TEXT, DataType.DATE_PERIOD, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATETIME, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.DATE_PERIOD, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDatetimeEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                ).withoutEvaluator()
            )
        );
    }

    private static List<TestCaseSupplier> datetimeTestCase(
        long value,
        long origin,
        String scale,
        Duration offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATETIME, DataType.DATETIME, DataType.TEXT, DataType.TIME_DURATION, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATETIME, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TIME_DURATION, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDatetimeEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                ).withoutEvaluator()
            )
        );
    }

    private static List<TestCaseSupplier> dateNanosScaleTextTestCase(
        long value,
        long origin,
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS, DataType.TEXT, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATE_NANOS, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATE_NANOS, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TEXT, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDateNanosEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> dateNanosScaleKeywordTestCase(
        long value,
        long origin,
        String scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS, DataType.KEYWORD, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATE_NANOS, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATE_NANOS, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.KEYWORD, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDateNanosEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                )
            )
        );
    }

    private static List<TestCaseSupplier> dateNanosTestCase(
        long value,
        long origin,
        Period scale,
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS, DataType.DATE_PERIOD, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATE_NANOS, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATE_NANOS, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.DATE_PERIOD, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDateNanosEvaluator["),
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
        String offset,
        double decay,
        String functionType,
        double expected
    ) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS, DataType.TIME_DURATION, DataType.TEXT, DataType.DOUBLE, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(value, DataType.DATE_NANOS, "value"),
                        new TestCaseSupplier.TypedData(origin, DataType.DATE_NANOS, "origin"),
                        new TestCaseSupplier.TypedData(scale, DataType.TIME_DURATION, "scale"),
                        new TestCaseSupplier.TypedData(offset, DataType.TEXT, "offset"),
                        new TestCaseSupplier.TypedData(decay, DataType.DOUBLE, "decay"),
                        new TestCaseSupplier.TypedData(functionType, DataType.KEYWORD, "type")
                    ),
                    startsWith("DecayDateNanosEvaluator["),
                    DataType.DOUBLE,
                    equalTo(expected)
                ).withoutEvaluator()
            )
        );
    }
}
