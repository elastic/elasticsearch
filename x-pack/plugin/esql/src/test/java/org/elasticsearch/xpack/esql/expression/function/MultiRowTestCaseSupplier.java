/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedDataSupplier;

/**
 * Extension of {@link TestCaseSupplier} that provided multi-row test cases.
 */
public final class MultiRowTestCaseSupplier {

    private MultiRowTestCaseSupplier() {}

    public static List<TypedDataSupplier> intCases(int minRows, int maxRows, int min, int max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0 <= max && 0 >= min && includeZero) {
            addSuppliers(cases, minRows, maxRows, "0 int", DataType.INTEGER, () -> 0);
        }

        if (max != 0) {
            addSuppliers(cases, minRows, maxRows, max + " int", DataType.INTEGER, () -> max);
        }

        if (min != 0 && min != max) {
            addSuppliers(cases, minRows, maxRows, min + " int", DataType.INTEGER, () -> min);
        }

        int lower = Math.max(min, 1);
        int upper = Math.min(max, Integer.MAX_VALUE);
        if (lower < upper) {
            addSuppliers(cases, minRows, maxRows, "positive int", DataType.INTEGER, () -> ESTestCase.randomIntBetween(lower, upper));
        }

        int lower1 = Math.max(min, Integer.MIN_VALUE);
        int upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            addSuppliers(cases, minRows, maxRows, "negative int", DataType.INTEGER, () -> ESTestCase.randomIntBetween(lower1, upper1));
        }

        if (min < 0 && max > 0) {
            addSuppliers(cases, minRows, maxRows, "random int", DataType.INTEGER, () -> {
                if (includeZero) {
                    return ESTestCase.randomIntBetween(min, max);
                }
                return randomBoolean() ? ESTestCase.randomIntBetween(min, -1) : ESTestCase.randomIntBetween(1, max);
            });
        }

        return cases;
    }

    public static List<TypedDataSupplier> longCases(int minRows, int maxRows, long min, long max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0 <= max && 0 >= min && includeZero) {
            addSuppliers(cases, minRows, maxRows, "0 long", DataType.LONG, () -> 0L);
        }

        if (max != 0) {
            addSuppliers(cases, minRows, maxRows, max + " long", DataType.LONG, () -> max);
        }

        if (min != 0 && min != max) {
            addSuppliers(cases, minRows, maxRows, min + " long", DataType.LONG, () -> min);
        }

        long lower = Math.max(min, 1);
        long upper = Math.min(max, Long.MAX_VALUE);
        if (lower < upper) {
            addSuppliers(cases, minRows, maxRows, "positive long", DataType.LONG, () -> ESTestCase.randomLongBetween(lower, upper));
        }

        long lower1 = Math.max(min, Long.MIN_VALUE);
        long upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            addSuppliers(cases, minRows, maxRows, "negative long", DataType.LONG, () -> ESTestCase.randomLongBetween(lower1, upper1));
        }

        if (min < 0 && max > 0) {
            addSuppliers(cases, minRows, maxRows, "random long", DataType.LONG, () -> {
                if (includeZero) {
                    return ESTestCase.randomLongBetween(min, max);
                }
                return randomBoolean() ? ESTestCase.randomLongBetween(min, -1) : ESTestCase.randomLongBetween(1, max);
            });
        }

        return cases;
    }

    public static List<TypedDataSupplier> ulongCases(int minRows, int maxRows, BigInteger min, BigInteger max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        // Zero
        if (BigInteger.ZERO.compareTo(max) <= 0 && BigInteger.ZERO.compareTo(min) >= 0 && includeZero) {
            addSuppliers(cases, minRows, maxRows, "0 unsigned long", DataType.UNSIGNED_LONG, () -> BigInteger.ZERO);
        }

        // Small values, less than Long.MAX_VALUE
        BigInteger lower1 = min.max(BigInteger.ONE);
        BigInteger upper1 = max.min(BigInteger.valueOf(Long.MAX_VALUE));
        if (lower1.compareTo(upper1) < 0) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "small unsigned long",
                DataType.UNSIGNED_LONG,
                () -> ESTestCase.randomUnsignedLongBetween(lower1, upper1)
            );
        }

        // Big values, greater than Long.MAX_VALUE
        BigInteger lower2 = min.max(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        BigInteger upper2 = max.min(ESTestCase.UNSIGNED_LONG_MAX);
        if (lower2.compareTo(upper2) < 0) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "big unsigned long",
                DataType.UNSIGNED_LONG,
                () -> ESTestCase.randomUnsignedLongBetween(lower2, upper2)
            );
        }

        return cases;
    }

    public static List<TypedDataSupplier> doubleCases(int minRows, int maxRows, double min, double max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0d <= max && 0d >= min && includeZero) {
            addSuppliers(cases, minRows, maxRows, "0 double", DataType.DOUBLE, () -> 0d);
            addSuppliers(cases, minRows, maxRows, "-0 double", DataType.DOUBLE, () -> -0d);
        }

        if (max != 0d) {
            addSuppliers(cases, minRows, maxRows, max + " double", DataType.DOUBLE, () -> max);
        }

        if (min != 0d && min != max) {
            addSuppliers(cases, minRows, maxRows, min + " double", DataType.DOUBLE, () -> min);
        }

        double lower1 = Math.max(min, 0d);
        double upper1 = Math.min(max, 1d);
        if (lower1 < upper1) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "small positive double",
                DataType.DOUBLE,
                () -> ESTestCase.randomDoubleBetween(lower1, upper1, true)
            );
        }

        double lower2 = Math.max(min, -1d);
        double upper2 = Math.min(max, 0d);
        if (lower2 < upper2) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "small negative double",
                DataType.DOUBLE,
                () -> ESTestCase.randomDoubleBetween(lower2, upper2, true)
            );
        }

        double lower3 = Math.max(min, 1d);
        double upper3 = Math.min(max, Double.MAX_VALUE);
        if (lower3 < upper3) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "big positive double",
                DataType.DOUBLE,
                () -> ESTestCase.randomDoubleBetween(lower3, upper3, true)
            );
        }

        double lower4 = Math.max(min, -Double.MAX_VALUE);
        double upper4 = Math.min(max, -1d);
        if (lower4 < upper4) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "big negative double",
                DataType.DOUBLE,
                () -> ESTestCase.randomDoubleBetween(lower4, upper4, true)
            );
        }

        if (min < 0 && max > 0) {
            addSuppliers(cases, minRows, maxRows, "random double", DataType.DOUBLE, () -> {
                if (includeZero) {
                    return ESTestCase.randomDoubleBetween(min, max, true);
                }
                return randomBoolean() ? ESTestCase.randomDoubleBetween(min, -1, true) : ESTestCase.randomDoubleBetween(1, max, true);
            });
        }

        return cases;
    }

    public static List<TypedDataSupplier> dateCases(int minRows, int maxRows) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(cases, minRows, maxRows, "1970-01-01T00:00:00Z date", DataType.DATETIME, () -> 0L);

        // 1970-01-01T00:00:00Z - 2286-11-20T17:46:40Z
        addSuppliers(cases, minRows, maxRows, "random date", DataType.DATETIME, () -> ESTestCase.randomLongBetween(0, 10 * (long) 10e11));

        // 2286-11-20T17:46:40Z - +292278994-08-17T07:12:55.807Z
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "far future date",
            DataType.DATETIME,
            () -> ESTestCase.randomLongBetween(10 * (long) 10e11, Long.MAX_VALUE)
        );

        // Very close to +292278994-08-17T07:12:55.807Z, the maximum supported millis since epoch
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "near the end of time date",
            DataType.DATETIME,
            () -> ESTestCase.randomLongBetween(Long.MAX_VALUE / 100 * 99, Long.MAX_VALUE)
        );

        return cases;
    }

    public static List<TypedDataSupplier> booleanCases(int minRows, int maxRows) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(cases, minRows, maxRows, "true boolean", DataType.BOOLEAN, () -> true);
        addSuppliers(cases, minRows, maxRows, "false boolean", DataType.BOOLEAN, () -> false);
        addSuppliers(cases, minRows, maxRows, "random boolean", DataType.BOOLEAN, ESTestCase::randomBoolean);

        return cases;
    }

    public static List<TypedDataSupplier> ipCases(int minRows, int maxRows) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(
            cases,
            minRows,
            maxRows,
            "127.0.0.1 ip",
            DataType.IP,
            () -> new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")))
        );
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "random v4 ip",
            DataType.IP,
            () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(true)))
        );
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "random v6 ip",
            DataType.IP,
            () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(false)))
        );

        return cases;
    }

    public static List<TypedDataSupplier> versionCases(int minRows, int maxRows) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(
            cases,
            minRows,
            maxRows,
            "major version",
            DataType.VERSION,
            () -> new Version(Integer.toString(ESTestCase.between(0, 100))).toBytesRef()
        );
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "major.minor version",
            DataType.VERSION,
            () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100)).toBytesRef()
        );
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "major.minor.patch version",
            DataType.VERSION,
            () -> new Version(ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100) + "." + ESTestCase.between(0, 100)).toBytesRef()
        );

        return cases;
    }

    public static List<TypedDataSupplier> geoPointCases(int minRows, int maxRows, boolean withAltitude) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(
            cases,
            minRows,
            maxRows,
            "<no alt geo_point>",
            DataType.GEO_POINT,
            () -> GEO.asWkb(GeometryTestUtils.randomPoint(false))
        );

        if (withAltitude) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "<with alt geo_point>",
                DataType.GEO_POINT,
                () -> GEO.asWkb(GeometryTestUtils.randomPoint(true))
            );
        }

        return cases;
    }

    public static List<TypedDataSupplier> cartesianPointCases(int minRows, int maxRows, boolean withAltitude) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(
            cases,
            minRows,
            maxRows,
            "<no alt cartesian_point>",
            DataType.CARTESIAN_POINT,
            () -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint(false))
        );

        if (withAltitude) {
            addSuppliers(
                cases,
                minRows,
                maxRows,
                "<with alt cartesian_point>",
                DataType.CARTESIAN_POINT,
                () -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint(true))
            );
        }

        return cases;
    }

    public static List<TypedDataSupplier> stringCases(int minRows, int maxRows, DataType type) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        addSuppliers(cases, minRows, maxRows, "empty " + type, type, () -> new BytesRef(""));
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "short alpha " + type,
            type,
            () -> new BytesRef(ESTestCase.randomAlphaOfLengthBetween(1, 30))
        );
        addSuppliers(
            cases,
            minRows,
            maxRows,
            "short unicode " + type,
            type,
            () -> new BytesRef(ESTestCase.randomRealisticUnicodeOfLengthBetween(1, 30))
        );

        if (minRows <= 100) {
            var longStringsMaxRows = Math.min(maxRows, 100);

            addSuppliers(
                cases,
                minRows,
                longStringsMaxRows,
                "long alpha " + type,
                type,
                () -> new BytesRef(ESTestCase.randomAlphaOfLengthBetween(300, 1000))
            );
            addSuppliers(
                cases,
                minRows,
                longStringsMaxRows,
                "long unicode " + type,
                type,
                () -> new BytesRef(ESTestCase.randomRealisticUnicodeOfLengthBetween(300, 1000))
            );
        }

        return cases;
    }

    private static <T> void addSuppliers(
        List<TypedDataSupplier> cases,
        int minRows,
        int maxRows,
        String name,
        DataType type,
        Supplier<T> valueSupplier
    ) {
        if (minRows <= 1 && maxRows >= 1) {
            cases.add(new TypedDataSupplier("<single " + name + ">", () -> randomList(1, 1, valueSupplier), type, false, true));
        }

        if (maxRows > 1) {
            cases.add(
                new TypedDataSupplier("<" + name + "s>", () -> randomList(Math.max(2, minRows), maxRows, valueSupplier), type, false, true)
            );
        }
    }
}
