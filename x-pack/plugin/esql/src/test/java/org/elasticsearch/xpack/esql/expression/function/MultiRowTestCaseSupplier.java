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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedDataSupplier;

/**
 * Extension of {@link TestCaseSupplier} that provided multi-row test cases.
 */
public final class MultiRowTestCaseSupplier {

    private MultiRowTestCaseSupplier() {}

    public static List<TypedDataSupplier> intCases(int minRows, int maxRows, int min, int max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0 <= max && 0 >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 ints>", () -> randomList(minRows, maxRows, () -> 0), DataType.INTEGER, false, true));
        }

        if (max != 0) {
            cases.add(
                new TypedDataSupplier("<" + max + " ints>", () -> randomList(minRows, maxRows, () -> max), DataType.INTEGER, false, true)
            );
        }

        if (min != 0 && min != max) {
            cases.add(
                new TypedDataSupplier("<" + min + " ints>", () -> randomList(minRows, maxRows, () -> min), DataType.INTEGER, false, true)
            );
        }

        int lower = Math.max(min, 1);
        int upper = Math.min(max, Integer.MAX_VALUE);
        if (lower < upper) {
            cases.add(
                new TypedDataSupplier(
                    "<positive ints>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomIntBetween(lower, upper)),
                    DataType.INTEGER,
                    false,
                    true
                )
            );
        }

        int lower1 = Math.max(min, Integer.MIN_VALUE);
        int upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(
                new TypedDataSupplier(
                    "<negative ints>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomIntBetween(lower1, upper1)),
                    DataType.INTEGER,
                    false,
                    true
                )
            );
        }

        if (min < 0 && max > 0) {
            cases.add(new TypedDataSupplier("<random ints>", () -> randomList(minRows, maxRows, () -> {
                if (includeZero) {
                    return ESTestCase.randomIntBetween(min, max);
                }
                return randomBoolean() ? ESTestCase.randomIntBetween(min, -1) : ESTestCase.randomIntBetween(1, max);
            }), DataType.INTEGER, false, true));
        }

        return cases;
    }

    public static List<TypedDataSupplier> longCases(int minRows, int maxRows, long min, long max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0 <= max && 0 >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 longs>", () -> randomList(minRows, maxRows, () -> 0L), DataType.LONG, false, true));
        }

        if (max != 0) {
            cases.add(
                new TypedDataSupplier("<" + max + " longs>", () -> randomList(minRows, maxRows, () -> max), DataType.LONG, false, true)
            );
        }

        if (min != 0 && min != max) {
            cases.add(
                new TypedDataSupplier("<" + min + " longs>", () -> randomList(minRows, maxRows, () -> min), DataType.LONG, false, true)
            );
        }

        long lower = Math.max(min, 1);
        long upper = Math.min(max, Long.MAX_VALUE);
        if (lower < upper) {
            cases.add(
                new TypedDataSupplier(
                    "<positive longs>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomLongBetween(lower, upper)),
                    DataType.LONG,
                    false,
                    true
                )
            );
        }

        long lower1 = Math.max(min, Long.MIN_VALUE);
        long upper1 = Math.min(max, -1);
        if (lower1 < upper1) {
            cases.add(
                new TypedDataSupplier(
                    "<negative longs>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomLongBetween(lower1, upper1)),
                    DataType.LONG,
                    false,
                    true
                )
            );
        }

        if (min < 0 && max > 0) {
            cases.add(new TypedDataSupplier("<random longs>", () -> randomList(minRows, maxRows, () -> {
                if (includeZero) {
                    return ESTestCase.randomLongBetween(min, max);
                }
                return randomBoolean() ? ESTestCase.randomLongBetween(min, -1) : ESTestCase.randomLongBetween(1, max);
            }), DataType.LONG, false, true));
        }

        return cases;
    }

    public static List<TypedDataSupplier> doubleCases(int minRows, int maxRows, double min, double max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        if (0d <= max && 0d >= min && includeZero) {
            cases.add(new TypedDataSupplier("<0 doubles>", () -> randomList(minRows, maxRows, () -> 0d), DataType.DOUBLE, false, true));
            cases.add(new TypedDataSupplier("<-0 doubles>", () -> randomList(minRows, maxRows, () -> -0d), DataType.DOUBLE, false, true));
        }

        if (max != 0d) {
            cases.add(
                new TypedDataSupplier("<" + max + " doubles>", () -> randomList(minRows, maxRows, () -> max), DataType.DOUBLE, false, true)
            );
        }

        if (min != 0d && min != max) {
            cases.add(
                new TypedDataSupplier("<" + min + " doubles>", () -> randomList(minRows, maxRows, () -> min), DataType.DOUBLE, false, true)
            );
        }

        double lower1 = Math.max(min, 0d);
        double upper1 = Math.min(max, 1d);
        if (lower1 < upper1) {
            cases.add(
                new TypedDataSupplier(
                    "<small positive doubles>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomDoubleBetween(lower1, upper1, true)),
                    DataType.DOUBLE,
                    false,
                    true
                )
            );
        }

        double lower2 = Math.max(min, -1d);
        double upper2 = Math.min(max, 0d);
        if (lower2 < upper2) {
            cases.add(
                new TypedDataSupplier(
                    "<small negative doubles>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomDoubleBetween(lower2, upper2, true)),
                    DataType.DOUBLE,
                    false,
                    true
                )
            );
        }

        double lower3 = Math.max(min, 1d);
        double upper3 = Math.min(max, Double.MAX_VALUE);
        if (lower3 < upper3) {
            cases.add(
                new TypedDataSupplier(
                    "<big positive doubles>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomDoubleBetween(lower3, upper3, true)),
                    DataType.DOUBLE,
                    false,
                    true
                )
            );
        }

        double lower4 = Math.max(min, -Double.MAX_VALUE);
        double upper4 = Math.min(max, -1d);
        if (lower4 < upper4) {
            cases.add(
                new TypedDataSupplier(
                    "<big negative doubles>",
                    () -> randomList(minRows, maxRows, () -> ESTestCase.randomDoubleBetween(lower4, upper4, true)),
                    DataType.DOUBLE,
                    false,
                    true
                )
            );
        }

        if (min < 0 && max > 0) {
            cases.add(new TypedDataSupplier("<random doubles>", () -> randomList(minRows, maxRows, () -> {
                if (includeZero) {
                    return ESTestCase.randomDoubleBetween(min, max, true);
                }
                return randomBoolean() ? ESTestCase.randomDoubleBetween(min, -1, true) : ESTestCase.randomDoubleBetween(1, max, true);
            }), DataType.DOUBLE, false, true));
        }

        return cases;
    }

    public static List<TypedDataSupplier> dateCases(int minRows, int maxRows) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        cases.add(
            new TypedDataSupplier(
                "<1970-01-01T00:00:00Z dates>",
                () -> randomList(minRows, maxRows, () -> 0L),
                DataType.DATETIME,
                false,
                true
            )
        );

        cases.add(
            new TypedDataSupplier(
                "<random dates>",
                // 1970-01-01T00:00:00Z - 2286-11-20T17:46:40Z
                () -> randomList(minRows, maxRows, () -> ESTestCase.randomLongBetween(0, 10 * (long) 10e11)),
                DataType.DATETIME,
                false,
                true
            )
        );

        cases.add(
            new TypedDataSupplier(
                "<far future dates>",
                // 2286-11-20T17:46:40Z - +292278994-08-17T07:12:55.807Z
                () -> randomList(minRows, maxRows, () -> ESTestCase.randomLongBetween(10 * (long) 10e11, Long.MAX_VALUE)),
                DataType.DATETIME,
                false,
                true
            )
        );

        cases.add(
            new TypedDataSupplier(
                "<near the end of time dates>",
                // very close to +292278994-08-17T07:12:55.807Z, the maximum supported millis since epoch
                () -> randomList(minRows, maxRows, () -> ESTestCase.randomLongBetween(Long.MAX_VALUE / 100 * 99, Long.MAX_VALUE)),
                DataType.DATETIME,
                false,
                true
            )
        );

        return cases;
    }

    public static List<TypedDataSupplier> booleanCases(int minRows, int maxRows) {
        return List.of(
            new TypedDataSupplier("<true booleans>", () -> randomList(minRows, maxRows, () -> true), DataType.BOOLEAN, false, true),
            new TypedDataSupplier("<false booleans>", () -> randomList(minRows, maxRows, () -> false), DataType.BOOLEAN, false, true),
            new TypedDataSupplier(
                "<random booleans>",
                () -> randomList(minRows, maxRows, ESTestCase::randomBoolean),
                DataType.BOOLEAN,
                false,
                true
            )
        );
    }

    public static List<TypedDataSupplier> ipCases(int minRows, int maxRows) {
        return List.of(
            new TypedDataSupplier(
                "<127.0.0.1 ips>",
                () -> randomList(minRows, maxRows, () -> new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")))),
                DataType.IP,
                false,
                true
            ),
            new TypedDataSupplier(
                "<v4 ips>",
                () -> randomList(minRows, maxRows, () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(true)))),
                DataType.IP,
                false,
                true
            ),
            new TypedDataSupplier(
                "<v6 ips>",
                () -> randomList(minRows, maxRows, () -> new BytesRef(InetAddressPoint.encode(ESTestCase.randomIp(false)))),
                DataType.IP,
                false,
                true
            )
        );
    }
}
