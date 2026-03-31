/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedDataSupplier;

/**
 * Extension of {@link TestCaseSupplier} that provided multivalue test cases.
 */
public final class MultivalueTestCaseSupplier {

    private static final int MIN_VALUES = 1;
    private static final int MAX_VALUES = 1000;

    private MultivalueTestCaseSupplier() {}

    public static List<TypedDataSupplier> intCases(int min, int max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            if (0 <= max && 0 >= min && includeZero) {
                cases.add(
                    new TypedDataSupplier(
                        "<0 mv " + ordering + " ints>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> 0), ordering),
                        DataType.INTEGER
                    )
                );
            }

            if (max != 0) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + max + " mv " + ordering + " ints>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> max), ordering),
                        DataType.INTEGER
                    )
                );
            }

            if (min != 0 && min != max) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + min + " mv " + ordering + " ints>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> min), ordering),
                        DataType.INTEGER
                    )
                );
            }

            int lower = Math.max(min, 1);
            int upper = Math.min(max, Integer.MAX_VALUE);
            if (lower < upper) {
                cases.add(
                    new TypedDataSupplier(
                        "<positive mv " + ordering + " ints>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomIntBetween(lower, upper)), ordering),
                        DataType.INTEGER
                    )
                );
            }

            int lower1 = Math.max(min, Integer.MIN_VALUE);
            int upper1 = Math.min(max, -1);
            if (lower1 < upper1) {
                cases.add(
                    new TypedDataSupplier(
                        "<negative mv " + ordering + " ints>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomIntBetween(lower1, upper1)), ordering),
                        DataType.INTEGER
                    )
                );
            }

            if (min < 0 && max > 0) {
                cases.add(
                    new TypedDataSupplier("<random mv " + ordering + " ints>", () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> {
                        if (includeZero) {
                            return ESTestCase.randomIntBetween(min, max);
                        }
                        return randomBoolean() ? ESTestCase.randomIntBetween(min, -1) : ESTestCase.randomIntBetween(1, max);
                    }), ordering), DataType.INTEGER)
                );
            }
        }

        return cases;
    }

    public static List<TypedDataSupplier> longCases(long min, long max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            if (0 <= max && 0 >= min && includeZero) {
                cases.add(
                    new TypedDataSupplier(
                        "<0 mv " + ordering + " longs>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> 0L), ordering),
                        DataType.LONG
                    )
                );
            }

            if (max != 0) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + max + " mv " + ordering + " longs>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> max), ordering),
                        DataType.LONG
                    )
                );
            }

            if (min != 0 && min != max) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + min + " mv " + ordering + " longs>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> min), ordering),
                        DataType.LONG
                    )
                );
            }

            long lower = Math.max(min, 1);
            long upper = Math.min(max, Long.MAX_VALUE);
            if (lower < upper) {
                cases.add(
                    new TypedDataSupplier(
                        "<positive mv " + ordering + " longs>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomLongBetween(lower, upper)), ordering),
                        DataType.LONG
                    )
                );
            }

            long lower1 = Math.max(min, Long.MIN_VALUE);
            long upper1 = Math.min(max, -1);
            if (lower1 < upper1) {
                cases.add(
                    new TypedDataSupplier(
                        "<negative mv " + ordering + " longs>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomLongBetween(lower1, upper1)), ordering),
                        DataType.LONG
                    )
                );
            }

            if (min < 0 && max > 0) {
                cases.add(
                    new TypedDataSupplier("<random mv " + ordering + " longs>", () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> {
                        if (includeZero) {
                            return ESTestCase.randomLongBetween(min, max);
                        }
                        return randomBoolean() ? ESTestCase.randomLongBetween(min, -1) : ESTestCase.randomLongBetween(1, max);
                    }), ordering), DataType.LONG)
                );
            }
        }

        return cases;
    }

    public static List<TypedDataSupplier> doubleCases(double min, double max, boolean includeZero) {
        List<TypedDataSupplier> cases = new ArrayList<>();

        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            if (0d <= max && 0d >= min && includeZero) {
                cases.add(
                    new TypedDataSupplier(
                        "<0 mv " + ordering + " doubles>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> 0d), ordering),
                        DataType.DOUBLE
                    )
                );
                cases.add(
                    new TypedDataSupplier(
                        "<-0 mv " + ordering + " doubles>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> -0d), ordering),
                        DataType.DOUBLE
                    )
                );
            }

            if (max != 0d) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + max + " mv " + ordering + " doubles>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> max), ordering),
                        DataType.DOUBLE
                    )
                );
            }

            if (min != 0d && min != max) {
                cases.add(
                    new TypedDataSupplier(
                        "<" + min + " mv " + ordering + " doubles>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> min), ordering),
                        DataType.DOUBLE
                    )
                );
            }

            double lower1 = Math.max(min, 0d);
            double upper1 = Math.min(max, 1d);
            if (lower1 < upper1) {
                cases.add(
                    new TypedDataSupplier(
                        "<small positive mv " + ordering + " doubles>",
                        () -> putInOrder(
                            randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomDoubleBetween(lower1, upper1, true)),
                            ordering
                        ),
                        DataType.DOUBLE
                    )
                );
            }

            double lower2 = Math.max(min, -1d);
            double upper2 = Math.min(max, 0d);
            if (lower2 < upper2) {
                cases.add(
                    new TypedDataSupplier(
                        "<small negative mv " + ordering + " doubles>",
                        () -> putInOrder(
                            randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomDoubleBetween(lower2, upper2, true)),
                            ordering
                        ),
                        DataType.DOUBLE
                    )
                );
            }

            double lower3 = Math.max(min, 1d);
            double upper3 = Math.min(max, Double.MAX_VALUE);
            if (lower3 < upper3) {
                cases.add(
                    new TypedDataSupplier(
                        "<big positive mv " + ordering + " doubles>",
                        () -> putInOrder(
                            randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomDoubleBetween(lower3, upper3, true)),
                            ordering
                        ),
                        DataType.DOUBLE
                    )
                );
            }

            double lower4 = Math.max(min, -Double.MAX_VALUE);
            double upper4 = Math.min(max, -1d);
            if (lower4 < upper4) {
                cases.add(
                    new TypedDataSupplier(
                        "<big negative mv " + ordering + " doubles>",
                        () -> putInOrder(
                            randomList(MIN_VALUES, MAX_VALUES, () -> ESTestCase.randomDoubleBetween(lower4, upper4, true)),
                            ordering
                        ),
                        DataType.DOUBLE
                    )
                );
            }

            if (min < 0 && max > 0) {
                cases.add(
                    new TypedDataSupplier(
                        "<random mv " + ordering + " doubles>",
                        () -> putInOrder(randomList(MIN_VALUES, MAX_VALUES, () -> {
                            if (includeZero) {
                                return ESTestCase.randomDoubleBetween(min, max, true);
                            }
                            return randomBoolean()
                                ? ESTestCase.randomDoubleBetween(min, -1, true)
                                : ESTestCase.randomDoubleBetween(1, max, true);
                        }), ordering),
                        DataType.DOUBLE
                    )
                );
            }
        }

        return cases;
    }

    private static <T extends Comparable<T>> List<T> putInOrder(List<T> mvData, Block.MvOrdering ordering) {
        switch (ordering) {
            case UNORDERED -> {
            }
            case DEDUPLICATED_UNORDERD -> {
                var dedup = new LinkedHashSet<>(mvData);
                mvData.clear();
                mvData.addAll(dedup);
            }
            case DEDUPLICATED_AND_SORTED_ASCENDING -> {
                var dedup = new HashSet<>(mvData);
                mvData.clear();
                mvData.addAll(dedup);
                Collections.sort(mvData);
            }
            case SORTED_ASCENDING -> {
                Collections.sort(mvData);
            }
            default -> throw new UnsupportedOperationException("unsupported ordering [" + ordering + "]");
        }

        return mvData;
    }
}
