/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Checks the result of an ES|QL query against the rows that another engine computed for the same query. The queries of the ClickBench
 * track sort by a key with ties and have no tie-breaking key, so two correct engines can return different rows. The check accepts every
 * valid choice and nothing else:
 *
 * <ul>
 *   <li>{@code ties} (the default): the sort keys of the rows must equal the expected ones in order; every row that is before the last
 *       row in the sort order must be one of the expected rows, once; and every row that has the key of the last row must be one of the
 *       rows with that key. The columns of the response are matched by name, and a query that sorts by a column that it does not
 *       return is checked without the order of the keys;</li>
 *   <li>{@code approx_rank}: for a ranking by an approximate count. Each row must be one of the expected top groups with a value inside
 *       the tolerance, and the values must not increase;</li>
 *   <li>{@code count_only}: for a query whose rows are arbitrary, such as a {@code LIMIT} without a sort: only the number of rows.</li>
 * </ul>
 *
 * The expected case is a map with the keys {@code cols}, {@code outcols}, {@code sort} (pairs of a column and {@code asc} or
 * {@code desc}), {@code limit}, {@code mode}, {@code tolerance} (one number for each of {@code cols}), {@code group}, {@code rows},
 * {@code ties} and {@code tiesComplete}.
 */
public final class ClickBenchResultCheck {

    private ClickBenchResultCheck() {}

    /** Returns {@code null} if the result is valid, and otherwise the first reason why it is not. */
    @SuppressWarnings("unchecked")
    public static String check(Map<String, Object> expectedCase, List<String> actualColumns, List<List<Object>> actual) {
        String mode = (String) expectedCase.get("mode");
        List<String> cols = (List<String>) expectedCase.get("cols");
        List<String> outcols = (List<String>) expectedCase.get("outcols");
        List<List<Object>> expectedRows = (List<List<Object>>) expectedCase.get("rows");
        List<List<Object>> tieRows = (List<List<Object>>) expectedCase.get("ties");
        boolean tiesComplete = (boolean) expectedCase.get("tiesComplete");
        int limit = ((Number) expectedCase.get("limit")).intValue();
        List<List<Object>> sort = (List<List<Object>>) expectedCase.get("sort");
        List<String> group = (List<String>) expectedCase.get("group");
        List<Number> toleranceList = (List<Number>) expectedCase.get("tolerance");

        if (actualColumns.size() != outcols.size() || actualColumns.containsAll(outcols) == false) {
            return "expected the columns " + outcols + " but the response has " + actualColumns;
        }
        // the tolerance of each output column, and the position of each output column in the response and in the expected rows
        double[] tolerance = new double[outcols.size()];
        int[] responsePosition = new int[outcols.size()];
        int[] expectedPosition = new int[outcols.size()];
        for (int i = 0; i < outcols.size(); i++) {
            expectedPosition[i] = cols.indexOf(outcols.get(i));
            responsePosition[i] = actualColumns.indexOf(outcols.get(i));
            tolerance[i] = toleranceList.get(expectedPosition[i]).doubleValue();
        }
        List<List<Object>> rows = project(actual, responsePosition);

        switch (mode) {
            case "count_only" -> {
                return rows.size() == expectedRows.size()
                    ? null
                    : "expected [" + expectedRows.size() + "] rows but got [" + rows.size() + "]";
            }
            case "approx_rank" -> {
                return approxRank(rows, project(expectedRows, expectedPosition), outcols, group, sort, limit, tolerance);
            }
            case "ties" -> {
                return ties(rows, expectedRows, tieRows, tiesComplete, cols, outcols, expectedPosition, sort, tolerance);
            }
            default -> throw new IllegalArgumentException("unknown mode [" + mode + "]");
        }
    }

    private static String approxRank(
        List<List<Object>> rows,
        List<List<Object>> expected,
        List<String> outcols,
        List<String> group,
        List<List<Object>> sort,
        int limit,
        double[] tolerance
    ) {
        if (rows.size() != limit) {
            return "expected [" + limit + "] rows but got [" + rows.size() + "]";
        }
        int value = outcols.indexOf((String) sort.get(0).get(0));
        List<Integer> keys = group.stream().map(outcols::indexOf).toList();
        Number previous = null;
        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            List<Object> match = null;
            for (List<Object> candidate : expected) {
                if (keys.stream().allMatch(k -> same(candidate.get(k), row.get(k), 0))) {
                    match = candidate;
                    break;
                }
            }
            if (match == null) {
                return "row [" + i + "] " + row + " is not one of the top groups by the exact count";
            }
            if (same(match.get(value), row.get(value), tolerance[value]) == false) {
                return "row ["
                    + i
                    + "] "
                    + row
                    + " has the value ["
                    + row.get(value)
                    + "] but the exact value is ["
                    + match.get(value)
                    + "]";
            }
            Number current = (Number) row.get(value);
            if (previous != null && current.doubleValue() > previous.doubleValue()) {
                return "row [" + i + "] has a larger value than the row before it: " + current + " after " + previous;
            }
            previous = current;
        }
        return null;
    }

    private static String ties(
        List<List<Object>> rows,
        List<List<Object>> expectedFull,
        List<List<Object>> tieFull,
        boolean tiesComplete,
        List<String> cols,
        List<String> outcols,
        int[] expectedPosition,
        List<List<Object>> sort,
        double[] tolerance
    ) {
        if (rows.size() != expectedFull.size()) {
            return "expected [" + expectedFull.size() + "] rows but got [" + rows.size() + "]";
        }
        List<List<Object>> expected = project(expectedFull, expectedPosition);
        List<List<Object>> tied = project(tieFull, expectedPosition);
        // the sort keys of the expected rows, by position in all the columns, and in the output when the response has them
        int[] keyInCols = new int[sort.size()];
        int[] keyInOut = new int[sort.size()];
        boolean keysVisible = true;
        for (int k = 0; k < sort.size(); k++) {
            String name = (String) sort.get(k).get(0);
            keyInCols[k] = cols.indexOf(name);
            keyInOut[k] = outcols.indexOf(name);
            keysVisible &= keyInOut[k] >= 0;
        }
        if (keysVisible) {
            for (int i = 0; i < rows.size(); i++) {
                for (int k = 0; k < sort.size(); k++) {
                    if (same(expected.get(i).get(keyInOut[k]), rows.get(i).get(keyInOut[k]), tolerance[keyInOut[k]]) == false) {
                        return "row ["
                            + i
                            + "] has the sort key ["
                            + rows.get(i).get(keyInOut[k])
                            + "] but ["
                            + expected.get(i).get(keyInOut[k])
                            + "] is expected";
                    }
                }
            }
        }
        // the rows before the last row in the sort order are fully determined, and the rows with the key of the last row are a choice
        List<List<Object>> determined = new ArrayList<>();
        if (sort.isEmpty()) {
            determined.addAll(expected);
        } else {
            List<Object> cutoff = expectedFull.get(expectedFull.size() - 1);
            for (int i = 0; i < expectedFull.size(); i++) {
                boolean tiesWithLast = true;
                for (int k = 0; k < sort.size() && tiesWithLast; k++) {
                    tiesWithLast = same(
                        expectedFull.get(i).get(keyInCols[k]),
                        cutoff.get(keyInCols[k]),
                        tolerance(cols, outcols, tolerance, keyInCols[k])
                    );
                }
                if (tiesWithLast == false) {
                    determined.add(expected.get(i));
                }
            }
        }
        List<List<Object>> extras = new ArrayList<>();
        for (List<Object> row : rows) {
            int found = indexOf(determined, row, tolerance);
            if (found >= 0) {
                determined.remove(found);
            } else {
                extras.add(row);
            }
        }
        if (determined.isEmpty() == false) {
            return "the rows " + determined + " are expected but missing";
        }
        if (tiesComplete) {
            for (List<Object> extra : extras) {
                int found = indexOf(tied, extra, tolerance);
                if (found < 0) {
                    return "the row " + extra + " is not one of the expected rows, and not one of the rows that tie with the last row";
                }
                tied.remove(found);
            }
        } else if (sort.isEmpty() && extras.isEmpty() == false) {
            return "the row " + extras.get(0) + " is not expected";
        }
        return null;
    }

    /** The tolerance of the column at a position in all the columns, which is the one of its output column or 0 if it has none. */
    private static double tolerance(List<String> cols, List<String> outcols, double[] outTolerance, int position) {
        int out = outcols.indexOf(cols.get(position));
        return out < 0 ? 0 : outTolerance[out];
    }

    private static int indexOf(List<List<Object>> rows, List<Object> row, double[] tolerance) {
        for (int i = 0; i < rows.size(); i++) {
            boolean equal = true;
            for (int c = 0; c < row.size() && equal; c++) {
                equal = same(rows.get(i).get(c), row.get(c), tolerance[c]);
            }
            if (equal) {
                return i;
            }
        }
        return -1;
    }

    private static List<List<Object>> project(List<List<Object>> rows, int[] positions) {
        List<List<Object>> projected = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            List<Object> values = new ArrayList<>(positions.length);
            for (int position : positions) {
                values.add(row.get(position));
            }
            projected.add(values);
        }
        return projected;
    }

    /** Whole numbers must be equal unless the tolerance is above {@code 1e-6}, and other numbers must be within the relative tolerance. */
    static boolean same(Object expected, Object actual, double tolerance) {
        if (expected instanceof Number e && actual instanceof Number a) {
            boolean integral = (e instanceof Double || e instanceof Float) == false && (a instanceof Double || a instanceof Float) == false;
            if (integral && tolerance <= 1e-6) {
                return e.longValue() == a.longValue();
            }
            return Math.abs(e.doubleValue() - a.doubleValue()) <= Math.max(tolerance, 1e-9) * Math.max(1.0, Math.abs(e.doubleValue()));
        }
        return expected == null ? actual == null : expected.equals(actual);
    }
}
