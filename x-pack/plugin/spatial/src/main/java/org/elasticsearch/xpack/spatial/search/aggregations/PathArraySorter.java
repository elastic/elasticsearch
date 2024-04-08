/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.search.sort.SortOrder;

import java.util.function.BiFunction;

/**
 * An {@link IntroSorter} that sorts <code>points</code> and <code>sortValues</code> using the
 */
abstract class PathArraySorter extends IntroSorter {

    private final long[] points;
    protected final double[] sortValues;
    protected double sortValuePivot;

    static BiFunction<long[], double[], PathArraySorter> forOrder(SortOrder sortOrder) {
        return switch (sortOrder) {
            case ASC -> Ascending::new;
            case DESC -> Descending::new;
        };
    }

    private static final class Ascending extends PathArraySorter {
        private Ascending(long[] points, double[] sortValues) {
            super(points, sortValues);
        }

        @Override
        protected int comparePivot(int j) {
            return Double.compare(sortValuePivot, sortValues[j]);
        }
    }

    private static final class Descending extends PathArraySorter {
        private Descending(long[] points, double[] sortValues) {
            super(points, sortValues);
        }

        @Override
        protected int comparePivot(int j) {
            return Double.compare(sortValues[j], sortValuePivot);
        }
    }

    protected PathArraySorter(long[] points, double[] sortValues) {
        assert points.length == sortValues.length;
        this.points = points;
        this.sortValues = sortValues;
        this.sortValuePivot = 0;
    }

    public void sort() {
        sort(0, points.length);
    }

    @Override
    protected void swap(int i, int j) {
        final long tmpPoint = points[i];
        points[i] = points[j];
        points[j] = tmpPoint;
        final double tmpSortValue = sortValues[i];
        sortValues[i] = sortValues[j];
        sortValues[j] = tmpSortValue;
    }

    @Override
    protected void setPivot(int i) {
        sortValuePivot = sortValues[i];
    }
}
