/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.search.sort.SortOrder;

/**
 * An {@link IntroSorter} that sorts <code>points</code> and <code>sortValues</code> using the
 */
final class PathArraySorter extends IntroSorter {

    private final long[] points;
    private final double[] sortValues;
    private double sortValuePivot;
    private final SortOrder sortOrder;

    PathArraySorter(long[] points, double[] sortValues, SortOrder sortOrder) {
        assert points.length == sortValues.length;
        this.points = points;
        this.sortValues = sortValues;
        this.sortValuePivot = 0;
        this.sortOrder = sortOrder;
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

    @Override
    protected int comparePivot(int j) {
        if (SortOrder.ASC.equals(sortOrder)) {
            return Double.compare(sortValuePivot, sortValues[j]);
        }
        return Double.compare(sortValues[j], sortValuePivot);
    }
}
