/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.util.IntroSorter;

final class PathArraySorter extends IntroSorter {

    private final long[] points;
    private final double[] sortValues;
    private double sortValuePivot;
    private int length;

    public PathArraySorter(long[] points, double[] sortValues, int length) {
        this.points = points;
        this.sortValues = sortValues;
        this.sortValuePivot = 0;
        this.length = length;
    }

    public final void sort() {
        sort(0, length);
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
        return Double.compare(sortValuePivot, sortValues[j]);
    }
}
