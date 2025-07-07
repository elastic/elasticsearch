/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.util.IntroSorter;

class AssignmentArraySorter extends IntroSorter {
    int pivot = -1;
    private final float[][] centroids;
    private final int[] centroidsOrds;
    private final int[] sortOrdering;

    AssignmentArraySorter(float[][] centroids, int[] centroidsOrds, int[] sortOrdering) {
        this.centroids = centroids;
        this.centroidsOrds = centroidsOrds;
        this.sortOrdering = sortOrdering;
    }

    @Override
    protected void setPivot(int i) {
        pivot = sortOrdering[i];
    }

    @Override
    protected int comparePivot(int j) {
        return Integer.compare(pivot, sortOrdering[j]);
    }

    @Override
    protected void swap(int i, int j) {
        final float[] tmpC = centroids[i];
        centroids[i] = centroids[j];
        centroids[j] = tmpC;

        final int tmpA = centroidsOrds[i];
        centroidsOrds[i] = centroidsOrds[j];
        centroidsOrds[j] = tmpA;

        final int tmpSort = sortOrdering[i];
        sortOrdering[i] = sortOrdering[j];
        sortOrdering[j] = tmpSort;
    }
}
