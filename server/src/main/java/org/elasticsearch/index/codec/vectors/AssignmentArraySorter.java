/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.util.TimSorter;

class AssignmentArraySorter extends TimSorter {

  private final float[][] centroids;
  private final float[][] tmpC;

  private final int[] centroidsOrds;
  private final int[] tmpA;

  private final int[] sortOrdering;
  private final int[] tmpSort;

  AssignmentArraySorter(float[][] centroids, int[] centroidsOrds, int[] sortOrdering) {
    super(centroids.length / 64);
    this.centroids = centroids;
    this.centroidsOrds = centroidsOrds;
    this.sortOrdering = sortOrdering;

    int maxTempSlots = centroids.length / 64;
    this.tmpC = new float[maxTempSlots][];
    this.tmpA = new int[maxTempSlots];
    this.tmpSort = new int[maxTempSlots];
  }

  @Override
  protected int compare(int i, int j) {
    return Integer.compare(sortOrdering[i], sortOrdering[j]);
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

  @Override
  protected void copy(int src, int dest) {
    centroids[dest] = centroids[src];
    centroidsOrds[dest] = centroidsOrds[src];
    sortOrdering[dest] = sortOrdering[src];
  }

  @Override
  protected void save(int start, int len) {
    System.arraycopy(centroids, start, tmpC, 0, len);
    System.arraycopy(centroidsOrds, start, tmpA, 0, len);
    System.arraycopy(sortOrdering, start, tmpSort, 0, len);
  }

  @Override
  protected void restore(int src, int dest) {
    centroids[dest] = tmpC[src];
    centroidsOrds[dest] = tmpA[src];
    sortOrdering[dest] = tmpSort[src];
  }

  @Override
  protected int compareSaved(int i, int j) {
    return Integer.compare(tmpSort[i], sortOrdering[j]);
  }
}
