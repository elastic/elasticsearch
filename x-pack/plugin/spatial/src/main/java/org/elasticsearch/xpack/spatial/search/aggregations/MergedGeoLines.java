/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

/**
 * Class to merge an arbitrary list of {@link InternalGeoLine} lines into a new line
 * with the appropriate max length. The final point and sort values can be found in
 * finalPoints and finalSortValues after merge is called.
 */
final class MergedGeoLines {

    private final List<InternalGeoLine> geoLines;
    private final int capacity;
    private final SortOrder sortOrder;
    private final int[] lineIndices; // index of which geoLine item represents
    private final int[] idxsWithinLine; // index within the geoLine for the item
    private int size;
    private final long[] finalPoints;       // the final sorted list of points, sorted by their respective sort-values. valid after merge
    private final double[] finalSortValues; // the final sorted list of sort-values. valid after merge.

    MergedGeoLines(List<InternalGeoLine> geoLines, int finalLength, SortOrder sortOrder) {
        this.geoLines = geoLines;
        this.capacity = geoLines.size();
        this.sortOrder = sortOrder;
        this.lineIndices = new int[capacity];
        this.idxsWithinLine = new int[capacity];
        this.size = 0;
        this.finalPoints = new long[finalLength];
        this.finalSortValues = new double[finalLength];
    }

    public long[] getFinalPoints() {
        return finalPoints;
    }

    public double[] getFinalSortValues() {
        return finalSortValues;
    }

    /**
     * merges <code>geoLines</code> into one sorted list of values representing the combined line.
     */
    public void merge() {
        // 1. add first element of each sub line to heap
        for (int i = 0; i < geoLines.size(); i++) {
            if (geoLines.get(i).length() > 0) {
                add(i, 0);
            }
        }

        // 2. take lowest/greatest value from heap and re-insert the next value from the same sub-line that specific value was chosen from.

        int i = 0;
        while (i < finalPoints.length && size > 0) {
            // take top from heap and place in finalLists
            int lineIdx = lineIndices[0];
            int idxInLine = idxsWithinLine[0];
            finalPoints[i] = getTopPoint();
            finalSortValues[i] = getTopSortValue();
            removeTop();
            InternalGeoLine lineChosen = geoLines.get(lineIdx);
            if (idxInLine + 1 < lineChosen.length()) {
                add(lineIdx, idxInLine + 1);
            }
            i++;
        }
    }

    private long getTopPoint() {
        InternalGeoLine line = geoLines.get(lineIndices[0]);
        return line.line()[idxsWithinLine[0]];
    }

    private double getTopSortValue() {
        InternalGeoLine line = geoLines.get(lineIndices[0]);
        return line.sortVals()[idxsWithinLine[0]];
    }

    private void removeTop() {
        if (size == 0) {
            throw new IllegalStateException();
        }
        lineIndices[0] = lineIndices[size - 1];
        idxsWithinLine[0] = idxsWithinLine[size - 1];
        size--;
        heapifyDown();
    }

    private void add(int lineIndex, int idxWithinLine) {
        if (size >= capacity) {
            throw new IllegalStateException();
        }
        lineIndices[size] = lineIndex;
        idxsWithinLine[size] = idxWithinLine;
        size++;
        heapifyUp();
    }

    private boolean correctOrdering(int i, int j) {
        InternalGeoLine lineI = geoLines.get(lineIndices[i]);
        InternalGeoLine lineJ = geoLines.get(lineIndices[j]);
        double valI = lineI.sortVals()[idxsWithinLine[i]];
        double valJ = lineJ.sortVals()[idxsWithinLine[j]];
        if (SortOrder.ASC.equals(sortOrder)) {
            return valI > valJ;
        }
        return valI < valJ;
    }

    private int getParentIndex(int i) {
        return (i - 1) / 2;
    }

    private int getLeftChildIndex(int i) {
        return 2 * i + 1;
    }

    private int getRightChildIndex(int i) {
        return 2 * i + 2;
    }

    private boolean hasParent(int i) {
        return i > 0;
    }

    private boolean hasLeftChild(int i) {
        return getLeftChildIndex(i) < size;
    }

    private boolean hasRightChild(int i) {
        return getRightChildIndex(i) < size;
    }

    private void heapifyUp() {
        int i = size - 1;
        while (hasParent(i) && correctOrdering(getParentIndex(i), i)) {
            int parentIndex = getParentIndex(i);
            swap(parentIndex, i);
            i = parentIndex;
        }
    }

    private void heapifyDown() {
        int i = 0;
        while (hasLeftChild(i)) {
            int childIndex = getLeftChildIndex(i);
            if (hasRightChild(i) && correctOrdering(getRightChildIndex(i), childIndex) == false) {
                childIndex = getRightChildIndex(i);
            }
            if (correctOrdering(childIndex, i)) {
                break;
            } else {
                swap(childIndex, i);
                i = childIndex;
            }
        }
    }

    private void swap(int i, int j) {
        int tmpLineIndex = lineIndices[i];
        int tmpIdxWithinLine = idxsWithinLine[i];
        lineIndices[i] = lineIndices[j];
        idxsWithinLine[i] = idxsWithinLine[j];
        lineIndices[j] = tmpLineIndex;
        idxsWithinLine[j] = tmpIdxWithinLine;
    }
}
