/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.hnsw.IntToIntFunction;

public class IntSorter extends IntroSorter {
    int pivot = -1;
    private final int[] arr;
    private final IntToIntFunction func;

    public IntSorter(int[] arr, IntToIntFunction func) {
        this.arr = arr;
        this.func = func;
    }

    @Override
    protected void setPivot(int i) {
        pivot = func.apply(arr[i]);
    }

    @Override
    protected int comparePivot(int j) {
        return Integer.compare(pivot, func.apply(arr[j]));
    }

    @Override
    protected int compare(int a, int b) {
        return Integer.compare(func.apply(arr[a]), func.apply(arr[b]));
    }

    @Override
    protected void swap(int i, int j) {
        final int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}
