/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.lang.reflect.Array;
import java.util.function.Consumer;

public class CarthesianCombinator<T> {
    private final T[] elems;
    private final int[] index;
    private final T[] result;
    private final int len;

    @SuppressWarnings("unchecked")
    CarthesianCombinator(T[] elems, int len) {
        if (elems.length == 0) {
            throw new IllegalArgumentException("elems must not be empty");
        }
        this.elems = elems;
        this.index = new int[len];
        this.result = (T[]) Array.newInstance(elems[0].getClass(), len);
        this.len = len;
    }

    private void init(int length) {
        for (int i = 0; i < length; i++) {
            index[i] = 0;
            result[i] = elems[0];
        }
    }

    public void forEach(Consumer<T[]> action) {
        // Initialize index and result
        init(len);

        int pos = 0;
        while (pos < len) {
            if (index[pos] < elems.length) {
                result[pos] = elems[index[pos]];
                action.accept(result);
                index[pos]++;
                continue;
            }
            while (pos < len && index[pos] + 1 >= elems.length) {
                pos++;
            }
            if (pos < len) {
                index[pos]++;
                result[pos] = elems[index[pos]];
                init(pos);
                pos = 0;
            }
        }
    }
}
