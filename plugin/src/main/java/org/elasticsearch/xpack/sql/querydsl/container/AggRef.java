/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.querydsl.agg.Agg;

public class AggRef implements Reference {
    private final String path;
    // agg1 = 0
    // agg1>agg2._count = 1
    // agg1>agg2>agg3.value = 1  (agg3.value has the same depth as agg2._count)
    // agg1>agg2>agg3._count = 2
    private final int depth;

    AggRef(String path) {
        this.path = path;

        int dpt = countCharIn(path, Agg.PATH_DELIMITER_CHAR);
        if (path.endsWith(Agg.PATH_VALUE)) {
            dpt = Math.max(0, dpt - 1);
        }
        depth = dpt;
    }

    @Override
    public String toString() {
        return path;
    }

    @Override
    public int depth() {
        return depth;
    }

    public String path() {
        return path;
    }

    private static int countCharIn(CharSequence sequence, char c) {
        int count = 0;
        for (int i = 0; i < sequence.length(); i++) {
            if (c == sequence.charAt(i)) {
                count++;
            }
        }
        return count;
    }
}