/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

public final class DocValuesSkipperUtils {

    private DocValuesSkipperUtils() {}

    public static Long getMinValue(IndexReader reader, String field) throws IOException {
        Long minValue = null;
        for (LeafReaderContext ctx : reader.leaves()) {
            var skipper = ctx.reader().getDocValuesSkipper(field);
            if (skipper == null) {
                continue;
            }
            long leafMinValue = skipper.minValue();
            if (minValue == null) {
                minValue = leafMinValue;
            } else {
                minValue = Long.min(minValue, leafMinValue);
            }
        }
        return minValue;
    }

    public static Long getMaxValue(IndexReader reader, String field) throws IOException {
        Long maxValue = null;
        for (LeafReaderContext ctx : reader.leaves()) {
            var skipper = ctx.reader().getDocValuesSkipper(field);
            if (skipper == null) {
                continue;
            }
            long leafMaxValue = skipper.maxValue();
            if (maxValue == null) {
                maxValue = leafMaxValue;
            } else {
                maxValue = Long.max(maxValue, leafMaxValue);
            }
        }
        return maxValue;
    }
}
