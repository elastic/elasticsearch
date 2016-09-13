/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.index;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

/**
 * Forked utility methods from Lucene's PointValues until LUCENE-7257 is released.
 */
public class XPointValues {
    /** Return the cumulated number of points across all leaves of the given
     * {@link IndexReader}. Leaves that do not have points for the given field
     * are ignored.
     *  @see PointValues#size(String) */
    public static long size(IndexReader reader, String field) throws IOException {
        long size = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo info = ctx.reader().getFieldInfos().fieldInfo(field);
            if (info == null || info.getPointDimensionCount() == 0) {
                continue;
            }
            PointValues values = ctx.reader().getPointValues();
            size += values.size(field);
        }
        return size;
    }

    /** Return the cumulated number of docs that have points across all leaves
     * of the given {@link IndexReader}. Leaves that do not have points for the
     * given field are ignored.
     *  @see PointValues#getDocCount(String) */
    public static int getDocCount(IndexReader reader, String field) throws IOException {
        int count = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo info = ctx.reader().getFieldInfos().fieldInfo(field);
            if (info == null || info.getPointDimensionCount() == 0) {
                continue;
            }
            PointValues values = ctx.reader().getPointValues();
            count += values.getDocCount(field);
        }
        return count;
    }

    /** Return the minimum packed values across all leaves of the given
     * {@link IndexReader}. Leaves that do not have points for the given field
     * are ignored.
     *  @see PointValues#getMinPackedValue(String) */
    public static byte[] getMinPackedValue(IndexReader reader, String field) throws IOException {
        byte[] minValue = null;
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo info = ctx.reader().getFieldInfos().fieldInfo(field);
            if (info == null || info.getPointDimensionCount() == 0) {
                continue;
            }
            PointValues values = ctx.reader().getPointValues();
            byte[] leafMinValue = values.getMinPackedValue(field);
            if (leafMinValue == null) {
                continue;
            }
            if (minValue == null) {
                minValue = leafMinValue.clone();
            } else {
                final int numDimensions = values.getNumDimensions(field);
                final int numBytesPerDimension = values.getBytesPerDimension(field);
                for (int i = 0; i < numDimensions; ++i) {
                    int offset = i * numBytesPerDimension;
                    if (StringHelper.compare(numBytesPerDimension, leafMinValue, offset, minValue, offset) < 0) {
                        System.arraycopy(leafMinValue, offset, minValue, offset, numBytesPerDimension);
                    }
                }
            }
        }
        return minValue;
    }

    /** Return the maximum packed values across all leaves of the given
     * {@link IndexReader}. Leaves that do not have points for the given field
     * are ignored.
     *  @see PointValues#getMaxPackedValue(String) */
    public static byte[] getMaxPackedValue(IndexReader reader, String field) throws IOException {
        byte[] maxValue = null;
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo info = ctx.reader().getFieldInfos().fieldInfo(field);
            if (info == null || info.getPointDimensionCount() == 0) {
                continue;
            }
            PointValues values = ctx.reader().getPointValues();
            byte[] leafMaxValue = values.getMaxPackedValue(field);
            if (leafMaxValue == null) {
                continue;
            }
            if (maxValue == null) {
                maxValue = leafMaxValue.clone();
            } else {
                final int numDimensions = values.getNumDimensions(field);
                final int numBytesPerDimension = values.getBytesPerDimension(field);
                for (int i = 0; i < numDimensions; ++i) {
                    int offset = i * numBytesPerDimension;
                    if (StringHelper.compare(numBytesPerDimension, leafMaxValue, offset, maxValue, offset) > 0) {
                        System.arraycopy(leafMaxValue, offset, maxValue, offset, numBytesPerDimension);
                    }
                }
            }
        }
        return maxValue;
    }

    /** Default constructor */
    private XPointValues() {
    }
}
