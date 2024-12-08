/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;

public class FieldInfosWithUsages extends FieldInfos {
    private final int totalUsages;

    public FieldInfosWithUsages(FieldInfo[] infos) {
        super(infos);
        this.totalUsages = computeUsages(infos);
    }

    public static int computeUsages(FieldInfo[] infos) {
        int usages = 0;
        for (FieldInfo fi : infos) {
            if (fi.getIndexOptions() != IndexOptions.NONE) {
                usages++;
            }
            if (fi.hasNorms()) {
                usages++;
            }
            if (fi.getDocValuesType() != DocValuesType.NONE) {
                usages++;
            }
            if (fi.getPointDimensionCount() > 0) {
                usages++;
            }
            if (fi.getVectorDimension() > 0) {
                usages++;
            }
        }
        return usages;
    }

    public int getTotalUsages() {
        return totalUsages;
    }
}
