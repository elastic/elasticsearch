/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

interface RowFiller {
    void writeKey(int i, Row row);

    void writeValues(int i, Row row);

    // When rows are very long, appending the values one by one can lead to lots of allocations.
    // To avoid this, pre-allocate at least as much size as in the last seen row.
    // Let the pre-allocation size decay in case we only have 1 huge row and smaller rows otherwise.
    static int newPreAllocSize(BreakingBytesRefBuilder builder, int spareValuesPreAllocSize) {
        return Math.max(builder.length(), spareValuesPreAllocSize / 2);
    }
}
