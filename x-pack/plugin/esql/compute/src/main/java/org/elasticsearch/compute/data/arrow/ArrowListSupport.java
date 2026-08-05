/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.vector.FieldVector;

/**
 * Helpers shared by the copy-based Arrow list -> ESQL block converters
 * ({@link BooleanArrowBlock}, {@link BytesRefArrowBlock}, {@link FloatToDoubleArrowBlock}).
 *
 * <h2>Arrow list vs. ESQL multi-value semantics</h2>
 * Arrow's {@code ListVector} permits two things that ESQL multi-values cannot represent:
 * <ul>
 *   <li>An empty list ({@code []}) distinct from a null list. ESQL encodes a null
 *       position as {@code valueCount(p) == 0}; there is no first-class empty
 *       multi-value, and {@code BlockBuilder.beginPositionEntry()} is documented as
 *       requiring at least one value to be emitted.</li>
 *   <li>Null elements interleaved with non-null elements within a list. ESQL
 *       multi-values are sequences of non-null values only.</li>
 * </ul>
 *
 * The converters that use this helper resolve those mismatches consistently:
 * <ul>
 *   <li>Null Arrow list -> ESQL null position.</li>
 *   <li>Empty Arrow list -> ESQL null position.</li>
 *   <li>Arrow list with mixed null and non-null children -> null children dropped,
 *       remaining values emitted in order.</li>
 *   <li>Arrow list whose children are all null -> ESQL null position.</li>
 * </ul>
 */
final class ArrowListSupport {

    private ArrowListSupport() {}

    /**
     * Counts non-null elements in {@code child} on the index range {@code [start, end)}.
     */
    static int countNonNull(FieldVector child, int start, int end) {
        int n = 0;
        for (int j = start; j < end; j++) {
            if (child.isNull(j) == false) {
                n++;
            }
        }
        return n;
    }
}
