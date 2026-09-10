/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.test.ESTestCase;

public class ColumNARDocValuesFormatTests extends ESTestCase {

    public void testInvalidBlockSizeRejected() {
        final NumericPipelineSelector sel = (f, t) -> NumericPipeline::defaultPipeline;
        for (int bs : new int[] { 0, -1, 64, 127, 384, 640, ColumNARDocValuesFormat.MAX_BLOCK_SIZE * 2 }) {
            final int blockSize = bs;
            expectThrows(IllegalArgumentException.class, () -> new ColumNARDocValuesFormat(sel, blockSize));
        }
    }
}
