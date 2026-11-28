/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.test.ESTestCase;

public class IVFVectorsReaderBudgetTests extends ESTestCase {

    public void testDefaultBudgetOfAMillionVectorSegment() {
        // k=10 with the default num_candidates of 15 scans about 1% of the vectors
        assertEquals(0.00976f, IVFVectorsReader.segmentVisitRatio(15, 10, 1_000_000), 1e-5f);
        // twice the ratio in posting list entries: SOAR puts a vector in up to two posting lists
        assertEquals(1_000_000, IVFVectorsReader.visitBudget(0.5f, 1_000_000));
    }
}
