/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

public class SampleReaderTests extends ESTestCase {

    public void testRandomSampling() {
        int randomLongLower = randomIntBetween(0, 1024 * 10);
        int randomLongUpper = randomIntBetween(randomLongLower, 1024 * 100);
        SampleReader.RandomLinearCongruentialMapper mapper = new SampleReader.RandomLinearCongruentialMapper(
            randomLongLower,
            randomLongUpper,
            random()
        );
        FixedBitSet valueSeen = new FixedBitSet(randomLongUpper + 1);
        for (int i = 0; i < randomLongLower; i++) {
            long mapped = mapper.map(i);
            assertTrue(mapped >= 0);
            assertTrue(mapped <= randomLongUpper);
            assertFalse(valueSeen.getAndSet((int) mapped));
        }
    }

}
