/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class FixedBitSetCostTests extends ESTestCase {

    public void testCoversRealBitSet() {
        for (int maxDoc : new int[] { 0, 1, 63, 64, 65, 1000, 1 << 20, between(1, 1 << 24) }) {
            FixedBitSet bitSet = new FixedBitSet(maxDoc);
            assertThat("maxDoc=" + maxDoc, FixedBitSetCost.bytes(maxDoc), greaterThanOrEqualTo(RamUsageTester.ramUsed(bitSet)));
            assertThat(
                "maxDoc=" + maxDoc,
                FixedBitSetCost.scorerBytes(maxDoc),
                greaterThanOrEqualTo(RamUsageTester.ramUsed(new BitSetIterator(bitSet, maxDoc)))
            );
        }
    }
}
