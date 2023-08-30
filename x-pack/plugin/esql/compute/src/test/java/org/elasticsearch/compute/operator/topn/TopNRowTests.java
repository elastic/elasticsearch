/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TopNRowTests extends ESTestCase {
    public void testRamBytesUsedEmpty() {
        TopNOperator.Row row = new TopNOperator.Row();
        // We double count the shared empty array for empty rows. This overcounting is *fine*, but throws off the test.
        assertThat(row.ramBytesUsed(), equalTo(RamUsageTester.ramUsed(row) + RamUsageTester.ramUsed(new byte[0])));
    }

    public void testRamBytesUsedSmall() {
        TopNOperator.Row row = new TopNOperator.Row();
        row.keys.append(randomByte());
        row.values.append(randomByte());
        assertThat(row.ramBytesUsed(), equalTo(RamUsageTester.ramUsed(row)));
    }

    public void testRamBytesUsedBig() {
        TopNOperator.Row row = new TopNOperator.Row();
        for (int i = 0; i < 10000; i++) {
            row.keys.append(randomByte());
            row.values.append(randomByte());
        }
        assertThat(row.ramBytesUsed(), equalTo(RamUsageTester.ramUsed(row)));
    }
}
