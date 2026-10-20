/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class BulkMaskTests extends ESTestCase {

    public void testAll() {
        for (int count = 1; count <= Integer.SIZE; count++) {
            int mask = BulkMask.all(count);
            assertEquals(count, Integer.bitCount(mask));
            assertTrue(BulkMask.isSelected(mask, count - 1));
        }
        assertEquals(-1, BulkMask.all(Integer.SIZE));
    }

    public void testSelect() {
        int index = randomInt(Integer.SIZE - 1);
        int mask = randomInt();
        int selected = BulkMask.select(mask, index);
        for (int i = 0; i < Integer.SIZE; i++) {
            assertEquals(i == index || BulkMask.isSelected(mask, i), BulkMask.isSelected(selected, i));
        }
    }

    public void testIterationVisitsSelectedInAscendingOrder() {
        int mask = randomInt();
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < Integer.SIZE; i++) {
            if (BulkMask.isSelected(mask, i)) {
                expected.add(i);
            }
        }
        List<Integer> visited = new ArrayList<>();
        for (int m = mask; m != 0; m = BulkMask.deselectFirst(m)) {
            visited.add(BulkMask.first(m));
        }
        assertEquals(expected, visited);
    }

    public void testValidateAcceptsMasksWithinTheBlock() {
        for (int count = 0; count <= Integer.SIZE; count++) {
            assertTrue(BulkMask.validate(0, count));
            assertTrue(BulkMask.validate(randomInt() & BulkMask.all(count), count));
        }
    }

    public void testValidateRejectsMasksPastTheBlock() {
        int count = randomInt(Integer.SIZE - 1);
        int mask = BulkMask.select(0, randomIntBetween(count, Integer.SIZE - 1));
        var ex = expectThrows(IllegalArgumentException.class, () -> BulkMask.validate(mask, count));
        assertThat(ex.getMessage(), containsString("selects vectors beyond count"));
    }

    public void testValidateRejectsBlocksLargerThanTheMask() {
        expectThrows(IndexOutOfBoundsException.class, () -> BulkMask.validate(0, Integer.SIZE + 1));
        expectThrows(IndexOutOfBoundsException.class, () -> BulkMask.validate(0, -1));
    }
}
