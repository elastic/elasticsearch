/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class BytesRefsTests extends ESTestCase {

    public void testRadixSortEmpty() {
        BytesRef[] arr = new BytesRef[0];
        BytesRefs.radixSort(arr);
        assertEquals(0, arr.length);
    }

    public void testRadixSortSingleElement() {
        BytesRef[] arr = { new BytesRef("only") };
        BytesRefs.radixSort(arr);
        assertEquals(new BytesRef("only"), arr[0]);
    }

    public void testRadixSortUnsorted() {
        BytesRef[] arr = { new BytesRef("zzz"), new BytesRef("aaa"), new BytesRef("mmm"), new BytesRef("bbb") };
        BytesRefs.radixSort(arr);
        assertEquals(new BytesRef("aaa"), arr[0]);
        assertEquals(new BytesRef("bbb"), arr[1]);
        assertEquals(new BytesRef("mmm"), arr[2]);
        assertEquals(new BytesRef("zzz"), arr[3]);
    }

    public void testRadixSortAlreadySorted() {
        BytesRef[] arr = { new BytesRef("aaa"), new BytesRef("bbb"), new BytesRef("zzz") };
        BytesRefs.radixSort(arr);
        assertEquals(new BytesRef("aaa"), arr[0]);
        assertEquals(new BytesRef("bbb"), arr[1]);
        assertEquals(new BytesRef("zzz"), arr[2]);
    }

    public void testIsSortedEmpty() {
        assertTrue(BytesRefs.isSorted(new BytesRef[0]));
    }

    public void testIsSortedSingleElement() {
        assertTrue(BytesRefs.isSorted(new BytesRef[] { new BytesRef("x") }));
    }

    public void testIsSortedTrue() {
        BytesRef[] arr = { new BytesRef("aaa"), new BytesRef("bbb"), new BytesRef("zzz") };
        assertTrue(BytesRefs.isSorted(arr));
    }

    public void testIsSortedFalse() {
        BytesRef[] arr = { new BytesRef("zzz"), new BytesRef("aaa") };
        assertFalse(BytesRefs.isSorted(arr));
    }

    public void testIsSortedDuplicates() {
        // equal adjacent elements are considered sorted
        BytesRef[] arr = { new BytesRef("aaa"), new BytesRef("aaa"), new BytesRef("bbb") };
        assertTrue(BytesRefs.isSorted(arr));
    }
}
