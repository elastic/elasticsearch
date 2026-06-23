/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EirfBatchSliceTests extends ESTestCase {

    private static EirfBatch encodeDocs(int count) throws IOException {
        List<BytesReference> sources = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            sources.add(new BytesArray("{\"id\":" + i + ",\"name\":\"doc-" + i + "\"}"));
        }
        return EirfEncoder.encode(sources, XContentType.JSON);
    }

    public void testSliceRoundtripsRowValues() throws IOException {
        try (EirfBatch parent = encodeDocs(8)) {
            int from = randomIntBetween(0, 4);
            int to = randomIntBetween(from + 1, 8);
            EirfBatch sliced = parent.slice(from, to);

            assertEquals(to - from, sliced.docCount());
            assertEquals(parent.schema().leafCount(), sliced.schema().leafCount());
            assertEquals(parent.schema().nonLeafCount(), sliced.schema().nonLeafCount());

            for (int i = 0; i < to - from; i++) {
                EirfRowReader parentRow = parent.getRowReader(from + i);
                EirfRowReader slicedRow = sliced.getRowReader(i);
                int expectedId = parentRow.getIntValue(0);
                int actualId = slicedRow.getIntValue(0);
                assertEquals("row " + i + " id mismatch", expectedId, actualId);
                String expectedName = parentRow.getStringValue(1).string();
                String actualName = slicedRow.getStringValue(1).string();
                assertEquals("row " + i + " name mismatch", expectedName, actualName);
            }
            sliced.close();
        }
    }

    public void testFullSliceEqualsParent() throws IOException {
        try (EirfBatch parent = encodeDocs(5)) {
            EirfBatch sliced = parent.slice(0, parent.docCount());
            assertEquals(parent.docCount(), sliced.docCount());
            for (int i = 0; i < parent.docCount(); i++) {
                EirfRowReader p = parent.getRowReader(i);
                EirfRowReader s = sliced.getRowReader(i);
                assertEquals(p.getIntValue(0), s.getIntValue(0));
                assertEquals(p.getStringValue(1).string(), s.getStringValue(1).string());
            }
            sliced.close();
        }
    }

    public void testNestedSlice() throws IOException {
        try (EirfBatch parent = encodeDocs(10)) {
            EirfBatch outer = parent.slice(2, 8); // docs 2..7
            EirfBatch inner = outer.slice(1, 4); // outer rows 1..3 == parent rows 3..5

            assertEquals(3, inner.docCount());
            for (int i = 0; i < 3; i++) {
                EirfRowReader expected = parent.getRowReader(3 + i);
                EirfRowReader actual = inner.getRowReader(i);
                assertEquals(expected.getIntValue(0), actual.getIntValue(0));
                assertEquals(expected.getStringValue(1).string(), actual.getStringValue(1).string());
            }
            inner.close();
            outer.close();
        }
    }

    public void testEmptySlice() throws IOException {
        try (EirfBatch parent = encodeDocs(4)) {
            EirfBatch empty = parent.slice(2, 2);
            assertEquals(0, empty.docCount());
            empty.close();
        }
    }

    public void testInvalidRangeThrows() throws IOException {
        try (EirfBatch parent = encodeDocs(3)) {
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(-1, 2));
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(0, 4));
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(2, 1));
        }
    }
}
