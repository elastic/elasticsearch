/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The block a page of values becomes, for documents holding no value, one, or several. */
public class ColumnarStringPageSinkTests extends ESTestCase {

    public void testOneValueADocument() throws IOException {
        final int docs = between(1, 500);
        final int[] counts = new int[docs];
        for (int d = 0; d < docs; d++) {
            counts[d] = 1;
        }
        // Null counts say every document holds one value, which is the shape a dense single-valued page arrives in.
        assertBlock(randomBoolean() ? null : counts, docs);
    }

    public void testDocumentsHoldingNoneOneOrSeveral() throws IOException {
        final int docs = between(1, 500);
        final int[] counts = new int[docs];
        for (int d = 0; d < docs; d++) {
            counts[d] = randomFrom(0, 0, 1, 1, 1, 2, between(3, 6));
        }
        assertBlock(counts, docs);
    }

    public void testTrailingDocumentsHoldNone() throws IOException {
        final int docs = between(5, 200);
        final int[] counts = new int[docs];
        counts[0] = 1;
        counts[1] = 3;
        assertBlock(counts, docs);
    }

    public void testNoDocumentHoldsAValue() throws IOException {
        final int docs = between(1, 50);
        assertBlock(new int[docs], docs);
    }

    private void assertBlock(int[] counts, int docs) throws IOException {
        final List<List<BytesRef>> expected = new ArrayList<>(docs);
        int valueCount = 0;
        for (int d = 0; d < docs; d++) {
            final List<BytesRef> held = new ArrayList<>();
            for (int i = 0; i < (counts == null ? 1 : counts[d]); i++) {
                held.add(new BytesRef(randomAlphaOfLengthBetween(0, 12)));
            }
            expected.add(held);
            valueCount += held.size();
        }

        final ColumnarStringPageReader.PageSink sink = new ColumnarStringPageReader.PageSink().forPage(TestBlock.factory());
        // One buffer for every value, as the column hands them over: each is only valid until the next arrives.
        final BytesRef scratch = new BytesRef();
        try (StringBlockSink.Values out = sink.values(valueCount, counts, docs)) {
            for (List<BytesRef> held : expected) {
                for (BytesRef value : held) {
                    scratch.bytes = value.bytes;
                    scratch.offset = value.offset;
                    scratch.length = value.length;
                    out.append(scratch);
                }
            }
            out.finish();
        }

        final TestBlock block = (TestBlock) sink.block;
        assertEquals("positions", docs, block.size());
        for (int d = 0; d < docs; d++) {
            final List<BytesRef> held = expected.get(d);
            final Object want = held.isEmpty() ? null : held.size() == 1 ? held.get(0) : held;
            assertEquals("document " + d, want, block.get(d));
        }
    }
}
