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

/**
 * The block a page becomes when its values are taken one at a time, against the block the same page becomes when it is
 * handed over gathered. Documents hold no value, one, or several.
 */
public class ColumnarStringPageSinkTests extends ESTestCase {

    public void testOneValueADocument() throws IOException {
        final int docs = between(1, 500);
        final int[] counts = new int[docs];
        for (int d = 0; d < docs; d++) {
            counts[d] = 1;
        }
        // Null counts say every document holds one value, which is the shape a dense single-valued page arrives in.
        assertStreamedMatchesGathered(randomBoolean() ? null : counts, docs);
    }

    public void testDocumentsHoldingNoneOneOrSeveral() throws IOException {
        final int docs = between(1, 500);
        final int[] counts = new int[docs];
        for (int d = 0; d < docs; d++) {
            counts[d] = randomFrom(0, 0, 1, 1, 1, 2, between(3, 6));
        }
        assertStreamedMatchesGathered(counts, docs);
    }

    public void testTrailingDocumentsHoldNone() throws IOException {
        final int docs = between(5, 200);
        final int[] counts = new int[docs];
        counts[0] = 1;
        counts[1] = 3;
        assertStreamedMatchesGathered(counts, docs);
    }

    /** A page of no values is not streamed: it is a block of nulls, which the gathered form says without a builder. */
    public void testPageOfNoValuesIsNotStreamed() {
        final ColumnarStringPageReader.PageSink sink = new ColumnarStringPageReader.PageSink().forPage(TestBlock.factory());
        assertNull(sink.values(0, new int[] { 0, 0, 0 }, 3));
    }

    private void assertStreamedMatchesGathered(int[] counts, int docs) throws IOException {
        int valueCount = 0;
        for (int d = 0; d < docs; d++) {
            valueCount += counts == null ? 1 : counts[d];
        }
        if (valueCount == 0) {
            return;
        }
        final List<BytesRef> values = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.add(new BytesRef(randomAlphaOfLengthBetween(0, 12)));
        }

        final ColumnarStringPageReader.PageSink gathered = new ColumnarStringPageReader.PageSink().forPage(TestBlock.factory());
        gathered.appendValues(values.toArray(BytesRef[]::new), valueCount, counts, docs);

        final ColumnarStringPageReader.PageSink streamed = new ColumnarStringPageReader.PageSink().forPage(TestBlock.factory());
        // One buffer for every value, as the column hands them over: each is only valid until the next arrives.
        final BytesRef scratch = new BytesRef();
        try (StringBlockSink.Values out = streamed.values(valueCount, counts, docs)) {
            for (BytesRef value : values) {
                scratch.bytes = value.bytes;
                scratch.offset = value.offset;
                scratch.length = value.length;
                out.append(scratch);
            }
            out.finish();
        }

        final TestBlock expected = (TestBlock) gathered.block;
        final TestBlock actual = (TestBlock) streamed.block;
        assertEquals("positions", expected.size(), actual.size());
        for (int d = 0; d < docs; d++) {
            assertEquals("document " + d, expected.get(d), actual.get(d));
        }
    }
}
