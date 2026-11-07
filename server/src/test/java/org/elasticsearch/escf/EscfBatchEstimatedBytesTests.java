/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * {@link EscfBatch#estimatedBytes()} must count only what Lucene ends up holding: the raw value
 * bytes of each column (8 per LONG, the string payload, one bit per BOOL, the ARRAY child's
 * elements) plus the full field paths, and none of the ESCF framing.
 */
public class EscfBatchEstimatedBytesTests extends ESTestCase {

    /** Every field is present in every doc, so each column's value bytes can be computed by hand. */
    private static final String[] DOCS = {
        "{\"i\":1,\"s\":\"ab\",\"b\":true,\"arr\":[1,2,3],\"n\":{\"k\":7}}",
        "{\"i\":2,\"s\":\"cde\",\"b\":false,\"arr\":[4],\"n\":{\"k\":8}}" };

    // schema: "i" + "s" + "b" + "arr" + "n.k"
    private static final int SCHEMA_BYTES = 1 + 1 + 1 + 3 + 3;

    public void testCountsColumnValuesAndSchemaOnly() throws IOException {
        try (EscfBatch batch = encode(DOCS)) {
            int longs = 2 * Long.BYTES;                       // i
            int strings = "ab".length() + "cde".length();     // s
            int bools = EscfBatchCodec.bitsetBytes(2);        // b: one bit per doc, rounded to a 64-bit word
            int arrayElements = 4 * Long.BYTES;               // arr: [1,2,3] + [4]
            int nested = 2 * Long.BYTES;                      // n.k
            assertEquals(SCHEMA_BYTES + longs + strings + bools + arrayElements + nested, batch.estimatedBytes());
            // the wire form carries header, column index, offsets and bitsets on top of the values
            assertTrue(batch.data().length() > batch.estimatedBytes());
        }
    }

    public void testSliceCountsOnlyItsOwnValues() throws IOException {
        try (EscfBatch batch = encode(DOCS)) {
            SourceBatch second = batch.slice(1, 2);
            int longs = Long.BYTES;                           // i
            int strings = "cde".length();                     // s
            int bools = EscfBatchCodec.bitsetBytes(1);        // b
            int arrayElements = 1 * Long.BYTES;               // arr: [4]
            int nested = Long.BYTES;                          // n.k
            assertEquals(SCHEMA_BYTES + longs + strings + bools + arrayElements + nested, second.estimatedBytes());
        }
    }

    private static EscfBatch encode(String[] docs) throws IOException {
        List<BytesReference> sources = List.of(docs).stream().<BytesReference>map(BytesArray::new).toList();
        return EscfEncoder.encode(sources, XContentType.JSON);
    }
}
