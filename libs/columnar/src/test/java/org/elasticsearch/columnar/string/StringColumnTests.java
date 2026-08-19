/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * End-to-end round-trip of string columns through a {@link Directory}. Each case asserts the values come back
 * byte-identical and in the exact order they were written, across the value shapes the encoder has to handle:
 * dense and sparse, empty values, wide values, and a spread of value counts.
 */
public class StringColumnTests extends ColumnarStringTestCase {

    public void testEmptyColumn() throws IOException {
        assertColumn(new BytesRef[between(1, 1000)]);
    }

    /** A handful of terms repeated across many documents — the shape a dictionary layout would target. */
    public void testRepeatedValues() throws IOException {
        String[] terms = { "nginx", "apache", "kafka", "elasticsearch" };
        int maxDoc = between(1, 3000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            docs[d] = new BytesRef(randomFrom(terms));
        }
        assertColumn(docs);
    }

    /** Every document a distinct value, so nothing repeats and every value carries its own bytes. */
    public void testAllDistinctValues() throws IOException {
        int maxDoc = between(1, 3000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            docs[d] = new BytesRef("term-" + d);
        }
        assertColumn(docs);
    }

    public void testSparseColumnRepeatedValues() throws IOException {
        int maxDoc = between(100, 4000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.3) {
                docs[d] = new BytesRef(randomFrom("a", "b", "c"));
            }
        }
        assertColumn(docs);
    }

    public void testSparseColumnDistinctValues() throws IOException {
        int maxDoc = between(100, 4000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.5) {
                docs[d] = new BytesRef("term-" + d);
            }
        }
        assertColumn(docs);
    }

    /** Empty values are legal and must survive: they encode as a zero length and no bytes. */
    public void testEmptyAndSingleByteValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(200, 600)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef(randomFrom("", "x", "yy"));
        }
        assertColumn(docs);
    }

    /** Every value empty, so every offset in the table is the same and no bytes are written at all. */
    public void testAllEmptyValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(1, 500)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef("");
        }
        assertColumn(docs);
    }

    /** A spread of value counts, including the smallest columns and ones around a power of two. */
    public void testAssortedValueCounts() throws IOException {
        for (int n : new int[] { 1, 5, 127, 128, 129, 130, 200, 257 }) {
            BytesRef[] docs = new BytesRef[n];
            for (int d = 0; d < n; d++) {
                docs[d] = new BytesRef("value-" + d);
            }
            assertColumn(docs);
        }
    }

    public void testWideValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(50, 300)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef(randomAlphaOfLength(between(200, 2000)));
        }
        assertColumn(docs);
    }

    public void testRandomValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(1, 2000)];
        for (int d = 0; d < docs.length; d++) {
            if (rarely()) {
                continue;
            }
            docs[d] = new BytesRef(randomRealisticUnicodeOfCodepointLength(between(1, 30)));
        }
        assertColumn(docs);
    }

    /** Writes {@code docValues} as a string column, reads it back, and asserts every value round-trips in order. */
    private void assertColumn(BytesRef[] docValues) throws IOException {
        final int numDocsWithField = numDocsWithField(docValues);
        withColumn(docValues, (metadata, reader) -> {
            assertFalse("string columns are single-valued for now", metadata.multiValued());
            assertEquals("recorded layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEquals("numValues", numDocsWithField, reader.numValues());

            int seenDocs = 0;
            ColumnIterator iterator = reader.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                int rank = iterator.rank();
                assertEquals("value count at doc " + doc, 1, reader.valueCount(rank));
                BytesRef actual = reader.valueAt(reader.firstValueAddress(rank));
                assertEquals("doc " + doc, docValues[doc], actual);
                seenDocs++;
            }
            assertEquals("documents with a value", numDocsWithField, seenDocs);
        });
    }
}
