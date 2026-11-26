/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

public class BinaryAndCountsTests extends ESTestCase {

    /** No counts field: single-valued. */
    public void testNoCountsField() throws IOException {
        assertCounts(doc -> {}, false);
    }

    /** Counts without a skipper, as in a flushed segment, are read. */
    public void testCountsWithoutSkipper() throws IOException {
        assertCounts(doc -> doc.add(new NumericDocValuesField("f" + COUNT_FIELD_SUFFIX, 2)), true);
    }

    /** A skipper with a maximum of 1 skips the counts. */
    public void testCountsSkipperOfSingleValues() throws IOException {
        assertCounts(doc -> doc.add(NumericDocValuesField.indexedField("f" + COUNT_FIELD_SUFFIX, 1)), false);
    }

    /** A skipper with a larger maximum keeps the counts. */
    public void testCountsSkipperOfMultipleValues() throws IOException {
        assertCounts(doc -> doc.add(NumericDocValuesField.indexedField("f" + COUNT_FIELD_SUFFIX, 2)), true);
    }

    private void assertCounts(CheckedConsumer<Document, IOException> counts, boolean expectCounts) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                doc.add(new BinaryDocValuesField("f", new BytesRef("payload")));
                counts.accept(doc);
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                BinaryAndCounts bc = BinaryAndCounts.get(breaker, reader.leaves().get(0), "f", true);
                try {
                    assertNotNull(bc);
                    assertEquals(expectCounts, bc.counts() != null);
                } finally {
                    bc.binary().close();
                    if (bc.counts() != null) {
                        bc.counts().close();
                    }
                }
                assertEquals(0, breaker.getUsed());
            }
        }
    }
}
