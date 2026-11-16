/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * {@link ColumnarSourceWriter.ReusableColumnarStoredLeafReader} is reused for every document of a batch, so what one document puts in
 * a slot must never be visible to the next one.
 */
public class ColumnarSourceWriterTests extends ESTestCase {

    private static final String NUMERIC = "numeric";
    private static final String BINARY = "binary";
    private static final String SORTED = "sorted";
    private static final String SORTED_SET = "sorted_set";
    private static final String SORTED_NUMERIC = "sorted_numeric";

    /** A reader with one slot of each type registered, the way building a source loader leaf registers them. */
    private static ColumnarSourceWriter.ReusableColumnarStoredLeafReader readerWithAllSlots() throws IOException {
        var reader = new ColumnarSourceWriter.ReusableColumnarStoredLeafReader();
        reader.getNumericDocValues(NUMERIC);
        reader.getBinaryDocValues(BINARY);
        reader.getSortedDocValues(SORTED);
        reader.getSortedSetDocValues(SORTED_SET);
        reader.getSortedNumericDocValues(SORTED_NUMERIC);
        return reader;
    }

    public void testRepopulateClearsSlotsTheNextDocumentDoesNotFill() throws IOException {
        var reader = readerWithAllSlots();

        LuceneDocument first = new LuceneDocument();
        first.add(new NumericDocValuesField(NUMERIC, 7));
        first.add(new BinaryDocValuesField(BINARY, new BytesRef("bin")));
        first.add(new SortedDocValuesField(SORTED, new BytesRef("sorted")));
        first.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef("b")));
        first.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef("a")));
        first.add(new SortedNumericDocValuesField(SORTED_NUMERIC, 9));
        first.add(new SortedNumericDocValuesField(SORTED_NUMERIC, 3));
        reader.repopulate(first);

        assertTrue(reader.getNumericDocValues(NUMERIC).advanceExact(0));
        assertTrue(reader.getBinaryDocValues(BINARY).advanceExact(0));
        assertTrue(reader.getSortedDocValues(SORTED).advanceExact(0));
        assertTrue(reader.getSortedSetDocValues(SORTED_SET).advanceExact(0));
        assertTrue(reader.getSortedNumericDocValues(SORTED_NUMERIC).advanceExact(0));

        // The second document only has the binary field.
        LuceneDocument second = new LuceneDocument();
        second.add(new BinaryDocValuesField(BINARY, new BytesRef("other")));
        reader.repopulate(second);

        assertFalse(reader.getNumericDocValues(NUMERIC).advanceExact(0));
        assertFalse(reader.getSortedDocValues(SORTED).advanceExact(0));
        assertFalse(reader.getSortedSetDocValues(SORTED_SET).advanceExact(0));
        assertFalse(reader.getSortedNumericDocValues(SORTED_NUMERIC).advanceExact(0));
        BinaryDocValues binary = reader.getBinaryDocValues(BINARY);
        assertTrue(binary.advanceExact(0));
        assertEquals(new BytesRef("other"), binary.binaryValue());

        // And a document without any of them leaves every slot empty.
        reader.repopulate(new LuceneDocument());
        assertFalse(reader.getBinaryDocValues(BINARY).advanceExact(0));
    }

    public void testRepopulateReplacesValuesOfSlotsFilledAgain() throws IOException {
        var reader = readerWithAllSlots();

        LuceneDocument first = new LuceneDocument();
        first.add(new NumericDocValuesField(NUMERIC, 1));
        first.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef("a")));
        first.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef("b")));
        first.add(new SortedNumericDocValuesField(SORTED_NUMERIC, 5));
        first.add(new SortedNumericDocValuesField(SORTED_NUMERIC, 6));
        reader.repopulate(first);

        LuceneDocument second = new LuceneDocument();
        second.add(new NumericDocValuesField(NUMERIC, 2));
        second.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef("c")));
        second.add(new SortedNumericDocValuesField(SORTED_NUMERIC, 8));
        reader.repopulate(second);

        NumericDocValues numeric = reader.getNumericDocValues(NUMERIC);
        assertTrue(numeric.advanceExact(0));
        assertEquals(2L, numeric.longValue());

        SortedSetDocValues sortedSet = reader.getSortedSetDocValues(SORTED_SET);
        assertTrue(sortedSet.advanceExact(0));
        assertEquals(1, sortedSet.docValueCount());
        assertEquals(new BytesRef("c"), sortedSet.lookupOrd(sortedSet.nextOrd()));

        SortedNumericDocValues sortedNumeric = reader.getSortedNumericDocValues(SORTED_NUMERIC);
        assertTrue(sortedNumeric.advanceExact(0));
        assertEquals(1, sortedNumeric.docValueCount());
        assertEquals(8L, sortedNumeric.nextValue());
    }

    /** Whatever the sequence of documents, a reused reader must hold the same as a reader that only ever saw the last document. */
    public void testReusedReaderMatchesAFreshReaderForEveryDocument() throws IOException {
        var reused = readerWithAllSlots();
        for (int iteration = 0; iteration < 200; iteration++) {
            LuceneDocument doc = randomDocument();
            reused.repopulate(doc);

            var fresh = readerWithAllSlots();
            fresh.repopulate(doc);

            assertSameState(fresh, reused);
        }
    }

    private static LuceneDocument randomDocument() {
        LuceneDocument doc = new LuceneDocument();
        if (randomBoolean()) {
            doc.add(new NumericDocValuesField(NUMERIC, randomLong()));
        }
        if (randomBoolean()) {
            doc.add(new BinaryDocValuesField(BINARY, new BytesRef(randomAlphaOfLength(5))));
        }
        if (randomBoolean()) {
            doc.add(new SortedDocValuesField(SORTED, new BytesRef(randomAlphaOfLength(5))));
        }
        for (int i = between(0, 3); i > 0; i--) {
            doc.add(new SortedSetDocValuesField(SORTED_SET, new BytesRef(randomAlphaOfLength(3))));
        }
        for (int i = between(0, 3); i > 0; i--) {
            doc.add(new SortedNumericDocValuesField(SORTED_NUMERIC, randomLong()));
        }
        return doc;
    }

    private static void assertSameState(
        ColumnarSourceWriter.ReusableColumnarStoredLeafReader expected,
        ColumnarSourceWriter.ReusableColumnarStoredLeafReader actual
    ) throws IOException {
        NumericDocValues expectedNumeric = expected.getNumericDocValues(NUMERIC);
        NumericDocValues actualNumeric = actual.getNumericDocValues(NUMERIC);
        assertEquals(expectedNumeric.advanceExact(0), actualNumeric.advanceExact(0));
        if (actualNumeric.docID() == 0) {
            assertEquals(expectedNumeric.longValue(), actualNumeric.longValue());
        }

        BinaryDocValues expectedBinary = expected.getBinaryDocValues(BINARY);
        BinaryDocValues actualBinary = actual.getBinaryDocValues(BINARY);
        assertEquals(expectedBinary.advanceExact(0), actualBinary.advanceExact(0));
        if (actualBinary.docID() == 0) {
            assertEquals(expectedBinary.binaryValue(), actualBinary.binaryValue());
        }

        SortedDocValues expectedSorted = expected.getSortedDocValues(SORTED);
        SortedDocValues actualSorted = actual.getSortedDocValues(SORTED);
        assertEquals(expectedSorted.advanceExact(0), actualSorted.advanceExact(0));
        if (actualSorted.docID() == 0) {
            assertEquals(expectedSorted.lookupOrd(expectedSorted.ordValue()), actualSorted.lookupOrd(actualSorted.ordValue()));
        }

        SortedSetDocValues expectedSet = expected.getSortedSetDocValues(SORTED_SET);
        SortedSetDocValues actualSet = actual.getSortedSetDocValues(SORTED_SET);
        assertEquals(expectedSet.advanceExact(0), actualSet.advanceExact(0));
        if (actualSet.docID() == 0) {
            assertEquals(expectedSet.docValueCount(), actualSet.docValueCount());
            for (int i = 0; i < actualSet.docValueCount(); i++) {
                assertEquals(expectedSet.lookupOrd(expectedSet.nextOrd()), actualSet.lookupOrd(actualSet.nextOrd()));
            }
        }

        SortedNumericDocValues expectedNumbers = expected.getSortedNumericDocValues(SORTED_NUMERIC);
        SortedNumericDocValues actualNumbers = actual.getSortedNumericDocValues(SORTED_NUMERIC);
        assertEquals(expectedNumbers.advanceExact(0), actualNumbers.advanceExact(0));
        if (actualNumbers.docID() == 0) {
            assertEquals(expectedNumbers.docValueCount(), actualNumbers.docValueCount());
            for (int i = 0; i < actualNumbers.docValueCount(); i++) {
                assertEquals(expectedNumbers.nextValue(), actualNumbers.nextValue());
            }
        }
    }
}
