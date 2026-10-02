/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Whether a column records that its values arrive in term order. A search bisects on the strength of this,
 * so it has to be exact: never claimed for a column that is out of order anywhere.
 */
public class ValuesSortedTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    public void testSortedPlainColumn() throws IOException {
        final BytesRef[] docValues = sorted(distinct(between(200, 2000)));
        assertSorted(docValues, DictionaryPolicy.NONE, true);
    }

    public void testUnsortedPlainColumn() throws IOException {
        final BytesRef[] docValues = distinct(between(200, 2000));
        // Force one inversion, in case the random order happened to come out sorted.
        docValues[0] = new BytesRef("zzzzzz");
        docValues[docValues.length - 1] = new BytesRef("aaaaaa");
        assertSorted(docValues, DictionaryPolicy.NONE, false);
    }

    /** Equal neighbours are in order: a run of one term does not break it. */
    public void testRunsOfEqualValuesAreSorted() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[900];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d / 300]);
        }
        assertSorted(docValues, DictionaryPolicy.NONE, true);
    }

    /** A dictionary column's ordinals are in term order, so sortedness follows them. */
    public void testSortedDictionaryColumn() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "WARN" };
        final BytesRef[] docValues = new BytesRef[1200];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d / 300]);
        }
        assertSorted(docValues, ROOMY, true);
    }

    public void testUnsortedDictionaryColumn() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "WARN" };
        final BytesRef[] docValues = new BytesRef[1200];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        assertSorted(docValues, ROOMY, false);
    }

    /**
     * An escaped value has no ordinal to place among the terms, so a column that lets anything escape is
     * not called sorted even when its values are in order.
     */
    public void testEscapesAreNotCalledSorted() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[900];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d / 300]);
        }
        docValues[450] = new BytesRef("bravo-once");
        final BytesRef[] ordered = sorted(docValues);
        withColumn(ordered, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertTrue("something escaped", dictionaryOf(metadata).hasEscapes());
            assertFalse("not claimed sorted", reader.valuesSorted());
        });
    }

    /** A column no document has a value in is trivially in order. */
    public void testEmptyColumnIsSorted() throws IOException {
        assertSorted(new BytesRef[between(1, 50)], DictionaryPolicy.NONE, true);
    }

    private void assertSorted(BytesRef[] docValues, DictionaryPolicy policy, boolean expected) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            assertEquals("valuesSorted", expected, reader.valuesSorted());
            assertEquals("survives the metadata", expected, metadata.valuesSorted());
        });
    }

    private BytesRef[] distinct(int count) {
        final BytesRef[] values = new BytesRef[count];
        for (int d = 0; d < count; d++) {
            values[d] = new BytesRef(randomAlphaOfLength(12));
        }
        return values;
    }

    private static BytesRef[] sorted(BytesRef[] values) {
        final BytesRef[] copy = values.clone();
        Arrays.sort(copy, (a, b) -> a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b)));
        return copy;
    }
}
