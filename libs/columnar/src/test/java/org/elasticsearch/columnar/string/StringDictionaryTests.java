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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * A column stored as an ordinal per value into a dictionary of its terms.
 *
 * <p>Which layout a column takes is a property of its values, so these tests state the values and assert
 * the layout, rather than asking for one. Only a vocabulary that names every value is written today, so a
 * column holding anything the dictionary would not is expected to stay plain.
 */
public class StringDictionaryTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /** A handful of terms over many documents: every value is named, and reads back as itself. */
    public void testRepeatedTermsTakeTheDictionary() throws IOException {
        final String[] terms = { "DEBUG", "ERROR", "INFO", "TRACE", "WARN" };
        final BytesRef[] docValues = new BytesRef[between(500, 3000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEquals("one ordinal per distinct term", terms.length, metadata.dictionarySize());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Nothing repeats, so there is no vocabulary and the values are stored as they are. */
    public void testAllDistinctValuesStayPlain() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(200, 1500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("id-" + d);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /**
     * One term seen once is enough to keep the column plain: that term is turned away by the survey, which
     * leaves it with no ordinal to take, and there is nowhere yet for a value to escape to.
     */
    public void testOneUnrepeatedValueKeepsItPlain() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(300, 900)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        docValues[between(0, docValues.length - 1)] = new BytesRef("seen-exactly-once");
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** A dictionary as large as the values it stands in for has bought nothing, so it is not written. */
    public void testDictionaryTooLargeAgainstTheColumnIsRefused() throws IOException {
        // Every term appears twice, so the vocabulary is complete, but it is half the column's bytes.
        final BytesRef[] docValues = new BytesRef[600];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef("value-" + (d / 2));
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** Documents without a value are skipped by the iterator, so their absence does not shift an ordinal. */
    public void testGapsAmongDictionaryValues() throws IOException {
        final String[] terms = { "red", "green", "blue" };
        final BytesRef[] docValues = new BytesRef[between(400, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> { assertEveryValueReadsBack(docValues, reader); });
    }

    /** Values of no bytes are terms like any other, and repeat like any other. */
    public void testEmptyValuesAreTerms() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1200)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(d % 3 == 0 ? "" : (d % 3 == 1 ? "yes" : "no"));
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            assertEveryValueReadsBack(docValues, reader);
        });
    }

    /** What a dictionary column records about itself survives the round trip through its metadata. */
    public void testMetadataRoundTrip() throws IOException {
        final String[] terms = { "GET", "POST", "PUT" };
        final BytesRef[] docValues = new BytesRef[between(300, 1200)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(terms[d % terms.length]);
        }
        withDictionary(docValues, (metadata, reader) -> {
            assertEquals("layout", StringColumnLayout.DICTIONARY, metadata.layout());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("layout", metadata.layout(), read.layout());
            assertEquals("dictionary size", metadata.dictionarySize(), read.dictionarySize());
            assertEquals("dictionary terms", metadata.dictionary().numValues(), read.dictionary().numValues());
            assertEquals("ordinals", metadata.ordinals().numValues(), read.ordinals().numValues());
            assertEquals("numValues", metadata.numValues(), read.numValues());
        });
    }

    private static StringColumnMetadata roundTrip(final StringColumnMetadata metadata, final int maxDoc) throws IOException {
        final byte[] buffer = new byte[1 << 16];
        final org.apache.lucene.store.ByteArrayDataOutput out = new org.apache.lucene.store.ByteArrayDataOutput(buffer);
        metadata.writeTo(out);
        final org.apache.lucene.store.ByteArrayDataInput in = new org.apache.lucene.store.ByteArrayDataInput(buffer, 0, out.getPosition());
        return StringColumnMetadata.readFrom(in, Math.max(maxDoc, 1), org.elasticsearch.columnar.FormatVersion.CURRENT);
    }

    private void withDictionary(final BytesRef[] docValues, final ColumnCheck check) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, check);
    }

    private static void assertEveryValueReadsBack(final BytesRef[] docValues, final StringColumnReader reader) throws IOException {
        final List<BytesRef> expected = new ArrayList<>();
        for (BytesRef value : docValues) {
            if (value != null) {
                expected.add(value);
            }
        }
        final ColumnIterator iterator = reader.iterator();
        int seen = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            assertEquals("value at doc " + doc, docValues[doc], reader.valueAt(reader.firstValueAddress(iterator.rank())));
            seen++;
        }
        assertEquals("documents with a value", expected.size(), seen);
    }
}
