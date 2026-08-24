/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.FormatVersion;

import java.io.IOException;

/** What a string column records about itself, written and read back on its own. */
public class StringColumnMetadataTests extends ColumnarStringTestCase {

    /** Everything a column records survives the round trip, over a column that was really written. */
    public void testRoundTrip() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(1, 40));
        }
        withColumn(docValues, (metadata, reader) -> {
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("numDocsWithField", metadata.numDocsWithField(), read.numDocsWithField());
            assertEquals("numValues", metadata.numValues(), read.numValues());
            assertEquals("layout", metadata.layout(), read.layout());
            assertEquals("stream values", metadata.values().numValues(), read.values().numValues());
            assertEquals("values per block", metadata.values().valuesPerBlock(), read.values().valuesPerBlock());
            assertEquals("stream value bytes", metadata.values().valueBytes(), read.values().valueBytes());
            assertEquals("multi-valued", metadata.multiValued(), read.multiValued());
        });
    }

    /**
     * A column no document has a value in stops after the document count, so nothing else it might have
     * recorded is written or read.
     */
    public void testEmptyColumnShortCircuits() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 200)];
        withColumn(docValues, (metadata, reader) -> {
            assertEquals("no documents have a value", 0, metadata.numDocsWithField());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("numDocsWithField", 0, read.numDocsWithField());
            assertEquals("numValues", 0L, read.numValues());
            assertFalse("single-valued", read.multiValued());
        });
    }

    /**
     * A column holds more values than it has documents exactly when a document holds more than one. The
     * writer builds no such column today, so the true case is pinned from a record built directly, using a
     * real column's iterator so nothing else about it is made up.
     */
    public void testMultiValuedFollowsFromTheCounts() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(2, 50)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(randomAlphaOfLengthBetween(1, 10));
        }
        withColumn(docValues, (metadata, reader) -> {
            assertFalse("as many values as documents", metadata.multiValued());
            final StringColumnMetadata several = StringColumnMetadata.plain(
                metadata.iterator(),
                metadata.numDocsWithField(),
                metadata.numValues() + 1,
                metadata.values()
            );
            assertTrue("more values than documents", several.multiValued());
        });
    }

    private static StringColumnMetadata roundTrip(StringColumnMetadata metadata, int maxDoc) throws IOException {
        final byte[] buffer = new byte[1 << 16];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        metadata.writeTo(out);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        return StringColumnMetadata.readFrom(in, Math.max(maxDoc, 1), FormatVersion.CURRENT);
    }
}
