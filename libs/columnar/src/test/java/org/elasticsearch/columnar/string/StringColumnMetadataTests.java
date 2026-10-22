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
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/** What a string column records about itself, written and read back on its own. */
public class StringColumnMetadataTests extends ColumnarStringTestCase {

    /** Everything a column records survives the round trip, over a column that was really written. */
    public void testRoundTrip() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(1, 40));
        }
        withColumn(docValues, (metadata, reader) -> assertRoundTrips(metadata, docValues.length));
    }

    /**
     * The two addressing tables are each written only when a count already on the wire says so, so all four
     * combinations of present and absent have to parse back to the same record.
     */
    public void testRoundTripAcrossBothTables() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            for (boolean nulls : new boolean[] { false, true }) {
                final BytesRef[][] docSlots = randomDocSlots(between(20, 400), multiValued ? 6 : 1, randomBoolean(), nulls);
                withColumn(docSlots, (metadata, reader) -> {
                    // A single-slot document always holds a value, so asking for nulls does not always get any.
                    assertEquals("null table present", numNullSlots(docSlots) > 0, metadata.hasNullSlots());
                    assertRoundTrips(metadata, docSlots.length);
                });
            }
        }
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
            assertEquals("numNullSlots", 0L, read.numNullSlots());
            assertFalse("single-valued", read.multiValued());
            assertFalse("no null slots", read.hasNullSlots());
        });
    }

    /** An empty array is a document with no slots, which puts the slots out of step with the documents too. */
    public void testEmptyArrayNeedsValueAddresses() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(2, 50), 1, false, false);
        docSlots[between(0, docSlots.length - 1)] = new BytesRef[0];
        withColumn(docSlots, (metadata, reader) -> {
            assertFalse("fewer slots than documents", metadata.multiValued());
            assertTrue("still needs a value-address table", metadata.hasValueAddresses());
            assertRoundTrips(metadata, docSlots.length);
        });
    }

    /** A column records whether one of its documents holds more than one slot, rather than deriving it. */
    public void testMultiValuedIsWhatTheColumnRecorded() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(2, 50), 1, false, false);
        withColumn(docSlots, (metadata, reader) -> assertFalse("one slot a document", metadata.multiValued()));

        final BytesRef[][] several = randomDocSlots(between(2, 50), 1, false, false);
        several[between(0, several.length - 1)] = new BytesRef[] { new BytesRef("a"), new BytesRef("b") };
        withColumn(several, (metadata, reader) -> {
            assertTrue("a document holds two", metadata.multiValued());
            assertEquals("numValues counts slots", numValues(several), metadata.numValues());
        });
    }

    /**
     * The counts cannot answer it: a document holding none and a document holding two leave as many slots as
     * documents, so multivaluedness is what the column recorded of the documents it wrote rather than what its
     * totals imply.
     */
    public void testMultiValuedWhereTheCountsCancel() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(4, 50), 1, false, false);
        docSlots[0] = new BytesRef[0];
        docSlots[1] = new BytesRef[] { new BytesRef("a"), new BytesRef("b") };
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals("the counts cancel", metadata.numValues(), metadata.numDocsWithField());
            assertTrue("a document holds two", metadata.multiValued());
        });
    }

    /**
     * A column whose every slot is null holds no value to measure, so both lengths are the absent {@code -1}
     * and are written and read as such rather than as a length of zero.
     */
    public void testNullSlotsHaveNoLengths() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(1, 200)][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[between(1, 3)];
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals("no shortest value", -1, metadata.minLength());
            assertEquals("no longest value", -1, metadata.maxLength());
            assertEquals("every slot is null", numValues(docSlots), metadata.numNullSlots());
            assertRoundTrips(metadata, docSlots.length);
        });
    }

    /** The bound a column records, and the cap that says when it still holds, survive the round trip. */
    public void testTheCoverageBoundRoundTrips() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(50, 400)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(randomBoolean() ? "head" : "tail-" + d);
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            StringColumnOptions.DEFAULT_DICTIONARY,
            (metadata, reader) -> {
                assertTrue("the column recorded something for a merge", metadata.hasSummary());
                final BestCoverage written = metadata.summary().bestCoverage();
                assertTrue("including a bound", written.known());
                final StringColumnMetadata read = roundTrip(metadata, docValues.length);
                assertEquals("the bound", written, read.summary().bestCoverage());
                assertEquals("the values it is a share of", written.numValues(), read.summary().bestCoverage().numValues());
                assertEquals("and the cap it holds under", written.cap(), read.summary().bestCoverage().cap());
            }
        );
    }

    // NOTE: only a column whose dictionary policy is enabled surveys, and a survey always takes a bound, so
    // nothing the writer produces today carries an unknown one. The shape is still readable and still has to
    // read back as unknown rather than as a bound of zero, which would refuse every later dictionary.
    public void testAnUnrecordedBoundReadsBackUnknown() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(50, 400)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(randomBoolean() ? "head" : "tail-" + d);
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            StringColumnOptions.DEFAULT_DICTIONARY,
            (metadata, reader) -> {
                final StringColumnMetadata.Summary recorded = metadata.summary();
                assertTrue("the column took a bound", recorded.bestCoverage().known());

                final StringColumnMetadata metadataWithUnknownBound = metadata.withSummary(
                    new StringColumnMetadata.Summary(
                        recorded.terms(),
                        recorded.countsOffset(),
                        recorded.countsLength(),
                        recorded.numValues(),
                        BestCoverage.UNKNOWN
                    )
                );
                final StringColumnMetadata read = roundTrip(metadataWithUnknownBound, docValues.length);

                assertFalse("which reads back unknown", read.summary().bestCoverage().known());
                assertFalse("and never bounds a dictionary away", read.summary().bestCoverage().validFor(1));
                assertEquals("while its non-null value count survives", recorded.numValues(), read.summary().numValues());
                assertEquals("as do the terms it recorded", recorded.countsOffset(), read.summary().countsOffset());
                assertEquals(recorded.countsLength(), read.summary().countsLength());
                assertFalse(
                    "and an unknown bound rules no later dictionary out",
                    StringColumnOptions.DEFAULT_DICTIONARY.rulesOut(read.summary().bestCoverage())
                );
            }
        );
    }

    private static void assertRoundTrips(StringColumnMetadata metadata, int maxDoc) throws IOException {
        final StringColumnMetadata read = roundTrip(metadata, maxDoc);
        assertEquals("numDocsWithField", metadata.numDocsWithField(), read.numDocsWithField());
        assertEquals("numValues", metadata.numValues(), read.numValues());
        assertEquals("numNullSlots", metadata.numNullSlots(), read.numNullSlots());
        assertEquals("valueBytes", metadata.valueBytes(), read.valueBytes());
        assertEquals("minLength", metadata.minLength(), read.minLength());
        assertEquals("maxLength", metadata.maxLength(), read.maxLength());
        assertEquals("layout", metadata.layout(), read.layout());
        assertEquals("stored values", plainOf(metadata).values().numValues(), plainOf(read).values().numValues());
        assertEquals("values per block", plainOf(metadata).values().valuesPerBlock(), plainOf(read).values().valuesPerBlock());
        assertEquals("constant length", plainOf(metadata).values().constantLength(), plainOf(read).values().constantLength());
        if (plainOf(metadata).values().constant() == false) {
            assertEquals(
                "lengths per block",
                plainOf(metadata).values().lengths().blockSize(),
                plainOf(read).values().lengths().blockSize()
            );
            assertTableRoundTrips("value starts", plainOf(metadata).values().starts(), plainOf(read).values().starts());
        }
        assertEquals("multi-valued", metadata.multiValued(), read.multiValued());
        assertEquals("has value addresses", metadata.hasValueAddresses(), read.hasValueAddresses());
        assertEquals("has null slots", metadata.hasNullSlots(), read.hasNullSlots());
        if (metadata.hasValueAddresses()) {
            assertTableRoundTrips("addressing bases", metadata.addressing().bases(), read.addressing().bases());
            assertEquals("addressing counts", metadata.addressing().counts().numValues(), read.addressing().counts().numValues());
            assertEquals("addressing counts per block", metadata.addressing().counts().blockSize(), read.addressing().counts().blockSize());
        }
    }

    private static void assertTableRoundTrips(String what, MonotonicWriter.Table written, MonotonicWriter.Table read) {
        assertEquals(what + " data offset", written.dataOffset(), read.dataOffset());
        assertEquals(what + " data length", written.dataLength(), read.dataLength());
        assertArrayEquals(what + " meta", written.meta(), read.meta());
    }

    private static StringColumnMetadata roundTrip(StringColumnMetadata metadata, int maxDoc) throws IOException {
        final byte[] buffer = new byte[1 << 16];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        metadata.writeTo(out);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        return StringColumnMetadata.readFrom(in, Math.max(maxDoc, 1), FormatVersion.CURRENT);
    }
}
