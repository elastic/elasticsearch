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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.lessThan;

public class PlainRangePresenceColumnTests extends ColumnarStringTestCase {

    private enum Presence {
        DENSE,
        SPARSE,
        MULTIVALUED
    }

    public void testMixedNavigationOverRealPresenceFollowsTheOracle() throws IOException {
        for (Presence shape : Presence.values()) {
            for (int round = 0; round < 4; round++) {
                final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(
                    randomValidBlockSize(),
                    randomFrom(64, 256, 4096),
                    randomFrom(128, 1000, 16384)
                );
                final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(3, 30));
                final BytesRef[][] docSlots = descending(shape, between(2, 2000), pool);
                withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
                    final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                    final StringColumnMetadata.Plain.Skipper skipper = column.skipper();
                    assertNotNull(skipper);
                    assertEquals(PlainSkipFixture.entriesOf(PlainSkipFixture.realOwners(docSlots, sizes)), skipper.entryCount());
                    try (
                        Directory dir = newDirectory();
                        PlainSkipFixture oracle = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.realOwners(docSlots, sizes), false)
                    ) {
                        final BitSet present = new BitSet();
                        oracle.docs.forEach(present::set);
                        for (int q = 0; q < 8; q++) {
                            final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                            final PlainRangePresence presence = presence(reader, skipper, column.values().numValues(), skip, query);
                            navigate(presence, PlainBlockOracle.keptDocs(oracle, query), present, shape != Presence.MULTIVALUED);
                        }
                    }
                });
            }
        }
    }

    public void testSingleSlotRunsStopAtTheEndOfTheEntryAndOtherRunsAreOneDocument() throws IOException {
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(128, 64, 100);
        for (Presence shape : new Presence[] { Presence.DENSE, Presence.MULTIVALUED }) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(10);
            final BytesRef[][] docSlots = descending(shape, 600, pool);
            final int[] owners = PlainSkipFixture.realOwners(docSlots, sizes);
            int firstEntryEnd = 0;
            while (firstEntryEnd < owners.length && owners[firstEntryEnd] == owners[0]) {
                firstEntryEnd++;
            }
            assertThat("the column has more than one entry", firstEntryEnd, lessThan(owners.length));
            final int expectedRunEnd = firstEntryEnd;
            withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
                final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                final PlainRangePresence presence = presence(
                    reader,
                    column.skipper(),
                    column.values().numValues(),
                    skip,
                    PlainSkipBounds.Query.range(null, true, null, true)
                );
                final int doc = presence.nextDoc();
                assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
                if (shape == Presence.DENSE) {
                    assertEquals(expectedRunEnd, presence.docIDRunEnd());
                } else {
                    assertEquals(doc + 1, presence.docIDRunEnd());
                }
            });
        }
    }

    private PlainRangePresence presence(
        StringColumnReader reader,
        StringColumnMetadata.Plain.Skipper skipper,
        long numValues,
        IndexInput skip,
        PlainSkipBounds.Query query
    ) throws IOException {
        return new PlainRangePresence(
            reader.iterator(),
            new PlainSkipBounds.Reader(skipper, numValues, skip),
            query,
            new PlainRangePresence.Slots() {
                @Override
                public long firstAddress(int rank) throws IOException {
                    return reader.firstValueAddress(rank);
                }

                @Override
                public long count(int rank) throws IOException {
                    return reader.valueCount(rank);
                }

                @Override
                public boolean oneSlotADocument() {
                    return reader.hasValueAddresses() == false;
                }
            }
        );
    }

    private void navigate(PlainRangePresence presence, List<Integer> kept, BitSet present, boolean checkRuns) throws IOException {
        final BitSet keptSet = new BitSet();
        kept.forEach(keptSet::set);
        int doc = -1;
        for (int step = 0; step < 150 && doc != DocIdSetIterator.NO_MORE_DOCS; step++) {
            final int operation = doc < 0 ? 0 : between(0, 3);
            switch (operation) {
                case 0 -> {
                    doc = presence.nextDoc();
                    assertEquals(firstKeptFrom(keptSet, doc < 0 ? 0 : presence.docID() >= 0 ? doc : 0), doc);
                }
                case 1 -> {
                    final int target = doc + between(1, 300);
                    doc = presence.advance(target);
                    assertEquals(firstKeptFrom(keptSet, target), doc);
                }
                case 2 -> {
                    final int runEnd = presence.docIDRunEnd();
                    assertTrue(runEnd > doc);
                    for (int inRun = doc; inRun < runEnd && checkRuns; inRun++) {
                        assertTrue(
                            "document " + inRun + " of the run [" + doc + ", " + runEnd + ")",
                            present.get(inRun) && keptSet.get(inRun)
                        );
                    }
                }
                case 3 -> {
                    final int upTo = doc + between(1, 300);
                    final int offset = between(-3, doc);
                    final FixedBitSet bits = new FixedBitSet(upTo - offset + 1);
                    presence.intoBitSet(upTo, bits, offset);
                    for (int inWindow = doc; inWindow < upTo; inWindow++) {
                        assertEquals("document " + inWindow, keptSet.get(inWindow), bits.get(inWindow - offset));
                    }
                    doc = presence.docID();
                    assertEquals(firstKeptFrom(keptSet, upTo), doc);
                }
                default -> throw new AssertionError(operation);
            }
        }
    }

    private static int firstKeptFrom(BitSet kept, int target) {
        final int next = kept.nextSetBit(target);
        return next < 0 ? DocIdSetIterator.NO_MORE_DOCS : next;
    }

    private BytesRef[][] descending(Presence shape, int count, BytesRef[] pool) {
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            final BytesRef value = pool[pool.length - 1 - (int) ((long) doc * pool.length / count)];
            docSlots[doc] = switch (shape) {
                case DENSE -> new BytesRef[] { value };
                case SPARSE -> randomInt(9) < 3 ? null : new BytesRef[] { value };
                case MULTIVALUED -> switch (between(0, 4)) {
                    case 0 -> new BytesRef[0];
                    case 1 -> null;
                    case 2 -> new BytesRef[] { value, randomFrom(pool) };
                    default -> new BytesRef[] { value };
                };
            };
        }
        docSlots[count - 1] = new BytesRef[] { pool[0] };
        return docSlots;
    }
}
