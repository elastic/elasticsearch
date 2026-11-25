/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.string.StringColumnMetadata.Dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class PlainStringColumnBoundaryTests extends ColumnarStringTestCase {

    private static final int BLOCK = 128;

    private static StringColumnOptions plainWithBounds() {
        return PlainStringColumnSkipperTests.plain(true, PlainStringColumnSkipperTests.sizes(BLOCK, 4096));
    }

    public void testProbesAroundEveryEdgeValueAreExactWhateverTheirOffsets() throws IOException {
        final byte[] highBytes = new byte[40];
        Arrays.fill(highBytes, (byte) 0xFF);
        final byte[] longValue = new byte[3000];
        random().nextBytes(longValue);
        final TreeSet<BytesRef> distinct = new TreeSet<>();
        for (BytesRef edge : new BytesRef[] {
            new BytesRef(),
            new BytesRef(new byte[] { 0 }),
            new BytesRef(new byte[] { 0, 0 }),
            new BytesRef("a"),
            new BytesRef("ab"),
            new BytesRef(highBytes),
            new BytesRef(Arrays.copyOf(highBytes, 39)),
            new BytesRef(longValue) }) {
            distinct.add(edge);
        }
        final BytesRef[] values = distinct.toArray(new BytesRef[0]);
        final int docs = 3 * BLOCK + 5;
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int doc = 0; doc < docs; doc++) {
            docSlots[doc] = new BytesRef[] { PlainSkipTestValues.maybeOffset(values[values.length - 1 - doc * values.length / docs]) };
        }
        final List<BytesRef> probes = new ArrayList<>();
        for (BytesRef value : values) {
            probes.add(PlainSkipTestValues.maybeOffset(value));
            probes.add(
                PlainSkipTestValues.maybeOffset(
                    new BytesRef(Arrays.copyOf(value.bytes, value.offset + value.length + 1), value.offset, value.length + 1)
                )
            );
            if (value.length > 0) {
                probes.add(PlainSkipTestValues.maybeOffset(new BytesRef(value.bytes, value.offset, value.length - 1)));
            }
        }
        withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (BytesRef probe : probes) {
                StringMatchOracle.assertMatches(docSlots, probe::equals, () -> reader.matchTerm(probe));
                StringMatchOracle.assertMatches(
                    docSlots,
                    value -> StringMatchOracle.startsWith(value, probe),
                    () -> reader.matchPrefix(probe)
                );
            }
            for (int i = 0; i < 300; i++) {
                final BytesRef lower = randomBoolean() ? null : randomFrom(probes);
                final BytesRef upper = randomBoolean() ? null : randomFrom(probes);
                StringMatchOracle.assertRange(reader, docSlots, lower, randomBoolean(), upper, randomBoolean());
            }
        });
    }

    public void testRunsOfEqualValuesAreExactWhereTheyCrossABlockEdge() throws IOException {
        for (int shift : new int[] { -1, 0, 1 }) {
            final int runs = 4;
            final BytesRef[][] docSlots = new BytesRef[runs * BLOCK][];
            for (int doc = 0; doc < docSlots.length; doc++) {
                final int run = Math.min(runs - 1, Math.max(0, (doc + shift) / BLOCK));
                docSlots[doc] = new BytesRef[] { new BytesRef("v" + (runs - 1 - run)) };
            }
            withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
                for (int run = -1; run <= runs; run++) {
                    final BytesRef term = new BytesRef("v" + run);
                    StringMatchOracle.assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
                    StringMatchOracle.assertRange(reader, docSlots, term, true, new BytesRef("v" + (run + 1)), false);
                    StringMatchOracle.assertRange(reader, docSlots, term, false, new BytesRef("v" + (run + 1)), true);
                }
            });
        }
    }

    public void testExtremaThatAppearOnlyInTheFinalSlotOfAPartialEntryAreFound() throws IOException {
        for (String extreme : new String[] { "a", "z" }) {
            final BytesRef[][] docSlots = new BytesRef[3 * BLOCK + 5][];
            for (int doc = 0; doc < docSlots.length; doc++) {
                docSlots[doc] = new BytesRef[] { new BytesRef(doc == docSlots.length - 1 ? extreme : "m" + (doc / 3)) };
            }
            withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
                final BytesRef term = new BytesRef(extreme);
                assertEquals(List.of(docSlots.length - 1), StringMatchOracle.matched(reader.matchTerm(term)));
                StringMatchOracle.assertRange(
                    reader,
                    docSlots,
                    extreme.equals("a") ? null : term,
                    true,
                    extreme.equals("a") ? term : null,
                    true
                );
            });
        }
    }

    public void testADocumentSpanningEntriesIsFoundWhicheverOfItsEntriesIsTheOnlyKeptOne() throws IOException {
        final BytesRef[] spanning = new BytesRef[300];
        for (int slot = 0; slot < spanning.length; slot++) {
            spanning[slot] = slot >= 2 * BLOCK && slot % 5 == 0 ? null : new BytesRef(slot < BLOCK ? "a" : slot < 2 * BLOCK ? "b" : "c");
        }
        final BytesRef[][] docSlots = new BytesRef[1 + 6 * BLOCK][];
        docSlots[0] = spanning;
        for (int doc = 1; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef("x") };
        }
        withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (String only : new String[] { "a", "b", "c", "x", "d" }) {
                final BytesRef term = new BytesRef(only);
                StringMatchOracle.assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
                StringMatchOracle.assertMatches(
                    docSlots,
                    value -> StringMatchOracle.startsWith(value, term),
                    () -> reader.matchPrefix(term)
                );
                StringMatchOracle.assertRange(reader, docSlots, term, true, term, true);
            }
        });
    }

    public void testAnEntryTheBoundsKeepIsStillConfirmedAgainstItsValues() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[4 * BLOCK][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc % 2 == 0 ? "a" : "e") };
        }
        final Counter seeks = new Counter();
        withColumnAndSkipIndex(
            docSlots,
            plainWithBounds(),
            directory -> new SeekCountingDirectory(directory, seeks),
            (metadata, reader, skip) -> {
                final BytesRef between = new BytesRef("c");
                final long before = seeks.get();
                assertEquals(List.of(), StringMatchOracle.matched(reader.matchRange(between, true, between, true)));
                assertEquals(List.of(), StringMatchOracle.matched(reader.matchTerm(between)));
                assertThat("the kept blocks were read and rejected", seeks.get() - before, greaterThan(0L));
            }
        );
    }

    public void testAnAscendingSparseColumnWritesARegionAndAnswersExactly() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[1000][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (randomInt(3) != 0) {
                docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "k%06d", doc)) };
            }
        }
        withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            StringMatchOracle.checkRandomQueries(reader, docSlots, PlainSkipTestValues.sortedPool(10), 20);
            StringMatchOracle.assertRange(reader, docSlots, new BytesRef("k000100"), true, new BytesRef("k000200"), false);
        });
    }

    public void testEscapesWithTheOptionOnAreAnsweredExactlyByTheDictionary() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[4000][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc % 7 == 0 ? "unique-" + doc : "hot-" + doc % 10) };
        }
        withColumn(
            docSlots,
            PlainStringColumnSkipperTests.dictionary(PlainStringColumnSkipperTests.sizes(BLOCK, 4096)),
            (metadata, reader) -> {
                assertThat(metadata, instanceOf(Dictionary.class));
                for (String term : new String[] { "hot-3", "unique-70", "unique-7", "absent" }) {
                    final BytesRef value = new BytesRef(term);
                    StringMatchOracle.assertMatches(docSlots, value::equals, () -> reader.matchTerm(value));
                    StringMatchOracle.assertMatches(
                        docSlots,
                        candidate -> StringMatchOracle.startsWith(candidate, value),
                        () -> reader.matchPrefix(value)
                    );
                    StringMatchOracle.assertRange(reader, docSlots, value, true, new BytesRef("zzz"), false);
                }
            }
        );
    }

    public void testTheLayoutAtThePolicyThresholdsIsTheSameWithTheOptionOnAndOff() throws IOException {
        for (double tail : new double[] { 0.0, 0.05, 0.2 }) {
            final int docs = 20_000;
            final BytesRef[][] docSlots = new BytesRef[docs][];
            for (int doc = 0; doc < docs; doc++) {
                docSlots[doc] = new BytesRef[] { new BytesRef(doc * tail % 1 < tail ? "tail-" + doc : "hot-" + doc % 10) };
            }
            final String[] layouts = new String[2];
            for (int arm = 0; arm < 2; arm++) {
                final int index = arm;
                withColumn(docSlots, StringColumnOptions.DEFAULT.withPlainSkipIndex(arm == 0), (metadata, reader) -> {
                    layouts[index] = metadata.layout().toString();
                    StringMatchOracle.checkRandomQueries(
                        reader,
                        docSlots,
                        new BytesRef[] { new BytesRef("hot-3"), new BytesRef("tail-5") },
                        10
                    );
                });
            }
            assertEquals("tail " + tail, layouts[0], layouts[1]);
        }
    }

    public void testWindowsWithOffsetsAndConjunctionsAgreeWithTheOracle() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(40);
        final BytesRef[][] docSlots = new BytesRef[2000][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (randomInt(9) != 0) {
                docSlots[doc] = new BytesRef[] { pool[pool.length - 1 - doc * pool.length / docSlots.length] };
            }
        }
        withColumn(docSlots, plainWithBounds(), (metadata, reader) -> {
            for (int round = 0; round < 10; round++) {
                final BytesRef lower = randomFrom(pool);
                final BytesRef upper = randomFrom(pool);
                final List<Integer> expectedRange = StringMatchOracle.expected(
                    docSlots,
                    value -> StringMatchOracle.inRange(value, lower, true, upper, true)
                );
                final DocIdSetIterator windows = reader.matchRange(lower, true, upper, true);
                final List<Integer> found = new ArrayList<>();
                int doc = windows.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    final int upTo = doc + between(1, 300);
                    final int offset = between(0, doc);
                    final FixedBitSet bits = new FixedBitSet(upTo - offset);
                    windows.intoBitSet(upTo, bits, offset);
                    for (int bit = bits.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bit + 1 < bits.length()
                        ? bits.nextSetBit(bit + 1)
                        : DocIdSetIterator.NO_MORE_DOCS) {
                        found.add(bit + offset);
                    }
                    doc = windows.docID();
                }
                assertEquals(expectedRange, found);

                final BytesRef source = randomFrom(pool);
                final BytesRef prefix = new BytesRef(source.bytes, source.offset, Math.min(1, source.length));
                final List<Integer> expectedBoth = new ArrayList<>(expectedRange);
                expectedBoth.retainAll(StringMatchOracle.expected(docSlots, value -> StringMatchOracle.startsWith(value, prefix)));
                final DocIdSetIterator both = ConjunctionUtils.intersectIterators(
                    List.of(reader.matchRange(lower, true, upper, true), reader.matchPrefix(prefix))
                );
                int last = -1;
                while (last != DocIdSetIterator.NO_MORE_DOCS) {
                    final boolean next = last >= 0 && randomBoolean();
                    final int target = next ? last + 1 : Math.max(0, last + between(1, 60));
                    final int reached = next ? both.nextDoc() : both.advance(target);
                    assertEquals(
                        expectedBoth.stream().filter(d -> d >= target).findFirst().orElse(DocIdSetIterator.NO_MORE_DOCS).intValue(),
                        reached
                    );
                    last = reached;
                }
            }
        });
    }
}
