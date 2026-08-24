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
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Which layout a column takes is decided from its values, so the shapes that decide it are generated here
 * rather than stated: every combination of cardinality, clustering, value length and sparsity against every
 * dictionary bound, read back both in order and at random.
 *
 * <p>The named tests elsewhere pin particular behaviours. This one exists to reach the combinations nobody
 * thought to name — a column whose vocabulary is exactly at its budget, one whose values are longer than a
 * chunk, one that escapes on a block boundary by accident.
 */
public class StringColumnStressTests extends ColumnarStringTestCase {

    private static final int ITERATIONS = 60;

    /** Random shapes under random bounds: whatever layout results, every value reads back as itself. */
    public void testRandomShapesUnderRandomBounds() throws IOException {
        for (int iter = 0; iter < ITERATIONS; iter++) {
            final BytesRef[] docValues = randomShape();
            withColumn(
                docValues,
                randomValidBlockSize(),
                randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD),
                randomTargetChunkBytes(),
                randomPolicy(),
                (metadata, reader) -> {
                    assertInOrder(docValues, reader);
                    assertAtRandom(docValues, reader);
                    assertSelfConsistent(docValues, metadata);
                }
            );
        }
    }

    /**
     * Bounds tightened one step at a time over the same values, which walks a column across the point where
     * a dictionary stops being worth keeping and where terms start escaping it.
     */
    public void testTighteningTheBoundsOverOneColumn() throws IOException {
        final BytesRef[] docValues = zipf(between(2000, 6000), between(4, 200));
        for (int maxBytes : new int[] { 0, 16, 64, 256, 1024, 8192, 512 * 1024 }) {
            for (double share : new double[] { 0.02, 0.2, 0.9 }) {
                final DictionaryPolicy policy = new DictionaryPolicy(maxBytes, 0.5, share);
                withColumn(docValues, randomValidBlockSize(), ChunkCodec.ZSTD, randomTargetChunkBytes(), policy, (metadata, reader) -> {
                    assertInOrder(docValues, reader);
                    assertSelfConsistent(docValues, metadata);
                });
            }
        }
    }

    private static DictionaryPolicy randomPolicy() {
        return switch (between(0, 3)) {
            case 0 -> DictionaryPolicy.NONE;
            case 1 -> new DictionaryPolicy(512 * 1024, 0.5, 0.2);
            case 2 -> new DictionaryPolicy(between(1, 512), randomDoubleBetween(0.1, 0.9, true), randomDoubleBetween(0.05, 0.5, true));
            default -> new DictionaryPolicy(between(1024, 64 * 1024), randomDoubleBetween(0.1, 0.9, true), 0.5);
        };
    }

    private BytesRef[] randomShape() {
        final int size = between(1, 4000);
        return switch (between(0, 7)) {
            // Nothing repeats, so there is nothing to name.
            case 0 -> distinct(size);
            // A handful of terms, which is where a dictionary is worth the most.
            case 1 -> zipf(size, between(1, 8));
            // A head over a long tail, which is the shape most real columns have.
            case 2 -> zipf(size, between(20, 400));
            // Arrived in term order, as under a primary sort.
            case 3 -> sorted(zipf(size, between(2, 60)));
            // Gaps, so a rank is not a document id.
            case 4 -> sparse(zipf(size, between(2, 60)));
            // Values longer than a chunk, so one spans blocks on its own.
            case 5 -> longValues(Math.min(size, 60));
            // Values of no bytes among others, which are terms like any other.
            case 6 -> withEmpties(zipf(size, between(2, 30)));
            // One value repeated, the narrowest dictionary there is.
            default -> zipf(size, 1);
        };
    }

    private BytesRef[] distinct(int size) {
        final BytesRef[] values = new BytesRef[size];
        for (int d = 0; d < size; d++) {
            values[d] = new BytesRef("id-" + d + "-" + randomAlphaOfLengthBetween(1, 20));
        }
        return values;
    }

    private BytesRef[] zipf(int size, int cardinality) {
        final String[] terms = new String[cardinality];
        for (int t = 0; t < cardinality; t++) {
            terms[t] = randomAlphaOfLengthBetween(1, 30);
        }
        final BytesRef[] values = new BytesRef[size];
        for (int d = 0; d < size; d++) {
            // Squaring biases towards the front of the vocabulary, so a few terms carry most of the column.
            final double skew = randomDouble() * randomDouble();
            values[d] = new BytesRef(terms[(int) (skew * cardinality)]);
        }
        return values;
    }

    private static BytesRef[] sorted(BytesRef[] values) {
        final BytesRef[] copy = values.clone();
        java.util.Arrays.sort(copy);
        return copy;
    }

    private BytesRef[] sparse(BytesRef[] values) {
        for (int d = 0; d < values.length; d++) {
            if (randomBoolean()) {
                values[d] = null;
            }
        }
        return values;
    }

    private BytesRef[] longValues(int size) {
        final BytesRef[] values = new BytesRef[size];
        for (int d = 0; d < size; d++) {
            values[d] = new BytesRef(randomAlphaOfLengthBetween(400, 2000));
        }
        return values;
    }

    private BytesRef[] withEmpties(BytesRef[] values) {
        for (int d = 0; d < values.length; d++) {
            if (rarely()) {
                values[d] = new BytesRef("");
            }
        }
        return values;
    }

    /** What the column says about itself has to agree with the values it was given. */
    private static void assertSelfConsistent(BytesRef[] docValues, StringColumnMetadata metadata) {
        int present = 0;
        for (BytesRef value : docValues) {
            if (value != null) {
                present++;
            }
        }
        assertEquals("documents with a value", present, metadata.numDocsWithField());
        assertEquals("values", present, metadata.numValues());
        if (metadata.layout() == StringColumnLayout.DICTIONARY) {
            assertTrue("a dictionary names something", metadata.dictionarySize() > 0);
            assertTrue("everything is either named or escaped", metadata.dictionarySize() + metadata.exceptions().numValues() > 0);
        }
    }

    private static void assertInOrder(BytesRef[] docValues, StringColumnReader reader) throws IOException {
        final ColumnIterator iterator = reader.iterator();
        int seen = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            assertEquals("doc " + doc, docValues[doc], reader.valueAt(reader.firstValueAddress(iterator.rank())));
            seen++;
        }
        assertEquals("documents with a value", numDocsWithField(docValues), seen);
    }

    /** Read out of order, so a block is reached without the one before it having been decoded. */
    private void assertAtRandom(BytesRef[] docValues, StringColumnReader reader) throws IOException {
        final List<Integer> present = new ArrayList<>();
        final ColumnIterator iterator = reader.iterator();
        final List<Integer> ranks = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            present.add(doc);
            ranks.add(iterator.rank());
        }
        final List<Integer> order = new ArrayList<>();
        for (int i = 0; i < present.size(); i++) {
            order.add(i);
        }
        Collections.shuffle(order, random());
        for (int i : order) {
            final int doc = present.get(i);
            assertEquals("doc " + doc + " out of order", docValues[doc], reader.valueAt(reader.firstValueAddress(ranks.get(i))));
        }
    }
}
