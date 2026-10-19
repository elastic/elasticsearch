/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import static org.hamcrest.Matchers.greaterThan;

public class PlainSkipBoundsFormatTests extends ESTestCase {

    private static final int DOCS = 20_000;

    public void testTheBytesOfSegmentsAndOfTheirMergeIncludeTheSkipIndex() throws IOException {
        final BytesRef[] values = StringData.DESCENDING_POD_NAME.generate(DOCS, new Random(randomLong()));
        final long oneEntry = Long.BYTES + Integer.BYTES + Long.BYTES + Byte.BYTES;
        for (boolean merge : new boolean[] { false, true }) {
            try (Directory plainDirectory = newDirectory(); Directory boundedDirectory = newDirectory()) {
                final long plain = StringFormat.COLUMNAR_PLAIN.writeSegments(plainDirectory, values, DOCS / 4, merge);
                final long bounded = StringFormat.COLUMNAR_PLAIN_BOUNDED.writeSegments(boundedDirectory, values, DOCS / 4, merge);
                assertThat("merge " + merge, bounded, greaterThan(plain + oneEntry));
            }
        }
        try (Directory plainDirectory = newDirectory(); Directory boundedDirectory = newDirectory()) {
            StringFormat.COLUMNAR_PLAIN.writeSegments(plainDirectory, values, DOCS / 4, false);
            StringFormat.COLUMNAR_PLAIN_BOUNDED.writeSegments(boundedDirectory, values, DOCS / 4, false);
            final long plain = StringFormat.COLUMNAR_PLAIN.mergeSegments(plainDirectory);
            final long bounded = StringFormat.COLUMNAR_PLAIN_BOUNDED.mergeSegments(boundedDirectory);
            assertThat("merge of written segments", bounded, greaterThan(plain + oneEntry));
        }
    }

    public void testBothFormatsAnswerAlikeAndOnlyTheBoundedOneWritesTheIndex() throws IOException {
        for (StringData data : new StringData[] {
            StringData.SORTED_POD_NAME,
            StringData.DESCENDING_POD_NAME,
            StringData.SHUFFLED_POD_NAME,
            StringData.TRACE_ID }) {
            final BytesRef[] values = data.generate(DOCS, new Random(randomLong()));
            final BytesRef[] sorted = values.clone();
            Arrays.sort(sorted);
            try (Directory plainDirectory = newDirectory(); Directory boundedDirectory = newDirectory()) {
                StringFormat.COLUMNAR_PLAIN.write(plainDirectory, values);
                StringFormat.COLUMNAR_PLAIN_BOUNDED.write(boundedDirectory, values);
                try (
                    StringFormat.Column plain = StringFormat.COLUMNAR_PLAIN.open(plainDirectory, DOCS, 1024);
                    StringFormat.Column bounded = StringFormat.COLUMNAR_PLAIN_BOUNDED.open(boundedDirectory, DOCS, 1024)
                ) {
                    StringFormat.COLUMNAR_PLAIN.requirePlainLayout(plain, DOCS);
                    StringFormat.COLUMNAR_PLAIN_BOUNDED.requirePlainLayout(bounded, DOCS);
                    assertThat(bounded.skipIndexBytes(), greaterThan(plain.skipIndexBytes()));
                    assertSameAnswers(data.toString(), plain, bounded, sorted);
                }
            }
        }
    }

    private static void assertSameAnswers(String label, StringFormat.Column plain, StringFormat.Column bounded, BytesRef[] sorted)
        throws IOException {
        for (int i = 0; i < 20; i++) {
            final int low = between(0, sorted.length - 2);
            final int high = Math.min(sorted.length - 1, low + between(0, sorted.length / 10));
            final long range = plain.queryRange(sorted[low], sorted[high]);
            assertThat(label + " range", range, greaterThan(0L));
            assertEquals(label + " range", range, bounded.queryRange(sorted[low], sorted[high]));

            final BytesRef value = sorted[between(0, sorted.length - 1)];
            final BytesRef prefix = new BytesRef(value.bytes, value.offset, between(1, value.length));
            assertEquals(label + " prefix", plain.queryPrefix(prefix), bounded.queryPrefix(prefix));

            final NavigableSet<BytesRef> terms = new TreeSet<>();
            for (int t = between(1, 8); t > 0; t--) {
                terms.add(sorted[between(0, sorted.length - 1)]);
            }
            terms.add(new BytesRef("absent-" + i));
            assertEquals(label + " terms", plain.queryTerms(terms), bounded.queryTerms(terms));
        }
    }
}
