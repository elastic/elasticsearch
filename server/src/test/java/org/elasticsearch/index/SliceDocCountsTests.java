/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.SliceDocCounts.Entry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SliceDocCountsTests extends ESTestCase {

    /** The default skip interval, past which a slice gets ranges of its own. */
    private static final int SKIP_INTERVAL = 4096;

    public void testLargestFindsTheDominantSlice() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("whale", 30_000, "b", 5_000, "c", 5_000, "d", 5_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Entry> largest = SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.25);
                assertThat(largest, hasSize(1));
                assertThat(largest.get(0).sliceKey(), equalTo(new BytesRef("whale")));
                assertThat(largest.get(0).heldDocs(), lessThanOrEqualTo(30_000L));
                assertThat(largest.get(0).heldDocs(), greaterThan(30_000L - 2 * SKIP_INTERVAL));
            }
        }
    }

    public void testLargestOrdersSlicesBySize() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("big", 30_000, "medium", 20_000, "small", 1_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Entry> largest = SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.25);
                assertThat(largest.stream().map(Entry::sliceKey).toList(), contains(new BytesRef("big"), new BytesRef("medium")));
            }
        }
    }

    public void testEvenSlicesHaveNoDominantSlice() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 10_000, "b", 10_000, "c", 10_000, "d", 10_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // Each slice holds a quarter of the index, so none holds the two fifths asked for.
                assertThat(SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.4), empty());
            }
        }
    }

    public void testCountsAreSummedAcrossSegments() throws IOException {
        Map<String, Integer> slices = Map.of("whale", 20_000, "b", 3_000);
        try (Directory dir = newDirectory()) {
            // Two writes with no merge, so each slice is spread over at least two segments.
            SliceTestIndex.write(dir, slices, false, SliceTestIndex.configWithoutMerges());
            SliceTestIndex.write(dir, slices, false, SliceTestIndex.configWithoutMerges());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), greaterThan(1));
                List<Entry> largest = SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.25);
                assertThat(largest, hasSize(1));
                assertThat(largest.get(0).sliceKey(), equalTo(new BytesRef("whale")));
                long shortfall = 40_000L - largest.get(0).heldDocs();
                assertThat(shortfall, lessThanOrEqualTo(2L * SKIP_INTERVAL * reader.leaves().size()));
            }
        }
    }

    public void testSliceAbsentFromSomeSegmentsIsStillCounted() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("whale", 20_000), false, SliceTestIndex.configWithoutMerges());
            // A second segment the whale has no documents in at all, so its ordinal is not in that dictionary.
            SliceTestIndex.write(dir, Map.of("other", 6_000), false, SliceTestIndex.configWithoutMerges());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), greaterThan(1));
                List<Entry> largest = SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.25);
                assertThat(largest, hasSize(1));
                assertThat(largest.get(0).sliceKey(), equalTo(new BytesRef("whale")));
                assertThat(largest.get(0).heldDocs(), equalTo(20_000L));
            }
        }
    }

    public void testWholeIndexIsOneSlice() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("only", 20_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Entry> largest = SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 1.0);
                assertThat(largest, hasSize(1));
                assertThat(largest.get(0).heldDocs(), equalTo(20_000L));
            }
        }
    }

    public void testLiveDocsOfASliceNotStartingOnAWordBoundary() throws IOException {
        // "a" is not a multiple of 64 documents, so "b" starts part way through a word of the live docs.
        int a = 5_003;
        int b = 9_001;
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", a, "b", b), true, SliceTestIndex.configWithoutMerges());
            int deleted = 0;
            try (IndexWriter writer = new IndexWriter(dir, SliceTestIndex.configWithoutMerges())) {
                for (int i = 0; i < b; i += 3) {
                    writer.deleteDocuments(new Term(SliceTestIndex.ID_FIELD, SliceTestIndex.id("b", i)));
                    deleted++;
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, new BytesRef("b")), equalTo((long) b - deleted));
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, new BytesRef("a")), equalTo((long) a));
            }
        }
    }

    public void testLiveDocsIsZeroWhenEveryDocumentIsDeleted() throws IOException {
        int docs = 2_000;
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", docs), false, SliceTestIndex.configWithoutMerges());
            try (IndexWriter writer = new IndexWriter(dir, SliceTestIndex.configWithoutMerges())) {
                for (int i = 0; i < docs; i++) {
                    writer.deleteDocuments(new Term(SliceTestIndex.ID_FIELD, SliceTestIndex.id("a", i)));
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, new BytesRef("a")), equalTo(0L));
            }
        }
    }

    public void testLiveDocsMatchesHeldDocsWithoutDeletions() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 20_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                BytesRef slice = new BytesRef("a");
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, slice), equalTo(20_000L));
            }
        }
    }

    public void testLiveDocsExcludesDeletedDocumentsAcrossWindows() throws IOException {
        // More documents than one live-docs window, so the windowed count is exercised.
        int docs = 70_000;
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", docs), false, SliceTestIndex.configWithoutMerges());
            int deleted = 0;
            try (IndexWriter writer = new IndexWriter(dir, SliceTestIndex.configWithoutMerges())) {
                for (int i = 0; i < docs; i += 7) {
                    writer.deleteDocuments(new Term(SliceTestIndex.ID_FIELD, SliceTestIndex.id("a", i)));
                    deleted++;
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(docs - deleted));
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, new BytesRef("a")), equalTo((long) docs - deleted));
            }
        }
    }

    public void testLiveDocsOfAnAbsentSliceIsZero() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 100), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(SliceDocCounts.liveDocs(reader, SliceTestIndex.FIELD, new BytesRef("absent")), equalTo(0L));
            }
        }
    }

    public void testMinFractionIsValidated() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 100), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                expectThrows(IllegalArgumentException.class, () -> SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 0.0));
                expectThrows(IllegalArgumentException.class, () -> SliceDocCounts.largest(reader, SliceTestIndex.FIELD, 1.5));
            }
        }
    }

    public void testUnmappedFieldIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 100), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                expectThrows(IllegalArgumentException.class, () -> SliceDocCounts.largest(reader, "missing", 0.25));
            }
        }
    }
}
