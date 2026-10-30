/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.SliceRanges.Range;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SliceRangesTests extends ESTestCase {

    public void testOneSliceIsOneRange() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("only", 20_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                List<Range> ranges = SliceRanges.collect(leaf, SliceTestIndex.FIELD);
                // A long run of one value is extended past the skip interval, and adjacent intervals of one slice are
                // reported as the single run of documents they are.
                assertThat(ranges, hasSize(1));
                Range range = ranges.get(0);
                assertTrue(range.isSingleSlice());
                assertThat(range.minDoc(), equalTo(0));
                assertThat(range.maxDoc(), equalTo(19_999));
                assertThat(range.docCount(), equalTo(20_000));
            }
        }
    }

    public void testRangesCoverTheSegmentInDocOrder() throws IOException {
        Map<String, Integer> slices = Map.of("a", 5_000, "b", 100, "c", 20_000, "d", 7);
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, slices, true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                List<Range> ranges = SliceRanges.collect(leaf, SliceTestIndex.FIELD);
                assertThat(ranges, hasSize(greaterThan(1)));

                int expectedNextDoc = 0;
                int expectedMinSliceId = 0;
                int totalDocs = 0;
                for (Range range : ranges) {
                    assertThat("ranges are contiguous and in doc order", range.minDoc(), equalTo(expectedNextDoc));
                    assertThat(range.maxDoc(), greaterThan(range.minDoc() - 1));
                    assertThat("slice ids do not go backwards", range.minSliceId(), greaterThan(expectedMinSliceId - 1));
                    assertThat(range.minSliceId(), lessThanOrEqualTo(range.maxSliceId()));
                    assertThat("the field is dense", range.docCount(), equalTo(range.maxDoc() - range.minDoc() + 1));
                    expectedNextDoc = range.maxDoc() + 1;
                    expectedMinSliceId = range.maxSliceId();
                    totalDocs += range.docCount();
                }
                assertThat(totalDocs, equalTo(slices.values().stream().mapToInt(Integer::intValue).sum()));
            }
        }
    }

    public void testRangeSliceIdsResolveToTheValueOfTheirDocuments() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 5_000, "b", 100, "c", 20_000), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                SliceRanges.Slices slices = SliceRanges.Slices.of(leaf, SliceTestIndex.FIELD);
                SortedDocValues values = leaf.getSortedDocValues(SliceTestIndex.FIELD);
                // A range's slice ids name the slices its first and last documents belong to.
                for (Range range : SliceRanges.collect(leaf, SliceTestIndex.FIELD)) {
                    assertTrue(values.advanceExact(range.minDoc()));
                    assertThat(values.lookupOrd(values.ordValue()), equalTo(slices.key(range.minSliceId())));
                    assertTrue(values.advanceExact(range.maxDoc()));
                    assertThat(values.lookupOrd(values.ordValue()), equalTo(slices.key(range.maxSliceId())));
                }
            }
        }
    }

    public void testSingleSliceRangesAreALowerBoundOnASliceSize() throws IOException {
        Map<String, Integer> slices = Map.of("a", 5_000, "b", 100, "c", 20_000, "d", 7);
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, slices, true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                List<Range> ranges = SliceRanges.collect(leaf, SliceTestIndex.FIELD);

                // Documents in a range that straddles a slice boundary are not attributed, so a slice's attributed
                // count falls short of its real one by at most the two straddling ranges around it.
                int widestMixedRange = 0;
                for (Range range : ranges) {
                    if (range.isSingleSlice() == false) {
                        widestMixedRange = Math.max(widestMixedRange, range.docCount());
                    }
                }
                SliceRanges.Slices ids = SliceRanges.Slices.of(leaf, SliceTestIndex.FIELD);
                for (Map.Entry<String, Integer> slice : slices.entrySet()) {
                    int sliceId = ids.id(new BytesRef(slice.getKey()));
                    int attributed = ranges.stream()
                        .filter(r -> r.isSingleSlice() && r.minSliceId() == sliceId)
                        .mapToInt(Range::docCount)
                        .sum();
                    assertThat(slice.getKey(), attributed, lessThanOrEqualTo(slice.getValue()));
                    assertThat(slice.getKey(), slice.getValue() - attributed, lessThanOrEqualTo(2 * widestMixedRange));
                }
            }
        }
    }

    public void testDocumentsWithoutTheFieldAreInNoRange() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 5_000), false, SliceTestIndex.configWithoutMerges());
            // A document with no slice sorts last and has no doc value, as a tombstone does.
            try (IndexWriter writer = new IndexWriter(dir, SliceTestIndex.config())) {
                for (int i = 0; i < 10; i++) {
                    writer.addDocument(new Document());
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                assertThat(leaf.maxDoc(), equalTo(5_010));
                List<Range> ranges = SliceRanges.collect(leaf, SliceTestIndex.FIELD);
                assertThat(ranges, hasSize(1));
                assertThat(ranges.get(0).docCount(), equalTo(5_000));
                assertThat(ranges.get(0).maxDoc(), equalTo(4_999));
            }
        }
    }

    public void testMultiValuedFieldIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            // A sorted set field can hold a skip index, but a slice is one value a document.
            IndexWriterConfig config = new IndexWriterConfig().setIndexSort(new Sort(new SortedSetSortField(SliceTestIndex.FIELD, false)));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(SortedSetDocValuesField.indexedField(SliceTestIndex.FIELD, new BytesRef("a")));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> SliceRanges.collect(leaf, SliceTestIndex.FIELD)
                );
                assertThat(e.getMessage(), containsString("expected one sorted value a document"));
            }
        }
    }

    public void testFieldWithoutASkipIndexIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            // The plain field type carries no skip index, unlike SortedDocValuesField#indexedField.
            try (IndexWriter writer = new IndexWriter(dir, SliceTestIndex.config())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new SortedDocValuesField(SliceTestIndex.FIELD, new BytesRef("a")));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                expectThrows(IllegalArgumentException.class, () -> SliceRanges.collect(leaf, SliceTestIndex.FIELD));
            }
        }
    }

    public void testUnmappedFieldIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            SliceTestIndex.write(dir, Map.of("a", 10), true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                expectThrows(IllegalArgumentException.class, () -> SliceRanges.collect(leaf, "missing"));
            }
        }
    }
}
