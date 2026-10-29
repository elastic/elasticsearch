/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.elasticsearch.lucene.store.PrefetchableDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class DocValuesBatchPrefetcherTests extends ESTestCase {

    /** One call to {@link PrefetchableDocValues#prefetch}. */
    private record Call(int segment, String field, int stage, int[] docs) {}

    /** A window of a single segment holds as many documents as the budget has room for across its columns. */
    public void testWindowIsSizedByTheBudget() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, 1, 100, 4);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Call> calls = new ArrayList<>();
                LeafReader leaf = recording(reader.leaves().get(0).reader(), 0, 2, calls);
                int[] docs = IntStream.range(0, 50).map(i -> i * 2).toArray();
                DocValuesBatchPrefetcher prefetcher = new DocValuesBatchPrefetcher(12);
                DocValuesBatchPrefetcher.Segment segment = prefetcher.segment(leaf, docs);
                segment.addFields(List.of("f0", "f1", "f2", "f3", "missing"));

                assertThat(prefetcher.cover(segment, 0), equalTo(3));
                // Within the window nothing more is asked for.
                assertThat(prefetcher.cover(segment, 2), equalTo(3));
                assertThat(calls.size(), equalTo(8));
                for (int i = 0; i < calls.size(); i++) {
                    Call call = calls.get(i);
                    // Every column runs a stage before any runs the next one.
                    assertThat(call.stage(), equalTo(i / 4));
                    assertArrayEquals(new int[] { 0, 2, 4 }, call.docs());
                }

                calls.clear();
                assertThat(prefetcher.cover(segment, 3), equalTo(6));
                assertArrayEquals(new int[] { 6, 8, 10 }, calls.get(0).docs());
            }
        }
    }

    /** A window that reaches the end of a segment runs on into the next one while the budget lasts. */
    public void testWindowRunsAcrossSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, 3, 10, 2);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(3));
                List<Call> calls = new ArrayList<>();
                DocValuesBatchPrefetcher prefetcher = new DocValuesBatchPrefetcher(20);
                List<DocValuesBatchPrefetcher.Segment> segments = new ArrayList<>();
                for (int s = 0; s < 3; s++) {
                    LeafReader leaf = recording(reader.leaves().get(s).reader(), s, 1, calls);
                    DocValuesBatchPrefetcher.Segment segment = prefetcher.segment(leaf, new int[] { 1, 3, 5, 7 });
                    segment.addFields(List.of("f0", "f1"));
                    segments.add(segment);
                }

                // Ten documents a round over two columns: the four of the first segment, the four of the second and two of
                // the third.
                assertThat(prefetcher.cover(segments.get(0), 0), equalTo(4));
                assertThat(calls.stream().mapToInt(Call::segment).distinct().toArray(), equalTo(new int[] { 0, 1, 2 }));
                assertArrayEquals(new int[] { 1, 3 }, calls.get(calls.size() - 1).docs());
                calls.clear();
                assertThat(prefetcher.cover(segments.get(1), 3), equalTo(4));
                assertThat(prefetcher.cover(segments.get(2), 1), equalTo(2));
                assertTrue("already covered", calls.isEmpty());
                assertThat(prefetcher.cover(segments.get(2), 2), equalTo(4));
                assertThat(calls.stream().mapToInt(Call::segment).distinct().toArray(), equalTo(new int[] { 2 }));
                assertArrayEquals(new int[] { 5, 7 }, calls.get(0).docs());
            }
        }
    }

    /** A document with more columns than the budget asks for them a group at a time, each group through all its stages. */
    public void testColumnsBeyondTheBudgetAreTakenAGroupAtATime() throws IOException {
        int fields = randomIntBetween(5, 30);
        int budget = randomIntBetween(1, fields - 1);
        int stages = randomIntBetween(1, 3);
        try (Directory dir = newDirectory()) {
            index(dir, 1, 10, fields);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Call> calls = new ArrayList<>();
                LeafReader leaf = recording(reader.leaves().get(0).reader(), 0, stages, calls);
                DocValuesBatchPrefetcher prefetcher = new DocValuesBatchPrefetcher(budget);
                DocValuesBatchPrefetcher.Segment segment = prefetcher.segment(leaf, new int[] { 4, 5 });
                segment.addFields(IntStream.range(0, fields).mapToObj(i -> "f" + i).toList());

                // A second segment, after a window that took more than the budget, stays out of it.
                LeafReader second = recording(reader.leaves().get(0).reader(), 1, stages, new ArrayList<>());
                prefetcher.segment(second, new int[] { 1 }).addFields(List.of("f0"));

                assertThat(prefetcher.cover(segment, 0), equalTo(1));
                assertThat(calls.size(), equalTo(fields * stages));
                int groupStart = 0;
                while (groupStart < calls.size()) {
                    int groupSize = Math.min(budget, fields - groupStart / stages);
                    for (int i = 0; i < groupSize * stages; i++) {
                        Call call = calls.get(groupStart + i);
                        assertThat(call.stage(), equalTo(i / groupSize));
                        assertArrayEquals(new int[] { 4 }, call.docs());
                    }
                    groupStart += groupSize * stages;
                }
            }
        }
    }

    private static void index(Directory dir, int segments, int docsPerSegment, int fields) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int s = 0; s < segments; s++) {
                for (int d = 0; d < docsPerSegment; d++) {
                    Document doc = new Document();
                    for (int f = 0; f < fields; f++) {
                        doc.add(new NumericDocValuesField("f" + f, d));
                    }
                    writer.addDocument(doc);
                }
                writer.flush();
            }
        }
    }

    /** A segment whose numeric columns record their prefetch calls, each taking {@code stages} stages. */
    private static LeafReader recording(LeafReader in, int segment, int stages, List<Call> calls) {
        return new FilterLeafReader(in) {
            @Override
            public NumericDocValues getNumericDocValues(String field) throws IOException {
                NumericDocValues values = super.getNumericDocValues(field);
                return values == null ? null : new RecordingColumn(values, segment, field, stages, calls);
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }

    private static class RecordingColumn extends FilterNumericDocValues implements PrefetchableDocValues {
        private final int segment;
        private final String field;
        private final int stages;
        private final List<Call> calls;

        RecordingColumn(NumericDocValues in, int segment, String field, int stages, List<Call> calls) {
            super(in);
            this.segment = segment;
            this.field = field;
            this.stages = stages;
            this.calls = calls;
        }

        @Override
        public boolean prefetch(int stage, int[] docs, int from, int to) {
            calls.add(new Call(segment, field, stage, Arrays.copyOfRange(docs, from, to)));
            return stage + 1 < stages;
        }
    }
}
