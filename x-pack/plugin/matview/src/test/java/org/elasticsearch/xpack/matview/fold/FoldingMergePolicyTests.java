/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.matview.fold.compute.ComputeReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Indexes many partial {@code {bucket, host, cpu.sum, cpu.value_count}} rows across several segments, with deletes,
 * and checks that a force merge leaves exactly one row per (bucket, host) group carrying the summed values, in index
 * sort order (host ascending, bucket descending). A second
 * round indexes more rows and merges again, so an already folded row folds further. Soft deletes are off: the writer
 * then accepts a merge wrapper that hides live documents without any Lucene change.
 */
public class FoldingMergePolicyTests extends ComputeTestCase {

    private static final long HOUR = 3_600_000L;
    private static final List<FoldSpec.ReducedField> REDUCED = List.of(
        new FoldSpec.ReducedField("cpu.sum", ElementType.DOUBLE, FoldSpec.Reduction.SUM),
        new FoldSpec.ReducedField("cpu.value_count", ElementType.INT, FoldSpec.Reduction.SUM)
    );
    /** Groups by time bucket only. */
    private static final FoldSpec BUCKET_SPEC = new FoldSpec("@timestamp", REDUCED);
    /** Groups by a keyword host and time bucket, in the order of the index sort the sorted strategy needs. */
    private static final FoldSpec BUCKET_HOST_SPEC = new FoldSpec(
        List.of(new FoldSpec.KeyField("host", ElementType.BYTES_REF), new FoldSpec.KeyField("@timestamp", ElementType.LONG)),
        REDUCED
    );

    /** Ordered like the index sort of the folded index: host ascending, then bucket descending. */
    private record Group(long bucket, String host) implements Comparable<Group> {
        @Override
        public int compareTo(Group o) {
            int c = host.compareTo(o.host);
            return c != 0 ? c : Long.compare(o.bucket, bucket);
        }
    }

    /** Expected reduced state per group, maintained by brute force. */
    private record Expected(double sum, long count) {
        Expected plus(double s, long c) {
            return new Expected(sum + s, count + c);
        }

        Expected minus(double s, long c) {
            return new Expected(sum - s, count - c);
        }
    }

    private record Row(Group group, double sum, long count) {}

    /** The index sort the sorted strategy relies on: host ascending, bucket descending. */
    private static Sort hostThenTimeDesc() {
        return new Sort(new SortField("host", SortField.Type.STRING), new SortedNumericSortField("@timestamp", SortField.Type.LONG, true));
    }

    private HashFoldStrategy hashStrategy(FoldSpec spec) {
        return new HashFoldStrategy(new ComputeReducer(blockFactory(), spec, between(1, 64)));
    }

    /** The merge-sort strategy needs the fold key to lead the index sort. */
    public void testSortedStrategyFoldsGroups() throws IOException {
        SortedFoldStrategy strategy = SortedFoldStrategy.forSort(BUCKET_HOST_SPEC, hostThenTimeDesc());
        assertNotNull(strategy);
        assertNull(
            "the key must be a sort prefix, in order",
            SortedFoldStrategy.forSort(
                BUCKET_HOST_SPEC,
                new Sort(new SortedNumericSortField("@timestamp", SortField.Type.LONG, true), new SortField("host", SortField.Type.STRING))
            )
        );
        assertNull("too short", SortedFoldStrategy.forSort(BUCKET_HOST_SPEC, new Sort(new SortField("host", SortField.Type.STRING))));
        foldsGroups(strategy, true);
    }

    /** The hash strategy does not care whether the index is sorted. */
    public void testHashStrategyFoldsGroups() throws IOException {
        foldsGroups(hashStrategy(BUCKET_HOST_SPEC), randomBoolean());
    }

    private void foldsGroups(FoldStrategy strategy, boolean sorted) throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            if (sorted) {
                iwc.setIndexSort(hostThenTimeDesc());
            }
            iwc.setMergePolicy(new FoldingMergePolicy(newLogMergePolicy(), BUCKET_HOST_SPEC, strategy, null));
            int numBuckets = between(1, 20);
            int numHosts = between(1, 5);
            Map<Group, Expected> expected = new TreeMap<>();
            Map<String, Row> byId = new HashMap<>();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                int nextId = indexRound(writer, 0, numBuckets, numHosts, expected, byId);
                writer.forceMerge(1);
                assertFolded(writer, expected, sorted);

                // a second round: new contributions fold into the already folded representatives
                indexRound(writer, nextId, numBuckets, numHosts, expected, byId);
                writer.forceMerge(1);
                assertFolded(writer, expected, sorted);
            }
        }
    }

    /**
     * Documents the guard excludes (here: those with an odd {@code seq}) must survive the merge untouched, even
     * when they share a bucket with folded documents, and must not contribute to the folded values.
     */
    public void testGuardedDocumentsAreNeitherFoldedNorHidden() throws IOException {
        FoldStrategy strategy = randomBoolean()
            ? hashStrategy(BUCKET_SPEC)
            : SortedFoldStrategy.forSort(BUCKET_SPEC, new Sort(new SortedNumericSortField("@timestamp", SortField.Type.LONG)));
        FoldGuard oddSeqIsRetained = reader -> {
            FixedBitSet foldable = new FixedBitSet(reader.maxDoc());
            var seq = reader.getNumericDocValues("seq");
            for (int doc = seq.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = seq.nextDoc()) {
                if (seq.longValue() % 2 == 0) {
                    foldable.set(doc);
                }
            }
            return foldable;
        };
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setIndexSort(new Sort(new SortedNumericSortField("@timestamp", SortField.Type.LONG)));
            iwc.setMergePolicy(new FoldingMergePolicy(newLogMergePolicy(), BUCKET_SPEC, strategy, oddSeqIsRetained));
            int numBuckets = between(1, 10);
            int numDocs = between(2, numBuckets * 20);
            Map<Long, Expected> folded = new TreeMap<>();
            List<double[]> retained = new ArrayList<>();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int seq = 0; seq < numDocs; seq++) {
                    long bucket = HOUR * randomIntBetween(0, numBuckets - 1);
                    double sum = randomDoubleBetween(-1000, 1000, true);
                    long count = randomIntBetween(1, 10);
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("@timestamp", bucket));
                    doc.add(new SortedNumericDocValuesField("cpu.sum", NumericUtils.doubleToSortableLong(sum)));
                    doc.add(new SortedNumericDocValuesField("cpu.value_count", count));
                    doc.add(new NumericDocValuesField("seq", seq));
                    writer.addDocument(doc);
                    if (seq % 2 == 0) {
                        folded.merge(bucket, new Expected(sum, count), (a, b) -> a.plus(b.sum, b.count));
                    } else {
                        retained.add(new double[] { bucket, sum, count });
                    }
                    if (seq == 0 || rarely()) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    LeafReader leaf = reader.leaves().get(0).reader();
                    assertThat(leaf.numDocs(), equalTo(folded.size() + retained.size()));
                    var seq = leaf.getNumericDocValues("seq");
                    var keys = leaf.getSortedNumericDocValues("@timestamp");
                    var sums = leaf.getSortedNumericDocValues("cpu.sum");
                    var counts = leaf.getSortedNumericDocValues("cpu.value_count");
                    int retainedSeen = 0;
                    int foldedSeen = 0;
                    for (int doc = seq.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = seq.nextDoc()) {
                        assertTrue(keys.advanceExact(doc) && sums.advanceExact(doc) && counts.advanceExact(doc));
                        long bucket = keys.nextValue();
                        double sum = NumericUtils.sortableLongToDouble(sums.nextValue());
                        long count = counts.nextValue();
                        if (seq.longValue() % 2 == 1) {
                            double[] original = retained.get((int) (seq.longValue() / 2));
                            assertThat(bucket, equalTo((long) original[0]));
                            assertThat(sum, equalTo(original[1]));
                            assertThat(count, equalTo((long) original[2]));
                            retainedSeen++;
                        } else {
                            Expected e = folded.get(bucket);
                            assertThat(sum, closeTo(e.sum(), 1e-6 * Math.max(1, Math.abs(e.sum()))));
                            assertThat(count, equalTo(e.count()));
                            foldedSeen++;
                        }
                    }
                    assertThat(retainedSeen, equalTo(retained.size()));
                    assertThat(foldedSeen, equalTo(folded.size()));
                }
            }
        }
    }

    /**
     * Indexes a random number of rows over random buckets and hosts, flushing now and then so the merge sees several
     * segments, and deletes a few rows again. Returns the next free id.
     */
    private int indexRound(
        IndexWriter writer,
        int firstId,
        int numBuckets,
        int numHosts,
        Map<Group, Expected> expected,
        Map<String, Row> byId
    ) throws IOException {
        int numDocs = between(2, numBuckets * numHosts * 10);
        int id = firstId;
        for (int i = 0; i < numDocs; i++, id++) {
            Group group = new Group(HOUR * randomIntBetween(0, numBuckets - 1), "host-" + randomIntBetween(0, numHosts - 1));
            double sum = randomDoubleBetween(-1000, 1000, true);
            long count = randomIntBetween(1, 10);
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField("@timestamp", group.bucket()));
            doc.add(new SortedDocValuesField("host", new BytesRef(group.host())));
            doc.add(new StringField("host", group.host(), Field.Store.NO));
            doc.add(new SortedNumericDocValuesField("cpu.sum", NumericUtils.doubleToSortableLong(sum)));
            doc.add(new SortedNumericDocValuesField("cpu.value_count", count));
            doc.add(new StringField("_id", Integer.toString(id), Field.Store.YES));
            doc.add(new StoredField("_source", "{}"));
            writer.addDocument(doc);
            expected.merge(group, new Expected(sum, count), (a, b) -> a.plus(b.sum, b.count));
            byId.put(Integer.toString(id), new Row(group, sum, count));
            if (i == 0 || rarely()) {
                // the first flush guarantees at least two segments for the merge to work on
                writer.flush();
            }
        }
        // delete some rows, but never the first of the round so the first segment is never dropped entirely
        for (int i = firstId + 1; i < id; i++) {
            if (randomInt(9) == 0) {
                String key = Integer.toString(i);
                Row row = byId.remove(key);
                writer.deleteDocuments(new Term("_id", key));
                Expected e = expected.get(row.group()).minus(row.sum(), row.count());
                if (e.count() == 0) {
                    expected.remove(row.group());
                } else {
                    expected.put(row.group(), e);
                }
            }
        }
        return id;
    }

    private static void assertFolded(IndexWriter writer, Map<Group, Expected> expected, boolean sorted) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertThat(reader.leaves(), hasSize(1));
            LeafReader leaf = reader.leaves().get(0).reader();
            assertThat(leaf.numDocs(), equalTo(expected.size()));
            assertThat(leaf.maxDoc(), equalTo(expected.size()));
            SortedNumericDocValues keys = leaf.getSortedNumericDocValues("@timestamp");
            SortedDocValues hosts = leaf.getSortedDocValues("host");
            SortedNumericDocValues sums = leaf.getSortedNumericDocValues("cpu.sum");
            SortedNumericDocValues counts = leaf.getSortedNumericDocValues("cpu.value_count");
            Group previous = null;
            int seen = 0;
            for (int doc = keys.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = keys.nextDoc()) {
                assertThat(keys.docValueCount(), equalTo(1));
                assertTrue(hosts.advanceExact(doc));
                Group group = new Group(keys.nextValue(), hosts.lookupOrd(hosts.ordValue()).utf8ToString());
                if (sorted) {
                    assertTrue("index sort must hold after folding", previous == null || group.compareTo(previous) > 0);
                }
                previous = group;
                Expected e = expected.get(group);
                assertNotNull("unexpected group " + group, e);
                assertTrue(sums.advanceExact(doc));
                assertThat(sums.docValueCount(), equalTo(1));
                double sum = NumericUtils.sortableLongToDouble(sums.nextValue());
                assertThat("sum of " + group, sum, closeTo(e.sum(), 1e-6 * Math.max(1, Math.abs(e.sum()))));
                assertTrue(counts.advanceExact(doc));
                assertThat(counts.docValueCount(), equalTo(1));
                assertThat("count of " + group, counts.nextValue(), equalTo(e.count()));
                assertNotNull(leaf.storedFields().document(doc).get("_id"));
                seen++;
            }
            assertThat(seen, equalTo(expected.size()));
            assertThat(leaf.terms("_id").size(), equalTo((long) expected.size()));
            // hidden documents' postings vanish with them: every host term that survives is used by a surviving document
            assertThat(leaf.terms("host").getDocCount(), equalTo(expected.size()));
        }
    }
}
