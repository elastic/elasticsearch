/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.TrackingPostingsInMemoryBytesCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class DirectoryReaderHeapEstimatorTests extends ESTestCase {

    private static IndexWriterConfig newIwc() {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        // Wrap with the tracking codec so segments carry the postings-bytes attribute used by the estimator.
        iwc.setCodec(new TrackingPostingsInMemoryBytesCodec(iwc.getCodec()));
        return iwc;
    }

    public void testEstimateSumsContributions() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            for (int seg = 0; seg < 3; seg++) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + seg, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(3, infos.size());

            long estimate = DirectoryReaderHeapEstimator.estimate(infos);
            // At minimum every segment contributes the baseline; postings + bitset add to it.
            assertTrue(
                "estimate must cover at least the per-segment baseline for every segment",
                estimate >= 3 * DirectoryReaderHeapEstimator.PER_SEGMENT_BASELINE_BYTES
            );
            // Sanity: the sum matches per-segment summation through the public surface.
            long sum = 0L;
            for (var sci : infos) {
                sum += DirectoryReaderHeapEstimator.segmentBytes(sci);
            }
            assertEquals(sum, estimate);
        }
    }

    public void testDeltaIsZeroWhenNothingChanges() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(singleDoc("1"));
            writer.commit();
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(0L, DirectoryReaderHeapEstimator.delta(infos, infos));
        }
    }

    public void testDeltaCountsOnlyNewBitsetWhenSoftDeleteGenChanges() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            // Two-doc segment so the segment isn't fully deleted by the soft delete below.
            writer.addDocument(singleDoc("1"));
            writer.addDocument(singleDoc("2"));
            writer.commit();
            SegmentInfos current = SegmentInfos.readLatestCommit(dir);
            assertEquals(1, current.size());

            writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);
            assertEquals(1, next.size());
            // Lucene assigns a new soft-delete generation when soft deletes are applied.
            assertTrue(next.asList().get(0).getDocValuesGen() > current.asList().get(0).getDocValuesGen());

            long delta = DirectoryReaderHeapEstimator.delta(current, next);
            // Only the bitset is incremental — baseline and postings are shared with the previous reader.
            assertEquals(DirectoryReaderHeapEstimator.softDeleteBitsetBytes(next.asList().get(0)), delta);
            // And that matches the closed-form for FixedBitSet over maxDoc longs.
            int maxDoc = next.asList().get(0).info.maxDoc();
            assertEquals(((((long) maxDoc) + 63) >>> 6) << 3, delta);
        }
    }

    public void testSoftDeleteBitsetBytesDoesNotOverflowAtMaxDoc() throws IOException {
        // Pin the int→long widening: maxDoc + 63 must happen in long arithmetic, otherwise the addition
        // overflows near Integer.MAX_VALUE and the unsigned right shift produces a huge bogus value.
        try (Directory dir = newDirectory()) {
            SegmentInfo info = new SegmentInfo(
                dir,
                Version.LATEST,
                Version.LATEST,
                "_overflow",
                Integer.MAX_VALUE,
                false,
                false,
                null,
                Map.of(),
                new byte[16],
                Map.of(),
                null
            );
            SegmentCommitInfo sci = new SegmentCommitInfo(info, 0, 1, -1L, -1L, -1L, new byte[16]);
            long expected = ((((long) Integer.MAX_VALUE) + 63) >>> 6) << 3;
            assertEquals(expected, DirectoryReaderHeapEstimator.softDeleteBitsetBytes(sci));
            assertTrue("bitset bytes must be positive for max-int maxDoc", DirectoryReaderHeapEstimator.softDeleteBitsetBytes(sci) > 0);
        }
    }

    public void testDeltaCountsFullSegmentBytesForNewSegment() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(singleDoc("1"));
            writer.commit();
            SegmentInfos current = SegmentInfos.readLatestCommit(dir);

            writer.addDocument(singleDoc("2"));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);
            assertEquals(current.size() + 1, next.size());

            long delta = DirectoryReaderHeapEstimator.delta(current, next);
            // Identify the new segment as the one not present in current by name.
            var newSegmentName = next.asList().get(next.size() - 1).info.name;
            var newSci = next.asList().stream().filter(s -> s.info.name.equals(newSegmentName)).findFirst().orElseThrow();
            assertEquals(DirectoryReaderHeapEstimator.segmentBytes(newSci), delta);
        }
    }

    public void testPostingsAttributeIsReadFromSegment() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(singleDoc("with-postings"));
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            // Tracking codec must have stamped the attribute on the written segment.
            String attr = infos.asList().get(0).info.getAttribute(TrackingPostingsInMemoryBytesCodec.IN_MEMORY_POSTINGS_BYTES_KEY);
            assertNotNull("tracking codec must record postings bytes on the segment", attr);
            long expected = Long.parseLong(attr);
            assertEquals(expected, DirectoryReaderHeapEstimator.postingsBytes(infos.asList().get(0)));
        }
    }

    private static Document singleDoc(String id) {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        return doc;
    }
}
