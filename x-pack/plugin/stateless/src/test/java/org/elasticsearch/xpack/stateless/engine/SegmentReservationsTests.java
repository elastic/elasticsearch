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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.ToLongFunction;

public class SegmentReservationsTests extends ESTestCase {

    /** Constant cost so test arithmetic is obvious. */
    private static final ToLongFunction<SegmentCommitInfo> CONSTANT_100 = sci -> 100L;

    private static IndexWriterConfig newIwc() {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        return iwc;
    }

    public void testReserveAndReleaseSingleReader() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(doc("1"));
            writer.addDocument(doc("2"));
            writer.commit();
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(1, infos.size());

            SegmentReservations reservations = new SegmentReservations();
            assertEquals(0, reservations.totalBytes());

            long delta = reservations.reserve(infos, CONSTANT_100);
            assertEquals(100L, delta);
            assertEquals(100L, reservations.totalBytes());
            assertEquals(1, reservations.trackedSegmentCount());

            long released = reservations.release(SegmentReservations.keysOf(infos));
            assertEquals(100L, released);
            assertEquals(0, reservations.totalBytes());
            assertEquals(0, reservations.trackedSegmentCount());
        }
    }

    public void testReserveSharedSegmentsAcrossReadersIsCountedOnce() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(doc("1"));
            writer.commit();
            SegmentInfos initial = SegmentInfos.readLatestCommit(dir);

            writer.addDocument(doc("2"));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);
            assertEquals(2, next.size());

            SegmentReservations reservations = new SegmentReservations();
            reservations.reserve(initial, CONSTANT_100);
            // The new infos contains the original segment shared with `initial` plus one new segment.
            long delta = reservations.reserve(next, CONSTANT_100);
            assertEquals("only the new segment must be charged", 100L, delta);
            assertEquals(200L, reservations.totalBytes());
            assertEquals(2, reservations.trackedSegmentCount());
        }
    }

    public void testReleaseHonorsRefcountFromSharedSegments() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(doc("1"));
            writer.commit();
            SegmentInfos initial = SegmentInfos.readLatestCommit(dir);

            writer.addDocument(doc("2"));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);
            assertEquals(2, next.size());

            SegmentReservations reservations = new SegmentReservations();
            reservations.reserve(initial, CONSTANT_100);
            reservations.reserve(next, CONSTANT_100);
            assertEquals(200L, reservations.totalBytes());

            // Closing the initial reader must NOT release the bytes for the segment it shares with `next`.
            long released = reservations.release(SegmentReservations.keysOf(initial));
            assertEquals("shared segment must stay reserved while another reader holds it", 0L, released);
            assertEquals("both segments are still tracked because `next` still holds them", 200L, reservations.totalBytes());

            released = reservations.release(SegmentReservations.keysOf(next));
            assertEquals("closing the last holder releases both segments", 200L, released);
            assertEquals(0L, reservations.totalBytes());
        }
    }

    public void testDocValuesGenChangeAllocatesSeparateReservation() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            // Two-doc segment so the segment isn't fully deleted when one doc is soft-deleted below.
            writer.addDocument(doc("1"));
            writer.addDocument(doc("2"));
            writer.commit();
            SegmentInfos initial = SegmentInfos.readLatestCommit(dir);

            writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);
            // Same segment data files, but a new doc-values generation — the upcoming SegmentReader will allocate
            // its own FixedBitSet, so it must be tracked as a distinct reservation.
            assertEquals(initial.size(), next.size());
            assertTrue(next.asList().get(0).getDocValuesGen() > initial.asList().get(0).getDocValuesGen());

            SegmentReservations reservations = new SegmentReservations();
            reservations.reserve(initial, CONSTANT_100);
            long delta = reservations.reserve(next, CONSTANT_100);
            assertEquals("new doc-values generation must be charged separately", 100L, delta);
            assertEquals(200L, reservations.totalBytes());
            assertEquals(2, reservations.trackedSegmentCount());

            // Each reader release frees only its own generation.
            assertEquals(100L, reservations.release(SegmentReservations.keysOf(initial)));
            assertEquals(100L, reservations.totalBytes());
            assertEquals(100L, reservations.release(SegmentReservations.keysOf(next)));
            assertEquals(0L, reservations.totalBytes());
        }
    }

    public void testPredictDeltaIsPure() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIwc())) {
            writer.addDocument(doc("1"));
            writer.commit();
            SegmentInfos initial = SegmentInfos.readLatestCommit(dir);

            writer.addDocument(doc("2"));
            writer.commit();
            SegmentInfos next = SegmentInfos.readLatestCommit(dir);

            SegmentReservations reservations = new SegmentReservations();
            reservations.reserve(initial, CONSTANT_100);
            assertEquals(100L, reservations.totalBytes());
            assertEquals(1, reservations.trackedSegmentCount());

            // predictDelta must not mutate any state.
            long predicted = reservations.predictDelta(next, CONSTANT_100);
            assertEquals(100L, predicted);
            assertEquals(100L, reservations.totalBytes());
            assertEquals(1, reservations.trackedSegmentCount());

            // And it must match what reserve() reports.
            long delta = reservations.reserve(next, CONSTANT_100);
            assertEquals(predicted, delta);
        }
    }

    private static Document doc(String id) {
        Document d = new Document();
        d.add(new StringField("id", id, Field.Store.YES));
        return d;
    }
}
