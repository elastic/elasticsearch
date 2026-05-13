/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * test per-segment lookup of version-related data structures
 */
public class VersionLookupTests extends ESTestCase {

    /**
     * test version lookup actually works
     */
    public void testSimple() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        // found doc
        DocIdAndVersion result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(0, result.docId);
        // not found doc
        assertNull(lookup.lookupVersion(new BytesRef("7"), randomBoolean(), segment));
        // deleted doc
        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "6"));
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertNull(lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment));
        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * test version lookup with two documents matching the ID
     */
    public void testTwoDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        // return the last doc when there are duplicates
        DocIdAndVersion result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete the first doc only
        assertTrue(writer.tryDeleteDocument(reader, 0) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete both docs
        assertTrue(writer.tryDeleteDocument(reader, 1) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertNull(lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment));
        reader.close();
        writer.close();
        dir.close();
    }

    public void testLoadTimestampRange() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, 1_000));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "8", Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, 1_000_000));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);

        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), true);
        assertTrue(lookup.loadedTimestampRange);
        assertEquals(1_000L, lookup.minTimestamp);
        assertEquals(1_000_000L, lookup.maxTimestamp);

        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertFalse(lookup.loadedTimestampRange);
        assertEquals(0L, lookup.minTimestamp);
        assertEquals(Long.MAX_VALUE, lookup.maxTimestamp);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupBasic() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 10L, 1L));
        writer.addDocument(makeDoc("b", 2L, 20L, 1L));
        writer.addDocument(makeDoc("c", 3L, 30L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c");

        assertNotNull(results[0]);
        assertEquals(1L, results[0].version);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);
        assertNotNull(results[2]);
        assertEquals(3L, results[2].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupPartialMiss() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 10L, 1L, 1L));
        writer.addDocument(makeDoc("c", 30L, 3L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c");

        assertNotNull(results[0]);
        assertEquals(10L, results[0].version);
        assertNull(results[1]);
        assertNotNull(results[2]);
        assertEquals(30L, results[2].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupMultiSegment() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.addDocument(makeDoc("b", 2L, 2L, 1L));
        writer.flush();
        writer.addDocument(makeDoc("c", 3L, 3L, 1L));
        writer.addDocument(makeDoc("d", 4L, 4L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c", "d");

        for (int i = 0; i < 4; i++) {
            assertNotNull(results[i]);
            assertEquals((i + 1), results[i].version);
        }

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupNewerSegmentWins() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.flush();
        writer.addDocument(makeDoc("a", 2L, 2L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchLookup(reader, "a");

        assertNotNull(results[0]);
        assertEquals(2L, results[0].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupLoadSeqNoFlag() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 5L, 42L, 7L));
        DirectoryReader reader = DirectoryReader.open(writer);

        BytesRef[] uids = { new BytesRef("a") };

        DocIdAndVersion[] withSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[] { true }, withSeqNo);
        assertNotNull(withSeqNo[0]);
        assertEquals(5L, withSeqNo[0].version);
        assertEquals(42L, withSeqNo[0].seqNo);
        assertEquals(7L, withSeqNo[0].primaryTerm);

        DocIdAndVersion[] withoutSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[] { false }, withoutSeqNo);
        assertNotNull(withoutSeqNo[0]);
        assertEquals(5L, withoutSeqNo[0].version);
        assertEquals(UNASSIGNED_SEQ_NO, withoutSeqNo[0].seqNo);
        assertEquals(UNASSIGNED_PRIMARY_TERM, withoutSeqNo[0].primaryTerm);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupDeletedDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.addDocument(makeDoc("b", 2L, 2L, 1L));
        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "a"));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b");

        assertNull(results[0]);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupEmpty() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        batchLookup(reader); // no exception

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupAllMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "x", "y", "z");

        assertNull(results[0]);
        assertNull(results[1]);
        assertNull(results[2]);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testLoadTimestampRangeWithDeleteTombstone() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        var randomSeqNoIndexOptions = randomFrom(SeqNoFieldMapper.SeqNoIndexOptions.values());
        writer.addDocument(ParsedDocument.deleteTombstone(randomSeqNoIndexOptions, "_id").docs().getFirst());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), true);
        assertTrue(lookup.loadedTimestampRange);
        assertEquals(0L, lookup.minTimestamp);
        assertEquals(Long.MAX_VALUE, lookup.maxTimestamp);
        reader.close();
        writer.close();
        dir.close();
    }

    private static IndexWriter newNoMergeWriter(Directory dir) throws IOException {
        return new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE));
    }

    private static DocIdAndVersion[] batchLookup(DirectoryReader reader, String... ids) throws IOException {
        BytesRef[] uids = new BytesRef[ids.length];
        for (int i = 0; i < ids.length; i++) {
            uids[i] = new BytesRef(ids[i]);
        }
        DocIdAndVersion[] results = new DocIdAndVersion[ids.length];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[ids.length], results);
        return results;
    }

    private static Document makeDoc(String id, long version, long seqNo, long primaryTerm) {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, id, Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, version));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTerm));
        return doc;
    }
}
