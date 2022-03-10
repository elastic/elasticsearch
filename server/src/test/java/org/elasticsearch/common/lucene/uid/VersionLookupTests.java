/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;

/**
 * test per-segment lookup of version-related data structures
 */
public class VersionLookupTests extends ESTestCase {

    /**
     * test version lookup actually works
     */
    public void testSimple() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(
            dir,
            new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                // to have deleted docs
                .setMergePolicy(NoMergePolicy.INSTANCE)
        );
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", ProvidedIdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().get(0);
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), IdFieldMapper.NAME);
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
        segment = reader.leaves().get(0);
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), IdFieldMapper.NAME);
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
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE));
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", ProvidedIdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().get(0);
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), IdFieldMapper.NAME);
        // return the last doc when there are duplicates
        DocIdAndVersion result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete the first doc only
        assertTrue(writer.tryDeleteDocument(reader, 0) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().get(0);
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), IdFieldMapper.NAME);
        result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete both docs
        assertTrue(writer.tryDeleteDocument(reader, 1) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().get(0);
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), IdFieldMapper.NAME);
        assertNull(lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment));
        reader.close();
        writer.close();
        dir.close();
    }
}
