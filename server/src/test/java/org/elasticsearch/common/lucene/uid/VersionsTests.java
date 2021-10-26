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
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.loadDocIdAndVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class VersionsTests extends ESTestCase {

    public static DirectoryReader reopen(DirectoryReader reader) throws IOException {
        return reopen(reader, true);
    }

    public static DirectoryReader reopen(DirectoryReader reader, boolean newReaderExpected) throws IOException {
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        if (newReader != null) {
            reader.close();
        } else {
            assertFalse(newReaderExpected);
        }
        return newReader;
    }

    public void testVersions() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());

        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(1L));

        doc = new Document();
        Field uid = new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE);
        Field version = new NumericDocValuesField(VersionFieldMapper.NAME, 2);
        doc.add(uid);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(2L));

        // test reuse of uid field
        doc = new Document();
        version.setLongValue(3);
        doc.add(uid);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.updateDocument(new Term(IdFieldMapper.NAME, "1"), doc);

        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(3L));

        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());
        directoryReader.close();
        writer.close();
        dir.close();
    }

    public void testNestedDocuments() throws IOException {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 4; ++i) {
            // Nested
            Document doc = new Document();
            doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            docs.add(doc);
        }
        // Root
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "1", IdFieldMapper.Defaults.FIELD_TYPE));
        NumericDocValuesField version = new NumericDocValuesField(VersionFieldMapper.NAME, 5L);
        doc.add(version);
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        docs.add(doc);

        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(5L));

        version.setLongValue(6L);
        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        version.setLongValue(7L);
        writer.updateDocuments(new Term(IdFieldMapper.NAME, "1"), docs);
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()).version, equalTo(7L));

        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(loadDocIdAndVersion(directoryReader, new Term(IdFieldMapper.NAME, "1"), randomBoolean()), nullValue());
        directoryReader.close();
        writer.close();
        dir.close();
    }

    /** Test that version map cache works, is evicted on close, etc */
    public void testCache() throws Exception {
        int size = VersionsAndSeqNoResolver.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        // should increase cache size by 1
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size+1, VersionsAndSeqNoResolver.lookupStates.size());
        // should be cache hit
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size+1, VersionsAndSeqNoResolver.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, VersionsAndSeqNoResolver.lookupStates.size());
        dir.close();
    }

    /** Test that version map cache behaves properly with a filtered reader */
    public void testCacheFilterReader() throws Exception {
        int size = VersionsAndSeqNoResolver.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, "6", IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(87, loadDocIdAndVersion(reader, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        assertEquals(size+1, VersionsAndSeqNoResolver.lookupStates.size());
        // now wrap the reader
        DirectoryReader wrapped = ElasticsearchDirectoryReader.wrap(reader, new ShardId("bogus", "_na_", 5));
        assertEquals(87, loadDocIdAndVersion(wrapped, new Term(IdFieldMapper.NAME, "6"), randomBoolean()).version);
        // same size map: core cache key is shared
        assertEquals(size+1, VersionsAndSeqNoResolver.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, VersionsAndSeqNoResolver.lookupStates.size());
        dir.close();
    }

    public void testLuceneVersionOnUnknownVersions() {
        // between two known versions, should use the lucene version of the previous version
        Version version = VersionUtils.getPreviousVersion(Version.CURRENT);
        final Version nextVersion = Version.fromId(version.id + 100);
        if (Version.getDeclaredVersions(Version.class).contains(nextVersion) == false) {
            // the version is not known, we make an assumption the Lucene version stays the same
            assertEquals(nextVersion.luceneVersion, version.luceneVersion);
        } else {
            // the version is known, the most we can assert is that the Lucene version is not earlier
            assertTrue(nextVersion.luceneVersion.onOrAfter(version.luceneVersion));
        }

        // too old version, major should be the oldest supported lucene version minus 1
        version = Version.fromString("5.2.1");
        assertEquals(VersionUtils.getFirstVersion().luceneVersion.major - 1, version.luceneVersion.major);

        // future version, should be the same version as today
        version = Version.fromId(Version.CURRENT.id + 100);
        assertEquals(Version.CURRENT.luceneVersion, version.luceneVersion);
    }
}
