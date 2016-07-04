/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        MatcherAssert.assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(Versions.NOT_FOUND));

        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "1", UidFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1));
        writer.updateDocument(new Term(UidFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(1L));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")).version, equalTo(1L));

        doc = new Document();
        Field uid = new Field(UidFieldMapper.NAME, "1", UidFieldMapper.Defaults.FIELD_TYPE);
        Field version = new NumericDocValuesField(VersionFieldMapper.NAME, 2);
        doc.add(uid);
        doc.add(version);
        writer.updateDocument(new Term(UidFieldMapper.NAME, "1"), doc);
        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(2L));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")).version, equalTo(2L));

        // test reuse of uid field
        doc = new Document();
        version.setLongValue(3);
        doc.add(uid);
        doc.add(version);
        writer.updateDocument(new Term(UidFieldMapper.NAME, "1"), doc);

        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(3L));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")).version, equalTo(3L));

        writer.deleteDocuments(new Term(UidFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(Versions.NOT_FOUND));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), nullValue());
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
            doc.add(new Field(UidFieldMapper.NAME, "1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
            docs.add(doc);
        }
        // Root
        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "1", UidFieldMapper.Defaults.FIELD_TYPE));
        NumericDocValuesField version = new NumericDocValuesField(VersionFieldMapper.NAME, 5L);
        doc.add(version);
        docs.add(doc);

        writer.updateDocuments(new Term(UidFieldMapper.NAME, "1"), docs);
        DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "_na_", 1));
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(5L));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")).version, equalTo(5L));

        version.setLongValue(6L);
        writer.updateDocuments(new Term(UidFieldMapper.NAME, "1"), docs);
        version.setLongValue(7L);
        writer.updateDocuments(new Term(UidFieldMapper.NAME, "1"), docs);
        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(7L));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")).version, equalTo(7L));

        writer.deleteDocuments(new Term(UidFieldMapper.NAME, "1"));
        directoryReader = reopen(directoryReader);
        assertThat(Versions.loadVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), equalTo(Versions.NOT_FOUND));
        assertThat(Versions.loadDocIdAndVersion(directoryReader, new Term(UidFieldMapper.NAME, "1")), nullValue());
        directoryReader.close();
        writer.close();
        dir.close();
    }

    /** Test that version map cache works, is evicted on close, etc */
    public void testCache() throws Exception {
        int size = Versions.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "6", UidFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        // should increase cache size by 1
        assertEquals(87, Versions.loadVersion(reader, new Term(UidFieldMapper.NAME, "6")));
        assertEquals(size+1, Versions.lookupStates.size());
        // should be cache hit
        assertEquals(87, Versions.loadVersion(reader, new Term(UidFieldMapper.NAME, "6")));
        assertEquals(size+1, Versions.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, Versions.lookupStates.size());
        dir.close();
    }

    /** Test that version map cache behaves properly with a filtered reader */
    public void testCacheFilterReader() throws Exception {
        int size = Versions.lookupStates.size();

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "6", UidFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(87, Versions.loadVersion(reader, new Term(UidFieldMapper.NAME, "6")));
        assertEquals(size+1, Versions.lookupStates.size());
        // now wrap the reader
        DirectoryReader wrapped = ElasticsearchDirectoryReader.wrap(reader, new ShardId("bogus", "_na_", 5));
        assertEquals(87, Versions.loadVersion(wrapped, new Term(UidFieldMapper.NAME, "6")));
        // same size map: core cache key is shared
        assertEquals(size+1, Versions.lookupStates.size());

        reader.close();
        writer.close();
        // core should be evicted from the map
        assertEquals(size, Versions.lookupStates.size());
        dir.close();
    }
}
