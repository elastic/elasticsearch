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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions.DocIdAndVersion;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;

/** 
 * test per-segment lookup of version-related datastructures
 */
public class VersionLookupTests extends ESTestCase {

    /** 
     * test version lookup actually works
     */
    public void testSimple() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "6", UidFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().get(0);
        PerThreadIDAndVersionLookup lookup = new PerThreadIDAndVersionLookup(segment.reader());
        // found doc
        DocIdAndVersion result = lookup.lookup(new BytesRef("6"), null, segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(0, result.docId);
        // not found doc
        assertNull(lookup.lookup(new BytesRef("7"), null, segment));
        // deleted doc
        assertNull(lookup.lookup(new BytesRef("6"), new Bits.MatchNoBits(1), segment));
        reader.close();
        writer.close();
        dir.close();
    }
    
    /** 
     * test version lookup with two documents matching the ID
     */
    public void testTwoDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field(UidFieldMapper.NAME, "6", UidFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        writer.addDocument(doc);
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().get(0);
        PerThreadIDAndVersionLookup lookup = new PerThreadIDAndVersionLookup(segment.reader());
        // return the last doc when there are duplicates
        DocIdAndVersion result = lookup.lookup(new BytesRef("6"), null, segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete the first doc only
        FixedBitSet live = new FixedBitSet(2);
        live.set(1);
        result = lookup.lookup(new BytesRef("6"), live, segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete the second doc only
        live.clear(1);
        live.set(0);
        result = lookup.lookup(new BytesRef("6"), live, segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(0, result.docId);
        // delete both docs
        assertNull(lookup.lookup(new BytesRef("6"), new Bits.MatchNoBits(2), segment));
        reader.close();
        writer.close();
        dir.close();
    }
}
