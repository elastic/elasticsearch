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

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Simple tests for this filterreader */
public class FieldSubsetReaderTests extends ElasticsearchLuceneTestCase {
    
    public void testSimple() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), Collections.singleton("fieldA"));
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Set<String> seenFields = new HashSet<>();
        for (String field : segmentReader.fields()) {
            seenFields.add(field);
        }
        assertEquals(Collections.singleton("fieldA"), seenFields);
        
        IOUtils.close(ir, iw, dir);
    }
    
    public void testFieldNames() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add document with 2 fields
        Document doc = new Document();
        doc.add(new StringField("fieldA", "test", Field.Store.NO));
        doc.add(new StringField("fieldB", "test", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldA", Field.Store.NO));
        doc.add(new StringField(FieldNamesFieldMapper.NAME, "fieldB", Field.Store.NO));
        iw.addDocument(doc);
        
        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), Collections.singleton("fieldA"));
        
        // see only one field
        LeafReader segmentReader = ir.leaves().get(0).reader();
        Terms terms = segmentReader.terms(FieldNamesFieldMapper.NAME);
        TermsEnum termsEnum = terms.iterator(null);
        assertEquals(new BytesRef("fieldA"), termsEnum.next());
        assertNull(termsEnum.next());
        
        IOUtils.close(ir, iw, dir);
    }
    
    /** Test that core cache key (needed for NRT) is working */
    public void testCoreCacheKey() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMaxBufferedDocs(100);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        // add two docs, id:0 and id:1
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        doc.add(idField);
        idField.setStringValue("0");
        iw.addDocument(doc);
        idField.setStringValue("1");
        iw.addDocument(doc);
        
        // open reader
        DirectoryReader ir = FieldSubsetReader.wrap(DirectoryReader.open(iw, true), Collections.singleton("id"));
        assertEquals(2, ir.numDocs());
        assertEquals(1, ir.leaves().size());

        // delete id:0 and reopen
        iw.deleteDocuments(new Term("id", "0"));
        DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
        
        // we should have the same cache key as before
        assertEquals(1, ir2.numDocs());
        assertEquals(1, ir2.leaves().size());
        assertSame(ir.leaves().get(0).reader().getCoreCacheKey(), ir2.leaves().get(0).reader().getCoreCacheKey());
        
        // this is kind of stupid, but for now its here
        assertNotSame(ir.leaves().get(0).reader().getCombinedCoreAndDeletesKey(), ir2.leaves().get(0).reader().getCombinedCoreAndDeletesKey());
        
        IOUtils.close(ir, ir2, iw, dir);
    }
}
